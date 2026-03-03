//! Standalone embedding service for generating dataset embeddings.
//!
//! This service is decoupled from harvesting — it processes datasets that are
//! already stored in the database with `embedding IS NULL`. This enables:
//!
//! - Harvesting metadata without an embedding API key
//! - Switching embedding providers without re-harvesting
//! - Backfilling embeddings after outages
//! - Independent scaling of harvest and embedding workloads
//!
//! # Example
//!
//! ```ignore
//! use ceres_core::embedding::EmbeddingService;
//!
//! let service = EmbeddingService::new(store, embedding_provider);
//!
//! // Embed all pending datasets
//! let stats = service.embed_pending(None, &reporter, cancel_token).await?;
//! println!("Embedded {} datasets", stats.embedded);
//! ```

use tokio_util::sync::CancellationToken;
use tracing;

use crate::AppError;
use crate::circuit_breaker::CircuitBreaker;
use crate::circuit_breaker::CircuitBreakerError;
use crate::config::EmbeddingServiceConfig;
use crate::models::NewDataset;
use crate::progress::{HarvestEvent, ProgressReporter};
use crate::traits::{DatasetStore, EmbeddingProvider};

/// Statistics from an embedding run.
#[derive(Debug, Clone, Default)]
pub struct EmbeddingStats {
    /// Number of datasets successfully embedded.
    pub embedded: usize,
    /// Number of datasets that failed embedding.
    pub failed: usize,
    /// Number of datasets skipped (circuit breaker open).
    pub skipped: usize,
    /// Total number of datasets that needed embedding.
    pub total: usize,
}

impl EmbeddingStats {
    /// Returns the number of datasets successfully processed (embedded).
    pub fn successful(&self) -> usize {
        self.embedded
    }
}

impl std::fmt::Display for EmbeddingStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "embedded: {}, failed: {}, skipped: {}, total: {}",
            self.embedded, self.failed, self.skipped, self.total
        )
    }
}

/// Standalone service for generating embeddings for datasets already in the database.
///
/// Queries datasets with `embedding IS NULL`, generates embeddings in batches
/// through a circuit breaker, and upserts them back to the database.
pub struct EmbeddingService<S, E>
where
    S: DatasetStore,
    E: EmbeddingProvider,
{
    store: S,
    embedding: E,
    config: EmbeddingServiceConfig,
}

impl<S, E> Clone for EmbeddingService<S, E>
where
    S: DatasetStore + Clone,
    E: EmbeddingProvider + Clone,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            embedding: self.embedding.clone(),
            config: self.config.clone(),
        }
    }
}

impl<S, E> EmbeddingService<S, E>
where
    S: DatasetStore,
    E: EmbeddingProvider,
{
    /// Creates a new embedding service with default configuration.
    pub fn new(store: S, embedding: E) -> Self {
        Self {
            store,
            embedding,
            config: EmbeddingServiceConfig::default(),
        }
    }

    /// Creates a new embedding service with custom configuration.
    pub fn with_config(store: S, embedding: E, config: EmbeddingServiceConfig) -> Self {
        Self {
            store,
            embedding,
            config,
        }
    }

    /// Returns a reference to the underlying embedding provider.
    pub fn embedding_provider(&self) -> &E {
        &self.embedding
    }

    /// Embeds all datasets with `embedding IS NULL`.
    ///
    /// Fetches pending datasets from the database, generates embeddings in
    /// batches through the circuit breaker, and upserts them back.
    ///
    /// # Arguments
    ///
    /// * `portal_filter` - Optional portal URL to scope the embedding pass
    /// * `reporter` - Progress reporter for UI/logging
    /// * `cancel_token` - Token for graceful cancellation
    pub async fn embed_pending(
        &self,
        portal_filter: Option<&str>,
        reporter: &impl ProgressReporter,
        cancel_token: CancellationToken,
    ) -> Result<EmbeddingStats, AppError> {
        let total = self.store.count_pending_embeddings(portal_filter).await? as usize;

        if total == 0 {
            tracing::info!("No datasets pending embedding");
            return Ok(EmbeddingStats::default());
        }

        tracing::info!(
            total,
            portal = portal_filter.unwrap_or("all"),
            provider = self.embedding.name(),
            "Starting embedding pass"
        );

        let mut stats = EmbeddingStats {
            total,
            ..Default::default()
        };

        let effective_batch_size =
            std::cmp::min(self.config.batch_size, self.embedding.max_batch_size()).max(1);

        let circuit_breaker =
            CircuitBreaker::new(self.embedding.name(), self.config.circuit_breaker.clone());

        let mut processed = 0usize;

        // Page through pending datasets to avoid loading everything into memory.
        // Each iteration fetches up to `page_size` rows, processes them, then
        // fetches the next page. This keeps memory bounded even with 350k+ pending.
        let page_size = effective_batch_size * 10; // ~10 batches per page
        loop {
            if cancel_token.is_cancelled() {
                tracing::info!("Embedding pass cancelled");
                break;
            }

            let page = self
                .store
                .list_pending_embeddings(portal_filter, Some(page_size))
                .await?;

            if page.is_empty() {
                break;
            }

            for batch in page.chunks(effective_batch_size) {
                if cancel_token.is_cancelled() {
                    tracing::info!("Embedding pass cancelled");
                    break;
                }

                self.process_batch(batch, &circuit_breaker, &mut stats)
                    .await;

                processed += batch.len();

                reporter.report(HarvestEvent::DatasetProcessed {
                    current: processed,
                    total,
                    created: 0,
                    updated: stats.embedded,
                    unchanged: 0,
                    failed: stats.failed,
                    skipped: stats.skipped,
                });
            }
        }

        tracing::info!(
            embedded = stats.embedded,
            failed = stats.failed,
            skipped = stats.skipped,
            total = stats.total,
            "Embedding pass complete"
        );

        Ok(stats)
    }

    /// Processes a batch of datasets: generates embeddings via circuit breaker,
    /// then upserts them back to the database.
    async fn process_batch(
        &self,
        datasets: &[crate::Dataset],
        circuit_breaker: &CircuitBreaker,
        stats: &mut EmbeddingStats,
    ) {
        // Compute text to embed for each dataset, filtering out empty text
        let embeddable: Vec<(&crate::Dataset, String)> = datasets
            .iter()
            .filter_map(|d| {
                let text = format!(
                    "{} {}",
                    d.title,
                    d.description.as_deref().unwrap_or_default()
                );
                if text.trim().is_empty() {
                    None
                } else {
                    Some((d, text))
                }
            })
            .collect();

        let skipped_empty = datasets.len() - embeddable.len();
        if skipped_empty > 0 {
            tracing::debug!(skipped_empty, "Skipped datasets with empty text");
            stats.failed += skipped_empty;
        }

        if embeddable.is_empty() {
            return;
        }

        let texts: Vec<String> = embeddable.iter().map(|(_, t)| t.clone()).collect();
        let batch_size = texts.len();

        match circuit_breaker
            .call(|| self.embedding.generate_batch(&texts))
            .await
        {
            Ok(embeddings) => {
                if embeddings.len() != batch_size {
                    tracing::warn!(
                        expected = batch_size,
                        got = embeddings.len(),
                        "Batch embedding count mismatch, failing batch"
                    );
                    stats.failed += batch_size;
                    return;
                }

                // Build NewDataset items with embeddings for upsert.
                // Use existing content_hash from DB — it's always present for stored datasets.
                let upsert_datasets: Vec<NewDataset> = embeddable
                    .iter()
                    .zip(embeddings)
                    .filter_map(|((d, _), emb)| {
                        let content_hash = match &d.content_hash {
                            Some(h) => h.clone(),
                            None => {
                                tracing::warn!(
                                    original_id = %d.original_id,
                                    "Dataset missing content_hash, skipping embedding upsert"
                                );
                                return None;
                            }
                        };
                        Some(NewDataset {
                            original_id: d.original_id.clone(),
                            source_portal: d.source_portal.clone(),
                            url: d.url.clone(),
                            title: d.title.clone(),
                            description: d.description.clone(),
                            embedding: Some(emb),
                            metadata: d.metadata.clone(),
                            content_hash,
                        })
                    })
                    .collect();

                let skipped_no_hash = batch_size - upsert_datasets.len();
                stats.failed += skipped_no_hash;
                let upsert_count = upsert_datasets.len();

                match self.store.batch_upsert(&upsert_datasets).await {
                    Ok(_) => {
                        stats.embedded += upsert_count;
                    }
                    Err(e) => {
                        tracing::warn!(
                            count = upsert_count,
                            error = %e,
                            "Failed to batch upsert datasets with embeddings"
                        );
                        stats.failed += upsert_count;
                    }
                }
            }
            Err(CircuitBreakerError::Open { retry_after, .. }) => {
                tracing::debug!(
                    batch_size,
                    retry_after_secs = retry_after.as_secs(),
                    "Skipping batch - circuit breaker open"
                );
                stats.skipped += batch_size;
            }
            Err(CircuitBreakerError::Inner(e)) => {
                tracing::warn!(
                    batch_size,
                    error = %e,
                    "Batch embedding generation failed"
                );
                stats.failed += batch_size;
            }
        }
    }
}
