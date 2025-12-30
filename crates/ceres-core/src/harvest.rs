//! Harvest service for portal synchronization.
//!
//! This module provides the core business logic for harvesting datasets from
//! open data portals, including delta detection, embedding generation, and persistence.
//!
//! # Architecture
//!
//! The [`HarvestService`] is generic over three traits:
//! - [`DatasetStore`] - for database operations
//! - [`EmbeddingProvider`] - for generating embeddings
//! - [`PortalClientFactory`] - for creating portal clients
//!
//! This enables:
//! - **Testing**: Mock implementations for unit tests
//! - **Flexibility**: Different backends (PostgreSQL, SQLite, different embedding APIs)
//! - **Decoupling**: Core logic independent of specific implementations
//!
//! # Future Improvements
//!
//! TODO(#10): Implement time-based incremental harvesting
//! Currently we fetch all package IDs and compare hashes. For large portals,
//! we could use CKAN's `package_search` with `fq=metadata_modified:[NOW-1DAY TO *]`
//! to only fetch recently modified datasets.
//! See: <https://github.com/AndreaBozzo/Ceres/issues/10>
//!
//! TODO(robustness): Add circuit breaker pattern for API failures
//! Currently no backpressure when Gemini/CKAN APIs fail repeatedly.
//! Consider: (1) Stop after N consecutive failures
//! (2) Exponential backoff on rate limits
//! (3) Health check before continuing after failure spike
//!
//! TODO(performance): Batch embedding API calls
//! Each dataset embedding is generated individually. Gemini API may support
//! batching multiple texts per request, reducing latency and API calls.

use std::sync::Arc;

use futures::stream::{self, StreamExt};
use pgvector::Vector;

use crate::progress::{HarvestEvent, ProgressReporter, SilentReporter};
use crate::sync::{AtomicSyncStats, SyncOutcome};
use crate::traits::{DatasetStore, EmbeddingProvider, PortalClient, PortalClientFactory};
use crate::{
    AppError, BatchHarvestSummary, PortalEntry, PortalHarvestResult, SyncConfig, SyncStats,
    needs_reprocessing,
};

/// Service for harvesting datasets from open data portals.
///
/// This service encapsulates the core harvesting business logic,
/// decoupled from CLI or server concerns.
///
/// # Type Parameters
///
/// * `S` - Dataset store implementation (e.g., `DatasetRepository`)
/// * `E` - Embedding provider implementation (e.g., `GeminiClient`)
/// * `F` - Portal client factory implementation
///
/// # Example
///
/// ```ignore
/// use ceres_core::harvest::HarvestService;
///
/// // Create service with concrete implementations
/// let harvest_service = HarvestService::new(repo, gemini, ckan_factory);
///
/// // Sync a portal
/// let stats = harvest_service.sync_portal("https://data.gov/api/3").await?;
/// println!("Synced {} datasets ({} created)", stats.total(), stats.created);
/// ```
pub struct HarvestService<S, E, F>
where
    S: DatasetStore,
    E: EmbeddingProvider,
    F: PortalClientFactory,
{
    store: S,
    embedding: E,
    portal_factory: F,
    config: SyncConfig,
}

impl<S, E, F> Clone for HarvestService<S, E, F>
where
    S: DatasetStore + Clone,
    E: EmbeddingProvider + Clone,
    F: PortalClientFactory + Clone,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            embedding: self.embedding.clone(),
            portal_factory: self.portal_factory.clone(),
            config: self.config.clone(),
        }
    }
}

impl<S, E, F> HarvestService<S, E, F>
where
    S: DatasetStore,
    E: EmbeddingProvider,
    F: PortalClientFactory,
{
    /// Creates a new harvest service with default configuration.
    ///
    /// # Arguments
    ///
    /// * `store` - Dataset store for persistence
    /// * `embedding` - Embedding provider for vector generation
    /// * `portal_factory` - Factory for creating portal clients
    pub fn new(store: S, embedding: E, portal_factory: F) -> Self {
        Self {
            store,
            embedding,
            portal_factory,
            config: SyncConfig::default(),
        }
    }

    /// Creates a harvest service with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `store` - Dataset store for persistence
    /// * `embedding` - Embedding provider for vector generation
    /// * `portal_factory` - Factory for creating portal clients
    /// * `config` - Sync configuration (concurrency, etc.)
    pub fn with_config(store: S, embedding: E, portal_factory: F, config: SyncConfig) -> Self {
        Self {
            store,
            embedding,
            portal_factory,
            config,
        }
    }

    /// Synchronizes a single portal and returns statistics.
    ///
    /// This is the core harvesting function. It:
    /// 1. Fetches all dataset IDs from the portal
    /// 2. Compares content hashes with existing data
    /// 3. Generates embeddings for new/updated datasets
    /// 4. Persists changes to the database
    ///
    /// # Arguments
    ///
    /// * `portal_url` - The portal API URL
    ///
    /// # Returns
    ///
    /// Statistics about the sync operation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The portal URL is invalid
    /// - The portal API is unreachable
    /// - Database operations fail
    pub async fn sync_portal(&self, portal_url: &str) -> Result<SyncStats, AppError> {
        self.sync_portal_with_progress(portal_url, &SilentReporter)
            .await
    }

    /// Synchronizes a single portal with progress reporting.
    ///
    /// Same as [`sync_portal`](Self::sync_portal), but emits progress events
    /// through the provided reporter.
    pub async fn sync_portal_with_progress<R: ProgressReporter>(
        &self,
        portal_url: &str,
        reporter: &R,
    ) -> Result<SyncStats, AppError> {
        let portal_client = self.portal_factory.create(portal_url)?;

        let existing_hashes = self.store.get_hashes_for_portal(portal_url).await?;
        reporter.report(HarvestEvent::ExistingDatasetsFound {
            count: existing_hashes.len(),
        });

        let ids = portal_client.list_dataset_ids().await?;
        let total = ids.len();
        reporter.report(HarvestEvent::PortalDatasetsFound { count: total });

        let stats = Arc::new(AtomicSyncStats::new());

        let _results: Vec<_> = stream::iter(ids.into_iter().enumerate())
            .map(|(_, id)| {
                let portal_client = portal_client.clone();
                let embedding = self.embedding.clone();
                let store = self.store.clone();
                let portal_url = portal_url.to_string();
                let existing_hashes = existing_hashes.clone();
                let stats = Arc::clone(&stats);

                async move {
                    let portal_data = match portal_client.get_dataset(&id).await {
                        Ok(data) => data,
                        Err(e) => {
                            stats.record(SyncOutcome::Failed);
                            return Err(e);
                        }
                    };

                    let mut new_dataset = F::Client::into_new_dataset(portal_data, &portal_url);
                    let decision = needs_reprocessing(
                        existing_hashes.get(&new_dataset.original_id),
                        &new_dataset.content_hash,
                    );

                    match decision.outcome {
                        SyncOutcome::Unchanged => {
                            stats.record(SyncOutcome::Unchanged);

                            if let Err(e) = store
                                .update_timestamp_only(&portal_url, &new_dataset.original_id)
                                .await
                            {
                                // Non-fatal: timestamp update failure doesn't affect data integrity
                                tracing::warn!(
                                    dataset_id = %new_dataset.original_id,
                                    error = %e,
                                    "Failed to update timestamp for unchanged dataset"
                                );
                            }
                            return Ok(());
                        }
                        SyncOutcome::Updated | SyncOutcome::Created => {
                            // Continue to embedding generation
                        }
                        SyncOutcome::Failed => {
                            unreachable!("needs_reprocessing never returns Failed")
                        }
                    }

                    if decision.needs_embedding {
                        let combined_text = format!(
                            "{} {}",
                            new_dataset.title,
                            new_dataset.description.as_deref().unwrap_or_default()
                        );

                        if !combined_text.trim().is_empty() {
                            match embedding.generate(&combined_text).await {
                                Ok(emb) => {
                                    new_dataset.embedding = Some(Vector::from(emb));
                                    stats.record(decision.outcome);
                                }
                                Err(e) => {
                                    stats.record(SyncOutcome::Failed);
                                    return Err(AppError::Generic(format!(
                                        "Failed to generate embedding: {e}"
                                    )));
                                }
                            }
                        }
                    }

                    match store.upsert(&new_dataset).await {
                        Ok(_uuid) => Ok(()),
                        Err(e) => {
                            stats.record(SyncOutcome::Failed);
                            Err(e)
                        }
                    }
                }
            })
            .buffer_unordered(self.config.concurrency)
            .collect()
            .await;

        Ok(stats.to_stats())
    }

    /// Harvests multiple portals sequentially with error isolation.
    ///
    /// Failure in one portal does not stop processing of others.
    ///
    /// # Arguments
    ///
    /// * `portals` - Slice of portal entries to harvest
    ///
    /// # Returns
    ///
    /// A summary of all portal harvest results.
    pub async fn batch_harvest(&self, portals: &[&PortalEntry]) -> BatchHarvestSummary {
        self.batch_harvest_with_progress(portals, &SilentReporter)
            .await
    }

    /// Harvests multiple portals with progress reporting.
    ///
    /// Same as [`batch_harvest`](Self::batch_harvest), but emits progress events
    /// through the provided reporter.
    pub async fn batch_harvest_with_progress<R: ProgressReporter>(
        &self,
        portals: &[&PortalEntry],
        reporter: &R,
    ) -> BatchHarvestSummary {
        let mut summary = BatchHarvestSummary::new();
        let total = portals.len();

        reporter.report(HarvestEvent::BatchStarted {
            total_portals: total,
        });

        for (i, portal) in portals.iter().enumerate() {
            reporter.report(HarvestEvent::PortalStarted {
                portal_index: i,
                total_portals: total,
                portal_name: &portal.name,
                portal_url: &portal.url,
            });

            match self.sync_portal_with_progress(&portal.url, reporter).await {
                Ok(stats) => {
                    reporter.report(HarvestEvent::PortalCompleted {
                        portal_index: i,
                        total_portals: total,
                        portal_name: &portal.name,
                        stats: &stats,
                    });
                    summary.add(PortalHarvestResult::success(
                        portal.name.clone(),
                        portal.url.clone(),
                        stats,
                    ));
                }
                Err(e) => {
                    let error_str = e.to_string();
                    reporter.report(HarvestEvent::PortalFailed {
                        portal_index: i,
                        total_portals: total,
                        portal_name: &portal.name,
                        error: &error_str,
                    });
                    summary.add(PortalHarvestResult::failure(
                        portal.name.clone(),
                        portal.url.clone(),
                        error_str,
                    ));
                }
            }
        }

        reporter.report(HarvestEvent::BatchCompleted { summary: &summary });
        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_config_default() {
        let config = SyncConfig::default();
        assert!(config.concurrency > 0, "concurrency should be positive");
    }
}
