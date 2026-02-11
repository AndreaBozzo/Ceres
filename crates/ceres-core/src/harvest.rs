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
//! # Features
//!
//! - **Incremental Harvesting**: Uses CKAN's `package_search` with `metadata_modified`
//!   filter to fetch only recently modified datasets (implemented in #10).
//! - **Fallback**: If incremental sync fails, automatically falls back to full sync.
//! - **Force Full Sync**: Use `--full-sync` flag to bypass incremental harvesting.
//!
//! # Circuit Breaker
//!
//! The service uses a circuit breaker pattern to protect against cascading failures
//! when external APIs (Gemini for embeddings) experience issues. The circuit breaker:
//! - Opens after N consecutive failures (configurable via `CircuitBreakerConfig`)
//! - Applies exponential backoff on rate limits (HTTP 429)
//! - Transitions through Closed -> Open -> HalfOpen -> Closed states
//! - Skips datasets when open (recorded as `SyncOutcome::Skipped`)
//!
//! # Batch Embedding
//!
//! The pipeline uses a two-phase architecture for efficient embedding:
//! 1. **Pre-process** (parallel): fetch, hash check, text extraction
//! 2. **Batch embed** (sequential batches): `generate_batch()` via circuit breaker
//!
//! Batch size is `min(SyncConfig::embedding_batch_size, provider.max_batch_size())`.
//!
//! # Job Queue
//!
//! The REST API uses a persistent job queue backed by a `harvest_jobs` table.
//! On `POST /api/v1/harvest`, a job is created with status='pending' and a 202
//! is returned. A background `WorkerService` polls for jobs and processes them
//! using `HarvestService`. See `ceres_core::worker` and `ceres_db::JobRepository`.
//!
//! # Cancellation Support
//!
//! The `*_cancellable` methods accept a `CancellationToken` for graceful shutdown:
//! - `sync_portal_cancellable` / `sync_portal_with_progress_cancellable`
//! - `batch_harvest_cancellable` / `batch_harvest_with_progress_cancellable`
//!
//! On cancellation, these methods:
//! - Stop making new API requests
//! - Allow in-flight operations to complete
//! - Save partial statistics to database with "cancelled" status
//! - Return early with a `SyncResult` indicating cancellation

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use futures::stream::{self, StreamExt};
use pgvector::Vector;

use tokio_util::sync::CancellationToken;

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerError};
use crate::config::PortalType;
use crate::models::NewDataset;
use crate::progress::{HarvestEvent, ProgressReporter, SilentReporter};
use crate::sync::{AtomicSyncStats, SyncOutcome, SyncResult, SyncStatus};
use crate::traits::{DatasetStore, EmbeddingProvider, PortalClient, PortalClientFactory};
use crate::{
    AppError, BatchHarvestSummary, PortalEntry, PortalHarvestResult, SyncConfig, SyncStats,
    needs_reprocessing,
};

/// A dataset that has been pre-processed (hash check, text extraction)
/// and is ready for batch embedding.
struct PreProcessedDataset {
    /// The dataset with all fields populated except possibly embedding.
    dataset: NewDataset,
    /// The combined text to embed, or None if embedding is not needed.
    text_to_embed: Option<String>,
    /// The sync outcome (Created or Updated).
    outcome: SyncOutcome,
}

/// Mode of sync operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SyncMode {
    /// Full sync: fetch all datasets from portal.
    Full,
    /// Incremental sync: fetch only datasets modified since last sync.
    Incremental,
}

impl SyncMode {
    /// Returns the string representation for database storage.
    fn as_str(&self) -> &'static str {
        match self {
            SyncMode::Full => "full",
            SyncMode::Incremental => "incremental",
        }
    }
}

/// Describes what data is available for the sync pipeline.
///
/// For full sync, we only have dataset IDs — each dataset must be fetched
/// individually inside the processing stream. For incremental sync, the
/// datasets were already fetched by `search_modified_since`.
/// For bulk full sync, all datasets were fetched via `search_all_datasets`.
enum SyncPlan<PD: Send> {
    /// Full sync (ID-by-ID): fetch each dataset individually in the stream pipeline.
    /// Used as fallback when the portal doesn't support bulk search.
    Full {
        /// Dataset IDs to fetch and process.
        ids: Vec<String>,
    },
    /// Full sync (bulk): all datasets pre-fetched via `search_all_datasets`.
    /// Much more efficient for large portals that enforce rate limits (e.g., HDX).
    FullBulk {
        /// Pre-fetched dataset objects.
        datasets: Vec<PD>,
    },
    /// Incremental sync: datasets already fetched.
    Incremental {
        /// Pre-fetched dataset objects.
        datasets: Vec<PD>,
    },
}

impl<PD: Send> SyncPlan<PD> {
    /// Returns the total number of items to process.
    fn len(&self) -> usize {
        match self {
            SyncPlan::Full { ids } => ids.len(),
            SyncPlan::FullBulk { datasets } => datasets.len(),
            SyncPlan::Incremental { datasets } => datasets.len(),
        }
    }
}

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
        self.sync_portal_with_progress(
            portal_url,
            None,
            "en",
            &SilentReporter,
            PortalType::default(),
        )
        .await
    }

    /// Synchronizes a single portal with progress reporting.
    ///
    /// Same as [`sync_portal`](Self::sync_portal), but emits progress events
    /// through the provided reporter.
    ///
    /// # Sync Strategy
    ///
    /// 1. If `force_full_sync` is enabled or this is the first sync: full sync
    /// 2. Otherwise, attempt incremental sync using `metadata_modified` filter
    /// 3. If incremental fails, fall back to full sync with a warning
    pub async fn sync_portal_with_progress<R: ProgressReporter>(
        &self,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
        reporter: &R,
        portal_type: PortalType,
    ) -> Result<SyncStats, AppError> {
        let result = self
            .sync_portal_with_progress_cancellable_internal(
                portal_url,
                url_template,
                language,
                reporter,
                CancellationToken::new(), // never cancelled
                self.config.force_full_sync,
                portal_type,
            )
            .await?;
        Ok(result.stats)
    }

    /// Determines the sync mode and returns a plan describing what to process.
    ///
    /// For full sync, returns dataset IDs (datasets will be fetched inside the
    /// processing stream). For incremental sync, returns pre-fetched datasets.
    async fn determine_sync_plan<R: ProgressReporter>(
        &self,
        portal_url: &str,
        portal_client: &F::Client,
        reporter: &R,
        force_full_sync: bool,
    ) -> Result<(SyncMode, SyncPlan<<F::Client as PortalClient>::PortalData>), AppError> {
        if force_full_sync {
            tracing::info!(portal = portal_url, "Force full sync requested");
            return self
                .full_sync_plan(portal_url, portal_client, reporter)
                .await;
        }

        let last_sync = self.store.get_last_sync_time(portal_url).await?;

        match last_sync {
            Some(since) => {
                tracing::info!(
                    portal = portal_url,
                    last_sync = %since,
                    "Attempting incremental sync"
                );

                match portal_client.search_modified_since(since).await {
                    Ok(datasets) => {
                        let count = datasets.len();
                        tracing::info!(
                            portal = portal_url,
                            modified_count = count,
                            "Incremental sync: found {} modified datasets",
                            count
                        );
                        reporter.report(HarvestEvent::PortalDatasetsFound { count });
                        Ok((SyncMode::Incremental, SyncPlan::Incremental { datasets }))
                    }
                    Err(e) => {
                        tracing::warn!(
                            portal = portal_url,
                            error = %e,
                            "Incremental sync failed, falling back to full sync"
                        );
                        self.full_sync_plan(portal_url, portal_client, reporter)
                            .await
                    }
                }
            }
            None => {
                tracing::info!(portal = portal_url, "First sync, using full sync");
                self.full_sync_plan(portal_url, portal_client, reporter)
                    .await
            }
        }
    }

    /// Determines the best full sync plan for a portal.
    ///
    /// Prefers bulk fetch via `search_all_datasets` (paginated `package_search`)
    /// which uses ~N/1000 API calls instead of N individual calls. Falls back to
    /// ID-by-ID fetching if the portal doesn't support bulk search.
    async fn full_sync_plan<R: ProgressReporter>(
        &self,
        portal_url: &str,
        portal_client: &F::Client,
        reporter: &R,
    ) -> Result<(SyncMode, SyncPlan<<F::Client as PortalClient>::PortalData>), AppError> {
        // Try bulk fetch first (much faster for large portals)
        match portal_client.search_all_datasets().await {
            Ok(datasets) => {
                let count = datasets.len();
                tracing::info!(
                    portal = portal_url,
                    count,
                    "Full sync: bulk-fetched all datasets via package_search"
                );
                reporter.report(HarvestEvent::PortalDatasetsFound { count });
                Ok((SyncMode::Full, SyncPlan::FullBulk { datasets }))
            }
            Err(AppError::RateLimitExceeded) => {
                // If bulk fetch was rate-limited, ID-by-ID would be even worse.
                // Propagate the error instead of falling back.
                tracing::warn!(
                    portal = portal_url,
                    "Bulk fetch rate-limited, not falling back to ID-by-ID (would be worse)"
                );
                Err(AppError::RateLimitExceeded)
            }
            Err(e) if e.to_string().contains("429") => {
                // Catch rate limit errors wrapped in ClientError too
                tracing::warn!(
                    portal = portal_url,
                    error = %e,
                    "Bulk fetch rate-limited, not falling back to ID-by-ID (would be worse)"
                );
                Err(AppError::RateLimitExceeded)
            }
            Err(e) => {
                tracing::info!(
                    portal = portal_url,
                    error = %e,
                    "Bulk fetch not available, falling back to ID-by-ID sync"
                );
                let ids = portal_client.list_dataset_ids().await?;
                reporter.report(HarvestEvent::PortalDatasetsFound { count: ids.len() });
                Ok((SyncMode::Full, SyncPlan::Full { ids }))
            }
        }
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
        self.batch_harvest_with_progress_cancellable(
            portals,
            reporter,
            CancellationToken::new(), // never cancelled
        )
        .await
    }

    // =========================================================================
    // Cancellable Methods
    // =========================================================================

    /// Synchronizes a single portal with cancellation support.
    ///
    /// Same as [`sync_portal`](Self::sync_portal), but accepts a `CancellationToken`
    /// for graceful shutdown.
    ///
    /// # Cancellation Behavior
    ///
    /// When cancelled:
    /// - Stops making new API requests
    /// - Allows currently processing items to complete
    /// - Saves partial statistics to database with "cancelled" status
    /// - Returns `SyncResult` with `SyncStatus::Cancelled`
    pub async fn sync_portal_cancellable(
        &self,
        portal_url: &str,
        cancel_token: CancellationToken,
    ) -> Result<SyncResult, AppError> {
        self.sync_portal_with_progress_cancellable_internal(
            portal_url,
            None,
            "en",
            &SilentReporter,
            cancel_token,
            self.config.force_full_sync,
            PortalType::default(),
        )
        .await
    }

    /// Synchronizes a single portal with progress reporting and cancellation support.
    ///
    /// Same as [`sync_portal_with_progress`](Self::sync_portal_with_progress), but
    /// accepts a `CancellationToken` for graceful shutdown.
    pub async fn sync_portal_with_progress_cancellable<R: ProgressReporter>(
        &self,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
        reporter: &R,
        cancel_token: CancellationToken,
        portal_type: PortalType,
    ) -> Result<SyncResult, AppError> {
        self.sync_portal_with_progress_cancellable_internal(
            portal_url,
            url_template,
            language,
            reporter,
            cancel_token,
            self.config.force_full_sync,
            portal_type,
        )
        .await
    }

    /// Synchronizes a single portal with cancellation support, allowing
    /// a per-call override of the `force_full_sync` flag.
    #[allow(clippy::too_many_arguments)]
    pub async fn sync_portal_with_progress_cancellable_with_options<R: ProgressReporter>(
        &self,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
        reporter: &R,
        cancel_token: CancellationToken,
        force_full_sync: bool,
        portal_type: PortalType,
    ) -> Result<SyncResult, AppError> {
        let force_full_sync = self.config.force_full_sync || force_full_sync;
        self.sync_portal_with_progress_cancellable_internal(
            portal_url,
            url_template,
            language,
            reporter,
            cancel_token,
            force_full_sync,
            portal_type,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn sync_portal_with_progress_cancellable_internal<R: ProgressReporter>(
        &self,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
        reporter: &R,
        cancel_token: CancellationToken,
        force_full_sync: bool,
        portal_type: PortalType,
    ) -> Result<SyncResult, AppError> {
        let sync_start = Utc::now();

        // Check cancellation before starting
        if cancel_token.is_cancelled() {
            if let Err(e) = self
                .store
                .record_sync_status(
                    portal_url,
                    sync_start,
                    "unknown",
                    SyncStatus::Cancelled.as_str(),
                    0,
                )
                .await
            {
                tracing::warn!(error = %e, "Failed to record cancelled sync status");
            }
            return Ok(SyncResult::cancelled(SyncStats::default()));
        }

        let portal_client = self.portal_factory.create(portal_url, portal_type)?;

        // Determine sync plan (IDs for full sync, datasets for incremental)
        let (sync_mode, plan) = self
            .determine_sync_plan(portal_url, &portal_client, reporter, force_full_sync)
            .await?;

        // Check cancellation after plan determination
        if cancel_token.is_cancelled() {
            if let Err(e) = self
                .store
                .record_sync_status(
                    portal_url,
                    sync_start,
                    sync_mode.as_str(),
                    SyncStatus::Cancelled.as_str(),
                    0,
                )
                .await
            {
                tracing::warn!(error = %e, "Failed to record cancelled sync status");
            }
            return Ok(SyncResult::cancelled(SyncStats::default()));
        }

        let existing_hashes = self.store.get_hashes_for_portal(portal_url).await?;
        reporter.report(HarvestEvent::ExistingDatasetsFound {
            count: existing_hashes.len(),
        });

        let total = plan.len();
        if total == 0 {
            tracing::info!(
                portal = portal_url,
                "No datasets to process (portal up to date)"
            );
            if let Err(e) = self
                .store
                .record_sync_status(
                    portal_url,
                    sync_start,
                    sync_mode.as_str(),
                    SyncStatus::Completed.as_str(),
                    0,
                )
                .await
            {
                tracing::warn!(error = %e, "Failed to record sync status");
            }
            return Ok(SyncResult::completed(SyncStats::default()));
        }

        let stats = Arc::new(AtomicSyncStats::new());
        let unchanged_ids: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let processed_count = Arc::new(AtomicUsize::new(0));
        let last_reported = Arc::new(AtomicUsize::new(0));
        let was_cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Create circuit breaker for embedding API resilience
        let circuit_breaker = CircuitBreaker::new("gemini", self.config.circuit_breaker.clone());

        let report_interval = std::cmp::max(total / 20, 50);
        let url_template_arc: Option<Arc<str>> = url_template.map(Arc::from);
        let language_arc: Arc<str> = Arc::from(language);

        // Effective batch size respects both config and provider limits
        let effective_batch_size = std::cmp::min(
            self.config.embedding_batch_size,
            self.embedding.max_batch_size(),
        )
        .max(1);

        // =====================================================================
        // Two-phase pipeline
        //
        // Phase 1: Pre-process datasets in parallel (fetch if needed, hash check,
        //          extract text). Unchanged datasets are handled inline and
        //          filtered out. Produces a stream of PreProcessedDataset items.
        //
        // Phase 2: Collect items into batches, call generate_batch() once per
        //          batch through the circuit breaker, then upsert all datasets.
        // =====================================================================

        // Phase 2 macro: consume a chunked stream of PreProcessedDatasets,
        // batch-embed and upsert, with progress reporting and cancellation.
        macro_rules! run_batch_phase {
            ($pre_processed_stream:expr) => {{
                use futures::stream::StreamExt as _;
                let mut chunked =
                    std::pin::pin!($pre_processed_stream.chunks(effective_batch_size));

                while let Some(batch) = chunked.next().await {
                    if cancel_token.is_cancelled() {
                        was_cancelled.store(true, Ordering::SeqCst);
                        break;
                    }

                    let batch_len = batch.len();

                    Self::process_embedding_batch(
                        batch,
                        &self.embedding,
                        &self.store,
                        &circuit_breaker,
                        &stats,
                    )
                    .await;

                    // Report progress for the batch
                    let current =
                        processed_count.fetch_add(batch_len, Ordering::Relaxed) + batch_len;
                    let last = last_reported.load(Ordering::Relaxed);
                    let should_report = current >= last + report_interval || current >= total;
                    if should_report
                        && last_reported
                            .compare_exchange(last, current, Ordering::SeqCst, Ordering::Relaxed)
                            .is_ok()
                    {
                        let current_stats = stats.to_stats();
                        reporter.report(HarvestEvent::DatasetProcessed {
                            current,
                            total,
                            created: current_stats.created,
                            updated: current_stats.updated,
                            unchanged: current_stats.unchanged,
                            failed: current_stats.failed,
                            skipped: current_stats.skipped,
                        });
                    }
                }
            }};
        }

        match plan {
            SyncPlan::Full { ids } => {
                let pre_processed = stream::iter(ids)
                    .map(|id| {
                        let portal_client = portal_client.clone();
                        let portal_url_owned = portal_url.to_string();
                        let stats = Arc::clone(&stats);
                        let cancel_token = cancel_token.clone();
                        let was_cancelled = Arc::clone(&was_cancelled);
                        let url_template = url_template_arc.clone();
                        let language = Arc::clone(&language_arc);
                        let existing_hashes = existing_hashes.clone();
                        let unchanged_ids = Arc::clone(&unchanged_ids);
                        let processed_count = Arc::clone(&processed_count);

                        async move {
                            if cancel_token.is_cancelled() {
                                was_cancelled.store(true, Ordering::SeqCst);
                                return None;
                            }

                            // Fetch the dataset by ID
                            let portal_data = match portal_client.get_dataset(&id).await {
                                Ok(data) => data,
                                Err(e) => {
                                    tracing::warn!(
                                        portal = portal_url_owned.as_str(),
                                        dataset_id = id,
                                        error = %e,
                                        "Failed to fetch dataset, skipping"
                                    );
                                    stats.record(SyncOutcome::Failed);
                                    processed_count.fetch_add(1, Ordering::Relaxed);
                                    return None;
                                }
                            };

                            if cancel_token.is_cancelled() {
                                was_cancelled.store(true, Ordering::SeqCst);
                                return None;
                            }

                            let new_dataset = F::Client::into_new_dataset(
                                portal_data,
                                &portal_url_owned,
                                url_template.as_deref(),
                                &language,
                            );
                            let decision = needs_reprocessing(
                                existing_hashes.get(&new_dataset.original_id),
                                &new_dataset.content_hash,
                            );

                            match decision.outcome {
                                SyncOutcome::Unchanged => {
                                    stats.record(SyncOutcome::Unchanged);
                                    if let Ok(mut ids) = unchanged_ids.lock() {
                                        ids.push(new_dataset.original_id);
                                    }
                                    processed_count.fetch_add(1, Ordering::Relaxed);
                                    None
                                }
                                SyncOutcome::Updated | SyncOutcome::Created => {
                                    let text_to_embed = if decision.needs_embedding {
                                        let combined = format!(
                                            "{} {}",
                                            new_dataset.title,
                                            new_dataset.description.as_deref().unwrap_or_default()
                                        );
                                        if combined.trim().is_empty() {
                                            None
                                        } else {
                                            Some(combined)
                                        }
                                    } else {
                                        None
                                    };

                                    Some(PreProcessedDataset {
                                        dataset: new_dataset,
                                        text_to_embed,
                                        outcome: decision.outcome,
                                    })
                                }
                                SyncOutcome::Failed | SyncOutcome::Skipped => {
                                    unreachable!(
                                        "needs_reprocessing never returns Failed or Skipped"
                                    )
                                }
                            }
                        }
                    })
                    .buffer_unordered(self.config.concurrency)
                    .take_while(|_| {
                        let is_cancelled = cancel_token.is_cancelled();
                        async move { !is_cancelled }
                    })
                    .filter_map(|opt| async { opt });

                run_batch_phase!(pre_processed);
            }
            SyncPlan::Incremental { datasets } | SyncPlan::FullBulk { datasets } => {
                let pre_processed = stream::iter(datasets)
                    .map(|portal_data| {
                        let portal_url_owned = portal_url.to_string();
                        let url_template = url_template_arc.clone();
                        let language = Arc::clone(&language_arc);
                        let existing_hashes = existing_hashes.clone();
                        let unchanged_ids = Arc::clone(&unchanged_ids);
                        let cancel_token = cancel_token.clone();
                        let was_cancelled = Arc::clone(&was_cancelled);
                        let stats = Arc::clone(&stats);
                        let processed_count = Arc::clone(&processed_count);

                        async move {
                            if cancel_token.is_cancelled() {
                                was_cancelled.store(true, Ordering::SeqCst);
                                return None;
                            }

                            let new_dataset = F::Client::into_new_dataset(
                                portal_data,
                                &portal_url_owned,
                                url_template.as_deref(),
                                &language,
                            );
                            let decision = needs_reprocessing(
                                existing_hashes.get(&new_dataset.original_id),
                                &new_dataset.content_hash,
                            );

                            match decision.outcome {
                                SyncOutcome::Unchanged => {
                                    stats.record(SyncOutcome::Unchanged);
                                    if let Ok(mut ids) = unchanged_ids.lock() {
                                        ids.push(new_dataset.original_id);
                                    }
                                    processed_count.fetch_add(1, Ordering::Relaxed);
                                    None
                                }
                                SyncOutcome::Updated | SyncOutcome::Created => {
                                    let text_to_embed = if decision.needs_embedding {
                                        let combined = format!(
                                            "{} {}",
                                            new_dataset.title,
                                            new_dataset.description.as_deref().unwrap_or_default()
                                        );
                                        if combined.trim().is_empty() {
                                            None
                                        } else {
                                            Some(combined)
                                        }
                                    } else {
                                        None
                                    };

                                    Some(PreProcessedDataset {
                                        dataset: new_dataset,
                                        text_to_embed,
                                        outcome: decision.outcome,
                                    })
                                }
                                SyncOutcome::Failed | SyncOutcome::Skipped => {
                                    unreachable!(
                                        "needs_reprocessing never returns Failed or Skipped"
                                    )
                                }
                            }
                        }
                    })
                    .buffer_unordered(self.config.concurrency)
                    .take_while(|_| {
                        let is_cancelled = cancel_token.is_cancelled();
                        async move { !is_cancelled }
                    })
                    .filter_map(|opt| async { opt });

                run_batch_phase!(pre_processed);
            }
        }

        // Batch update timestamps for unchanged datasets
        let unchanged_list = unchanged_ids
            .lock()
            .ok()
            .map(|g| g.clone())
            .unwrap_or_default();
        if !unchanged_list.is_empty() {
            if let Err(e) = self
                .store
                .batch_update_timestamps(portal_url, &unchanged_list)
                .await
            {
                tracing::warn!(
                    count = unchanged_list.len(),
                    error = %e,
                    "Failed to batch update timestamps for unchanged datasets"
                );
            }
        }

        let final_stats = stats.to_stats();
        let is_cancelled = was_cancelled.load(Ordering::SeqCst) || cancel_token.is_cancelled();

        // Determine final status
        let status = if is_cancelled {
            SyncStatus::Cancelled
        } else {
            SyncStatus::Completed
        };

        // Record sync status
        if let Err(e) = self
            .store
            .record_sync_status(
                portal_url,
                sync_start,
                sync_mode.as_str(),
                status.as_str(),
                final_stats.total() as i32,
            )
            .await
        {
            tracing::warn!(error = %e, "Failed to record sync status");
        }

        if is_cancelled {
            tracing::info!(
                portal = portal_url,
                processed = final_stats.total(),
                "Sync cancelled - partial progress saved"
            );
            Ok(SyncResult::cancelled(final_stats))
        } else {
            Ok(SyncResult::completed(final_stats))
        }
    }

    /// Processes a batch of pre-processed datasets: generates embeddings in a
    /// single API call through the circuit breaker, then upserts all datasets.
    async fn process_embedding_batch(
        batch: Vec<PreProcessedDataset>,
        embedding: &E,
        store: &S,
        circuit_breaker: &CircuitBreaker,
        stats: &Arc<AtomicSyncStats>,
    ) {
        // Partition: items needing embedding vs those that don't
        let (needs_embed, no_embed): (Vec<_>, Vec<_>) =
            batch.into_iter().partition(|p| p.text_to_embed.is_some());

        // Upsert items that don't need embedding immediately
        for item in no_embed {
            match store.upsert(&item.dataset).await {
                Ok(_) => stats.record(item.outcome),
                Err(e) => {
                    tracing::warn!(
                        dataset_id = %item.dataset.original_id,
                        error = %e,
                        "Failed to upsert dataset"
                    );
                    stats.record(SyncOutcome::Failed);
                }
            }
        }

        if needs_embed.is_empty() {
            return;
        }

        // Collect texts for batch API call
        let texts: Vec<String> = needs_embed
            .iter()
            .map(|p| p.text_to_embed.clone().unwrap())
            .collect();

        let batch_size = needs_embed.len();

        match circuit_breaker
            .call(|| embedding.generate_batch(&texts))
            .await
        {
            Ok(embeddings) => {
                if embeddings.len() != batch_size {
                    tracing::warn!(
                        expected = batch_size,
                        got = embeddings.len(),
                        "Batch embedding count mismatch, failing batch"
                    );
                    for _ in 0..batch_size {
                        stats.record(SyncOutcome::Failed);
                    }
                    return;
                }
                // Assign embeddings and upsert each dataset
                for (mut item, emb) in needs_embed.into_iter().zip(embeddings) {
                    item.dataset.embedding = Some(Vector::from(emb));
                    match store.upsert(&item.dataset).await {
                        Ok(_) => stats.record(item.outcome),
                        Err(e) => {
                            tracing::warn!(
                                dataset_id = %item.dataset.original_id,
                                error = %e,
                                "Failed to upsert dataset"
                            );
                            stats.record(SyncOutcome::Failed);
                        }
                    }
                }
            }
            Err(CircuitBreakerError::Open { retry_after, .. }) => {
                tracing::debug!(
                    batch_size,
                    retry_after_secs = retry_after.as_secs(),
                    "Skipping batch - circuit breaker open"
                );
                for _ in 0..batch_size {
                    stats.record(SyncOutcome::Skipped);
                }
            }
            Err(CircuitBreakerError::Inner(e)) => {
                tracing::warn!(
                    batch_size,
                    error = %e,
                    "Batch embedding generation failed"
                );
                for _ in 0..batch_size {
                    stats.record(SyncOutcome::Failed);
                }
            }
        }
    }

    /// Harvests multiple portals with cancellation support.
    ///
    /// Same as [`batch_harvest`](Self::batch_harvest), but accepts a
    /// `CancellationToken` for graceful shutdown.
    pub async fn batch_harvest_cancellable(
        &self,
        portals: &[&PortalEntry],
        cancel_token: CancellationToken,
    ) -> BatchHarvestSummary {
        self.batch_harvest_with_progress_cancellable(portals, &SilentReporter, cancel_token)
            .await
    }

    /// Harvests multiple portals with progress reporting and cancellation support.
    ///
    /// Same as [`batch_harvest_with_progress`](Self::batch_harvest_with_progress),
    /// but accepts a `CancellationToken` for graceful shutdown.
    pub async fn batch_harvest_with_progress_cancellable<R: ProgressReporter>(
        &self,
        portals: &[&PortalEntry],
        reporter: &R,
        cancel_token: CancellationToken,
    ) -> BatchHarvestSummary {
        let mut summary = BatchHarvestSummary::new();
        let total = portals.len();

        reporter.report(HarvestEvent::BatchStarted {
            total_portals: total,
        });

        for (i, portal) in portals.iter().enumerate() {
            // Check cancellation before starting each portal
            if cancel_token.is_cancelled() {
                reporter.report(HarvestEvent::BatchCancelled {
                    completed_portals: i,
                    total_portals: total,
                });
                break;
            }

            reporter.report(HarvestEvent::PortalStarted {
                portal_index: i,
                total_portals: total,
                portal_name: &portal.name,
                portal_url: &portal.url,
            });

            match self
                .sync_portal_with_progress_cancellable(
                    &portal.url,
                    portal.url_template.as_deref(),
                    portal.language(),
                    reporter,
                    cancel_token.clone(),
                    portal.portal_type,
                )
                .await
            {
                Ok(result) => {
                    if result.is_cancelled() {
                        reporter.report(HarvestEvent::PortalCancelled {
                            portal_index: i,
                            total_portals: total,
                            portal_name: &portal.name,
                            stats: &result.stats,
                        });
                        summary.add(PortalHarvestResult::success(
                            portal.name.clone(),
                            portal.url.clone(),
                            result.stats,
                        ));
                        // Don't continue to next portal after cancellation
                        reporter.report(HarvestEvent::BatchCancelled {
                            completed_portals: i + 1,
                            total_portals: total,
                        });
                        break;
                    } else {
                        reporter.report(HarvestEvent::PortalCompleted {
                            portal_index: i,
                            total_portals: total,
                            portal_name: &portal.name,
                            stats: &result.stats,
                        });
                        summary.add(PortalHarvestResult::success(
                            portal.name.clone(),
                            portal.url.clone(),
                            result.stats,
                        ));
                    }
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

        // Only report BatchCompleted if we weren't cancelled
        if !cancel_token.is_cancelled() {
            reporter.report(HarvestEvent::BatchCompleted { summary: &summary });
        }

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
