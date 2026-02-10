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
//! TODO(performance): Batch embedding API calls
//! Each dataset embedding is generated individually. Gemini API may support
//! batching multiple texts per request, reducing latency and API calls.
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
use crate::progress::{HarvestEvent, ProgressReporter, SilentReporter};
use crate::sync::{AtomicSyncStats, SyncOutcome, SyncResult, SyncStatus};
use crate::traits::{DatasetStore, EmbeddingProvider, PortalClient, PortalClientFactory};
use crate::{
    AppError, BatchHarvestSummary, PortalEntry, PortalHarvestResult, SyncConfig, SyncStats,
    needs_reprocessing,
};

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
enum SyncPlan<PD: Send> {
    /// Full sync: fetch each dataset by ID in the stream pipeline.
    Full {
        /// Dataset IDs to fetch and process.
        ids: Vec<String>,
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
        self.sync_portal_with_progress(portal_url, None, &SilentReporter, PortalType::default())
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
        reporter: &R,
        portal_type: PortalType,
    ) -> Result<SyncStats, AppError> {
        let result = self
            .sync_portal_with_progress_cancellable_internal(
                portal_url,
                url_template,
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
            let ids = portal_client.list_dataset_ids().await?;
            reporter.report(HarvestEvent::PortalDatasetsFound { count: ids.len() });
            return Ok((SyncMode::Full, SyncPlan::Full { ids }));
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
                        let ids = portal_client.list_dataset_ids().await?;
                        reporter.report(HarvestEvent::PortalDatasetsFound { count: ids.len() });
                        Ok((SyncMode::Full, SyncPlan::Full { ids }))
                    }
                }
            }
            None => {
                tracing::info!(portal = portal_url, "First sync, using full sync");
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
        reporter: &R,
        cancel_token: CancellationToken,
        portal_type: PortalType,
    ) -> Result<SyncResult, AppError> {
        self.sync_portal_with_progress_cancellable_internal(
            portal_url,
            url_template,
            reporter,
            cancel_token,
            self.config.force_full_sync,
            portal_type,
        )
        .await
    }

    /// Synchronizes a single portal with cancellation support, allowing
    /// a per-call override of the `force_full_sync` flag.
    pub async fn sync_portal_with_progress_cancellable_with_options<R: ProgressReporter>(
        &self,
        portal_url: &str,
        url_template: Option<&str>,
        reporter: &R,
        cancel_token: CancellationToken,
        force_full_sync: bool,
        portal_type: PortalType,
    ) -> Result<SyncResult, AppError> {
        let force_full_sync = self.config.force_full_sync || force_full_sync;
        self.sync_portal_with_progress_cancellable_internal(
            portal_url,
            url_template,
            reporter,
            cancel_token,
            force_full_sync,
            portal_type,
        )
        .await
    }

    async fn sync_portal_with_progress_cancellable_internal<R: ProgressReporter>(
        &self,
        portal_url: &str,
        url_template: Option<&str>,
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

        // Build the processing closure shared by both Full and Incremental paths.
        // The closure takes an already-fetched PortalData and processes it
        // (hash check → embed → upsert).
        macro_rules! process_dataset {
            ($portal_data:expr) => {{
                let embedding = self.embedding.clone();
                let store = self.store.clone();
                let portal_url = portal_url.to_string();
                let url_template = url_template_arc.clone();
                let existing_hashes = existing_hashes.clone();
                let stats = Arc::clone(&stats);
                let unchanged_ids = Arc::clone(&unchanged_ids);
                let cancel_token = cancel_token.clone();
                let was_cancelled = Arc::clone(&was_cancelled);
                let circuit_breaker = circuit_breaker.clone();

                let portal_data = $portal_data;
                async move {
                    // Check cancellation before processing each item
                    if cancel_token.is_cancelled() {
                        was_cancelled.store(true, Ordering::SeqCst);
                        return Ok::<(), AppError>(());
                    }

                    let mut new_dataset = F::Client::into_new_dataset(
                        portal_data,
                        &portal_url,
                        url_template.as_deref(),
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
                            return Ok(());
                        }
                        SyncOutcome::Updated | SyncOutcome::Created => {
                            // Continue to embedding generation
                        }
                        SyncOutcome::Failed | SyncOutcome::Skipped => {
                            unreachable!("needs_reprocessing never returns Failed or Skipped")
                        }
                    }

                    // Check cancellation before expensive embedding generation
                    if cancel_token.is_cancelled() {
                        was_cancelled.store(true, Ordering::SeqCst);
                        return Ok(());
                    }

                    if decision.needs_embedding {
                        let combined_text = format!(
                            "{} {}",
                            new_dataset.title,
                            new_dataset.description.as_deref().unwrap_or_default()
                        );

                        if !combined_text.trim().is_empty() {
                            match circuit_breaker
                                .call(|| embedding.generate(&combined_text))
                                .await
                            {
                                Ok(emb) => {
                                    new_dataset.embedding = Some(Vector::from(emb));
                                }
                                Err(CircuitBreakerError::Open { retry_after, .. }) => {
                                    tracing::debug!(
                                        dataset_id = %new_dataset.original_id,
                                        retry_after_secs = retry_after.as_secs(),
                                        "Skipping dataset - circuit breaker open"
                                    );
                                    stats.record(SyncOutcome::Skipped);
                                    return Ok(());
                                }
                                Err(CircuitBreakerError::Inner(e)) => {
                                    stats.record(SyncOutcome::Failed);
                                    return Err(AppError::Generic(format!(
                                        "Failed to generate embedding: {e}"
                                    )));
                                }
                            }
                        }
                    }

                    match store.upsert(&new_dataset).await {
                        Ok(_uuid) => {
                            stats.record(decision.outcome);
                            Ok(())
                        }
                        Err(e) => {
                            stats.record(SyncOutcome::Failed);
                            Err(e)
                        }
                    }
                }
            }};
        }

        // Progress + cancellation combinators shared by both stream paths
        macro_rules! apply_stream_combinators {
            ($stream:expr) => {
                $stream
                    .buffer_unordered(self.config.concurrency)
                    .inspect(|_| {
                        let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
                        let last = last_reported.load(Ordering::Relaxed);

                        let should_report = current >= last + report_interval || current == total;
                        if should_report
                            && last_reported
                                .compare_exchange(
                                    last,
                                    current,
                                    Ordering::SeqCst,
                                    Ordering::Relaxed,
                                )
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
                    })
                    .take_while(|_| {
                        let is_cancelled = cancel_token.is_cancelled();
                        async move { !is_cancelled }
                    })
                    .for_each(|_| async {})
                    .await
            };
        }

        // Run the appropriate stream pipeline based on sync plan
        match plan {
            SyncPlan::Full { ids } => {
                // Unified fetch+process stream: each item fetches a single dataset
                // by ID and then processes it. Memory is bounded by concurrency,
                // not total dataset count.
                let stream = stream::iter(ids).map(|id| {
                    let portal_client = portal_client.clone();
                    let portal_url_owned = portal_url.to_string();
                    let stats = Arc::clone(&stats);
                    let cancel_token = cancel_token.clone();
                    let was_cancelled = Arc::clone(&was_cancelled);
                    let embedding = self.embedding.clone();
                    let store = self.store.clone();
                    let url_template = url_template_arc.clone();
                    let existing_hashes = existing_hashes.clone();
                    let unchanged_ids = Arc::clone(&unchanged_ids);
                    let circuit_breaker = circuit_breaker.clone();

                    async move {
                        // Check cancellation before fetching
                        if cancel_token.is_cancelled() {
                            was_cancelled.store(true, Ordering::SeqCst);
                            return Ok::<(), AppError>(());
                        }

                        // Fetch the dataset
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
                                return Ok(());
                            }
                        };

                        // Process: hash check → embed → upsert
                        if cancel_token.is_cancelled() {
                            was_cancelled.store(true, Ordering::SeqCst);
                            return Ok(());
                        }

                        let mut new_dataset = F::Client::into_new_dataset(
                            portal_data,
                            &portal_url_owned,
                            url_template.as_deref(),
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
                                return Ok(());
                            }
                            SyncOutcome::Updated | SyncOutcome::Created => {}
                            SyncOutcome::Failed | SyncOutcome::Skipped => {
                                unreachable!("needs_reprocessing never returns Failed or Skipped")
                            }
                        }

                        if cancel_token.is_cancelled() {
                            was_cancelled.store(true, Ordering::SeqCst);
                            return Ok(());
                        }

                        if decision.needs_embedding {
                            let combined_text = format!(
                                "{} {}",
                                new_dataset.title,
                                new_dataset.description.as_deref().unwrap_or_default()
                            );

                            if !combined_text.trim().is_empty() {
                                match circuit_breaker
                                    .call(|| embedding.generate(&combined_text))
                                    .await
                                {
                                    Ok(emb) => {
                                        new_dataset.embedding = Some(Vector::from(emb));
                                    }
                                    Err(CircuitBreakerError::Open { retry_after, .. }) => {
                                        tracing::debug!(
                                            dataset_id = %new_dataset.original_id,
                                            retry_after_secs = retry_after.as_secs(),
                                            "Skipping dataset - circuit breaker open"
                                        );
                                        stats.record(SyncOutcome::Skipped);
                                        return Ok(());
                                    }
                                    Err(CircuitBreakerError::Inner(e)) => {
                                        stats.record(SyncOutcome::Failed);
                                        return Err(AppError::Generic(format!(
                                            "Failed to generate embedding: {e}"
                                        )));
                                    }
                                }
                            }
                        }

                        match store.upsert(&new_dataset).await {
                            Ok(_uuid) => {
                                stats.record(decision.outcome);
                                Ok(())
                            }
                            Err(e) => {
                                stats.record(SyncOutcome::Failed);
                                Err(e)
                            }
                        }
                    }
                });
                apply_stream_combinators!(stream);
            }
            SyncPlan::Incremental { datasets } => {
                // Incremental: datasets already fetched, just process them
                let stream =
                    stream::iter(datasets).map(|portal_data| process_dataset!(portal_data));
                apply_stream_combinators!(stream);
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
