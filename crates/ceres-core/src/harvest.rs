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
//! TODO(server): Implement persistent job queue for REST API
//! When transitioning to ceres-server, avoid spawning long-running harvest tasks
//! directly in HTTP handlers. Instead:
//! - Create a `harvest_jobs` table in Postgres (consider `sqlx-mq` crate)
//! - On POST /api/harvest, insert job with status='pending', return 202 Accepted + job_id
//! - Separate worker process picks up jobs, updates status: running -> completed/failed
//! - This ensures recoverability on server restart and visibility into failed harvests
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

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerError, CircuitState};
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
        self.sync_portal_with_progress(portal_url, None, &SilentReporter)
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
    ) -> Result<SyncStats, AppError> {
        let portal_client = self.portal_factory.create(portal_url)?;
        let sync_start = Utc::now();

        // Determine sync mode
        let (sync_mode, datasets_to_process) = self
            .determine_sync_mode_and_fetch(portal_url, &portal_client, reporter)
            .await?;

        let existing_hashes = self.store.get_hashes_for_portal(portal_url).await?;
        reporter.report(HarvestEvent::ExistingDatasetsFound {
            count: existing_hashes.len(),
        });

        let total = datasets_to_process.len();
        if total == 0 {
            tracing::info!(
                portal = portal_url,
                "No datasets to process (portal up to date)"
            );
            // Record successful sync even if no datasets changed
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
            return Ok(SyncStats::default());
        }

        let stats = Arc::new(AtomicSyncStats::new());
        let unchanged_ids: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let processed_count = Arc::new(AtomicUsize::new(0));
        let last_reported = Arc::new(AtomicUsize::new(0));

        // Create circuit breaker for embedding API resilience
        let circuit_breaker = CircuitBreaker::new("gemini", self.config.circuit_breaker.clone());

        // Check if circuit breaker is already open from a previous run
        if circuit_breaker.state() == CircuitState::Open {
            let cb_stats = circuit_breaker.stats();
            reporter.report(HarvestEvent::CircuitBreakerOpen {
                service: "gemini",
                retry_after: cb_stats.time_until_half_open.unwrap_or_default(),
            });
            tracing::warn!(
                portal = portal_url,
                "Embedding service circuit breaker is already open"
            );
        }

        // Report progress every 5% or minimum 50 items
        let report_interval = std::cmp::max(total / 20, 50);

        // Process datasets - for incremental sync we already have full dataset objects
        let url_template_owned = url_template.map(|s| s.to_string());
        let _results: Vec<_> = stream::iter(datasets_to_process.into_iter())
            .map(|portal_data| {
                let embedding = self.embedding.clone();
                let store = self.store.clone();
                let portal_url = portal_url.to_string();
                let url_template = url_template_owned.clone();
                let existing_hashes = existing_hashes.clone();
                let stats = Arc::clone(&stats);
                let unchanged_ids = Arc::clone(&unchanged_ids);
                let circuit_breaker = circuit_breaker.clone();

                async move {
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
                            // Collect ID for batch update instead of individual update
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

                    if decision.needs_embedding {
                        let combined_text = format!(
                            "{} {}",
                            new_dataset.title,
                            new_dataset.description.as_deref().unwrap_or_default()
                        );

                        if !combined_text.trim().is_empty() {
                            // Use circuit breaker to protect embedding generation
                            match circuit_breaker
                                .call(|| embedding.generate(&combined_text))
                                .await
                            {
                                Ok(emb) => {
                                    new_dataset.embedding = Some(Vector::from(emb));
                                }
                                Err(CircuitBreakerError::Open { retry_after, .. }) => {
                                    // Circuit is open - skip this dataset
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
            })
            .buffer_unordered(self.config.concurrency)
            .inspect(|_| {
                let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
                let last = last_reported.load(Ordering::Relaxed);

                // Report progress at intervals
                let should_report = current >= last + report_interval || current == total;
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
            })
            .collect()
            .await;

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

        // Record successful sync
        if let Err(e) = self
            .store
            .record_sync_status(
                portal_url,
                sync_start,
                sync_mode.as_str(),
                SyncStatus::Completed.as_str(),
                final_stats.total() as i32,
            )
            .await
        {
            tracing::warn!(error = %e, "Failed to record sync status");
        }

        Ok(final_stats)
    }

    /// Determines the sync mode and fetches the datasets to process.
    ///
    /// Returns a tuple of (sync_mode, datasets_to_process).
    async fn determine_sync_mode_and_fetch<R: ProgressReporter>(
        &self,
        portal_url: &str,
        portal_client: &F::Client,
        reporter: &R,
    ) -> Result<(SyncMode, Vec<<F::Client as PortalClient>::PortalData>), AppError> {
        // Check if we should use incremental sync
        if self.config.force_full_sync {
            tracing::info!(portal = portal_url, "Force full sync requested");
            return self.do_full_sync(portal_url, portal_client, reporter).await;
        }

        // Check last sync time
        let last_sync = self.store.get_last_sync_time(portal_url).await?;

        match last_sync {
            Some(since) => {
                tracing::info!(
                    portal = portal_url,
                    last_sync = %since,
                    "Attempting incremental sync"
                );

                // Try incremental sync
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
                        Ok((SyncMode::Incremental, datasets))
                    }
                    Err(e) => {
                        tracing::warn!(
                            portal = portal_url,
                            error = %e,
                            "Incremental sync failed, falling back to full sync"
                        );
                        self.do_full_sync(portal_url, portal_client, reporter).await
                    }
                }
            }
            None => {
                tracing::info!(portal = portal_url, "First sync, using full sync");
                self.do_full_sync(portal_url, portal_client, reporter).await
            }
        }
    }

    /// Performs a full sync by fetching all dataset IDs and then each dataset.
    async fn do_full_sync<R: ProgressReporter>(
        &self,
        portal_url: &str,
        portal_client: &F::Client,
        reporter: &R,
    ) -> Result<(SyncMode, Vec<<F::Client as PortalClient>::PortalData>), AppError> {
        let ids = portal_client.list_dataset_ids().await?;
        let total = ids.len();
        reporter.report(HarvestEvent::PortalDatasetsFound { count: total });

        // Fetch all datasets
        let mut datasets = Vec::with_capacity(total);
        for id in ids {
            match portal_client.get_dataset(&id).await {
                Ok(data) => datasets.push(data),
                Err(e) => {
                    tracing::warn!(
                        portal = portal_url,
                        dataset_id = id,
                        error = %e,
                        "Failed to fetch dataset, skipping"
                    );
                }
            }
        }

        Ok((SyncMode::Full, datasets))
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

            match self
                .sync_portal_with_progress(&portal.url, portal.url_template.as_deref(), reporter)
                .await
            {
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
    ) -> Result<SyncResult, AppError> {
        self.sync_portal_with_progress_cancellable_internal(
            portal_url,
            url_template,
            reporter,
            cancel_token,
            self.config.force_full_sync,
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
    ) -> Result<SyncResult, AppError> {
        let force_full_sync = self.config.force_full_sync || force_full_sync;
        self.sync_portal_with_progress_cancellable_internal(
            portal_url,
            url_template,
            reporter,
            cancel_token,
            force_full_sync,
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

        let portal_client = self.portal_factory.create(portal_url)?;

        // Determine sync mode and fetch datasets (check cancellation during fetch)
        let (sync_mode, datasets_to_process) = self
            .determine_sync_mode_and_fetch_cancellable(
                portal_url,
                &portal_client,
                reporter,
                &cancel_token,
                force_full_sync,
            )
            .await?;

        // Check cancellation after fetch
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

        let total = datasets_to_process.len();
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

        // Process datasets with cancellation checks
        let url_template_owned = url_template.map(|s| s.to_string());
        let _results: Vec<_> = stream::iter(datasets_to_process.into_iter())
            .map(|portal_data| {
                let embedding = self.embedding.clone();
                let store = self.store.clone();
                let portal_url = portal_url.to_string();
                let url_template = url_template_owned.clone();
                let existing_hashes = existing_hashes.clone();
                let stats = Arc::clone(&stats);
                let unchanged_ids = Arc::clone(&unchanged_ids);
                let cancel_token = cancel_token.clone();
                let was_cancelled = Arc::clone(&was_cancelled);
                let circuit_breaker = circuit_breaker.clone();

                async move {
                    // Check cancellation before processing each item
                    if cancel_token.is_cancelled() {
                        was_cancelled.store(true, Ordering::SeqCst);
                        return Ok(());
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
                            // Use circuit breaker to protect embedding generation
                            match circuit_breaker
                                .call(|| embedding.generate(&combined_text))
                                .await
                            {
                                Ok(emb) => {
                                    new_dataset.embedding = Some(Vector::from(emb));
                                }
                                Err(CircuitBreakerError::Open { retry_after, .. }) => {
                                    // Circuit is open - skip this dataset
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
            })
            .buffer_unordered(self.config.concurrency)
            .inspect(|_| {
                let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
                let last = last_reported.load(Ordering::Relaxed);

                let should_report = current >= last + report_interval || current == total;
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
            })
            .take_while(|_| {
                let is_cancelled = cancel_token.is_cancelled();
                async move { !is_cancelled }
            })
            .collect()
            .await;

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

    /// Determines sync mode and fetches datasets with cancellation support.
    async fn determine_sync_mode_and_fetch_cancellable<R: ProgressReporter>(
        &self,
        portal_url: &str,
        portal_client: &F::Client,
        reporter: &R,
        cancel_token: &CancellationToken,
        force_full_sync: bool,
    ) -> Result<(SyncMode, Vec<<F::Client as PortalClient>::PortalData>), AppError> {
        if force_full_sync {
            tracing::info!(portal = portal_url, "Force full sync requested");
            return self
                .do_full_sync_cancellable(portal_url, portal_client, reporter, cancel_token)
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
                        Ok((SyncMode::Incremental, datasets))
                    }
                    Err(e) => {
                        tracing::warn!(
                            portal = portal_url,
                            error = %e,
                            "Incremental sync failed, falling back to full sync"
                        );
                        self.do_full_sync_cancellable(
                            portal_url,
                            portal_client,
                            reporter,
                            cancel_token,
                        )
                        .await
                    }
                }
            }
            None => {
                tracing::info!(portal = portal_url, "First sync, using full sync");
                self.do_full_sync_cancellable(portal_url, portal_client, reporter, cancel_token)
                    .await
            }
        }
    }

    /// Performs a full sync with cancellation support.
    async fn do_full_sync_cancellable<R: ProgressReporter>(
        &self,
        portal_url: &str,
        portal_client: &F::Client,
        reporter: &R,
        cancel_token: &CancellationToken,
    ) -> Result<(SyncMode, Vec<<F::Client as PortalClient>::PortalData>), AppError> {
        let ids = portal_client.list_dataset_ids().await?;
        let total = ids.len();
        reporter.report(HarvestEvent::PortalDatasetsFound { count: total });

        let mut datasets = Vec::with_capacity(total);
        for id in ids {
            // Check cancellation before each fetch
            if cancel_token.is_cancelled() {
                tracing::info!(
                    portal = portal_url,
                    fetched = datasets.len(),
                    total = total,
                    "Cancellation requested during dataset fetch"
                );
                break;
            }

            match portal_client.get_dataset(&id).await {
                Ok(data) => datasets.push(data),
                Err(e) => {
                    tracing::warn!(
                        portal = portal_url,
                        dataset_id = id,
                        error = %e,
                        "Failed to fetch dataset, skipping"
                    );
                }
            }
        }

        Ok((SyncMode::Full, datasets))
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
