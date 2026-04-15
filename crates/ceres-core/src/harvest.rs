//! Harvest service for portal synchronization.
//!
//! This module provides the core business logic for harvesting dataset metadata
//! from open data portals. Embedding generation is handled separately by
//! [`EmbeddingService`](crate::embedding::EmbeddingService).
//!
//! # Architecture
//!
//! The [`HarvestService`] is generic over two traits:
//! - [`DatasetStore`] - for database operations
//! - [`PortalClientFactory`] - for creating portal clients
//!
//! This enables:
//! - **Testing**: Mock implementations for unit tests
//! - **Flexibility**: Different backends (PostgreSQL, SQLite, etc.)
//! - **Decoupling**: Harvesting is independent of embedding
//!
//! # Features
//!
//! - **Incremental Harvesting**: Uses CKAN's `package_search` with `metadata_modified`
//!   filter to fetch only recently modified datasets (implemented in #10).
//! - **Fallback**: If incremental sync fails, automatically falls back to full sync.
//! - **Force Full Sync**: Use `--full-sync` flag to bypass incremental harvesting.
//! - **Metadata-only**: No embedding provider required — datasets are stored with
//!   `embedding = NULL` and the existing SQL `COALESCE` preserves any prior embeddings.
//!
//! # Pipeline
//!
//! The harvest pipeline uses a two-phase architecture:
//! 1. **Pre-process**: delta detection via content hash comparison.
//!    - Full (ID-by-ID): concurrent HTTP fetches via `buffer_unordered`
//!    - FullBulk/Incremental: synchronous loop (no async overhead)
//! 2. **Persist**: batch upsert to database (batches of 500, no embedding)
//!
//! Embedding is applied separately via [`EmbeddingService`](crate::embedding::EmbeddingService)
//! or via the combined [`HarvestPipeline`](crate::pipeline::HarvestPipeline).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use futures::stream::{self, StreamExt};
use tokio_util::sync::CancellationToken;

use crate::config::{HarvestConfig, PortalType};
use crate::models::NewDataset;
use crate::progress::{HarvestEvent, ProgressReporter, SilentReporter};
use crate::sync::{
    AtomicSyncStats, ContentHashDetector, DeltaDetector, SyncOutcome, SyncResult, SyncStatus,
};
use crate::traits::{DatasetStore, PortalClient, PortalClientFactory};
use crate::{AppError, BatchHarvestSummary, PortalEntry, PortalHarvestResult, SyncStats};

/// A dataset that has been pre-processed (hash check) and is ready for persistence.
struct PreProcessedDataset {
    /// The dataset with all fields populated except embedding (always None).
    dataset: NewDataset,
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

/// Shared state passed to the per-dataset pre-processing step.
#[derive(Clone)]
struct PreprocessContext<D: DeltaDetector> {
    existing_hashes: HashMap<String, Option<String>>,
    all_seen_ids: Arc<Mutex<Vec<String>>>,
    stats: Arc<AtomicSyncStats>,
    processed_count: Arc<AtomicUsize>,
    delta_detector: D,
}

/// Pre-processes a single portal dataset: converts to `NewDataset`, runs delta detection,
/// and returns a `PreProcessedDataset` if it needs persistence, or `None` if unchanged/skipped.
fn preprocess_dataset<C: PortalClient, D: DeltaDetector>(
    portal_data: C::PortalData,
    portal_url: &str,
    url_template: Option<&str>,
    language: &str,
    ctx: &PreprocessContext<D>,
) -> Option<PreProcessedDataset> {
    let new_dataset = C::into_new_dataset(portal_data, portal_url, url_template, language);
    let decision = ctx.delta_detector.needs_reprocessing(
        ctx.existing_hashes.get(&new_dataset.original_id),
        &new_dataset.content_hash,
    );

    match decision.outcome {
        SyncOutcome::Unchanged => {
            ctx.stats.record(SyncOutcome::Unchanged);
            if let Ok(mut ids) = ctx.all_seen_ids.lock() {
                ids.push(new_dataset.original_id);
            }
            ctx.processed_count.fetch_add(1, Ordering::Relaxed);
            None
        }
        SyncOutcome::Updated | SyncOutcome::Created => {
            if let Ok(mut ids) = ctx.all_seen_ids.lock() {
                ids.push(new_dataset.original_id.clone());
            }
            Some(PreProcessedDataset {
                dataset: new_dataset,
                outcome: decision.outcome,
            })
        }
        SyncOutcome::Failed | SyncOutcome::Skipped => {
            ctx.stats.record(decision.outcome);
            ctx.processed_count.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
}

/// Describes what data is available for the sync pipeline.
///
/// For full sync, we only have dataset IDs — each dataset must be fetched
/// individually inside the processing stream. For incremental sync, the
/// datasets were already fetched by `search_modified_since`.
/// For bulk full sync, all datasets were fetched via `search_all_datasets`.
#[allow(dead_code)]
enum SyncPlan<PD: Send> {
    /// Full sync (ID-by-ID): fetch each dataset individually in the stream pipeline.
    /// Retained as fallback for portals that don't support streaming search.
    Full {
        /// Dataset IDs to fetch and process.
        ids: Vec<String>,
    },
    /// Full sync (streaming): datasets streamed page-by-page from the portal.
    /// Memory is bounded to one page at a time instead of the entire catalog.
    FullBulkStream {
        /// Estimated total datasets (from a lightweight count query, 0 if unknown).
        estimated_total: usize,
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
            SyncPlan::FullBulkStream { estimated_total } => *estimated_total,
            SyncPlan::Incremental { datasets } => datasets.len(),
        }
    }
}

/// Service for harvesting dataset metadata from open data portals.
///
/// This service handles metadata fetching, delta detection, and persistence.
/// Embedding generation is handled separately by [`EmbeddingService`](crate::embedding::EmbeddingService).
///
/// # Type Parameters
///
/// * `S` - Dataset store implementation (e.g., `DatasetRepository`)
/// * `F` - Portal client factory implementation
/// * `D` - Delta detector strategy (defaults to [`ContentHashDetector`])
///
/// # Example
///
/// ```ignore
/// use ceres_core::harvest::HarvestService;
///
/// // Create service — no embedding provider needed
/// let harvest_service = HarvestService::new(repo, ckan_factory);
///
/// // Sync a portal (metadata only, embedding = NULL)
/// let stats = harvest_service.sync_portal("https://data.gov/api/3").await?;
/// println!("Synced {} datasets ({} created)", stats.total(), stats.created);
/// ```
pub struct HarvestService<S, F, D = ContentHashDetector>
where
    S: DatasetStore,
    F: PortalClientFactory,
    D: DeltaDetector,
{
    store: S,
    portal_factory: F,
    delta_detector: D,
    config: HarvestConfig,
}

impl<S, F, D> Clone for HarvestService<S, F, D>
where
    S: DatasetStore + Clone,
    F: PortalClientFactory + Clone,
    D: DeltaDetector + Clone,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            portal_factory: self.portal_factory.clone(),
            delta_detector: self.delta_detector.clone(),
            config: self.config.clone(),
        }
    }
}

impl<S, F> HarvestService<S, F, ContentHashDetector>
where
    S: DatasetStore,
    F: PortalClientFactory,
{
    /// Creates a new harvest service with default configuration.
    ///
    /// Uses [`ContentHashDetector`] as the default delta detection strategy.
    ///
    /// # Arguments
    ///
    /// * `store` - Dataset store for persistence
    /// * `portal_factory` - Factory for creating portal clients
    pub fn new(store: S, portal_factory: F) -> Self {
        Self {
            store,
            portal_factory,
            delta_detector: ContentHashDetector,
            config: HarvestConfig::default(),
        }
    }

    /// Creates a harvest service with custom configuration.
    ///
    /// Uses [`ContentHashDetector`] as the default delta detection strategy.
    ///
    /// # Arguments
    ///
    /// * `store` - Dataset store for persistence
    /// * `portal_factory` - Factory for creating portal clients
    /// * `config` - Harvest configuration (concurrency, etc.)
    pub fn with_config(store: S, portal_factory: F, config: HarvestConfig) -> Self {
        Self {
            store,
            portal_factory,
            delta_detector: ContentHashDetector,
            config,
        }
    }
}

impl<S, F, D> HarvestService<S, F, D>
where
    S: DatasetStore,
    F: PortalClientFactory,
    D: DeltaDetector,
{
    /// Creates a harvest service with a custom delta detection strategy.
    ///
    /// # Arguments
    ///
    /// * `store` - Dataset store for persistence
    /// * `portal_factory` - Factory for creating portal clients
    /// * `delta_detector` - Delta detection strategy
    /// * `config` - Harvest configuration (concurrency, etc.)
    pub fn with_delta_detector(
        store: S,
        portal_factory: F,
        delta_detector: D,
        config: HarvestConfig,
    ) -> Self {
        Self {
            store,
            portal_factory,
            delta_detector,
            config,
        }
    }

    /// Synchronizes a single portal and returns statistics.
    ///
    /// This is the core harvesting function. It:
    /// 1. Fetches all dataset IDs from the portal
    /// 2. Compares content hashes with existing data
    /// 3. Persists new/updated datasets to the database (with `embedding = NULL`)
    ///
    /// # Arguments
    ///
    /// * `portal_url` - The portal API URL
    ///
    /// # Returns
    ///
    /// Statistics about the sync operation.
    pub async fn sync_portal(&self, portal_url: &str) -> Result<SyncStats, AppError> {
        self.sync_portal_with_progress(
            portal_url,
            None,
            "en",
            &SilentReporter,
            PortalType::default(),
            None,
            None,
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
    #[allow(clippy::too_many_arguments)]
    pub async fn sync_portal_with_progress<R: ProgressReporter>(
        &self,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
        reporter: &R,
        portal_type: PortalType,
        profile: Option<&str>,
        sparql_endpoint: Option<&str>,
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
                profile,
                sparql_endpoint,
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

    /// Determines the full sync plan for a portal.
    ///
    /// Always uses streaming (`FullBulkStream`). Attempts a lightweight count
    /// query for progress reporting; if unavailable, streams without a known total.
    async fn full_sync_plan<R: ProgressReporter>(
        &self,
        portal_url: &str,
        portal_client: &F::Client,
        reporter: &R,
    ) -> Result<(SyncMode, SyncPlan<<F::Client as PortalClient>::PortalData>), AppError> {
        // Always stream page-by-page for full syncs. Try to get a dataset count
        // for progress reporting; if unavailable, stream without a known total.
        let estimated_total = portal_client.dataset_count().await.unwrap_or(0);
        if estimated_total > 0 {
            tracing::info!(
                portal = portal_url,
                count = estimated_total,
                "Full sync: streaming {} datasets page-by-page",
                estimated_total
            );
            reporter.report(HarvestEvent::PortalDatasetsFound {
                count: estimated_total,
            });
        } else {
            tracing::info!(
                portal = portal_url,
                "Full sync: streaming datasets (total unknown)"
            );
        }
        Ok((SyncMode::Full, SyncPlan::FullBulkStream { estimated_total }))
    }

    /// Harvests multiple portals sequentially with error isolation.
    ///
    /// Failure in one portal does not stop processing of others.
    pub async fn batch_harvest(&self, portals: &[&PortalEntry]) -> BatchHarvestSummary {
        self.batch_harvest_with_progress(portals, &SilentReporter)
            .await
    }

    /// Harvests multiple portals with progress reporting.
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
            None,
            None,
        )
        .await
    }

    /// Synchronizes a single portal with progress reporting and cancellation support.
    #[allow(clippy::too_many_arguments)]
    pub async fn sync_portal_with_progress_cancellable<R: ProgressReporter>(
        &self,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
        reporter: &R,
        cancel_token: CancellationToken,
        portal_type: PortalType,
        profile: Option<&str>,
        sparql_endpoint: Option<&str>,
    ) -> Result<SyncResult, AppError> {
        self.sync_portal_with_progress_cancellable_internal(
            portal_url,
            url_template,
            language,
            reporter,
            cancel_token,
            self.config.force_full_sync,
            portal_type,
            profile,
            sparql_endpoint,
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
        profile: Option<&str>,
        sparql_endpoint: Option<&str>,
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
            profile,
            sparql_endpoint,
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
        profile: Option<&str>,
        sparql_endpoint: Option<&str>,
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

        let portal_client = self.portal_factory.create(
            portal_url,
            portal_type,
            language,
            profile,
            sparql_endpoint,
        )?;

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
        // For FullBulkStream, total may be 0 (unknown count) — don't short-circuit.
        // For Full/Incremental, 0 genuinely means no datasets to process.
        let is_definitely_empty = total == 0 && !matches!(plan, SyncPlan::FullBulkStream { .. });
        if is_definitely_empty {
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
        let all_seen_ids: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let processed_count = Arc::new(AtomicUsize::new(0));
        let last_reported = Arc::new(AtomicUsize::new(0));
        let was_cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));

        reporter.report(HarvestEvent::PreprocessingStarted { total });

        let report_interval = std::cmp::max(total / 20, 50);
        let url_template_arc: Option<Arc<str>> = url_template.map(Arc::from);
        let language_arc: Arc<str> = Arc::from(language);

        let upsert_batch_size = self.config.upsert_batch_size.max(1);

        // =====================================================================
        // Two-phase pipeline
        //
        // Phase 1: Pre-process datasets (fetch if needed, hash check, delta detection).
        //          Unchanged datasets are recorded and filtered out.
        //
        // Phase 2: Batch upsert changed datasets to database.
        //          Datasets are stored with embedding = NULL; the SQL COALESCE
        //          preserves any existing embeddings.
        //
        // For Full (ID-by-ID) sync: Phase 1 uses buffer_unordered for concurrent
        // HTTP fetches. Phase 2 accumulates items into full batches before upserting.
        //
        // For FullBulkStream: datasets are streamed page-by-page from the portal.
        // Each page is processed and dropped before fetching the next, bounding
        // peak memory to one page (~1000 datasets) + one upsert batch (500).
        //
        // For Incremental: datasets are already in memory (typically small).
        // =====================================================================

        match plan {
            SyncPlan::Full { ids } => {
                let ctx = PreprocessContext {
                    existing_hashes,
                    all_seen_ids: Arc::clone(&all_seen_ids),
                    stats: Arc::clone(&stats),
                    processed_count: Arc::clone(&processed_count),
                    delta_detector: self.delta_detector.clone(),
                };
                let portal_url_arc: Arc<str> = Arc::from(portal_url);
                let pre_processed = stream::iter(ids)
                    .map(|id| {
                        let portal_client = portal_client.clone();
                        let portal_url_owned = Arc::clone(&portal_url_arc);
                        let cancel_token = cancel_token.clone();
                        let was_cancelled = Arc::clone(&was_cancelled);
                        let url_template = url_template_arc.clone();
                        let language = Arc::clone(&language_arc);
                        let ctx = ctx.clone();

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
                                        portal = portal_url_owned.as_ref(),
                                        dataset_id = id,
                                        error = %e,
                                        "Failed to fetch dataset, skipping"
                                    );
                                    ctx.stats.record(SyncOutcome::Failed);
                                    ctx.processed_count.fetch_add(1, Ordering::Relaxed);
                                    return None;
                                }
                            };

                            if cancel_token.is_cancelled() {
                                was_cancelled.store(true, Ordering::SeqCst);
                                return None;
                            }

                            preprocess_dataset::<F::Client, D>(
                                portal_data,
                                &portal_url_owned,
                                url_template.as_deref(),
                                &language,
                                &ctx,
                            )
                        }
                    })
                    .buffer_unordered(self.config.concurrency)
                    .take_while(|_| {
                        let is_cancelled = cancel_token.is_cancelled();
                        async move { !is_cancelled }
                    })
                    .filter_map(|opt| async { opt });

                if self.config.dry_run {
                    use futures::stream::StreamExt as _;
                    let mut stream = std::pin::pin!(pre_processed);
                    while let Some(item) = stream.next().await {
                        stats.record(item.outcome);
                        processed_count.fetch_add(1, Ordering::Relaxed);
                    }
                    if cancel_token.is_cancelled() {
                        was_cancelled.store(true, Ordering::SeqCst);
                    }
                } else {
                    // Consume the async stream, accumulating full batches before upserting
                    let mut batch = Vec::with_capacity(upsert_batch_size);
                    let mut stream = std::pin::pin!(pre_processed);

                    while let Some(item) = stream.next().await {
                        if cancel_token.is_cancelled() {
                            was_cancelled.store(true, Ordering::SeqCst);
                            break;
                        }

                        batch.push(item);
                        if batch.len() >= upsert_batch_size {
                            let full_batch = std::mem::replace(
                                &mut batch,
                                Vec::with_capacity(upsert_batch_size),
                            );
                            let batch_len = full_batch.len();
                            Self::process_upsert_batch(full_batch, &self.store, &stats).await;
                            Self::report_progress(
                                &processed_count,
                                batch_len,
                                total,
                                report_interval,
                                &last_reported,
                                &stats,
                                reporter,
                            );
                        }
                    }

                    // Flush remaining items
                    if !batch.is_empty() {
                        let batch_len = batch.len();
                        Self::process_upsert_batch(batch, &self.store, &stats).await;
                        Self::report_progress(
                            &processed_count,
                            batch_len,
                            total,
                            report_interval,
                            &last_reported,
                            &stats,
                            reporter,
                        );
                    }
                }
            }
            SyncPlan::FullBulkStream { estimated_total: _ } => {
                // Stream datasets page-by-page from the portal. Each page is
                // processed and dropped before fetching the next, bounding peak
                // memory to one page + one upsert batch.
                let ctx = PreprocessContext {
                    existing_hashes,
                    all_seen_ids: Arc::clone(&all_seen_ids),
                    stats: Arc::clone(&stats),
                    processed_count: Arc::clone(&processed_count),
                    delta_detector: self.delta_detector.clone(),
                };

                let url_template_ref = url_template_arc.as_deref();
                let mut page_stream = portal_client.search_all_datasets_stream();
                let mut batch = Vec::with_capacity(upsert_batch_size);

                while let Some(page_result) = page_stream.next().await {
                    if cancel_token.is_cancelled() {
                        was_cancelled.store(true, Ordering::SeqCst);
                        break;
                    }

                    let page = match page_result {
                        Ok(datasets) => datasets,
                        Err(e) => {
                            tracing::warn!(
                                portal = portal_url,
                                error = %e,
                                "Page fetch failed during streaming harvest"
                            );
                            // Record failures for the remaining estimated datasets
                            stats.record(SyncOutcome::Failed);
                            break;
                        }
                    };

                    for portal_data in page {
                        if cancel_token.is_cancelled() {
                            was_cancelled.store(true, Ordering::SeqCst);
                            break;
                        }

                        if self.config.dry_run {
                            if let Some(item) = preprocess_dataset::<F::Client, D>(
                                portal_data,
                                portal_url,
                                url_template_ref,
                                language,
                                &ctx,
                            ) {
                                stats.record(item.outcome);
                                let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
                                let last = last_reported.load(Ordering::Relaxed);
                                if (current >= last + report_interval
                                    || (total > 0 && current >= total))
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
                            }
                        } else if let Some(item) = preprocess_dataset::<F::Client, D>(
                            portal_data,
                            portal_url,
                            url_template_ref,
                            language,
                            &ctx,
                        ) {
                            batch.push(item);
                            if batch.len() >= upsert_batch_size {
                                let full_batch = std::mem::replace(
                                    &mut batch,
                                    Vec::with_capacity(upsert_batch_size),
                                );
                                let batch_len = full_batch.len();
                                Self::process_upsert_batch(full_batch, &self.store, &stats).await;
                                Self::report_progress(
                                    &processed_count,
                                    batch_len,
                                    total,
                                    report_interval,
                                    &last_reported,
                                    &stats,
                                    reporter,
                                );
                            }
                        }
                    }
                    // page is dropped here — memory freed
                }

                // Flush remaining items
                if !batch.is_empty() {
                    let batch_len = batch.len();
                    Self::process_upsert_batch(batch, &self.store, &stats).await;
                    Self::report_progress(
                        &processed_count,
                        batch_len,
                        total,
                        report_interval,
                        &last_reported,
                        &stats,
                        reporter,
                    );
                }
            }
            SyncPlan::Incremental { datasets } => {
                // Datasets are already in memory — preprocessing is synchronous
                // (hash comparison only, no HTTP). Incremental results are typically
                // small (only datasets modified since last sync).
                let ctx = PreprocessContext {
                    existing_hashes,
                    all_seen_ids: Arc::clone(&all_seen_ids),
                    stats: Arc::clone(&stats),
                    processed_count: Arc::clone(&processed_count),
                    delta_detector: self.delta_detector.clone(),
                };

                let url_template_ref = url_template_arc.as_deref();

                if self.config.dry_run {
                    for portal_data in datasets {
                        if cancel_token.is_cancelled() {
                            was_cancelled.store(true, Ordering::SeqCst);
                            break;
                        }
                        if let Some(item) = preprocess_dataset::<F::Client, D>(
                            portal_data,
                            portal_url,
                            url_template_ref,
                            language,
                            &ctx,
                        ) {
                            stats.record(item.outcome);
                            let current = processed_count.fetch_add(1, Ordering::Relaxed) + 1;
                            let last = last_reported.load(Ordering::Relaxed);
                            if (current >= last + report_interval || current >= total)
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
                        }
                    }
                } else {
                    let mut batch = Vec::with_capacity(upsert_batch_size);

                    for portal_data in datasets {
                        if cancel_token.is_cancelled() {
                            was_cancelled.store(true, Ordering::SeqCst);
                            break;
                        }

                        if let Some(item) = preprocess_dataset::<F::Client, D>(
                            portal_data,
                            portal_url,
                            url_template_ref,
                            language,
                            &ctx,
                        ) {
                            batch.push(item);
                            if batch.len() >= upsert_batch_size {
                                let full_batch = std::mem::replace(
                                    &mut batch,
                                    Vec::with_capacity(upsert_batch_size),
                                );
                                let batch_len = full_batch.len();
                                Self::process_upsert_batch(full_batch, &self.store, &stats).await;
                                Self::report_progress(
                                    &processed_count,
                                    batch_len,
                                    total,
                                    report_interval,
                                    &last_reported,
                                    &stats,
                                    reporter,
                                );
                            }
                        }
                    }

                    // Flush remaining items
                    if !batch.is_empty() {
                        let batch_len = batch.len();
                        Self::process_upsert_batch(batch, &self.store, &stats).await;
                        Self::report_progress(
                            &processed_count,
                            batch_len,
                            total,
                            report_interval,
                            &last_reported,
                            &stats,
                            reporter,
                        );
                    }
                }
            }
        }

        // Report processing summary before finalization steps
        // (stale detection, sync status recording).
        // Note: in the streaming pipeline, preprocessing and persistence
        // are interleaved, so stats here reflect both phases.
        {
            let preprocess_stats = stats.to_stats();
            reporter.report(HarvestEvent::PreprocessingCompleted {
                changed: preprocess_stats.created + preprocess_stats.updated,
                unchanged: preprocess_stats.unchanged,
                failed: preprocess_stats.failed,
            });
        }

        // In dry-run mode, skip all DB writes and return stats early
        if self.config.dry_run {
            let final_stats = stats.to_stats();
            let is_cancelled = was_cancelled.load(Ordering::SeqCst) || cancel_token.is_cancelled();

            if is_cancelled {
                tracing::info!(
                    portal = portal_url,
                    created = final_stats.created,
                    updated = final_stats.updated,
                    unchanged = final_stats.unchanged,
                    failed = final_stats.failed,
                    "Dry run cancelled — no changes written"
                );
                return Ok(SyncResult::cancelled(final_stats));
            } else {
                tracing::info!(
                    portal = portal_url,
                    created = final_stats.created,
                    updated = final_stats.updated,
                    unchanged = final_stats.unchanged,
                    failed = final_stats.failed,
                    "Dry run complete — no changes written"
                );
                return Ok(SyncResult::completed(final_stats));
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

        // Mark stale datasets after a clean full sync.
        // Only full syncs can definitively say "this dataset is gone" because
        // incremental syncs only fetch changed datasets.
        // Uses ID-based exclusion: any dataset on this portal whose original_id
        // is NOT in the set of all seen IDs is marked as stale. This avoids
        // the expensive per-row timestamp update that the old approach required.
        if sync_mode == SyncMode::Full
            && !is_cancelled
            && final_stats.failed == 0
            && final_stats.skipped == 0
        {
            let seen_list = all_seen_ids
                .lock()
                .ok()
                .map(|g| g.to_vec())
                .unwrap_or_default();
            match self
                .store
                .mark_stale_by_exclusion(portal_url, &seen_list)
                .await
            {
                Ok(stale_count) if stale_count > 0 => {
                    tracing::warn!(
                        portal = portal_url,
                        stale_count,
                        "Marked {} dataset(s) as stale (not found on portal)",
                        stale_count
                    );
                    reporter.report(HarvestEvent::StaleDetected {
                        count: stale_count as usize,
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        portal = portal_url,
                        error = %e,
                        "Failed to mark stale datasets (non-fatal)"
                    );
                }
                _ => {}
            }
        }

        // Record sync status
        tracing::info!(
            portal = portal_url,
            status = status.as_str(),
            "Finalizing: recording sync status..."
        );
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

    /// Batch upserts pre-processed datasets to the database.
    ///
    /// Datasets are stored with `embedding = None`. The SQL COALESCE ensures
    /// existing embeddings are preserved on update.
    /// Reports progress after a batch has been processed.
    fn report_progress(
        processed_count: &Arc<AtomicUsize>,
        batch_len: usize,
        total: usize,
        report_interval: usize,
        last_reported: &Arc<AtomicUsize>,
        stats: &Arc<AtomicSyncStats>,
        reporter: &impl ProgressReporter,
    ) {
        let current = processed_count.fetch_add(batch_len, Ordering::Relaxed) + batch_len;
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

    async fn process_upsert_batch(
        batch: Vec<PreProcessedDataset>,
        store: &S,
        stats: &Arc<AtomicSyncStats>,
    ) {
        let outcomes: Vec<SyncOutcome> = batch.iter().map(|i| i.outcome).collect();
        let datasets: Vec<NewDataset> = batch.into_iter().map(|i| i.dataset).collect();

        match store.batch_upsert(&datasets).await {
            Ok(_) => {
                for outcome in outcomes {
                    stats.record(outcome);
                }
            }
            Err(e) => {
                tracing::warn!(
                    count = datasets.len(),
                    error = %e,
                    "Failed to batch upsert datasets"
                );
                for _ in 0..datasets.len() {
                    stats.record(SyncOutcome::Failed);
                }
            }
        }
    }

    /// Harvests multiple portals with cancellation support.
    pub async fn batch_harvest_cancellable(
        &self,
        portals: &[&PortalEntry],
        cancel_token: CancellationToken,
    ) -> BatchHarvestSummary {
        self.batch_harvest_with_progress_cancellable(portals, &SilentReporter, cancel_token)
            .await
    }

    /// Harvests multiple portals with progress reporting and cancellation support.
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
                    portal.profile(),
                    portal.sparql_endpoint(),
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
    use crate::config::HarvestConfig;

    #[test]
    fn test_harvest_config_default() {
        let config = HarvestConfig::default();
        assert!(config.concurrency > 0, "concurrency should be positive");
    }
}
