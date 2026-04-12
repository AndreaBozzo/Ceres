//! Combined harvest + embed pipeline.
//!
//! The [`HarvestPipeline`] composes [`HarvestService`] and [`EmbeddingService`]
//! for the common workflow of harvesting metadata and then immediately generating
//! embeddings for newly harvested datasets.
//!
//! For users who only need metadata harvesting (no API key), use [`HarvestService`] directly.
//! For standalone embedding (backfill, provider switch), use [`EmbeddingService`] directly.

use tokio_util::sync::CancellationToken;

use crate::config::SyncConfig;
use crate::embedding::{EmbeddingService, EmbeddingStats};
use crate::harvest::HarvestService;
use crate::progress::{ProgressReporter, SilentReporter};
use crate::sync::{BatchHarvestSummary, ContentHashDetector, DeltaDetector, SyncResult, SyncStats};
use crate::traits::{DatasetStore, EmbeddingProvider, PortalClientFactory};
use crate::{AppError, PortalEntry, PortalType};

/// Combined harvest + embed pipeline.
///
/// Composes [`HarvestService`] and [`EmbeddingService`] for the common
/// workflow of harvesting metadata then immediately generating embeddings.
///
/// # Example
///
/// ```ignore
/// use ceres_core::pipeline::HarvestPipeline;
/// use ceres_core::SyncConfig;
///
/// // Create from SyncConfig (backward compatible)
/// let pipeline = HarvestPipeline::from_sync_config(repo, embedding, factory, SyncConfig::default());
///
/// // Harvest + embed in one call
/// let (sync_result, embed_stats) = pipeline
///     .sync_portal_with_progress("https://data.gov", None, "en", &reporter, PortalType::Ckan)
///     .await?;
/// ```
pub struct HarvestPipeline<S, E, F, D = ContentHashDetector>
where
    S: DatasetStore,
    E: EmbeddingProvider,
    F: PortalClientFactory,
    D: DeltaDetector,
{
    /// The harvest service for metadata fetching.
    pub harvest: HarvestService<S, F, D>,
    /// The embedding service for vector generation.
    pub embedding: EmbeddingService<S, E>,
}

impl<S, E, F, D> Clone for HarvestPipeline<S, E, F, D>
where
    S: DatasetStore + Clone,
    E: EmbeddingProvider + Clone,
    F: PortalClientFactory + Clone,
    D: DeltaDetector + Clone,
{
    fn clone(&self) -> Self {
        Self {
            harvest: self.harvest.clone(),
            embedding: self.embedding.clone(),
        }
    }
}

impl<S, E, F> HarvestPipeline<S, E, F, ContentHashDetector>
where
    S: DatasetStore + Clone,
    E: EmbeddingProvider,
    F: PortalClientFactory,
{
    /// Creates a pipeline from the legacy [`SyncConfig`] for backward compatibility.
    ///
    /// Decomposes `SyncConfig` into `HarvestConfig` + `EmbeddingServiceConfig`.
    pub fn from_sync_config(store: S, embedding: E, portal_factory: F, config: SyncConfig) -> Self {
        let harvest_config = config.harvest_config();
        let embedding_config = config.embedding_service_config();
        Self {
            harvest: HarvestService::with_config(store.clone(), portal_factory, harvest_config),
            embedding: EmbeddingService::with_config(store, embedding, embedding_config),
        }
    }

    /// Creates a pipeline with default configuration.
    pub fn new(store: S, embedding: E, portal_factory: F) -> Self {
        Self::from_sync_config(store, embedding, portal_factory, SyncConfig::default())
    }
}

impl<S, E, F, D> HarvestPipeline<S, E, F, D>
where
    S: DatasetStore + Clone,
    E: EmbeddingProvider,
    F: PortalClientFactory,
    D: DeltaDetector,
{
    /// Synchronizes a single portal: harvest metadata then embed pending datasets.
    pub async fn sync_portal(&self, portal_url: &str) -> Result<SyncStats, AppError> {
        let (result, _) = self
            .sync_portal_with_progress(
                portal_url,
                None,
                "en",
                &SilentReporter,
                PortalType::default(),
                None,
            )
            .await?;
        Ok(result.stats)
    }

    /// Synchronizes a single portal with progress reporting: harvest then embed.
    pub async fn sync_portal_with_progress<R: ProgressReporter>(
        &self,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
        reporter: &R,
        portal_type: PortalType,
        profile: Option<&str>,
    ) -> Result<(SyncResult, EmbeddingStats), AppError> {
        self.sync_portal_with_progress_cancellable_with_options(
            portal_url,
            url_template,
            language,
            reporter,
            CancellationToken::new(),
            false,
            portal_type,
            profile,
        )
        .await
    }

    /// Synchronizes a single portal with progress, cancellation, and per-call options.
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
    ) -> Result<(SyncResult, EmbeddingStats), AppError> {
        // Step 1: Harvest metadata
        let sync_result = self
            .harvest
            .sync_portal_with_progress_cancellable_with_options(
                portal_url,
                url_template,
                language,
                reporter,
                cancel_token.clone(),
                force_full_sync,
                portal_type,
                profile,
            )
            .await?;

        if cancel_token.is_cancelled() || sync_result.is_cancelled() {
            return Ok((sync_result, EmbeddingStats::default()));
        }

        // Step 2: Embed pending datasets (scoped to this portal)
        let embed_stats = self
            .embedding
            .embed_pending(Some(portal_url), reporter, cancel_token)
            .await?;

        Ok((sync_result, embed_stats))
    }

    /// Harvests multiple portals: harvest metadata then embed all pending datasets.
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
        self.batch_harvest_with_progress_cancellable(portals, reporter, CancellationToken::new())
            .await
    }

    /// Harvests multiple portals with progress and cancellation.
    ///
    /// Delegates metadata harvesting to [`HarvestService`], then runs a single
    /// embedding pass for all pending datasets across all portals.
    pub async fn batch_harvest_with_progress_cancellable<R: ProgressReporter>(
        &self,
        portals: &[&PortalEntry],
        reporter: &R,
        cancel_token: CancellationToken,
    ) -> BatchHarvestSummary {
        // Step 1: Harvest all portals (metadata only)
        let summary = self
            .harvest
            .batch_harvest_with_progress_cancellable(portals, reporter, cancel_token.clone())
            .await;

        // Step 2: Single embedding pass for all pending datasets
        if !cancel_token.is_cancelled() {
            match self
                .embedding
                .embed_pending(None, reporter, cancel_token)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "Post-batch embedding pass failed");
                }
            }
        }

        summary
    }
}
