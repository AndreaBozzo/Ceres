use tokio_util::sync::CancellationToken;

use ceres_client::{EmbeddingProviderEnum, PortalClientFactoryEnum};
use ceres_core::{ExportService, HarvestService, PortalsConfig, SearchService};
use ceres_db::{DatasetRepository, JobRepository};

/// Shared application state for all handlers.
///
/// This is wrapped in Arc internally by Axum when using `with_state()`,
/// so all fields must implement Clone (which they do via internal `Arc<Pool>`).
// TODO(design): consider pub(crate) fields to enforce service-layer boundary
#[derive(Clone)]
pub struct AppState {
    /// Search service for semantic search operations
    pub search_service: SearchService<DatasetRepository, EmbeddingProviderEnum>,

    /// Harvest service for portal synchronization
    pub harvest_service:
        HarvestService<DatasetRepository, EmbeddingProviderEnum, PortalClientFactoryEnum>,

    /// Export service for streaming data exports
    pub export_service: ExportService<DatasetRepository>,

    /// Dataset repository for direct database queries
    pub dataset_repo: DatasetRepository,

    /// Job repository for harvest job management
    pub job_repo: JobRepository,

    /// Portal configuration (loaded from portals.toml)
    pub portals_config: Option<PortalsConfig>,

    /// Cancellation token for graceful shutdown
    pub shutdown_token: CancellationToken,
}

impl AppState {
    /// Creates a new application state with all services initialized.
    pub fn new(
        pool: sqlx::PgPool,
        embedding_client: EmbeddingProviderEnum,
        portals_config: Option<PortalsConfig>,
        shutdown_token: CancellationToken,
    ) -> Self {
        let dataset_repo = DatasetRepository::new(pool.clone());
        let job_repo = JobRepository::new(pool);
        let portal_factory = PortalClientFactoryEnum::new();

        Self {
            search_service: SearchService::new(dataset_repo.clone(), embedding_client.clone()),
            harvest_service: HarvestService::new(
                dataset_repo.clone(),
                embedding_client,
                portal_factory,
            ),
            export_service: ExportService::new(dataset_repo.clone()),
            dataset_repo,
            job_repo,
            portals_config,
            shutdown_token,
        }
    }
}
