//! Response DTOs for API endpoints.

use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;
use uuid::Uuid;

use ceres_core::{DatabaseStats, HarvestJob, SearchResult, SyncStats};

// =============================================================================
// Health & Stats
// =============================================================================

/// Health check response.
#[derive(Debug, Serialize, ToSchema)]
pub struct HealthResponse {
    /// Health status ("healthy", "degraded", or "unhealthy")
    pub status: String,
    /// Server version
    pub version: String,
    /// Database connectivity status
    pub database: ServiceStatus,
}

/// Status of an individual service component.
#[derive(Debug, Serialize, ToSchema)]
pub struct ServiceStatus {
    /// Whether the service is reachable
    pub healthy: bool,
    /// Optional message (e.g., error details)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Database statistics response.
#[derive(Debug, Serialize, ToSchema)]
pub struct StatsResponse {
    /// Total number of datasets in the database
    pub total_datasets: i64,
    /// Number of datasets with generated embeddings
    pub datasets_with_embeddings: i64,
    /// Number of unique indexed portals
    pub total_portals: i64,
    /// Timestamp of the last update
    pub last_update: Option<DateTime<Utc>>,
}

impl From<DatabaseStats> for StatsResponse {
    fn from(s: DatabaseStats) -> Self {
        Self {
            total_datasets: s.total_datasets,
            datasets_with_embeddings: s.datasets_with_embeddings,
            total_portals: s.total_portals,
            last_update: s.last_update,
        }
    }
}

// =============================================================================
// Search
// =============================================================================

/// Semantic search response.
#[derive(Debug, Serialize, ToSchema)]
pub struct SearchResponse {
    /// The original search query
    pub query: String,
    /// Number of results returned
    pub count: usize,
    /// Search results ordered by similarity
    pub results: Vec<SearchResultDto>,
}

/// Individual search result with similarity score.
#[derive(Debug, Serialize, ToSchema)]
pub struct SearchResultDto {
    /// Dataset UUID
    pub id: Uuid,
    /// Dataset title
    pub title: String,
    /// Dataset description
    pub description: Option<String>,
    /// Dataset landing page URL
    pub url: String,
    /// Source portal URL
    pub source_portal: String,
    /// Similarity score (0.0 to 1.0)
    pub similarity_score: f32,
}

impl From<SearchResult> for SearchResultDto {
    fn from(r: SearchResult) -> Self {
        Self {
            id: r.dataset.id,
            title: r.dataset.title,
            description: r.dataset.description,
            url: r.dataset.url,
            source_portal: r.dataset.source_portal,
            similarity_score: r.similarity_score,
        }
    }
}

// =============================================================================
// Portals
// =============================================================================

/// Portal information with sync status.
#[derive(Debug, Serialize, ToSchema)]
pub struct PortalInfoResponse {
    /// Portal name
    pub name: String,
    /// Portal base URL
    pub url: String,
    /// Portal type (ckan, socrata, dcat)
    pub portal_type: String,
    /// Whether the portal is enabled for harvesting
    pub enabled: bool,
    /// Portal description
    pub description: Option<String>,
    /// Last successful sync timestamp
    pub last_sync: Option<DateTime<Utc>>,
    /// Number of datasets from this portal
    pub dataset_count: Option<i64>,
}

/// Portal statistics response.
#[derive(Debug, Serialize, ToSchema)]
pub struct PortalStatsResponse {
    /// Portal name
    pub name: String,
    /// Portal URL
    pub url: String,
    /// Number of datasets from this portal
    pub dataset_count: i64,
    /// Last successful sync timestamp
    pub last_sync: Option<DateTime<Utc>>,
    /// Last sync mode (full or incremental)
    pub last_sync_mode: Option<String>,
    /// Last sync status (completed or cancelled)
    pub last_sync_status: Option<String>,
    /// Datasets synced in last sync
    pub last_sync_datasets: Option<i32>,
}

// =============================================================================
// Harvest
// =============================================================================

/// Harvest job response.
#[derive(Debug, Serialize, ToSchema)]
pub struct HarvestJobResponse {
    /// Job UUID
    pub job_id: Uuid,
    /// Current job status
    pub status: String,
    /// Target portal URL
    pub portal_url: String,
    /// Portal name
    pub portal_name: Option<String>,
    /// Job creation timestamp
    pub created_at: DateTime<Utc>,
    /// Job start timestamp
    pub started_at: Option<DateTime<Utc>>,
    /// Job completion timestamp
    pub completed_at: Option<DateTime<Utc>>,
    /// Final sync statistics
    pub sync_stats: Option<SyncStatsDto>,
    /// Error message if failed
    pub error_message: Option<String>,
}

impl From<HarvestJob> for HarvestJobResponse {
    fn from(job: HarvestJob) -> Self {
        Self {
            job_id: job.id,
            status: job.status.as_str().to_string(),
            portal_url: job.portal_url,
            portal_name: job.portal_name,
            created_at: job.created_at,
            started_at: job.started_at,
            completed_at: job.completed_at,
            sync_stats: job.sync_stats.map(SyncStatsDto::from),
            error_message: job.error_message,
        }
    }
}

/// Sync statistics for harvest operations.
#[derive(Debug, Serialize, ToSchema)]
pub struct SyncStatsDto {
    /// Datasets unchanged (no update needed)
    pub unchanged: usize,
    /// Datasets updated
    pub updated: usize,
    /// New datasets created
    pub created: usize,
    /// Datasets that failed processing
    pub failed: usize,
    /// Datasets skipped (circuit breaker)
    pub skipped: usize,
    /// Total datasets processed
    pub total: usize,
}

impl From<SyncStats> for SyncStatsDto {
    fn from(s: SyncStats) -> Self {
        Self {
            unchanged: s.unchanged,
            updated: s.updated,
            created: s.created,
            failed: s.failed,
            skipped: s.skipped,
            total: s.total(),
        }
    }
}

/// Harvest status overview.
#[derive(Debug, Serialize, ToSchema)]
pub struct HarvestStatusResponse {
    /// Number of pending jobs
    pub pending_jobs: i64,
    /// Number of running jobs
    pub running_jobs: i64,
    /// Recent harvest jobs
    pub recent_jobs: Vec<HarvestJobResponse>,
}

// =============================================================================
// Datasets
// =============================================================================

/// Dataset details response.
#[derive(Debug, Serialize, ToSchema)]
pub struct DatasetResponse {
    /// Dataset UUID
    pub id: Uuid,
    /// Original ID from source portal
    pub original_id: String,
    /// Source portal URL
    pub source_portal: String,
    /// Dataset landing page URL
    pub url: String,
    /// Dataset title
    pub title: String,
    /// Dataset description
    pub description: Option<String>,
    /// Additional metadata
    pub metadata: serde_json::Value,
    /// First indexed timestamp
    pub first_seen_at: DateTime<Utc>,
    /// Last update timestamp
    pub last_updated_at: DateTime<Utc>,
}

impl From<ceres_core::Dataset> for DatasetResponse {
    fn from(d: ceres_core::Dataset) -> Self {
        Self {
            id: d.id,
            original_id: d.original_id,
            source_portal: d.source_portal,
            url: d.url,
            title: d.title,
            description: d.description,
            metadata: d.metadata,
            first_seen_at: d.first_seen_at,
            last_updated_at: d.last_updated_at,
        }
    }
}
