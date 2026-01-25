//! Request DTOs for API endpoints.

use serde::Deserialize;
use utoipa::{IntoParams, ToSchema};

/// Maximum allowed search query length.
pub const MAX_SEARCH_QUERY_LENGTH: usize = 2000;

/// Query parameters for semantic search.
#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct SearchQuery {
    /// The search query text (max: 2000 characters)
    #[param(example = "air quality monitoring")]
    pub q: String,

    /// Maximum number of results (default: 10, max: 100)
    #[param(example = 10)]
    pub limit: Option<usize>,
}

/// Maximum allowed export limit to prevent resource exhaustion.
pub const MAX_EXPORT_LIMIT: usize = 1_000_000;

/// Query parameters for dataset export.
#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ExportQuery {
    /// Export format: json, jsonl, or csv (default: jsonl)
    #[param(example = "jsonl")]
    pub format: Option<String>,

    /// Filter by source portal URL
    #[param(example = "https://dati.comune.milano.it")]
    pub portal: Option<String>,

    /// Maximum number of records to export (max: 1,000,000)
    #[param(example = 1000)]
    pub limit: Option<usize>,
}

/// Request body for triggering harvest operations.
#[derive(Debug, Default, Deserialize, ToSchema)]
pub struct TriggerHarvestRequest {
    /// Force full sync even if incremental is available
    #[serde(default)]
    pub force_full_sync: bool,
}
