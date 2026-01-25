//! Request DTOs for API endpoints.

use serde::Deserialize;
use utoipa::{IntoParams, ToSchema};

/// Query parameters for semantic search.
#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct SearchQuery {
    /// The search query text
    #[param(example = "air quality monitoring")]
    pub q: String,

    /// Maximum number of results (default: 10, max: 100)
    #[param(example = 10)]
    pub limit: Option<usize>,
}

/// Query parameters for dataset export.
#[derive(Debug, Deserialize, IntoParams, ToSchema)]
pub struct ExportQuery {
    /// Export format: json, jsonl, or csv (default: jsonl)
    #[param(example = "jsonl")]
    pub format: Option<String>,

    /// Filter by source portal URL
    #[param(example = "https://dati.comune.milano.it")]
    pub portal: Option<String>,

    /// Maximum number of records to export
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
