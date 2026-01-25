//! Statistics endpoint.

use axum::{Json, extract::State};

use crate::dto::StatsResponse;
use crate::error::ApiError;
use crate::state::AppState;

/// Get database statistics.
///
/// Returns global statistics about indexed datasets.
#[utoipa::path(
    get,
    path = "/api/v1/stats",
    responses(
        (status = 200, description = "Database statistics", body = StatsResponse),
        (status = 500, description = "Internal server error"),
    ),
    tag = "system"
)]
pub async fn get_stats(State(state): State<AppState>) -> Result<Json<StatsResponse>, ApiError> {
    let stats = state
        .dataset_repo
        .get_stats()
        .await
        .map_err(ApiError::from)?;

    Ok(Json(StatsResponse::from(stats)))
}
