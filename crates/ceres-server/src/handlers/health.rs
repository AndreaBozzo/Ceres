//! Health check endpoint.

use axum::{Json, extract::State};

use crate::dto::HealthResponse;
use crate::error::ApiError;
use crate::state::AppState;

/// Health check endpoint.
///
/// Returns the server health status and version.
#[utoipa::path(
    get,
    path = "/api/v1/health",
    responses(
        (status = 200, description = "Server is healthy", body = HealthResponse),
    ),
    tag = "system"
)]
pub async fn health_check(
    State(_state): State<AppState>,
) -> Result<Json<HealthResponse>, ApiError> {
    Ok(Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    }))
}
