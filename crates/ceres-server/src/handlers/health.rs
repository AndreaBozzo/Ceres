//! Health check endpoint.

use axum::{Json, extract::State};

use crate::dto::{HealthResponse, ServiceStatus};
use crate::error::ApiError;
use crate::state::AppState;

/// Health check endpoint.
///
/// Returns the server health status including database connectivity.
/// - "healthy": All services are operational
/// - "degraded": Some non-critical services are unavailable
/// - "unhealthy": Critical services (database) are unavailable
#[utoipa::path(
    get,
    path = "/api/v1/health",
    responses(
        (status = 200, description = "Server health status", body = HealthResponse),
    ),
    tag = "system"
)]
pub async fn health_check(State(state): State<AppState>) -> Result<Json<HealthResponse>, ApiError> {
    // Check database connectivity
    let db_status = match state.dataset_repo.health_check().await {
        Ok(()) => ServiceStatus {
            healthy: true,
            message: None,
        },
        Err(e) => ServiceStatus {
            healthy: false,
            message: Some(e.to_string()),
        },
    };

    let overall_status = if db_status.healthy {
        "healthy"
    } else {
        "unhealthy"
    };

    Ok(Json(HealthResponse {
        status: overall_status.to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        database: db_status,
    }))
}
