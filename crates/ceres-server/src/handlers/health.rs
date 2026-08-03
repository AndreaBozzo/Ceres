//! Container liveness and database readiness endpoints.

use axum::{Json, extract::State, http::StatusCode};

use crate::dto::{HealthResponse, ProbeResponse, ServiceStatus};
use crate::state::AppState;

/// Liveness probe. This only verifies that the HTTP process can respond.
#[utoipa::path(
    get,
    path = "/api/v1/health/live",
    responses(
        (status = 200, description = "Server process is alive", body = ProbeResponse),
    ),
    tag = "system"
)]
pub async fn liveness_check() -> Json<ProbeResponse> {
    Json(ProbeResponse {
        status: "alive".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Readiness probe. Returns 503 until the database is reachable.
#[utoipa::path(
    get,
    path = "/api/v1/health/ready",
    responses(
        (status = 200, description = "Server and database are ready", body = HealthResponse),
        (status = 503, description = "Database is unavailable", body = HealthResponse),
    ),
    tag = "system"
)]
pub async fn readiness_check(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    database_readiness(&state).await
}

/// Backward-compatible readiness alias.
#[utoipa::path(
    get,
    path = "/api/v1/health",
    responses(
        (status = 200, description = "Server and database are ready", body = HealthResponse),
        (status = 503, description = "Database is unavailable", body = HealthResponse),
    ),
    tag = "system"
)]
pub async fn health_check(State(state): State<AppState>) -> (StatusCode, Json<HealthResponse>) {
    database_readiness(&state).await
}

async fn database_readiness(state: &AppState) -> (StatusCode, Json<HealthResponse>) {
    let db_status = match state.dataset_repo.health_check().await {
        Ok(()) => ServiceStatus {
            healthy: true,
            message: None,
        },
        Err(error) => ServiceStatus {
            healthy: false,
            message: Some(error.to_string()),
        },
    };

    let (status_code, overall_status) = if db_status.healthy {
        (StatusCode::OK, "healthy")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "unhealthy")
    };

    (
        status_code,
        Json(HealthResponse {
            status: overall_status.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            database: db_status,
        }),
    )
}
