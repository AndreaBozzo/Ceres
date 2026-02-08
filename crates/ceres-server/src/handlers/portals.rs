//! Portal management endpoints.

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};

use ceres_core::{CreateJobRequest, JobQueue};

use crate::dto::{
    HarvestJobResponse, PortalInfoResponse, PortalStatsResponse, TriggerHarvestRequest,
};
use crate::error::ApiError;
use crate::state::AppState;

/// List all configured portals with sync status.
#[utoipa::path(
    get,
    path = "/api/v1/portals",
    responses(
        (status = 200, description = "List of portals", body = Vec<PortalInfoResponse>),
        (status = 500, description = "Internal server error"),
    ),
    tag = "portals"
)]
pub async fn list_portals(
    State(state): State<AppState>,
) -> Result<Json<Vec<PortalInfoResponse>>, ApiError> {
    let Some(config) = &state.portals_config else {
        return Ok(Json(vec![]));
    };

    let mut portals = Vec::new();

    for portal in &config.portals {
        // Get sync status for this portal
        let sync_status = state
            .dataset_repo
            .get_sync_status(&portal.url)
            .await
            .ok()
            .flatten();

        portals.push(PortalInfoResponse {
            name: portal.name.clone(),
            url: portal.url.clone(),
            portal_type: portal.portal_type.clone(),
            enabled: portal.enabled,
            description: portal.description.clone(),
            last_sync: sync_status.as_ref().and_then(|s| s.last_successful_sync),
            dataset_count: sync_status.map(|s| s.datasets_synced as i64),
        });
    }

    Ok(Json(portals))
}

/// Get statistics for a specific portal.
#[utoipa::path(
    get,
    path = "/api/v1/portals/{name}/stats",
    params(
        ("name" = String, Path, description = "Portal name")
    ),
    responses(
        (status = 200, description = "Portal statistics", body = PortalStatsResponse),
        (status = 404, description = "Portal not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "portals"
)]
pub async fn get_portal_stats(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<PortalStatsResponse>, ApiError> {
    let Some(config) = &state.portals_config else {
        return Err(ApiError::NotFound("No portals configured".to_string()));
    };

    let portal = config
        .portals
        .iter()
        .find(|p| p.name.to_lowercase() == name.to_lowercase())
        .ok_or_else(|| ApiError::NotFound(format!("Portal not found: {}", name)))?;

    // Get sync status
    let sync_status = state
        .dataset_repo
        .get_sync_status(&portal.url)
        .await
        .map_err(ApiError::from)?;

    let (last_sync, last_sync_mode, last_sync_status, last_sync_datasets) =
        if let Some(status) = sync_status {
            (
                status.last_successful_sync,
                status.last_sync_mode,
                status.sync_status,
                Some(status.datasets_synced),
            )
        } else {
            (None, None, None, None)
        };

    Ok(Json(PortalStatsResponse {
        name: portal.name.clone(),
        url: portal.url.clone(),
        dataset_count: last_sync_datasets.unwrap_or(0) as i64,
        last_sync,
        last_sync_mode,
        last_sync_status,
        last_sync_datasets,
    }))
}

/// Trigger harvest for a specific portal.
///
/// Creates a harvest job and returns immediately with the job ID.
/// Use GET /api/v1/harvest/status to check progress.
#[utoipa::path(
    post,
    path = "/api/v1/portals/{name}/harvest",
    params(
        ("name" = String, Path, description = "Portal name")
    ),
    request_body = TriggerHarvestRequest,
    responses(
        (status = 202, description = "Harvest job created", body = HarvestJobResponse),
        (status = 404, description = "Portal not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "portals"
)]
pub async fn trigger_portal_harvest(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<TriggerHarvestRequest>,
) -> Result<(StatusCode, Json<HarvestJobResponse>), ApiError> {
    let Some(config) = &state.portals_config else {
        return Err(ApiError::NotFound("No portals configured".to_string()));
    };

    let portal = config
        .portals
        .iter()
        .find(|p| p.name.to_lowercase() == name.to_lowercase())
        .ok_or_else(|| ApiError::NotFound(format!("Portal not found: {}", name)))?;

    let mut job_request = CreateJobRequest::new(&portal.url).with_name(&portal.name);

    if let Some(ref tmpl) = portal.url_template {
        job_request = job_request.with_url_template(tmpl);
    }

    if request.force_full_sync {
        job_request = job_request.with_full_sync();
    }

    let job = state
        .job_repo
        .create_job(job_request)
        .await
        .map_err(ApiError::from)?;

    Ok((StatusCode::ACCEPTED, Json(HarvestJobResponse::from(job))))
}
