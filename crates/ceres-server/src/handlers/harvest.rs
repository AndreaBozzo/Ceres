//! Harvest management endpoints.

use axum::{Json, extract::State, http::StatusCode};

use ceres_core::{CreateJobRequest, JobQueue, JobStatus, PortalType};

use crate::dto::{HarvestJobResponse, HarvestStatusResponse, TriggerHarvestRequest};
use crate::error::ApiError;
use crate::state::AppState;

/// Trigger harvest for all enabled portals.
///
/// Creates harvest jobs for all enabled portals and returns immediately.
/// Use GET /api/v1/harvest/status to check progress.
#[utoipa::path(
    post,
    path = "/api/v1/harvest",
    request_body = TriggerHarvestRequest,
    responses(
        (status = 202, description = "Harvest jobs created", body = Vec<HarvestJobResponse>),
        (status = 400, description = "No portals configured"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "harvest"
)]
pub async fn trigger_harvest_all(
    State(state): State<AppState>,
    Json(request): Json<TriggerHarvestRequest>,
) -> Result<(StatusCode, Json<Vec<HarvestJobResponse>>), ApiError> {
    let Some(config) = &state.portals_config else {
        return Err(ApiError::BadRequest("No portals configured".to_string()));
    };

    let enabled_portals: Vec<_> = config.portals.iter().filter(|p| p.enabled).collect();

    if enabled_portals.is_empty() {
        return Err(ApiError::BadRequest("No enabled portals found".to_string()));
    }

    let mut jobs = Vec::new();

    for portal in enabled_portals {
        // Only CKAN portals are supported for job-based harvesting for now
        if portal.portal_type != PortalType::Ckan {
            tracing::warn!(
                portal = %portal.name,
                portal_type = %portal.portal_type,
                "Skipping portal: only 'ckan' is supported for job-based harvesting"
            );
            continue;
        }

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

        jobs.push(HarvestJobResponse::from(job));
    }

    Ok((StatusCode::ACCEPTED, Json(jobs)))
}

/// Get current harvest status and recent jobs.
#[utoipa::path(
    get,
    path = "/api/v1/harvest/status",
    responses(
        (status = 200, description = "Harvest status", body = HarvestStatusResponse),
        (status = 500, description = "Internal server error"),
    ),
    tag = "harvest"
)]
pub async fn get_harvest_status(
    State(state): State<AppState>,
) -> Result<Json<HarvestStatusResponse>, ApiError> {
    let pending_jobs = state
        .job_repo
        .count_by_status(JobStatus::Pending)
        .await
        .map_err(ApiError::from)?;

    let running_jobs = state
        .job_repo
        .count_by_status(JobStatus::Running)
        .await
        .map_err(ApiError::from)?;

    let recent_jobs = state
        .job_repo
        .list_jobs(None, 20)
        .await
        .map_err(ApiError::from)?;

    Ok(Json(HarvestStatusResponse {
        pending_jobs,
        running_jobs,
        recent_jobs: recent_jobs
            .into_iter()
            .map(HarvestJobResponse::from)
            .collect(),
    }))
}
