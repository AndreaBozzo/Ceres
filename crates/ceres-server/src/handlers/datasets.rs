//! Dataset endpoints.

use axum::{
    Json,
    extract::{Path, State},
};
use uuid::Uuid;

use ceres_core::DatasetStore;

use crate::dto::DatasetResponse;
use crate::error::ApiError;
use crate::state::AppState;

/// Get a dataset by ID.
///
/// Returns the full dataset details including metadata.
#[utoipa::path(
    get,
    path = "/api/v1/datasets/{id}",
    params(
        ("id" = Uuid, Path, description = "Dataset UUID")
    ),
    responses(
        (status = 200, description = "Dataset found", body = DatasetResponse),
        (status = 404, description = "Dataset not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "datasets"
)]
pub async fn get_dataset_by_id(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<DatasetResponse>, ApiError> {
    let dataset = state
        .dataset_repo
        .get_by_id(id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::NotFound(format!("Dataset not found: {}", id)))?;

    Ok(Json(DatasetResponse::from(dataset)))
}
