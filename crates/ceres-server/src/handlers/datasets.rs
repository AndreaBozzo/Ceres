//! Dataset endpoints.

use axum::{
    Json,
    extract::{Path, State},
};
use uuid::Uuid;

use ceres_core::DatasetStore;

use crate::dto::{DatasetResponse, DatasetSchemaResponse};
use crate::error::ApiError;
use crate::state::AppState;

/// Get a dataset by ID.
///
/// Returns dataset details including source-specific raw metadata after
/// configured sensitive keys are removed recursively. The `metadata` shape is
/// source-specific and best-effort, and is not a stable resource/distribution
/// contract. Use `GET /api/v1/datasets/{id}/schema` for normalized resource
/// metadata.
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

    let mut response = DatasetResponse::from(dataset);
    state.metadata_redactor.redact(&mut response.metadata);

    Ok(Json(response))
}

/// Get the resource schema for a dataset.
///
/// Returns the supported, normalized resource/distribution contract (format,
/// media type, access URL, description, and column-level fields when the
/// portal exposed them), derived on read from harvested raw metadata.
#[utoipa::path(
    get,
    path = "/api/v1/datasets/{id}/schema",
    params(
        ("id" = Uuid, Path, description = "Dataset UUID")
    ),
    responses(
        (status = 200, description = "Normalized resource schema", body = DatasetSchemaResponse),
        (status = 404, description = "Dataset not found"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "datasets"
)]
pub async fn get_dataset_schema(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<DatasetSchemaResponse>, ApiError> {
    let dataset = state
        .dataset_repo
        .get_by_id(id)
        .await
        .map_err(ApiError::from)?
        .ok_or_else(|| ApiError::NotFound(format!("Dataset not found: {}", id)))?;

    Ok(Json(DatasetSchemaResponse::from(dataset)))
}
