//! Search endpoint.

use axum::{
    Json,
    extract::{Query, State},
};

use crate::dto::{MAX_SEARCH_QUERY_LENGTH, SearchQuery, SearchResponse, SearchResultDto};
use crate::error::ApiError;
use crate::state::AppState;

/// Semantic search across indexed datasets.
///
/// Performs vector similarity search using the query text.
#[utoipa::path(
    get,
    path = "/api/v1/search",
    params(SearchQuery),
    responses(
        (status = 200, description = "Search results", body = SearchResponse),
        (status = 400, description = "Invalid query parameters"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "search"
)]
pub async fn search(
    State(state): State<AppState>,
    Query(params): Query<SearchQuery>,
) -> Result<Json<SearchResponse>, ApiError> {
    // Validate and cap limit
    let limit = params.limit.unwrap_or(10).min(100);

    if params.q.trim().is_empty() {
        return Err(ApiError::BadRequest("Query cannot be empty".to_string()));
    }

    if params.q.len() > MAX_SEARCH_QUERY_LENGTH {
        return Err(ApiError::BadRequest(format!(
            "Query exceeds maximum length of {} characters",
            MAX_SEARCH_QUERY_LENGTH
        )));
    }

    let results = state
        .search_service
        .search(&params.q, limit)
        .await
        .map_err(ApiError::from)?;

    let dto_results: Vec<SearchResultDto> =
        results.into_iter().map(SearchResultDto::from).collect();

    Ok(Json(SearchResponse {
        query: params.q,
        count: dto_results.len(),
        results: dto_results,
    }))
}
