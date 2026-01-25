use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

use ceres_core::error::{AppError, GeminiErrorKind};

/// API error type that maps to HTTP responses.
#[derive(Debug, Error)]
pub enum ApiError {
    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Bad request: {0}")]
    BadRequest(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Service unavailable: {0}")]
    ServiceUnavailable(String),

    #[error("Rate limit exceeded")]
    RateLimitExceeded,
}

/// JSON error response body
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, error_type, message) = match &self {
            ApiError::NotFound(msg) => (StatusCode::NOT_FOUND, "not_found", msg.clone()),
            ApiError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "bad_request", msg.clone()),
            ApiError::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
                msg.clone(),
            ),
            ApiError::ServiceUnavailable(msg) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "service_unavailable",
                msg.clone(),
            ),
            ApiError::RateLimitExceeded => (
                StatusCode::TOO_MANY_REQUESTS,
                "rate_limit_exceeded",
                "Rate limit exceeded. Please wait and try again.".to_string(),
            ),
        };

        let body = Json(ErrorResponse {
            error: error_type.to_string(),
            message,
            details: None,
        });

        (status, body).into_response()
    }
}

impl From<AppError> for ApiError {
    fn from(err: AppError) -> Self {
        match &err {
            AppError::DatasetNotFound(id) => {
                ApiError::NotFound(format!("Dataset not found: {}", id))
            }
            AppError::DatabaseError(_) => ApiError::Internal("Database error".to_string()),
            AppError::RateLimitExceeded => ApiError::RateLimitExceeded,
            AppError::GeminiError(details) => match details.kind {
                GeminiErrorKind::RateLimit => ApiError::RateLimitExceeded,
                GeminiErrorKind::Authentication | GeminiErrorKind::QuotaExceeded => {
                    ApiError::ServiceUnavailable("Embedding service unavailable".to_string())
                }
                _ => ApiError::Internal("Embedding service error".to_string()),
            },
            AppError::InvalidPortalUrl(url) => {
                ApiError::BadRequest(format!("Invalid portal URL: {}", url))
            }
            AppError::InvalidUrl(url) => ApiError::BadRequest(format!("Invalid URL: {}", url)),
            AppError::ConfigError(msg) => {
                ApiError::Internal(format!("Configuration error: {}", msg))
            }
            AppError::EmptyResponse => ApiError::NotFound("No data available".to_string()),
            AppError::NetworkError(_) | AppError::Timeout(_) | AppError::ClientError(_) => {
                ApiError::ServiceUnavailable("External service unavailable".to_string())
            }
            _ => ApiError::Internal(err.to_string()),
        }
    }
}
