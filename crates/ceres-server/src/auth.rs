//! Authentication middleware for protecting admin endpoints.

use axum::extract::State;
use axum::http::{self, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use subtle::ConstantTimeEq;

use crate::error::ErrorResponse;
use crate::state::AppState;

/// Middleware that validates `Authorization: Bearer <token>` against the configured admin token.
///
/// - If no admin token is configured, returns 403 Forbidden (admin endpoints disabled).
/// - If the token is missing or invalid, returns 401 Unauthorized.
pub async fn require_api_key(
    State(state): State<AppState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let expected_token = match &state.admin_token {
        Some(token) => token,
        None => {
            let body = ErrorResponse {
                error: "forbidden".to_string(),
                message: "Admin endpoints are disabled (no CERES_ADMIN_TOKEN configured)"
                    .to_string(),
                details: None,
            };
            return (StatusCode::FORBIDDEN, axum::Json(body)).into_response();
        }
    };

    let auth_header = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    let authenticated = match auth_header {
        Some(header) => {
            if let Some((scheme, token)) = header.split_once(' ') {
                scheme.eq_ignore_ascii_case("bearer")
                    && bool::from(token.as_bytes().ct_eq(expected_token.as_bytes()))
            } else {
                false
            }
        }
        None => false,
    };

    if !authenticated {
        let body = ErrorResponse {
            error: "unauthorized".to_string(),
            message: "Missing or invalid Authorization header. Expected: Bearer <api_key>"
                .to_string(),
            details: None,
        };
        return (StatusCode::UNAUTHORIZED, axum::Json(body)).into_response();
    }

    next.run(request).await
}
