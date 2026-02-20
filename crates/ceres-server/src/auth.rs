//! Authentication middleware for protecting admin endpoints.

use axum::extract::State;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::error::ErrorResponse;
use crate::state::AppState;

/// Constant-time byte comparison to prevent timing attacks on API key validation.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

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
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    let authenticated = match auth_header {
        Some(header) => header
            .strip_prefix("Bearer ")
            .is_some_and(|token| constant_time_eq(token.as_bytes(), expected_token.as_bytes())),
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
