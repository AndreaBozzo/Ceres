//! Handler integration tests.
//!
//! Tests the HTTP API endpoints via the actual Axum router with a real
//! PostgreSQL database and mock embedding provider.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

use super::common::TestApp;

/// Helper to read response body as JSON value.
async fn body_json(body: Body) -> serde_json::Value {
    let bytes = body.collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "Failed to parse JSON: {}. Body: {:?}",
            e,
            String::from_utf8_lossy(&bytes)
        )
    })
}

// =============================================================================
// Health endpoint
// =============================================================================

#[tokio::test]
async fn test_health_check() {
    let app = TestApp::new().await;

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let json = body_json(response.into_body()).await;
    assert_eq!(status, StatusCode::OK, "Response: {:?}", json);
    assert_eq!(json["status"], "healthy");
    assert!(json["version"].is_string());
    assert_eq!(json["database"]["healthy"], true);
}

// =============================================================================
// Search endpoint
// =============================================================================

#[tokio::test]
async fn test_search_empty_query_returns_400() {
    let app = TestApp::new().await;

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/search?q=")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let json = body_json(response.into_body()).await;
    let message = json["message"].as_str().unwrap_or("");
    assert!(
        message.contains("cannot be empty") || message.contains("empty"),
        "Expected 'cannot be empty' in message, got: {}",
        message
    );
}

#[tokio::test]
async fn test_search_valid_query() {
    let app = TestApp::new().await;

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/search?q=climate+data")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let json = body_json(response.into_body()).await;
    assert_eq!(json["query"], "climate data");
    assert!(json["count"].is_number());
    assert!(json["results"].is_array());
}

// =============================================================================
// Auth / protected endpoints
// =============================================================================

#[tokio::test]
async fn test_protected_endpoint_without_auth_returns_401() {
    let app = TestApp::with_admin_token("test-secret").await;

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/harvest")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"force_full_sync": false}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_protected_endpoint_disabled_returns_403() {
    // No admin token configured → admin endpoints disabled
    let app = TestApp::new().await;

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/harvest")
                .header("content-type", "application/json")
                .header("authorization", "Bearer some-token")
                .body(Body::from(r#"{"force_full_sync": false}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}
