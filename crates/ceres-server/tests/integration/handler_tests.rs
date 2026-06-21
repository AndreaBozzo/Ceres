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

// =============================================================================
// Dataset schema endpoint
// =============================================================================

#[tokio::test]
async fn test_dataset_schema_returns_normalized_resources() {
    let app = TestApp::new().await;

    // Seed a dataset whose metadata carries CKAN-style resources with an
    // inline Frictionless field schema.
    let metadata = serde_json::json!({
        "resources": [{
            "name": "Air quality 2024",
            "format": "CSV",
            "mimetype": "text/csv",
            "url": "https://example.org/aq.csv",
            "schema": {
                "fields": [
                    {"name": "station", "type": "string", "description": "Station id"},
                    {"name": "pm10", "type": "number"}
                ]
            }
        }]
    });

    let id: uuid::Uuid = sqlx::query_scalar(
        "INSERT INTO datasets (original_id, source_portal, url, title, description, metadata, content_hash) \
         VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
    )
    .bind("ds-schema-1")
    .bind("https://example.org")
    .bind("https://example.org/dataset/ds-schema-1")
    .bind("Air quality dataset")
    .bind(Some("desc"))
    .bind(metadata)
    .bind("hash-1")
    .fetch_one(&app.pool)
    .await
    .expect("failed to seed dataset");

    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/datasets/{}/schema", id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let status = response.status();
    let json = body_json(response.into_body()).await;
    assert_eq!(status, StatusCode::OK, "Response: {:?}", json);

    assert_eq!(json["id"], id.to_string());
    assert_eq!(json["source_portal"], "https://example.org");
    let resources = json["resources"].as_array().expect("resources array");
    assert_eq!(resources.len(), 1);
    let r = &resources[0];
    assert_eq!(r["name"], "Air quality 2024");
    assert_eq!(r["format"], "CSV");
    assert_eq!(r["media_type"], "text/csv");
    assert_eq!(r["url"], "https://example.org/aq.csv");

    let fields = r["fields"].as_array().expect("fields array");
    assert_eq!(fields.len(), 2);
    assert_eq!(fields[0]["name"], "station");
    assert_eq!(fields[0]["type"], "string");
    assert_eq!(fields[0]["description"], "Station id");
    assert_eq!(fields[1]["name"], "pm10");
}

#[tokio::test]
async fn test_dataset_schema_not_found_returns_404() {
    let app = TestApp::new().await;

    let missing = uuid::Uuid::new_v4();
    let response = app
        .router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/datasets/{}/schema", missing))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
