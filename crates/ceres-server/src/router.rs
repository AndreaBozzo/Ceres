//! Router configuration and route composition.

use std::sync::Arc;
use std::time::Duration;

use axum::http::{HeaderValue, Method};
use axum::{
    Router, middleware,
    routing::{get, post},
};
use tower_governor::{GovernorLayer, governor::GovernorConfigBuilder};
use tower_http::{compression::CompressionLayer, cors::CorsLayer, trace::TraceLayer};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::auth::require_api_key;
use crate::config::ServerConfig;
use crate::handlers::{datasets, export, harvest, health, portals, search, stats};
use crate::openapi::ApiDoc;
use crate::state::AppState;

/// Creates the main application router with all routes and middleware.
pub fn create_router(state: AppState, config: &ServerConfig) -> Router {
    // Public routes (no authentication required)
    let public_routes = Router::new()
        .route("/health", get(health::health_check))
        .route("/stats", get(stats::get_stats))
        .route("/search", get(search::search))
        .route("/portals", get(portals::list_portals))
        .route("/portals/:name/stats", get(portals::get_portal_stats))
        .route("/harvest/status", get(harvest::get_harvest_status))
        .route("/datasets/:id", get(datasets::get_dataset_by_id));

    // Protected routes (require Bearer token)
    let protected_routes = Router::new()
        .route(
            "/portals/:name/harvest",
            post(portals::trigger_portal_harvest),
        )
        .route("/harvest", post(harvest::trigger_harvest_all))
        .route("/export", get(export::export_datasets))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_key,
        ));

    let api_routes = public_routes.merge(protected_routes);

    // Configure rate limiting (Arc required for cloning in layers)
    let governor_config = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(config.rate_limit_rps.into())
            .burst_size(config.rate_limit_burst)
            .finish()
            .expect("Invalid rate limit configuration"),
    );

    // Configure CORS based on environment
    let cors_layer = build_cors_layer(&config.cors_origins);

    Router::new()
        .nest("/api/v1", api_routes)
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        // Middleware layers (order matters: bottom layers run first)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(cors_layer)
        .layer(GovernorLayer {
            config: governor_config,
        })
        .with_state(state)
}

/// Build CORS layer from configuration.
///
/// If `origins` is "*", allows any origin (for development).
/// Otherwise, parses comma-separated origins.
fn build_cors_layer(origins: &str) -> CorsLayer {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::header::AUTHORIZATION,
            axum::http::header::ACCEPT,
        ])
        .max_age(Duration::from_secs(3600));

    if origins == "*" {
        cors.allow_origin(tower_http::cors::Any)
    } else {
        let allowed: Vec<HeaderValue> = origins
            .split(',')
            .filter_map(|s| s.trim().parse().ok())
            .collect();
        cors.allow_origin(allowed)
    }
}
