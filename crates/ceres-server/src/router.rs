//! Router configuration and route composition.

use axum::{
    Router,
    routing::{get, post},
};
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    trace::TraceLayer,
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::handlers::{datasets, export, harvest, health, portals, search, stats};
use crate::openapi::ApiDoc;
use crate::state::AppState;

/// Creates the main application router with all routes and middleware.
pub fn create_router(state: AppState) -> Router {
    let api_routes = Router::new()
        // System endpoints
        .route("/health", get(health::health_check))
        .route("/stats", get(stats::get_stats))
        // Search
        .route("/search", get(search::search))
        // Portals
        .route("/portals", get(portals::list_portals))
        .route("/portals/:name/stats", get(portals::get_portal_stats))
        .route("/portals/:name/harvest", post(portals::trigger_portal_harvest))
        // Harvest
        .route("/harvest", post(harvest::trigger_harvest_all))
        .route("/harvest/status", get(harvest::get_harvest_status))
        // Export
        .route("/export", get(export::export_datasets))
        // Datasets
        .route("/datasets/:id", get(datasets::get_dataset_by_id));

    Router::new()
        .nest("/api/v1", api_routes)
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        // Middleware layers
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(state)
}
