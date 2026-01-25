//! Ceres REST API Server
//!
//! This binary starts the Ceres REST API server, exposing endpoints for
//! semantic search, portal management, and data harvesting.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use dotenvy::dotenv;
use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::{Level, info};
use tracing_subscriber::FmtSubscriber;

use ceres_client::GeminiClient;
use ceres_core::load_portals_config;

use ceres_server::{AppState, ServerConfig, create_router};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from .env file
    dotenv().ok();

    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    // Parse command line arguments
    let config = ServerConfig::parse();

    // Connect to database
    info!("Connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&config.database_url)
        .await
        .context("Failed to connect to database")?;
    info!("Database connection established");

    // Initialize Gemini client
    let gemini_client =
        GeminiClient::new(&config.gemini_api_key).context("Failed to initialize Gemini client")?;

    // Load portal configuration
    let portals_config = if let Some(path) = &config.portals_config {
        load_portals_config(Some(path.clone()))?
    } else {
        load_portals_config(None).unwrap_or(None)
    };

    if let Some(ref portals_cfg) = portals_config {
        info!(
            "Loaded {} portals from configuration",
            portals_cfg.portals.len()
        );
    }

    // Create shutdown token for graceful shutdown
    let shutdown_token = CancellationToken::new();

    // Create application state
    let app_state = AppState::new(pool, gemini_client, portals_config, shutdown_token.clone());

    // Build router
    let app = create_router(app_state);

    // Bind to address
    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .context("Invalid address")?;

    let listener = TcpListener::bind(addr)
        .await
        .context("Failed to bind to address")?;

    info!("Starting Ceres API server on http://{}", addr);
    info!("Swagger UI available at http://{}/swagger-ui", addr);

    // Start server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_token))
        .await
        .context("Server error")?;

    info!("Server shutdown complete");
    Ok(())
}

/// Wait for shutdown signal (Ctrl+C or SIGTERM).
async fn shutdown_signal(shutdown_token: CancellationToken) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received, starting graceful shutdown...");

    // Cancel the shutdown token to signal workers
    shutdown_token.cancel();

    // Give workers time to finish current jobs
    tokio::time::sleep(Duration::from_secs(2)).await;
}
