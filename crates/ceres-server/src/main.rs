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

use ceres_client::EmbeddingProviderEnum;
use ceres_core::config::EmbeddingProviderType;
use ceres_core::traits::EmbeddingProvider;
use ceres_core::{
    TracingReporter, TracingWorkerReporter, WorkerConfig, WorkerService, load_portals_config,
};
use ceres_db::DatasetRepository;

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

    // Initialize embedding provider based on configuration
    let embedding_client = create_embedding_provider(&config)?;

    // Validate embedding dimension matches database configuration
    let repo = DatasetRepository::new(pool.clone());
    repo.validate_embedding_dimension(embedding_client.dimension())
        .await
        .context("Embedding provider validation failed")?;

    info!(
        "Using {} embedding provider ({} dimensions)",
        embedding_client.name(),
        embedding_client.dimension()
    );

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
    let app_state = AppState::new(
        pool,
        embedding_client,
        portals_config,
        shutdown_token.clone(),
    );

    // Build router with rate limiting and CORS configuration
    let app = create_router(app_state.clone(), &config);

    // Start background worker for processing harvest jobs
    let worker_shutdown = shutdown_token.clone();
    let worker_handle = {
        let worker = WorkerService::new(
            app_state.job_repo.clone(),
            app_state.harvest_service.clone(),
            WorkerConfig::default(),
        );
        tokio::spawn(async move {
            info!("Starting background worker for harvest jobs");
            if let Err(e) = worker
                .run(worker_shutdown, &TracingWorkerReporter, &TracingReporter)
                .await
            {
                tracing::error!("Worker error: {}", e);
            }
            info!("Background worker stopped");
        })
    };

    // Bind to address
    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .context("Invalid address")?;

    let listener = TcpListener::bind(addr)
        .await
        .context("Failed to bind to address")?;

    info!("Starting Ceres API server on http://{}", addr);
    info!("Swagger UI available at http://{}/swagger-ui", addr);
    info!(
        "Rate limiting: {} req/s, burst size {}",
        config.rate_limit_rps, config.rate_limit_burst
    );

    // Start server with graceful shutdown
    // Use into_make_service_with_connect_info to enable peer IP extraction for rate limiting
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal(shutdown_token))
    .await
    .context("Server error")?;

    // Wait for worker to finish
    let _ = worker_handle.await;

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

    // Cancel the shutdown token to signal the background worker
    shutdown_token.cancel();

    // Allow time for in-flight HTTP requests and worker jobs to complete
    tokio::time::sleep(Duration::from_secs(2)).await;
}

/// Creates an embedding provider based on server configuration.
fn create_embedding_provider(config: &ServerConfig) -> anyhow::Result<EmbeddingProviderEnum> {
    let provider_type: EmbeddingProviderType = config
        .embedding_provider
        .parse()
        .context("Invalid embedding provider")?;

    match provider_type {
        EmbeddingProviderType::Gemini => {
            let api_key = config.gemini_api_key.as_ref().ok_or_else(|| {
                anyhow::anyhow!("GEMINI_API_KEY required when using gemini provider")
            })?;
            EmbeddingProviderEnum::gemini(api_key).context("Failed to initialize Gemini client")
        }
        EmbeddingProviderType::OpenAI => {
            let api_key = config.openai_api_key.as_ref().ok_or_else(|| {
                anyhow::anyhow!("OPENAI_API_KEY required when using openai provider")
            })?;

            if let Some(model) = &config.embedding_model {
                EmbeddingProviderEnum::openai_with_model(api_key, model)
                    .context("Failed to initialize OpenAI client")
            } else {
                EmbeddingProviderEnum::openai(api_key).context("Failed to initialize OpenAI client")
            }
        }
    }
}
