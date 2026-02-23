//! Test infrastructure for server integration tests.
//!
//! Provides `TestApp` which spins up an isolated PostgreSQL container,
//! runs all migrations, and builds the Axum router for handler testing.

use std::net::SocketAddr;

use axum::Router;
use axum::extract::ConnectInfo;
use ceres_client::EmbeddingProviderEnum;
use ceres_server::config::ServerConfig;
use ceres_server::create_router;
use ceres_server::state::AppState;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio_util::sync::CancellationToken;

/// All SQL migrations needed for a complete server schema.
const MIGRATIONS: &[&str] = &[
    // 1. Core tables
    "CREATE EXTENSION IF NOT EXISTS vector",
    r#"CREATE TABLE IF NOT EXISTS datasets (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        original_id VARCHAR NOT NULL,
        source_portal VARCHAR NOT NULL,
        url VARCHAR NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        embedding vector(768),
        metadata JSONB DEFAULT '{}'::jsonb,
        first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        content_hash VARCHAR(64),
        CONSTRAINT uk_portal_original_id UNIQUE (source_portal, original_id)
    )"#,
    "CREATE INDEX IF NOT EXISTS idx_datasets_embedding ON datasets USING hnsw (embedding vector_cosine_ops)",
    "CREATE INDEX IF NOT EXISTS idx_datasets_portal_hash ON datasets(source_portal) INCLUDE (original_id, content_hash)",
    // 2. Portal sync status
    r#"CREATE TABLE IF NOT EXISTS portal_sync_status (
        portal_url VARCHAR PRIMARY KEY,
        last_successful_sync TIMESTAMPTZ,
        last_sync_mode VARCHAR(20),
        datasets_synced INTEGER DEFAULT 0,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )"#,
    "ALTER TABLE portal_sync_status ADD COLUMN IF NOT EXISTS sync_status VARCHAR(20) DEFAULT 'completed'",
    // 3. Harvest jobs
    r#"CREATE TABLE IF NOT EXISTS harvest_jobs (
        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        portal_url VARCHAR NOT NULL,
        portal_name VARCHAR,
        status VARCHAR(20) NOT NULL DEFAULT 'pending',
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        started_at TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        retry_count INTEGER NOT NULL DEFAULT 0,
        max_retries INTEGER NOT NULL DEFAULT 3,
        next_retry_at TIMESTAMPTZ,
        error_message TEXT,
        sync_stats JSONB,
        worker_id VARCHAR(255),
        force_full_sync BOOLEAN NOT NULL DEFAULT FALSE,
        url_template TEXT,
        language TEXT,
        CONSTRAINT chk_harvest_jobs_status CHECK (
            status IN ('pending', 'running', 'completed', 'failed', 'cancelled')
        )
    )"#,
    "CREATE INDEX IF NOT EXISTS idx_harvest_jobs_pending ON harvest_jobs(created_at) WHERE status = 'pending'",
    "CREATE INDEX IF NOT EXISTS idx_harvest_jobs_status ON harvest_jobs(status, created_at DESC)",
    // 4. Embedding config
    r#"CREATE TABLE IF NOT EXISTS embedding_config (
        id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
        provider_name VARCHAR(50) NOT NULL,
        model_name VARCHAR(100) NOT NULL,
        dimension INTEGER NOT NULL CHECK (dimension > 0),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )"#,
];

/// Test application wrapping router + DB pool + container lifecycle.
pub struct TestApp {
    pub router: Router,
    #[allow(dead_code)]
    pub pool: PgPool,
    // Keep container alive for the duration of the test
    #[allow(dead_code)]
    _container: ContainerAsync<GenericImage>,
}

impl TestApp {
    /// Create a test app with no portals config and no admin token.
    pub async fn new() -> Self {
        Self::build(None, None).await
    }

    /// Create a test app with an admin token configured.
    pub async fn with_admin_token(token: &str) -> Self {
        Self::build(None, Some(token.to_string())).await
    }

    async fn build(
        portals_config: Option<ceres_core::PortalsConfig>,
        admin_token: Option<String>,
    ) -> Self {
        let (pool, container) = setup_test_db().await;

        let embedding = EmbeddingProviderEnum::mock();
        let state = AppState::new(
            pool.clone(),
            embedding,
            portals_config,
            CancellationToken::new(),
            admin_token,
        );

        // Build a ServerConfig with permissive rate limits for tests
        let config = ServerConfig {
            database_url: "unused".to_string(),
            embedding_provider: "mock".to_string(),
            gemini_api_key: None,
            openai_api_key: None,
            embedding_model: None,
            port: 3000,
            host: "127.0.0.1".to_string(),
            portals_config: None,
            cors_origins: "*".to_string(),
            rate_limit_rps: 1000,
            rate_limit_burst: 2000,
            admin_token: None,
        };

        // Wrap with a layer that injects ConnectInfo<SocketAddr> into request
        // extensions. tower_governor's PeerIpKeyExtractor reads this directly
        // from extensions (not via axum's extractor), so MockConnectInfo won't
        // work — we need the real ConnectInfo type in extensions.
        let router = create_router(state, &config).layer(axum::middleware::from_fn(
            |mut req: axum::http::Request<axum::body::Body>, next: axum::middleware::Next| async {
                req.extensions_mut()
                    .insert(ConnectInfo(SocketAddr::from(([127, 0, 0, 1], 0))));
                next.run(req).await
            },
        ));

        Self {
            router,
            pool,
            _container: container,
        }
    }
}

/// Spin up an isolated PostgreSQL container with pgvector and run all migrations.
async fn setup_test_db() -> (PgPool, ContainerAsync<GenericImage>) {
    let container = GenericImage::new("pgvector/pgvector", "pg16")
        .with_exposed_port(ContainerPort::Tcp(5432))
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_PASSWORD", "postgres")
        .with_env_var("POSTGRES_DB", "postgres")
        .start()
        .await
        .expect("Failed to start PostgreSQL container");

    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get port");

    let connection_string = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

    const MAX_RETRIES: u32 = 30;
    let mut retries = 0;
    let pool = loop {
        match PgPoolOptions::new()
            .max_connections(5)
            .connect(&connection_string)
            .await
        {
            Ok(pool) => break pool,
            Err(e) => {
                retries += 1;
                if retries >= MAX_RETRIES {
                    panic!(
                        "Failed to connect to database after {} retries: {}",
                        MAX_RETRIES, e
                    );
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    };

    for migration in MIGRATIONS {
        sqlx::query(migration)
            .execute(&pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to run migration: {}\nSQL: {}", e, migration));
    }

    (pool, container)
}
