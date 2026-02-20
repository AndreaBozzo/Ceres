//! Test utilities for integration tests.
//!
//! Provides helper functions to set up isolated PostgreSQL containers
//! with pgvector extension for each test.

use ceres_core::models::NewDataset;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

/// SQL migrations to initialize the test database schema.
/// Each statement must be executed separately due to sqlx limitations.
const MIGRATIONS: &[&str] = &[
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
];

/// Sets up a PostgreSQL container with pgvector and returns a connection pool.
///
/// Uses the official pgvector/pgvector:pg16 image which includes the vector extension.
/// Each call creates a fresh, isolated database container. The container is
/// automatically cleaned up when the returned `ContainerAsync` is dropped.
///
/// # Returns
///
/// A tuple of (PgPool, ContainerAsync) - keep the container alive for the test duration.
pub async fn setup_test_db() -> (PgPool, ContainerAsync<GenericImage>) {
    // Use pgvector image which includes the vector extension pre-installed
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

    // Get connection details from the container
    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("Failed to get port");

    let connection_string = format!("postgresql://postgres:postgres@{}:{}/postgres", host, port);

    // Create connection pool with retry logic for container startup
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

    // Run migrations (each statement separately)
    for migration in MIGRATIONS {
        sqlx::query(migration)
            .execute(&pool)
            .await
            .expect("Failed to run migration");
    }

    (pool, container)
}

/// Creates a sample NewDataset for testing.
///
/// # Arguments
///
/// * `id` - The original_id for the dataset
/// * `portal` - The source_portal URL
///
/// # Returns
///
/// A NewDataset with sample data and a 768-dimensional embedding.
pub fn sample_new_dataset(id: &str, portal: &str) -> NewDataset {
    let title = format!("Test Dataset {}", id);
    let description = Some(format!("Description for dataset {}", id));
    let content_hash = NewDataset::compute_content_hash(&title, description.as_deref());

    // Create a simple embedding (768 dimensions for Gemini compatibility)
    let embedding_vec: Vec<f32> = (0..768).map(|i| i as f32 / 768.0).collect();

    NewDataset {
        original_id: id.to_string(),
        source_portal: portal.to_string(),
        url: format!("{}/dataset/{}", portal, id),
        title,
        description,
        embedding: Some(embedding_vec),
        metadata: serde_json::json!({
            "test": true,
            "id": id
        }),
        content_hash,
    }
}

/// Creates a random vector of the specified dimensions.
///
/// Useful for testing vector similarity search with varied embeddings.
#[allow(dead_code)]
pub fn random_vector(dims: usize) -> Vec<f32> {
    use rand::Rng;
    let mut rng = rand::rng();
    (0..dims).map(|_| rng.random::<f32>()).collect()
}
