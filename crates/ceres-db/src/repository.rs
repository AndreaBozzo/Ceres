//! Dataset repository for PostgreSQL with pgvector support.
//!
//! # Testing
//!
//! TODO(#12): Improve test coverage for repository methods
//! Current tests only cover struct/serialization. Integration tests needed for:
//! - `upsert()` - insert and update paths
//! - `search()` - vector similarity queries
//! - `get_hashes_for_portal()` - delta detection queries
//! - `update_timestamp_only()` - timestamp-only updates
//!
//! Consider using testcontainers-rs for isolated PostgreSQL instances:
//! <https://github.com/testcontainers/testcontainers-rs>
//!
//! See: <https://github.com/AndreaBozzo/Ceres/issues/12>

use ceres_core::error::AppError;
use ceres_core::models::{DatabaseStats, Dataset, NewDataset, SearchResult};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use pgvector::Vector;
use sqlx::types::Json;
use sqlx::{PgPool, Pool, Postgres};
use std::collections::HashMap;
use uuid::Uuid;

/// Column list for SELECT queries. Must remain a const literal to ensure SQL safety
/// since format!() bypasses sqlx compile-time validation.
const DATASET_COLUMNS: &str = "id, original_id, source_portal, url, title, description, embedding, metadata, first_seen_at, last_updated_at, content_hash";

// Static queries for list_all_stream to avoid lifetime issues with BoxStream
const LIST_ALL_QUERY: &str = "SELECT id, original_id, source_portal, url, title, description, embedding, metadata, first_seen_at, last_updated_at, content_hash FROM datasets ORDER BY last_updated_at DESC";
const LIST_ALL_LIMIT_QUERY: &str = "SELECT id, original_id, source_portal, url, title, description, embedding, metadata, first_seen_at, last_updated_at, content_hash FROM datasets ORDER BY last_updated_at DESC LIMIT $1";
const LIST_ALL_PORTAL_QUERY: &str = "SELECT id, original_id, source_portal, url, title, description, embedding, metadata, first_seen_at, last_updated_at, content_hash FROM datasets WHERE source_portal = $1 ORDER BY last_updated_at DESC";
const LIST_ALL_PORTAL_LIMIT_QUERY: &str = "SELECT id, original_id, source_portal, url, title, description, embedding, metadata, first_seen_at, last_updated_at, content_hash FROM datasets WHERE source_portal = $1 ORDER BY last_updated_at DESC LIMIT $2";

/// Repository for dataset persistence in PostgreSQL with pgvector.
///
/// # Examples
///
/// ```no_run
/// use sqlx::postgres::PgPoolOptions;
/// use ceres_db::DatasetRepository;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let pool = PgPoolOptions::new()
///     .max_connections(5)
///     .connect("postgresql://localhost/ceres")
///     .await?;
///
/// let repo = DatasetRepository::new(pool);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct DatasetRepository {
    pool: Pool<Postgres>,
}

impl DatasetRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts or updates a dataset. Returns the UUID of the affected row.
    ///
    /// TODO(robustness): Return UpsertOutcome to distinguish insert vs update
    /// Currently returns only UUID without indicating operation type.
    /// Consider: `pub enum UpsertOutcome { Created(Uuid), Updated(Uuid) }`
    /// This enables accurate progress reporting in sync statistics.
    pub async fn upsert(&self, new_data: &NewDataset) -> Result<Uuid, AppError> {
        let embedding_vector = new_data.embedding.as_ref().cloned();

        let rec: (Uuid,) = sqlx::query_as(
            r#"
            INSERT INTO datasets (
                original_id,
                source_portal,
                url,
                title,
                description,
                embedding,
                metadata,
                content_hash,
                last_updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
            ON CONFLICT (source_portal, original_id)
            DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                url = EXCLUDED.url,
                embedding = COALESCE(EXCLUDED.embedding, datasets.embedding),
                metadata = EXCLUDED.metadata,
                content_hash = EXCLUDED.content_hash,
                last_updated_at = NOW()
            RETURNING id
            "#,
        )
        .bind(&new_data.original_id)
        .bind(&new_data.source_portal)
        .bind(&new_data.url)
        .bind(&new_data.title)
        .bind(&new_data.description)
        .bind(embedding_vector)
        .bind(serde_json::to_value(&new_data.metadata).unwrap_or(serde_json::json!({})))
        .bind(&new_data.content_hash)
        .fetch_one(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?;

        Ok(rec.0)
    }

    /// Returns a map of original_id → content_hash for all datasets from a portal.
    ///
    /// TODO(performance): Optimize for large portals (100k+ datasets)
    /// Currently loads entire HashMap into memory. Consider:
    /// (1) Streaming hash comparison during sync, or
    /// (2) Database-side hash check with WHERE clause, or
    /// (3) Bloom filter for approximate membership testing
    pub async fn get_hashes_for_portal(
        &self,
        portal_url: &str,
    ) -> Result<HashMap<String, Option<String>>, AppError> {
        let rows: Vec<HashRow> = sqlx::query_as(
            r#"
            SELECT original_id, content_hash
            FROM datasets
            WHERE source_portal = $1
            "#,
        )
        .bind(portal_url)
        .fetch_all(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?;

        let hash_map: HashMap<String, Option<String>> = rows
            .into_iter()
            .map(|row| (row.original_id, row.content_hash))
            .collect();

        Ok(hash_map)
    }

    /// Updates only the timestamp for unchanged datasets. Returns true if a row was updated.
    pub async fn update_timestamp_only(
        &self,
        portal_url: &str,
        original_id: &str,
    ) -> Result<bool, AppError> {
        let result = sqlx::query(
            r#"
            UPDATE datasets
            SET last_updated_at = NOW()
            WHERE source_portal = $1 AND original_id = $2
            "#,
        )
        .bind(portal_url)
        .bind(original_id)
        .execute(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?;

        Ok(result.rows_affected() > 0)
    }

    /// Batch updates timestamps for multiple unchanged datasets.
    ///
    /// Uses a single UPDATE with ANY() for efficiency instead of N individual updates.
    /// Returns the number of rows actually updated.
    pub async fn batch_update_timestamps(
        &self,
        portal_url: &str,
        original_ids: &[String],
    ) -> Result<u64, AppError> {
        if original_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            UPDATE datasets
            SET last_updated_at = NOW()
            WHERE source_portal = $1 AND original_id = ANY($2)
            "#,
        )
        .bind(portal_url)
        .bind(original_ids)
        .execute(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?;

        Ok(result.rows_affected())
    }

    /// Retrieves a dataset by UUID.
    pub async fn get(&self, id: Uuid) -> Result<Option<Dataset>, AppError> {
        let query = format!("SELECT {} FROM datasets WHERE id = $1", DATASET_COLUMNS);
        let result = sqlx::query_as::<_, Dataset>(&query)
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(AppError::DatabaseError)?;

        Ok(result)
    }

    /// Semantic search using cosine similarity. Returns results ordered by similarity.
    pub async fn search(
        &self,
        query_vector: Vector,
        limit: usize,
    ) -> Result<Vec<SearchResult>, AppError> {
        let query = format!(
            "SELECT {}, 1 - (embedding <=> $1) as similarity_score FROM datasets WHERE embedding IS NOT NULL ORDER BY embedding <=> $1 LIMIT $2",
            DATASET_COLUMNS
        );
        let results = sqlx::query_as::<_, SearchResultRow>(&query)
            .bind(query_vector)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(AppError::DatabaseError)?;

        Ok(results
            .into_iter()
            .map(|row| SearchResult {
                dataset: Dataset {
                    id: row.id,
                    original_id: row.original_id,
                    source_portal: row.source_portal,
                    url: row.url,
                    title: row.title,
                    description: row.description,
                    embedding: row.embedding,
                    metadata: row.metadata,
                    first_seen_at: row.first_seen_at,
                    last_updated_at: row.last_updated_at,
                    content_hash: row.content_hash,
                },
                similarity_score: row.similarity_score as f32,
            })
            .collect())
    }

    /// Lists datasets with optional portal filter and limit.
    ///
    /// For memory-efficient exports of large datasets, use [`list_all_stream`] instead.
    ///
    /// TODO(config): Make default limit configurable via DEFAULT_EXPORT_LIMIT env var
    /// Currently hardcoded to 10000.
    pub async fn list_all(
        &self,
        portal_filter: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<Dataset>, AppError> {
        // TODO(config): Read default from DEFAULT_EXPORT_LIMIT env var
        let limit_val = limit.unwrap_or(10000) as i64;

        let datasets = if let Some(portal) = portal_filter {
            let query = format!(
                "SELECT {} FROM datasets WHERE source_portal = $1 ORDER BY last_updated_at DESC LIMIT $2",
                DATASET_COLUMNS
            );
            sqlx::query_as::<_, Dataset>(&query)
                .bind(portal)
                .bind(limit_val)
                .fetch_all(&self.pool)
                .await
                .map_err(AppError::DatabaseError)?
        } else {
            let query = format!(
                "SELECT {} FROM datasets ORDER BY last_updated_at DESC LIMIT $1",
                DATASET_COLUMNS
            );
            sqlx::query_as::<_, Dataset>(&query)
                .bind(limit_val)
                .fetch_all(&self.pool)
                .await
                .map_err(AppError::DatabaseError)?
        };

        Ok(datasets)
    }

    /// Lists datasets as a stream with optional portal filter.
    ///
    /// Unlike [`list_all`], this method streams results directly from the database
    /// without loading everything into memory. Suitable for large exports.
    ///
    /// # Arguments
    ///
    /// * `portal_filter` - Optional portal URL to filter by
    /// * `limit` - Optional maximum number of records (no default limit for streaming)
    pub fn list_all_stream<'a>(
        &'a self,
        portal_filter: Option<&'a str>,
        limit: Option<usize>,
    ) -> BoxStream<'a, Result<Dataset, AppError>> {
        match (portal_filter, limit) {
            (Some(portal), Some(lim)) => Box::pin(
                sqlx::query_as::<_, Dataset>(LIST_ALL_PORTAL_LIMIT_QUERY)
                    .bind(portal)
                    .bind(lim as i64)
                    .fetch(&self.pool)
                    .map(|r| r.map_err(AppError::DatabaseError)),
            ),
            (Some(portal), None) => Box::pin(
                sqlx::query_as::<_, Dataset>(LIST_ALL_PORTAL_QUERY)
                    .bind(portal)
                    .fetch(&self.pool)
                    .map(|r| r.map_err(AppError::DatabaseError)),
            ),
            (None, Some(lim)) => Box::pin(
                sqlx::query_as::<_, Dataset>(LIST_ALL_LIMIT_QUERY)
                    .bind(lim as i64)
                    .fetch(&self.pool)
                    .map(|r| r.map_err(AppError::DatabaseError)),
            ),
            (None, None) => Box::pin(
                sqlx::query_as::<_, Dataset>(LIST_ALL_QUERY)
                    .fetch(&self.pool)
                    .map(|r| r.map_err(AppError::DatabaseError)),
            ),
        }
    }

    // =========================================================================
    // Portal Sync Status Methods (for incremental harvesting)
    // =========================================================================

    /// Retrieves the sync status for a portal.
    /// Returns None if this portal has never been synced.
    pub async fn get_sync_status(
        &self,
        portal_url: &str,
    ) -> Result<Option<PortalSyncStatus>, AppError> {
        let result = sqlx::query_as::<_, PortalSyncStatus>(
            r#"
            SELECT portal_url, last_successful_sync, last_sync_mode, sync_status, datasets_synced, created_at, updated_at
            FROM portal_sync_status
            WHERE portal_url = $1
            "#,
        )
        .bind(portal_url)
        .fetch_optional(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?;

        Ok(result)
    }

    /// Updates or inserts the sync status for a portal.
    ///
    /// The `sync_status` parameter indicates the outcome: "completed" or "cancelled".
    /// Only updates `last_successful_sync` when status is "completed", preserving
    /// the last successful sync time for incremental harvesting after cancellations.
    pub async fn upsert_sync_status(
        &self,
        portal_url: &str,
        last_sync: DateTime<Utc>,
        sync_mode: &str,
        sync_status: &str,
        datasets_synced: i32,
    ) -> Result<(), AppError> {
        sqlx::query(
            r#"
            INSERT INTO portal_sync_status (portal_url, last_successful_sync, last_sync_mode, sync_status, datasets_synced, updated_at)
            VALUES (
                $1,
                CASE WHEN $4 = 'completed' THEN $2 ELSE NULL END,
                $3,
                $4,
                $5,
                NOW()
            )
            ON CONFLICT (portal_url)
            DO UPDATE SET
                last_successful_sync = CASE
                    WHEN EXCLUDED.sync_status = 'completed' THEN $2
                    ELSE portal_sync_status.last_successful_sync
                END,
                last_sync_mode = EXCLUDED.last_sync_mode,
                sync_status = EXCLUDED.sync_status,
                datasets_synced = EXCLUDED.datasets_synced,
                updated_at = NOW()
            "#,
        )
        .bind(portal_url)
        .bind(last_sync)
        .bind(sync_mode)
        .bind(sync_status)
        .bind(datasets_synced)
        .execute(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?;

        Ok(())
    }

    /// Checks database connectivity by executing a simple query.
    pub async fn health_check(&self) -> Result<(), AppError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(AppError::DatabaseError)?;
        Ok(())
    }

    // =========================================================================
    // Embedding Configuration Methods
    // =========================================================================

    /// Retrieves the current embedding provider configuration from the database.
    ///
    /// Returns None if no configuration exists (fresh database).
    pub async fn get_embedding_config(&self) -> Result<Option<EmbeddingConfigRow>, AppError> {
        let result = sqlx::query_as::<_, EmbeddingConfigRow>(
            r#"
            SELECT provider_name, model_name, dimension
            FROM embedding_config
            WHERE id = 1
            "#,
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?;

        Ok(result)
    }

    /// Validates that the embedding provider's dimension matches the database configuration.
    ///
    /// This should be called at startup to fail fast if there's a mismatch.
    /// Returns Ok(()) if dimensions match or if no config exists (first run).
    ///
    /// # Arguments
    ///
    /// * `provider_dimension` - The dimension from the embedding provider
    ///
    /// # Errors
    ///
    /// Returns `AppError::ConfigError` if dimensions don't match.
    pub async fn validate_embedding_dimension(
        &self,
        provider_dimension: usize,
    ) -> Result<(), AppError> {
        if let Some(config) = self.get_embedding_config().await? {
            if config.dimension as usize != provider_dimension {
                return Err(AppError::ConfigError(format!(
                    "Embedding dimension mismatch: database configured for {} dimensions ({}), \
                     but provider generates {} dimensions. \
                     To switch providers, clear existing embeddings and update the embedding_config table.",
                    config.dimension, config.provider_name, provider_dimension
                )));
            }
        }
        Ok(())
    }

    /// Updates the embedding provider configuration in the database.
    ///
    /// WARNING: This should only be called after clearing existing embeddings
    /// and altering the vector column dimension.
    pub async fn update_embedding_config(
        &self,
        provider_name: &str,
        model_name: &str,
        dimension: usize,
    ) -> Result<(), AppError> {
        sqlx::query(
            r#"
            INSERT INTO embedding_config (id, provider_name, model_name, dimension, updated_at)
            VALUES (1, $1, $2, $3, NOW())
            ON CONFLICT (id) DO UPDATE SET
                provider_name = EXCLUDED.provider_name,
                model_name = EXCLUDED.model_name,
                dimension = EXCLUDED.dimension,
                updated_at = NOW()
            "#,
        )
        .bind(provider_name)
        .bind(model_name)
        .bind(dimension as i32)
        .execute(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?;

        Ok(())
    }

    /// Returns aggregated database statistics.
    pub async fn get_stats(&self) -> Result<DatabaseStats, AppError> {
        let row: StatsRow = sqlx::query_as(
            r#"
            SELECT
                COUNT(*) as total,
                COUNT(embedding) as with_embeddings,
                COUNT(DISTINCT source_portal) as portals,
                MAX(last_updated_at) as last_update
            FROM datasets
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?;

        Ok(DatabaseStats {
            total_datasets: row.total.unwrap_or(0),
            datasets_with_embeddings: row.with_embeddings.unwrap_or(0),
            total_portals: row.portals.unwrap_or(0),
            last_update: row.last_update,
        })
    }
}

/// Helper struct for deserializing stats query results
#[derive(sqlx::FromRow)]
struct StatsRow {
    total: Option<i64>,
    with_embeddings: Option<i64>,
    portals: Option<i64>,
    last_update: Option<DateTime<Utc>>,
}

/// Helper struct for deserializing search query results
#[derive(sqlx::FromRow)]
struct SearchResultRow {
    id: Uuid,
    original_id: String,
    source_portal: String,
    url: String,
    title: String,
    description: Option<String>,
    embedding: Option<Vector>,
    metadata: Json<serde_json::Value>,
    first_seen_at: DateTime<Utc>,
    last_updated_at: DateTime<Utc>,
    content_hash: Option<String>,
    similarity_score: f64,
}

/// Helper struct for deserializing hash lookup query results
#[derive(sqlx::FromRow)]
struct HashRow {
    original_id: String,
    content_hash: Option<String>,
}

/// Represents the sync status for a portal, used for incremental harvesting.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PortalSyncStatus {
    pub portal_url: String,
    pub last_successful_sync: Option<DateTime<Utc>>,
    pub last_sync_mode: Option<String>,
    pub sync_status: Option<String>,
    pub datasets_synced: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Represents the embedding provider configuration stored in the database.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct EmbeddingConfigRow {
    pub provider_name: String,
    pub model_name: String,
    pub dimension: i32,
}

// =============================================================================
// Trait Implementation: DatasetStore
// =============================================================================

impl ceres_core::traits::DatasetStore for DatasetRepository {
    async fn get_by_id(&self, id: Uuid) -> Result<Option<Dataset>, AppError> {
        DatasetRepository::get(self, id).await
    }

    async fn get_hashes_for_portal(
        &self,
        portal_url: &str,
    ) -> Result<HashMap<String, Option<String>>, AppError> {
        DatasetRepository::get_hashes_for_portal(self, portal_url).await
    }

    async fn update_timestamp_only(
        &self,
        portal_url: &str,
        original_id: &str,
    ) -> Result<(), AppError> {
        DatasetRepository::update_timestamp_only(self, portal_url, original_id).await?;
        Ok(())
    }

    async fn batch_update_timestamps(
        &self,
        portal_url: &str,
        original_ids: &[String],
    ) -> Result<u64, AppError> {
        DatasetRepository::batch_update_timestamps(self, portal_url, original_ids).await
    }

    async fn upsert(&self, dataset: &NewDataset) -> Result<Uuid, AppError> {
        DatasetRepository::upsert(self, dataset).await
    }

    async fn search(
        &self,
        query_vector: Vector,
        limit: usize,
    ) -> Result<Vec<SearchResult>, AppError> {
        DatasetRepository::search(self, query_vector, limit).await
    }

    fn list_stream<'a>(
        &'a self,
        portal_filter: Option<&'a str>,
        limit: Option<usize>,
    ) -> BoxStream<'a, Result<Dataset, AppError>> {
        DatasetRepository::list_all_stream(self, portal_filter, limit)
    }

    async fn get_last_sync_time(
        &self,
        portal_url: &str,
    ) -> Result<Option<DateTime<Utc>>, AppError> {
        let status = DatasetRepository::get_sync_status(self, portal_url).await?;
        Ok(status.and_then(|s| s.last_successful_sync))
    }

    async fn record_sync_status(
        &self,
        portal_url: &str,
        sync_time: DateTime<Utc>,
        sync_mode: &str,
        sync_status: &str,
        datasets_synced: i32,
    ) -> Result<(), AppError> {
        DatasetRepository::upsert_sync_status(
            self,
            portal_url,
            sync_time,
            sync_mode,
            sync_status,
            datasets_synced,
        )
        .await
    }

    async fn health_check(&self) -> Result<(), AppError> {
        DatasetRepository::health_check(self).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_new_dataset_structure() {
        let title = "Test Dataset";
        let description = Some("Test description".to_string());
        let content_hash = NewDataset::compute_content_hash(title, description.as_deref());

        let new_dataset = NewDataset {
            original_id: "test-id".to_string(),
            source_portal: "https://example.com".to_string(),
            url: "https://example.com/dataset/test".to_string(),
            title: title.to_string(),
            description,
            embedding: Some(Vector::from(vec![0.1, 0.2, 0.3])),
            metadata: json!({"key": "value"}),
            content_hash,
        };

        assert_eq!(new_dataset.original_id, "test-id");
        assert_eq!(new_dataset.title, "Test Dataset");
        assert!(new_dataset.embedding.is_some());
        assert_eq!(new_dataset.content_hash.len(), 64);
    }

    #[test]
    fn test_embedding_vector_conversion() {
        let vec_f32 = vec![0.1_f32, 0.2, 0.3, 0.4];
        let vector = Vector::from(vec_f32.clone());
        assert_eq!(vector.as_slice().len(), vec_f32.len());
    }

    #[test]
    fn test_metadata_serialization() {
        let metadata = json!({
            "organization": "test-org",
            "tags": ["tag1", "tag2"]
        });

        let serialized = serde_json::to_value(&metadata).unwrap();
        assert!(serialized.is_object());
        assert_eq!(serialized["organization"], "test-org");
    }
}
