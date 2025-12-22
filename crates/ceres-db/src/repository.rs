use ceres_core::error::AppError;
use ceres_core::models::{DatabaseStats, Dataset, NewDataset, SearchResult};
use chrono::{DateTime, Utc};
use pgvector::Vector;
use sqlx::types::Json;
use sqlx::{PgPool, Pool, Postgres};
use std::collections::HashMap;
use uuid::Uuid;

/// Repository for managing dataset persistence in PostgreSQL with pgvector.
///
/// This repository provides methods to store, update, and retrieve datasets
/// with vector embeddings for semantic search capabilities.
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
    /// Creates a new repository instance with the given database connection pool.
    ///
    /// # Arguments
    ///
    /// * `pool` - A PostgreSQL connection pool
    ///
    /// # Returns
    ///
    /// A new `DatasetRepository` instance.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a new dataset or updates an existing one based on unique constraints.
    ///
    /// This method performs an UPSERT operation using PostgreSQL's `ON CONFLICT`
    /// clause. If a dataset with the same `(source_portal, original_id)` pair
    /// exists, it will be updated. Otherwise, a new record is inserted.
    ///
    /// # Arguments
    ///
    /// * `new_data` - The dataset to insert or update
    ///
    /// # Returns
    ///
    /// The UUID of the inserted or updated dataset.
    ///
    /// # Errors
    ///
    /// Returns `AppError::DatabaseError` if the database operation fails.
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

    /// Retrieves all content hashes for datasets from a specific portal.
    ///
    /// This method is optimized for delta detection during harvesting.
    /// It returns a HashMap for O(1) lookup by original_id.
    ///
    /// # Arguments
    ///
    /// * `portal_url` - The source portal URL to filter by
    ///
    /// # Returns
    ///
    /// A HashMap where:
    /// - Key: `original_id` (String)
    /// - Value: `content_hash` (Option<String>) - None if hash was never computed
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

    /// Updates only the last_updated_at timestamp for a dataset.
    ///
    /// This is an optimization for delta harvesting when content hasn't changed.
    /// Instead of a full upsert, we only touch the timestamp.
    ///
    /// # Arguments
    ///
    /// * `portal_url` - The source portal URL
    /// * `original_id` - The original ID from the portal
    ///
    /// # Returns
    ///
    /// true if a row was updated, false if no matching row found.
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

    /// Retrieves a dataset by its unique identifier.
    ///
    /// # Arguments
    ///
    /// * `id` - The UUID of the dataset to retrieve
    ///
    /// # Returns
    ///
    /// Returns `Some(Dataset)` if found, `None` if no dataset exists with the given ID.
    ///
    /// # Errors
    ///
    /// Returns `AppError::DatabaseError` if the database query fails.
    pub async fn get(&self, id: Uuid) -> Result<Option<Dataset>, AppError> {
        let result = sqlx::query_as::<_, Dataset>(
            r#"
            SELECT
                id,
                original_id,
                source_portal,
                url,
                title,
                description,
                embedding,
                metadata,
                first_seen_at,
                last_updated_at,
                content_hash
            FROM datasets
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?;

        Ok(result)
    }

    /// Semantic search using cosine similarity with pgvector
    ///
    /// Searches for datasets similar to the provided query using cosine distance.
    /// Returns only datasets with embeddings, ordered by similarity (descending).
    ///
    /// # Arguments
    ///
    /// * `query_vector` - The embedding vector of the search query
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// A list of `SearchResult` ordered by similarity score (best matches first).
    ///
    /// # Errors
    ///
    /// Returns `AppError::DatabaseError` if the database query fails.
    pub async fn search(
        &self,
        query_vector: Vector,
        limit: usize,
    ) -> Result<Vec<SearchResult>, AppError> {
        let results = sqlx::query_as::<_, SearchResultRow>(
            r#"
            SELECT
                id,
                original_id,
                source_portal,
                url,
                title,
                description,
                embedding,
                metadata,
                first_seen_at,
                last_updated_at,
                content_hash,
                1 - (embedding <=> $1) as similarity_score
            FROM datasets
            WHERE embedding IS NOT NULL
            ORDER BY embedding <=> $1
            LIMIT $2
            "#,
        )
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

    /// Lists all datasets with optional filtering by portal.
    ///
    /// # Arguments
    ///
    /// * `portal_filter` - Optional portal URL to filter by
    /// * `limit` - Optional maximum number of results
    ///
    /// # Returns
    ///
    /// A vector of all matching datasets.
    ///
    /// # Errors
    ///
    /// Returns `AppError::DatabaseError` if the database query fails.
    pub async fn list_all(
        &self,
        portal_filter: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<Dataset>, AppError> {
        let limit_val = limit.unwrap_or(10000) as i64;

        let datasets = if let Some(portal) = portal_filter {
            sqlx::query_as::<_, Dataset>(
                r#"
                SELECT
                    id,
                    original_id,
                    source_portal,
                    url,
                    title,
                    description,
                    embedding,
                    metadata,
                    first_seen_at,
                    last_updated_at,
                    content_hash
                FROM datasets
                WHERE source_portal = $1
                ORDER BY last_updated_at DESC
                LIMIT $2
                "#,
            )
            .bind(portal)
            .bind(limit_val)
            .fetch_all(&self.pool)
            .await
            .map_err(AppError::DatabaseError)?
        } else {
            sqlx::query_as::<_, Dataset>(
                r#"
                SELECT
                    id,
                    original_id,
                    source_portal,
                    url,
                    title,
                    description,
                    embedding,
                    metadata,
                    first_seen_at,
                    last_updated_at,
                    content_hash
                FROM datasets
                ORDER BY last_updated_at DESC
                LIMIT $1
                "#,
            )
            .bind(limit_val)
            .fetch_all(&self.pool)
            .await
            .map_err(AppError::DatabaseError)?
        };

        Ok(datasets)
    }

    /// Gets aggregated database statistics
    ///
    /// Provides an overview of the current database state, including
    /// total counts and last update information.
    ///
    /// # Returns
    ///
    /// A `DatabaseStats` struct with aggregated statistics.
    ///
    /// # Errors
    ///
    /// Returns `AppError::DatabaseError` if the database query fails.
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
