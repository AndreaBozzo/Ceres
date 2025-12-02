use ceres_core::error::AppError;
use ceres_core::models::{DatabaseStats, Dataset, NewDataset, SearchResult};
use chrono::{DateTime, Utc};
use pgvector::Vector;
use sqlx::types::Json;
use sqlx::{PgPool, Pool, Postgres};
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
                last_updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
            ON CONFLICT (source_portal, original_id) 
            DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                url = EXCLUDED.url,
                embedding = EXCLUDED.embedding,
                metadata = EXCLUDED.metadata,
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
        .fetch_one(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?;

        Ok(rec.0)
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
                last_updated_at
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
                },
                similarity_score: row.similarity_score,
            })
            .collect())
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
    similarity_score: f32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_new_dataset_structure() {
        let new_dataset = NewDataset {
            original_id: "test-id".to_string(),
            source_portal: "https://example.com".to_string(),
            url: "https://example.com/dataset/test".to_string(),
            title: "Test Dataset".to_string(),
            description: Some("Test description".to_string()),
            embedding: Some(Vector::from(vec![0.1, 0.2, 0.3])),
            metadata: json!({"key": "value"}),
        };

        assert_eq!(new_dataset.original_id, "test-id");
        assert_eq!(new_dataset.title, "Test Dataset");
        assert!(new_dataset.embedding.is_some());
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
