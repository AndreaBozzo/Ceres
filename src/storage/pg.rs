use crate::error::AppError;
use crate::models::{Dataset, NewDataset};
use pgvector::Vector;
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
/// use ceres::storage::DatasetRepository;
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use sqlx::postgres::PgPoolOptions;
    /// use ceres::storage::DatasetRepository;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let pool = PgPoolOptions::new().connect("postgresql://localhost/ceres").await?;
    /// let repo = DatasetRepository::new(pool);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Inserts a new dataset or updates an existing one based on unique constraints.
    ///
    /// This method performs an UPSERT operation using PostgreSQL's `ON CONFLICT`
    /// clause. If a dataset with the same `(source_portal, original_id)` pair
    /// exists, it will be updated. Otherwise, a new record is inserted.
    ///
    /// The `last_updated_at` timestamp is automatically set to the current time
    /// on both insert and update operations.
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ceres::models::NewDataset;
    /// use ceres::storage::DatasetRepository;
    /// # use sqlx::PgPool;
    ///
    /// # async fn example(repo: DatasetRepository) -> Result<(), Box<dyn std::error::Error>> {
    /// let dataset = NewDataset {
    ///     original_id: "dataset-123".to_string(),
    ///     source_portal: "https://dati.gov.it".to_string(),
    ///     url: "https://dati.gov.it/dataset/my-dataset".to_string(),
    ///     title: "My Dataset".to_string(),
    ///     description: Some("A test dataset".to_string()),
    ///     embedding: None,
    ///     metadata: serde_json::json!({}),
    /// };
    ///
    /// let uuid = repo.upsert(&dataset).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn upsert(&self, new_data: &NewDataset) -> Result<Uuid, AppError> {
        // Convertiamo il Vec<f32> in pgvector::Vector se presente
        let embedding_vector = new_data.embedding.as_ref().cloned();

        let rec = sqlx::query!(
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
            new_data.original_id,
            new_data.source_portal,
            new_data.url,
            new_data.title,
            new_data.description,
            embedding_vector as Option<Vector>, // Casting esplicito per sqlx
            serde_json::to_value(&new_data.metadata).unwrap_or(serde_json::json!({}))
        )
        .fetch_one(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?; // Mappa l'errore SQLx nel tuo AppError

        Ok(rec.id)
    }

    /// Retrieves a dataset by its unique identifier.
    ///
    /// This method fetches a complete dataset record including all metadata,
    /// embedding vector, and timestamps.
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use uuid::Uuid;
    /// use ceres::storage::DatasetRepository;
    /// # use sqlx::PgPool;
    ///
    /// # async fn example(repo: DatasetRepository) -> Result<(), Box<dyn std::error::Error>> {
    /// let id = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000")?;
    /// if let Some(dataset) = repo.get(id).await? {
    ///     println!("Found dataset: {}", dataset.title);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get(&self, id: Uuid) -> Result<Option<Dataset>, AppError> {
        let result = sqlx::query_as!(
            Dataset,
            r#"
            SELECT
                id,
                original_id,
                source_portal,
                url,
                title,
                description,
                embedding as "embedding: _",
                metadata as "metadata!: _",
                first_seen_at,
                last_updated_at
            FROM datasets
            WHERE id = $1
            "#,
            id
        )
        .fetch_optional(&self.pool)
        .await
        .map_err(AppError::DatabaseError)?;

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Note: These are unit tests for the repository structure.
    // Full integration tests with actual database would use #[sqlx::test]
    // and require a running PostgreSQL instance with pgvector extension.

    #[test]
    fn test_new_repository() {
        // We can't create an actual pool without a database connection,
        // but we can verify the struct is properly defined
        assert_eq!(
            std::mem::size_of::<DatasetRepository>(),
            std::mem::size_of::<PgPool>()
        );
    }

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

        // Verify conversion works
        assert_eq!(vector.as_slice().len(), vec_f32.len());
    }

    #[test]
    fn test_metadata_serialization() {
        let metadata = json!({
            "organization": "test-org",
            "tags": ["tag1", "tag2"],
            "resources": [{"name": "file.csv", "format": "CSV"}]
        });

        // Verify metadata can be serialized/deserialized
        let serialized = serde_json::to_value(&metadata).unwrap();
        assert!(serialized.is_object());
        assert_eq!(serialized["organization"], "test-org");
    }

    // Integration tests would go in a separate file: tests/storage_integration.rs
    // Example structure:
    //
    // #[sqlx::test]
    // async fn test_upsert_new_dataset(pool: PgPool) -> sqlx::Result<()> {
    //     let repo = DatasetRepository::new(pool);
    //     let new_dataset = NewDataset { ... };
    //     let uuid = repo.upsert(&new_dataset).await?;
    //     assert!(uuid != Uuid::nil());
    //     Ok(())
    // }
}
