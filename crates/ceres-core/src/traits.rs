//! Trait definitions for external dependencies.
//!
//! This module defines traits that abstract over external dependencies
//! (embedding providers, portal clients, data stores), enabling:
//!
//! - **Testability**: Mock implementations for unit testing
//! - **Flexibility**: Different backend implementations (e.g., different embedding APIs)
//! - **Decoupling**: Core business logic doesn't depend on specific implementations
//!
//! # Example
//!
//! ```
//! use ceres_core::traits::{EmbeddingProvider, DatasetStore};
//! use pgvector::Vector;
//!
//! // Business logic uses traits, not concrete types
//! async fn search_datasets<E, S>(
//!     embedding: &E,
//!     store: &S,
//!     query: &str,
//! ) -> Result<Vec<ceres_core::SearchResult>, ceres_core::AppError>
//! where
//!     E: EmbeddingProvider,
//!     S: DatasetStore,
//! {
//!     let vector: Vector = embedding.generate(query).await?.into();
//!     store.search(vector, 10).await
//! }
//! ```

use std::collections::HashMap;
use std::future::Future;

use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use pgvector::Vector;
use uuid::Uuid;

use crate::{AppError, Dataset, NewDataset, SearchResult};

/// Provider for generating text embeddings.
///
/// Implementations convert text into vector representations for semantic search.
pub trait EmbeddingProvider: Send + Sync + Clone {
    /// Generates an embedding vector for the given text.
    ///
    /// # Arguments
    ///
    /// * `text` - The text to embed
    ///
    /// # Returns
    ///
    /// A vector of floating-point values representing the text embedding.
    fn generate(&self, text: &str) -> impl Future<Output = Result<Vec<f32>, AppError>> + Send;
}

/// Client for accessing open data portals (CKAN, Socrata, etc.).
///
/// Implementations fetch dataset metadata from portal APIs.
pub trait PortalClient: Send + Sync + Clone {
    /// Type representing raw portal data before transformation.
    type PortalData: Send;

    /// Lists all dataset IDs available on the portal.
    fn list_dataset_ids(&self) -> impl Future<Output = Result<Vec<String>, AppError>> + Send;

    /// Fetches detailed metadata for a specific dataset.
    ///
    /// # Arguments
    ///
    /// * `id` - The dataset identifier
    fn get_dataset(
        &self,
        id: &str,
    ) -> impl Future<Output = Result<Self::PortalData, AppError>> + Send;

    /// Converts portal-specific data into a normalized NewDataset.
    ///
    /// # Arguments
    ///
    /// * `data` - The raw portal data
    /// * `portal_url` - The portal URL for source tracking
    fn into_new_dataset(data: Self::PortalData, portal_url: &str) -> NewDataset;

    /// Searches for datasets modified since the given timestamp.
    ///
    /// Used for incremental harvesting to fetch only recently modified datasets.
    /// Returns full dataset objects, eliminating the need for separate get_dataset calls.
    ///
    /// # Arguments
    ///
    /// * `since` - Only return datasets modified after this timestamp
    ///
    /// # Returns
    ///
    /// A vector of portal-specific dataset objects modified since the given time.
    /// Returns an error if the portal doesn't support incremental search.
    fn search_modified_since(
        &self,
        since: DateTime<Utc>,
    ) -> impl Future<Output = Result<Vec<Self::PortalData>, AppError>> + Send;
}

/// Factory for creating portal clients.
///
/// Separate from PortalClient to avoid issues with async trait constructors.
pub trait PortalClientFactory: Send + Sync + Clone {
    /// The type of portal client this factory creates.
    type Client: PortalClient;

    /// Creates a new portal client for the given URL.
    ///
    /// # Arguments
    ///
    /// * `portal_url` - The portal API base URL
    fn create(&self, portal_url: &str) -> Result<Self::Client, AppError>;
}

/// Store for dataset persistence and retrieval.
///
/// Implementations handle database operations for datasets.
pub trait DatasetStore: Send + Sync + Clone {
    /// Retrieves content hashes for all datasets from a specific portal.
    ///
    /// Used for delta detection to determine which datasets need reprocessing.
    ///
    /// # Arguments
    ///
    /// * `portal_url` - The source portal URL
    ///
    /// # Returns
    ///
    /// A map from original_id to optional content_hash.
    fn get_hashes_for_portal(
        &self,
        portal_url: &str,
    ) -> impl Future<Output = Result<HashMap<String, Option<String>>, AppError>> + Send;

    /// Updates only the timestamp for an unchanged dataset.
    ///
    /// Used when content hash matches but we want to track "last seen" time.
    ///
    /// # Arguments
    ///
    /// * `portal_url` - The source portal URL
    /// * `original_id` - The dataset's original ID from the portal
    fn update_timestamp_only(
        &self,
        portal_url: &str,
        original_id: &str,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Batch updates timestamps for multiple unchanged datasets.
    ///
    /// More efficient than calling `update_timestamp_only` for each dataset.
    ///
    /// # Arguments
    ///
    /// * `portal_url` - The source portal URL
    /// * `original_ids` - Slice of dataset original IDs to update
    ///
    /// # Returns
    ///
    /// The number of rows actually updated.
    fn batch_update_timestamps(
        &self,
        portal_url: &str,
        original_ids: &[String],
    ) -> impl Future<Output = Result<u64, AppError>> + Send;

    /// Inserts or updates a dataset.
    ///
    /// # Arguments
    ///
    /// * `dataset` - The dataset to upsert
    ///
    /// # Returns
    ///
    /// The UUID of the affected row.
    fn upsert(&self, dataset: &NewDataset) -> impl Future<Output = Result<Uuid, AppError>> + Send;

    /// Performs vector similarity search.
    ///
    /// # Arguments
    ///
    /// * `query_vector` - The embedding vector to search for
    /// * `limit` - Maximum number of results
    ///
    /// # Returns
    ///
    /// Datasets ranked by similarity score (highest first).
    fn search(
        &self,
        query_vector: Vector,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<SearchResult>, AppError>> + Send;

    /// Lists datasets as a stream with optional filtering.
    ///
    /// This method returns a stream of datasets for memory-efficient
    /// processing of large result sets. Unlike batch methods, it streams
    /// results directly from the database without loading everything into memory.
    ///
    /// # Arguments
    ///
    /// * `portal_filter` - Optional portal URL to filter by
    /// * `limit` - Optional maximum number of records
    fn list_stream<'a>(
        &'a self,
        portal_filter: Option<&'a str>,
        limit: Option<usize>,
    ) -> BoxStream<'a, Result<Dataset, AppError>>;

    /// Retrieves the last successful sync timestamp for a portal.
    ///
    /// Used for incremental harvesting to determine which datasets
    /// have been modified since the last sync.
    ///
    /// # Arguments
    ///
    /// * `portal_url` - The source portal URL
    ///
    /// # Returns
    ///
    /// The timestamp of the last successful sync, or None if never synced.
    fn get_last_sync_time(
        &self,
        portal_url: &str,
    ) -> impl Future<Output = Result<Option<DateTime<Utc>>, AppError>> + Send;

    /// Records a sync status for a portal.
    ///
    /// Called after a harvest operation to update the sync status.
    /// The `sync_status` parameter indicates the outcome: "completed" or "cancelled".
    ///
    /// # Arguments
    ///
    /// * `portal_url` - The source portal URL
    /// * `sync_time` - The timestamp of this sync
    /// * `sync_mode` - Either "full" or "incremental"
    /// * `sync_status` - The outcome: "completed" or "cancelled"
    /// * `datasets_synced` - Number of datasets processed
    fn record_sync_status(
        &self,
        portal_url: &str,
        sync_time: DateTime<Utc>,
        sync_mode: &str,
        sync_status: &str,
        datasets_synced: i32,
    ) -> impl Future<Output = Result<(), AppError>> + Send;
}
