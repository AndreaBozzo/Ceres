//! Test utilities and mock implementations for integration tests.
//!
//! Provides mock implementations of the core traits for testing
//! `HarvestService` and `SearchService` in isolation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ceres_core::models::NewDataset;
use ceres_core::traits::{DatasetStore, EmbeddingProvider, PortalClient, PortalClientFactory};
use ceres_core::{AppError, Dataset, SearchResult};
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use pgvector::Vector;
use sqlx::types::Json;
use uuid::Uuid;

// =============================================================================
// MockEmbeddingProvider
// =============================================================================

/// Mock embedding provider that returns deterministic vectors.
///
/// Returns vectors based on the input text length, making embeddings
/// reproducible for testing. Dimension is configurable for testing
/// different provider configurations.
#[derive(Clone)]
pub struct MockEmbeddingProvider {
    dimension: usize,
}

impl MockEmbeddingProvider {
    /// Creates a new mock provider with 768 dimensions (Gemini default).
    pub fn new() -> Self {
        Self { dimension: 768 }
    }

    /// Creates a mock provider with a custom dimension.
    ///
    /// # Examples
    ///
    /// ```
    /// // Simulate OpenAI text-embedding-3-small (1536 dimensions)
    /// let provider = MockEmbeddingProvider::with_dimension(1536);
    /// ```
    #[allow(dead_code)]
    pub fn with_dimension(dimension: usize) -> Self {
        Self { dimension }
    }
}

impl Default for MockEmbeddingProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl EmbeddingProvider for MockEmbeddingProvider {
    fn name(&self) -> &'static str {
        "mock"
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    async fn generate(&self, text: &str) -> Result<Vec<f32>, AppError> {
        // Generate a deterministic embedding based on text length
        let seed = text.len() as f32;
        let embedding: Vec<f32> = (0..self.dimension)
            .map(|i| (seed + i as f32) / 1000.0)
            .collect();
        Ok(embedding)
    }
}

// =============================================================================
// MockPortalClient
// =============================================================================

/// Mock portal data for testing.
#[derive(Clone)]
pub struct MockPortalData {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
}

/// Mock portal client with configurable datasets.
#[derive(Clone)]
pub struct MockPortalClient {
    #[allow(dead_code)]
    portal_url: String,
    datasets: Vec<MockPortalData>,
}

impl MockPortalClient {
    pub fn new(portal_url: &str, datasets: Vec<MockPortalData>) -> Self {
        Self {
            portal_url: portal_url.to_string(),
            datasets,
        }
    }
}

impl PortalClient for MockPortalClient {
    type PortalData = MockPortalData;

    fn portal_type(&self) -> &'static str {
        "mock"
    }

    fn base_url(&self) -> &str {
        &self.portal_url
    }

    async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        Ok(self.datasets.iter().map(|d| d.id.clone()).collect())
    }

    async fn get_dataset(&self, id: &str) -> Result<Self::PortalData, AppError> {
        self.datasets
            .iter()
            .find(|d| d.id == id)
            .cloned()
            .ok_or_else(|| AppError::Generic(format!("Dataset not found: {}", id)))
    }

    fn into_new_dataset(
        data: Self::PortalData,
        portal_url: &str,
        url_template: Option<&str>,
    ) -> NewDataset {
        let content_hash =
            NewDataset::compute_content_hash(&data.title, data.description.as_deref());

        // Mock uses `id` for both `{id}` and `{name}` since MockPortalData
        // has no separate name/slug field. This is sufficient for testing
        // the template plumbing; CKAN-specific substitution is tested in ckan.rs.
        let url = match url_template {
            Some(template) => template
                .replace("{id}", &data.id)
                .replace("{name}", &data.id),
            None => format!("{}/dataset/{}", portal_url.trim_end_matches('/'), data.id),
        };

        NewDataset {
            original_id: data.id.clone(),
            source_portal: portal_url.to_string(),
            url,
            title: data.title,
            description: data.description,
            embedding: None,
            metadata: serde_json::json!({}),
            content_hash,
        }
    }

    async fn search_modified_since(
        &self,
        _since: DateTime<Utc>,
    ) -> Result<Vec<Self::PortalData>, AppError> {
        // For testing, return all datasets as "modified"
        Ok(self.datasets.clone())
    }
}

// =============================================================================
// MockPortalClientFactory
// =============================================================================

/// Factory for creating mock portal clients.
#[derive(Clone)]
pub struct MockPortalClientFactory {
    datasets: Vec<MockPortalData>,
}

impl MockPortalClientFactory {
    pub fn new(datasets: Vec<MockPortalData>) -> Self {
        Self { datasets }
    }
}

impl PortalClientFactory for MockPortalClientFactory {
    type Client = MockPortalClient;

    fn create(
        &self,
        portal_url: &str,
        _portal_type: ceres_core::config::PortalType,
    ) -> Result<Self::Client, AppError> {
        Ok(MockPortalClient::new(portal_url, self.datasets.clone()))
    }
}

// =============================================================================
// MockDatasetStore
// =============================================================================

/// Records a call to record_sync_status
#[derive(Clone, Debug)]
pub struct SyncStatusRecord {
    pub portal_url: String,
    #[allow(dead_code)]
    pub sync_mode: String,
    pub sync_status: String,
    pub datasets_synced: i32,
}

/// In-memory dataset store for testing.
///
/// Stores datasets in a `HashMap` and provides full implementation of
/// `DatasetStore` trait for verifying harvest/search logic.
#[derive(Clone)]
pub struct MockDatasetStore {
    /// Stored datasets keyed by (source_portal, original_id)
    datasets: Arc<Mutex<HashMap<(String, String), StoredDataset>>>,
    /// History of sync status records
    pub sync_history: Arc<Mutex<Vec<SyncStatusRecord>>>,
}

/// Internal representation of a stored dataset.
#[derive(Clone)]
struct StoredDataset {
    id: Uuid,
    dataset: NewDataset,
}

impl MockDatasetStore {
    pub fn new() -> Self {
        Self {
            datasets: Arc::new(Mutex::new(HashMap::new())),
            sync_history: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns the number of stored datasets.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.datasets.lock().unwrap().len()
    }

    /// Returns true if no datasets are stored.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Checks if a dataset exists for the given portal and original_id.
    #[allow(dead_code)]
    pub fn contains(&self, portal_url: &str, original_id: &str) -> bool {
        let key = (portal_url.to_string(), original_id.to_string());
        self.datasets.lock().unwrap().contains_key(&key)
    }

    /// Gets a dataset by portal URL and original ID.
    #[allow(dead_code)]
    pub fn get(&self, portal_url: &str, original_id: &str) -> Option<NewDataset> {
        let key = (portal_url.to_string(), original_id.to_string());
        self.datasets
            .lock()
            .unwrap()
            .get(&key)
            .map(|s| s.dataset.clone())
    }
}

impl Default for MockDatasetStore {
    fn default() -> Self {
        Self::new()
    }
}

impl DatasetStore for MockDatasetStore {
    async fn get_by_id(&self, id: Uuid) -> Result<Option<Dataset>, AppError> {
        let datasets = self.datasets.lock().unwrap();
        for stored in datasets.values() {
            if stored.id == id {
                return Ok(Some(Dataset {
                    id: stored.id,
                    original_id: stored.dataset.original_id.clone(),
                    source_portal: stored.dataset.source_portal.clone(),
                    url: stored.dataset.url.clone(),
                    title: stored.dataset.title.clone(),
                    description: stored.dataset.description.clone(),
                    embedding: stored.dataset.embedding.clone(),
                    metadata: Json(stored.dataset.metadata.clone()),
                    first_seen_at: chrono::Utc::now(),
                    last_updated_at: chrono::Utc::now(),
                    content_hash: Some(stored.dataset.content_hash.clone()),
                }));
            }
        }
        Ok(None)
    }

    async fn get_hashes_for_portal(
        &self,
        portal_url: &str,
    ) -> Result<HashMap<String, Option<String>>, AppError> {
        let datasets = self.datasets.lock().unwrap();
        let hashes: HashMap<String, Option<String>> = datasets
            .iter()
            .filter(|((portal, _), _)| portal == portal_url)
            .map(|((_, original_id), stored)| {
                (
                    original_id.clone(),
                    Some(stored.dataset.content_hash.clone()),
                )
            })
            .collect();
        Ok(hashes)
    }

    async fn update_timestamp_only(
        &self,
        _portal_url: &str,
        _original_id: &str,
    ) -> Result<(), AppError> {
        // No-op for mock - we don't track timestamps
        Ok(())
    }

    async fn batch_update_timestamps(
        &self,
        _portal_url: &str,
        original_ids: &[String],
    ) -> Result<u64, AppError> {
        // Return count as if all were updated
        Ok(original_ids.len() as u64)
    }

    async fn upsert(&self, dataset: &NewDataset) -> Result<Uuid, AppError> {
        let mut datasets = self.datasets.lock().unwrap();
        let key = (dataset.source_portal.clone(), dataset.original_id.clone());

        let id = if let Some(existing) = datasets.get(&key) {
            existing.id
        } else {
            Uuid::new_v4()
        };

        datasets.insert(
            key,
            StoredDataset {
                id,
                dataset: dataset.clone(),
            },
        );

        Ok(id)
    }

    async fn search(
        &self,
        _query_vector: Vector,
        limit: usize,
    ) -> Result<Vec<SearchResult>, AppError> {
        // Simple mock: return first `limit` datasets with fake similarity scores
        let datasets = self.datasets.lock().unwrap();
        let results: Vec<SearchResult> = datasets
            .values()
            .take(limit)
            .enumerate()
            .map(|(i, stored)| {
                // Create a minimal Dataset for SearchResult
                SearchResult {
                    dataset: ceres_core::Dataset {
                        id: stored.id,
                        original_id: stored.dataset.original_id.clone(),
                        source_portal: stored.dataset.source_portal.clone(),
                        url: stored.dataset.url.clone(),
                        title: stored.dataset.title.clone(),
                        description: stored.dataset.description.clone(),
                        embedding: stored.dataset.embedding.clone(),
                        metadata: Json(stored.dataset.metadata.clone()),
                        first_seen_at: chrono::Utc::now(),
                        last_updated_at: chrono::Utc::now(),
                        content_hash: Some(stored.dataset.content_hash.clone()),
                    },
                    similarity_score: 1.0 - (i as f32 * 0.1),
                }
            })
            .collect();
        Ok(results)
    }

    fn list_stream<'a>(
        &'a self,
        portal_filter: Option<&'a str>,
        limit: Option<usize>,
    ) -> BoxStream<'a, Result<Dataset, AppError>> {
        // Simple mock: collect all matching datasets and stream them
        let datasets = self.datasets.lock().unwrap();
        let results: Vec<Result<Dataset, AppError>> = datasets
            .iter()
            .filter(|((portal, _), _)| portal_filter.is_none_or(|filter| portal == filter))
            .take(limit.unwrap_or(usize::MAX))
            .map(|((_, _), stored)| {
                Ok(Dataset {
                    id: stored.id,
                    original_id: stored.dataset.original_id.clone(),
                    source_portal: stored.dataset.source_portal.clone(),
                    url: stored.dataset.url.clone(),
                    title: stored.dataset.title.clone(),
                    description: stored.dataset.description.clone(),
                    embedding: stored.dataset.embedding.clone(),
                    metadata: Json(stored.dataset.metadata.clone()),
                    first_seen_at: chrono::Utc::now(),
                    last_updated_at: chrono::Utc::now(),
                    content_hash: Some(stored.dataset.content_hash.clone()),
                })
            })
            .collect();
        Box::pin(futures::stream::iter(results))
    }

    async fn get_last_sync_time(
        &self,
        _portal_url: &str,
    ) -> Result<Option<DateTime<Utc>>, AppError> {
        // Always return None to simulate first sync (full sync)
        Ok(None)
    }

    async fn record_sync_status(
        &self,
        portal_url: &str,
        _sync_time: DateTime<Utc>,
        sync_mode: &str,
        sync_status: &str,
        datasets_synced: i32,
    ) -> Result<(), AppError> {
        self.sync_history.lock().unwrap().push(SyncStatusRecord {
            portal_url: portal_url.to_string(),
            sync_mode: sync_mode.to_string(),
            sync_status: sync_status.to_string(),
            datasets_synced,
        });
        Ok(())
    }

    async fn health_check(&self) -> Result<(), AppError> {
        // Mock is always healthy
        Ok(())
    }
}
