//! Unit tests for `SearchService`.
//!
//! Tests the search orchestration logic using mock implementations
//! of `DatasetStore` and `EmbeddingProvider`.

use std::collections::{HashMap, HashSet};

use ceres_core::error::AppError;
use ceres_core::models::NewDataset;
use ceres_core::search::SearchService;
use ceres_core::traits::{DatasetStore, EmbeddingProvider};
use ceres_core::{Dataset, SearchResult};
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use uuid::Uuid;

use super::common::{MockDatasetStore, MockEmbeddingProvider};

// =============================================================================
// Test-local helpers
// =============================================================================

/// Embedding provider that always returns an error.
#[derive(Clone)]
struct FailingEmbeddingProvider;

impl EmbeddingProvider for FailingEmbeddingProvider {
    fn name(&self) -> &'static str {
        "failing"
    }
    fn dimension(&self) -> usize {
        768
    }
    async fn generate(&self, _text: &str) -> Result<Vec<f32>, AppError> {
        Err(AppError::Generic("embedding generation failed".to_string()))
    }
}

/// Dataset store where `search()` always errors.
#[derive(Clone)]
struct FailingDatasetStore;

impl DatasetStore for FailingDatasetStore {
    async fn search(
        &self,
        _query_vector: Vec<f32>,
        _limit: usize,
    ) -> Result<Vec<SearchResult>, AppError> {
        Err(AppError::DatabaseError("database unavailable".to_string()))
    }

    // Methods below are never called by SearchService::search()
    async fn get_by_id(&self, _id: Uuid) -> Result<Option<Dataset>, AppError> {
        unimplemented!()
    }
    async fn get_hashes_for_portal(
        &self,
        _portal_url: &str,
    ) -> Result<HashMap<String, Option<String>>, AppError> {
        unimplemented!()
    }
    async fn update_timestamp_only(
        &self,
        _portal_url: &str,
        _original_id: &str,
    ) -> Result<(), AppError> {
        unimplemented!()
    }
    async fn batch_update_timestamps(
        &self,
        _portal_url: &str,
        _original_ids: &[String],
    ) -> Result<u64, AppError> {
        unimplemented!()
    }
    async fn upsert(&self, _dataset: &NewDataset) -> Result<Uuid, AppError> {
        unimplemented!()
    }
    async fn batch_upsert(&self, _datasets: &[NewDataset]) -> Result<Vec<Uuid>, AppError> {
        unimplemented!()
    }
    fn list_stream<'a>(
        &'a self,
        _portal_filter: Option<&'a str>,
        _limit: Option<usize>,
    ) -> BoxStream<'a, Result<Dataset, AppError>> {
        Box::pin(futures::stream::empty())
    }
    async fn get_last_sync_time(
        &self,
        _portal_url: &str,
    ) -> Result<Option<DateTime<Utc>>, AppError> {
        unimplemented!()
    }
    async fn record_sync_status(
        &self,
        _portal_url: &str,
        _sync_time: DateTime<Utc>,
        _sync_mode: &str,
        _sync_status: &str,
        _datasets_synced: i32,
    ) -> Result<(), AppError> {
        unimplemented!()
    }
    async fn get_duplicate_titles(&self) -> Result<HashSet<String>, AppError> {
        unimplemented!()
    }
    async fn list_pending_embeddings(
        &self,
        _portal_filter: Option<&str>,
        _limit: Option<usize>,
    ) -> Result<Vec<Dataset>, AppError> {
        unimplemented!()
    }
    async fn count_pending_embeddings(
        &self,
        _portal_filter: Option<&str>,
    ) -> Result<i64, AppError> {
        unimplemented!()
    }
    async fn health_check(&self) -> Result<(), AppError> {
        unimplemented!()
    }
}

/// Seed the mock store with a dataset so search returns results.
async fn seeded_store() -> MockDatasetStore {
    let store = MockDatasetStore::new();
    let dataset = NewDataset {
        original_id: "d1".to_string(),
        source_portal: "https://test.example.com".to_string(),
        url: "https://test.example.com/dataset/d1".to_string(),
        title: "Climate Data 2024".to_string(),
        description: Some("Annual climate measurements".to_string()),
        embedding: Some(vec![0.1; 768]),
        metadata: serde_json::json!({}),
        content_hash: "abc123".to_string(),
    };
    store.upsert(&dataset).await.unwrap();
    store
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test]
async fn test_search_returns_results() {
    let store = seeded_store().await;
    let embedding = MockEmbeddingProvider::new();
    let service = SearchService::new(store, embedding);

    let results = service.search("climate", 10).await.unwrap();
    assert!(!results.is_empty());
    assert!(results[0].similarity_score > 0.0);
    assert_eq!(results[0].dataset.title, "Climate Data 2024");
}

#[tokio::test]
async fn test_search_empty_store_returns_empty() {
    let store = MockDatasetStore::new();
    let embedding = MockEmbeddingProvider::new();
    let service = SearchService::new(store, embedding);

    let results = service.search("anything", 10).await.unwrap();
    assert!(results.is_empty());
}

#[tokio::test]
async fn test_search_respects_limit() {
    let store = MockDatasetStore::new();
    // Insert multiple datasets
    for i in 0..5 {
        let dataset = NewDataset {
            original_id: format!("d{}", i),
            source_portal: "https://test.example.com".to_string(),
            url: format!("https://test.example.com/dataset/d{}", i),
            title: format!("Dataset {}", i),
            description: None,
            embedding: Some(vec![0.1; 768]),
            metadata: serde_json::json!({}),
            content_hash: format!("hash{}", i),
        };
        store.upsert(&dataset).await.unwrap();
    }

    let embedding = MockEmbeddingProvider::new();
    let service = SearchService::new(store, embedding);

    let results = service.search("test", 2).await.unwrap();
    assert!(results.len() <= 2);
}

#[tokio::test]
async fn test_search_embedding_error_propagates() {
    let store = MockDatasetStore::new();
    let embedding = FailingEmbeddingProvider;
    let service = SearchService::new(store, embedding);

    let result = service.search("test", 10).await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("embedding generation failed"));
}

#[tokio::test]
async fn test_search_store_error_propagates() {
    let store = FailingDatasetStore;
    let embedding = MockEmbeddingProvider::new();
    let service = SearchService::new(store, embedding);

    let result = service.search("test", 10).await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("database unavailable"));
}
