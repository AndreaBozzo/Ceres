//! Integration tests for HarvestService.
//!
//! These tests verify the core harvesting logic using mock implementations.

use crate::integration::common::{
    MockDatasetStore, MockEmbeddingProvider, MockPortalClientFactory, MockPortalData,
};
use ceres_core::SyncConfig;
use ceres_core::harvest::HarvestService;

const TEST_PORTAL_URL: &str = "https://test-portal.example.com";

/// Test 1: Verify that harvesting creates a new dataset with embedding.
///
/// When a portal has a dataset that doesn't exist in the store,
/// the harvest should:
/// - Fetch the dataset from the portal
/// - Generate an embedding
/// - Store the dataset with the embedding
#[tokio::test]
async fn test_harvest_creates_new_dataset() {
    // Arrange
    let datasets = vec![MockPortalData {
        id: "dataset-001".to_string(),
        title: "Climate Data 2024".to_string(),
        description: Some("Annual climate measurements for 2024".to_string()),
    }];

    let store = MockDatasetStore::new();
    let embedding = MockEmbeddingProvider::new();
    let factory = MockPortalClientFactory::new(datasets);

    let config = SyncConfig {
        concurrency: 1,
        force_full_sync: true, // Force full sync for tests
        ..Default::default()
    };
    let service = HarvestService::with_config(store.clone(), embedding, factory, config);

    // Act
    let stats = service.sync_portal(TEST_PORTAL_URL).await.unwrap();

    // Assert
    assert_eq!(stats.created, 1, "Should have created 1 dataset");
    assert_eq!(stats.updated, 0, "Should have updated 0 datasets");
    assert_eq!(stats.unchanged, 0, "Should have 0 unchanged datasets");
    assert_eq!(stats.failed, 0, "Should have 0 failed datasets");
    assert_eq!(stats.total(), 1, "Total should be 1");

    // Verify the dataset was stored
    assert!(
        store.contains(TEST_PORTAL_URL, "dataset-001"),
        "Dataset should be stored"
    );

    // Verify embedding was generated
    let stored = store.get(TEST_PORTAL_URL, "dataset-001").unwrap();
    assert!(
        stored.embedding.is_some(),
        "Dataset should have an embedding"
    );
}

/// Test 2: Verify that unchanged datasets are skipped.
///
/// When a dataset already exists in the store with the same content hash,
/// the harvest should:
/// - Skip embedding generation
/// - Mark the dataset as unchanged
/// - Not re-upsert the dataset
#[tokio::test]
async fn test_harvest_skips_unchanged_dataset() {
    // Arrange
    let datasets = vec![MockPortalData {
        id: "dataset-002".to_string(),
        title: "Population Statistics".to_string(),
        description: Some("City population data".to_string()),
    }];

    let store = MockDatasetStore::new();
    let embedding = MockEmbeddingProvider::new();
    let factory = MockPortalClientFactory::new(datasets);

    let config = SyncConfig {
        concurrency: 1,
        force_full_sync: true, // Force full sync for tests
        ..Default::default()
    };
    let service = HarvestService::with_config(store.clone(), embedding, factory, config);

    // First harvest - should create
    let stats1 = service.sync_portal(TEST_PORTAL_URL).await.unwrap();
    assert_eq!(stats1.created, 1, "First harvest should create 1 dataset");

    // Second harvest with same data - should be unchanged
    let stats2 = service.sync_portal(TEST_PORTAL_URL).await.unwrap();

    // Assert
    assert_eq!(stats2.created, 0, "Second harvest should create 0 datasets");
    assert_eq!(stats2.updated, 0, "Second harvest should update 0 datasets");
    assert_eq!(
        stats2.unchanged, 1,
        "Second harvest should have 1 unchanged"
    );
    assert_eq!(stats2.failed, 0, "Should have 0 failed datasets");

    // Store should still have exactly 1 dataset
    assert_eq!(store.len(), 1, "Store should have exactly 1 dataset");
}

// =============================================================================
// TODO: Future integration tests for ceres-core
// =============================================================================
//
// The following tests should be implemented as the project matures:
//
// ## HarvestService tests
//
// TODO(#29): test_harvest_updates_changed_dataset
// - When content hash differs, dataset should be re-processed with new embedding
//
// TODO(#29): test_harvest_handles_embedding_error
// - When embedding generation fails, dataset should be marked as failed
// - Other datasets should continue processing
//
// TODO(#29): test_harvest_handles_portal_error
// - When portal API fails for a dataset, it should be marked as failed
// - Other datasets should continue processing
//
// TODO(#29): test_batch_harvest_multiple_portals
// - Verify batch_harvest processes multiple portals
// - Verify error in one portal doesn't stop others
// - Verify aggregated statistics are correct
//
// TODO(#29): test_harvest_progress_reporting
// - Verify HarvestEvent events are emitted correctly
// - Verify progress intervals work as expected
//
// TODO(#29): test_harvest_concurrency
// - Verify concurrent processing works correctly
// - Test with different concurrency levels
//
// ## SearchService tests
//
// TODO(#29): test_search_returns_results
// - Verify search generates query embedding
// - Verify results are returned from store
//
// TODO(#29): test_search_handles_embedding_error
// - When query embedding fails, search should return error
//
// ## Edge cases
//
// TODO(#29): test_harvest_empty_portal
// - Portal with no datasets should return empty stats
//
// TODO(#29): test_harvest_dataset_without_description
// - Datasets with None description should still work
//
// TODO(#29): test_harvest_empty_title_and_description
// - Datasets with empty text should not generate embedding
