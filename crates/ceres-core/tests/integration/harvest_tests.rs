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
// Batch embedding tests (#95)
// =============================================================================

/// Test: Multiple new datasets are created with batch embedding.
///
/// Verifies that the two-phase pipeline (pre-process → batch embed → upsert)
/// correctly processes multiple datasets in a single batch.
#[tokio::test]
async fn test_harvest_batch_creates_multiple_datasets() {
    let datasets: Vec<MockPortalData> = (0..5)
        .map(|i| MockPortalData {
            id: format!("batch-ds-{}", i),
            title: format!("Batch Dataset {}", i),
            description: Some(format!("Description for dataset {}", i)),
        })
        .collect();

    let store = MockDatasetStore::new();
    let embedding = MockEmbeddingProvider::new();
    let factory = MockPortalClientFactory::new(datasets);

    let config = SyncConfig {
        concurrency: 2,
        force_full_sync: true,
        ..Default::default()
    };
    let service = HarvestService::with_config(store.clone(), embedding, factory, config);

    let stats = service.sync_portal(TEST_PORTAL_URL).await.unwrap();

    assert_eq!(stats.created, 5, "Should have created 5 datasets");
    assert_eq!(stats.failed, 0, "Should have 0 failed datasets");
    assert_eq!(stats.total(), 5, "Total should be 5");

    // Verify all datasets were stored with embeddings
    for i in 0..5 {
        let id = format!("batch-ds-{}", i);
        assert!(
            store.contains(TEST_PORTAL_URL, &id),
            "Dataset {} should be stored",
            id
        );
        let stored = store.get(TEST_PORTAL_URL, &id).unwrap();
        assert!(
            stored.embedding.is_some(),
            "Dataset {} should have an embedding",
            id
        );
    }
}

/// Test: Mix of new and unchanged datasets with batching.
///
/// First harvest creates all datasets. Second harvest should detect
/// unchanged datasets in Phase 1 and only batch-embed new ones.
#[tokio::test]
async fn test_harvest_batch_with_unchanged_mixed() {
    // First harvest: 3 datasets
    let datasets = vec![
        MockPortalData {
            id: "ds-a".to_string(),
            title: "Dataset A".to_string(),
            description: Some("Description A".to_string()),
        },
        MockPortalData {
            id: "ds-b".to_string(),
            title: "Dataset B".to_string(),
            description: Some("Description B".to_string()),
        },
        MockPortalData {
            id: "ds-c".to_string(),
            title: "Dataset C".to_string(),
            description: Some("Description C".to_string()),
        },
    ];

    let store = MockDatasetStore::new();
    let embedding = MockEmbeddingProvider::new();
    let factory = MockPortalClientFactory::new(datasets);

    let config = SyncConfig {
        concurrency: 1,
        force_full_sync: true,
        ..Default::default()
    };
    let service = HarvestService::with_config(store.clone(), embedding, factory, config);

    // First harvest
    let stats1 = service.sync_portal(TEST_PORTAL_URL).await.unwrap();
    assert_eq!(stats1.created, 3, "First harvest should create 3 datasets");

    // Second harvest with same data
    let stats2 = service.sync_portal(TEST_PORTAL_URL).await.unwrap();
    assert_eq!(
        stats2.unchanged, 3,
        "Second harvest should have 3 unchanged"
    );
    assert_eq!(stats2.created, 0, "Second harvest should create 0 datasets");
    assert_eq!(stats2.failed, 0, "Should have 0 failed datasets");
}

/// Test: Small batch size creates multiple batches.
///
/// With embedding_batch_size=2 and 5 datasets, we should get 3 batches (2+2+1).
/// All datasets should still be processed correctly.
#[tokio::test]
async fn test_harvest_batch_size_respected() {
    let datasets: Vec<MockPortalData> = (0..5)
        .map(|i| MockPortalData {
            id: format!("small-batch-{}", i),
            title: format!("Small Batch Dataset {}", i),
            description: Some(format!("Description {}", i)),
        })
        .collect();

    let store = MockDatasetStore::new();
    let embedding = MockEmbeddingProvider::new();
    let factory = MockPortalClientFactory::new(datasets);

    let config = SyncConfig {
        concurrency: 1,
        embedding_batch_size: 2, // Small batch size
        force_full_sync: true,
        ..Default::default()
    };
    let service = HarvestService::with_config(store.clone(), embedding, factory, config);

    let stats = service.sync_portal(TEST_PORTAL_URL).await.unwrap();

    assert_eq!(stats.created, 5, "Should have created all 5 datasets");
    assert_eq!(stats.failed, 0, "Should have 0 failed datasets");

    // Verify all datasets stored correctly
    for i in 0..5 {
        let id = format!("small-batch-{}", i);
        assert!(
            store.contains(TEST_PORTAL_URL, &id),
            "Dataset {} should be stored",
            id
        );
    }
}

/// Test: Batch size of 1 is equivalent to sequential processing.
///
/// With embedding_batch_size=1, each dataset gets its own API call.
/// This verifies backward compatibility with the pre-batching behavior.
#[tokio::test]
async fn test_harvest_batch_size_one_sequential() {
    let datasets: Vec<MockPortalData> = (0..3)
        .map(|i| MockPortalData {
            id: format!("seq-{}", i),
            title: format!("Sequential Dataset {}", i),
            description: Some(format!("Description {}", i)),
        })
        .collect();

    let store = MockDatasetStore::new();
    let embedding = MockEmbeddingProvider::new();
    let factory = MockPortalClientFactory::new(datasets);

    let config = SyncConfig {
        concurrency: 1,
        embedding_batch_size: 1, // Sequential mode
        force_full_sync: true,
        ..Default::default()
    };
    let service = HarvestService::with_config(store.clone(), embedding, factory, config);

    let stats = service.sync_portal(TEST_PORTAL_URL).await.unwrap();

    assert_eq!(stats.created, 3, "Should have created 3 datasets");
    assert_eq!(stats.failed, 0, "Should have 0 failed datasets");

    for i in 0..3 {
        let stored = store.get(TEST_PORTAL_URL, &format!("seq-{}", i)).unwrap();
        assert!(
            stored.embedding.is_some(),
            "Dataset should have an embedding"
        );
    }
}

// =============================================================================
// Dry-run tests (#99)
// =============================================================================

/// Test: Dry run collects stats without writing to DB or generating embeddings.
///
/// With `dry_run: true`, the harvest should:
/// - Fetch datasets from the portal (validates connectivity)
/// - Compute content hashes and compare against existing data
/// - Return accurate created/updated/unchanged counts
/// - NOT upsert any datasets to the store
/// - NOT record sync status
#[tokio::test]
async fn test_harvest_dry_run_no_side_effects() {
    let datasets = vec![
        MockPortalData {
            id: "dry-1".to_string(),
            title: "Dry Run Dataset 1".to_string(),
            description: Some("Description 1".to_string()),
        },
        MockPortalData {
            id: "dry-2".to_string(),
            title: "Dry Run Dataset 2".to_string(),
            description: Some("Description 2".to_string()),
        },
        MockPortalData {
            id: "dry-3".to_string(),
            title: "Dry Run Dataset 3".to_string(),
            description: Some("Description 3".to_string()),
        },
    ];

    let store = MockDatasetStore::new();
    let embedding = MockEmbeddingProvider::new();
    let factory = MockPortalClientFactory::new(datasets);

    let config = SyncConfig {
        concurrency: 1,
        force_full_sync: true,
        dry_run: true,
        ..Default::default()
    };
    let service = HarvestService::with_config(store.clone(), embedding, factory, config);

    let stats = service.sync_portal(TEST_PORTAL_URL).await.unwrap();

    // Stats should reflect what would happen
    assert_eq!(
        stats.created, 3,
        "Should report 3 datasets would be created"
    );
    assert_eq!(stats.failed, 0, "Should have 0 failed datasets");

    // Store should be empty — no upserts in dry-run
    assert!(
        store.is_empty(),
        "Store should be empty in dry-run mode (no upserts)"
    );

    // No sync status should be recorded
    let sync_history = store.sync_history.lock().unwrap();
    assert!(
        sync_history.is_empty(),
        "No sync status should be recorded in dry-run mode"
    );
}

/// Test: Dry run correctly reports unchanged datasets.
///
/// When datasets already exist in the store, dry-run should detect them
/// as unchanged without modifying anything.
#[tokio::test]
async fn test_harvest_dry_run_detects_unchanged() {
    let datasets = vec![MockPortalData {
        id: "existing-1".to_string(),
        title: "Existing Dataset".to_string(),
        description: Some("Already harvested".to_string()),
    }];

    let store = MockDatasetStore::new();
    let embedding = MockEmbeddingProvider::new();
    let factory = MockPortalClientFactory::new(datasets);

    // First: normal harvest to populate the store
    let normal_config = SyncConfig {
        concurrency: 1,
        force_full_sync: true,
        ..Default::default()
    };
    let service = HarvestService::with_config(
        store.clone(),
        embedding.clone(),
        factory.clone(),
        normal_config,
    );
    let stats1 = service.sync_portal(TEST_PORTAL_URL).await.unwrap();
    assert_eq!(stats1.created, 1, "Normal harvest should create 1 dataset");
    assert_eq!(store.len(), 1, "Store should have 1 dataset");

    // Clear sync history from the normal harvest
    store.sync_history.lock().unwrap().clear();

    // Second: dry-run harvest — should detect unchanged, not modify store
    let dry_config = SyncConfig {
        concurrency: 1,
        force_full_sync: true,
        dry_run: true,
        ..Default::default()
    };
    let dry_service = HarvestService::with_config(store.clone(), embedding, factory, dry_config);
    let stats2 = dry_service.sync_portal(TEST_PORTAL_URL).await.unwrap();

    assert_eq!(
        stats2.unchanged, 1,
        "Dry run should detect 1 unchanged dataset"
    );
    assert_eq!(stats2.created, 0, "Dry run should report 0 created");
    assert_eq!(store.len(), 1, "Store should still have exactly 1 dataset");

    // No sync status should be recorded for dry-run
    let sync_history = store.sync_history.lock().unwrap();
    assert!(
        sync_history.is_empty(),
        "No sync status should be recorded in dry-run mode"
    );
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
