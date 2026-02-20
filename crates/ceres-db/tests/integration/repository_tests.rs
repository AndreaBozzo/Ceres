//! Integration tests for DatasetRepository.
//!
//! These tests verify the repository layer against a real PostgreSQL database
//! with pgvector extension. Each test runs in an isolated container.

use ceres_core::models::NewDataset;
use ceres_db::DatasetRepository;

use crate::integration::common::{sample_new_dataset, setup_test_db};

/// Test 1: Verify successful insertion of a new dataset
#[tokio::test]
async fn test_upsert_insert_new_dataset() {
    let (pool, _container) = setup_test_db().await;
    let repo = DatasetRepository::new(pool);

    // Create and insert a dataset
    let dataset = sample_new_dataset("test-123", "https://example.com");
    let id = repo.upsert(&dataset).await.expect("upsert should succeed");

    // Verify UUID is valid
    assert!(!id.is_nil(), "returned UUID should not be nil");

    // Retrieve and verify the dataset
    let retrieved = repo
        .get(id)
        .await
        .expect("get should succeed")
        .expect("dataset should exist");

    assert_eq!(retrieved.original_id, "test-123");
    assert_eq!(retrieved.source_portal, "https://example.com");
    assert_eq!(retrieved.title, dataset.title);
    assert_eq!(retrieved.description, dataset.description);
    assert!(retrieved.embedding.is_some(), "embedding should be stored");
    assert!(
        retrieved.first_seen_at <= retrieved.last_updated_at,
        "first_seen_at should be <= last_updated_at"
    );
    assert_eq!(
        retrieved.content_hash,
        Some(dataset.content_hash.clone()),
        "content_hash should match"
    );
}

/// Test 2: Verify ON CONFLICT updates existing dataset
#[tokio::test]
async fn test_upsert_updates_existing_dataset() {
    let (pool, _container) = setup_test_db().await;
    let repo = DatasetRepository::new(pool);

    // Insert original dataset
    let original = sample_new_dataset("test-123", "https://example.com");
    let id1 = repo
        .upsert(&original)
        .await
        .expect("first upsert should succeed");

    // Update with same (portal, original_id) but different content
    let updated = NewDataset {
        original_id: "test-123".to_string(),
        source_portal: "https://example.com".to_string(),
        url: "https://example.com/dataset/test-updated".to_string(),
        title: "Updated Title".to_string(),
        description: Some("Updated description".to_string()),
        embedding: None, // Test COALESCE - should preserve original embedding
        metadata: serde_json::json!({"updated": true}),
        content_hash: NewDataset::compute_content_hash(
            "Updated Title",
            Some("Updated description"),
        ),
    };
    let id2 = repo
        .upsert(&updated)
        .await
        .expect("second upsert should succeed");

    // Verify same UUID returned (update, not insert)
    assert_eq!(id1, id2, "upsert should return same UUID for updates");

    // Retrieve and verify updates
    let retrieved = repo
        .get(id1)
        .await
        .expect("get should succeed")
        .expect("dataset should exist");

    assert_eq!(retrieved.title, "Updated Title");
    assert_eq!(
        retrieved.description,
        Some("Updated description".to_string())
    );
    assert!(
        retrieved.embedding.is_some(),
        "COALESCE should preserve original embedding when new is NULL"
    );
    assert!(
        retrieved.last_updated_at > retrieved.first_seen_at,
        "last_updated_at should be after first_seen_at"
    );
}

/// Test 3: Verify vector search returns results ordered by similarity
#[tokio::test]
async fn test_search_returns_ordered_by_similarity() {
    let (pool, _container) = setup_test_db().await;
    let repo = DatasetRepository::new(pool);

    // Query vector: all 1.0s (uniform values across 768 dimensions)
    let query_vec: Vec<f32> = vec![1.0; 768];

    // Dataset A: high similarity (same direction as query)
    let mut ds_a = sample_new_dataset("ds-a", "https://example.com");
    ds_a.title = "High Similarity Dataset".to_string();
    ds_a.content_hash = NewDataset::compute_content_hash(&ds_a.title, ds_a.description.as_deref());
    ds_a.embedding = Some(vec![1.0_f32; 768]);
    repo.upsert(&ds_a)
        .await
        .expect("insert ds_a should succeed");

    // Dataset B: lower similarity (different direction)
    let mut ds_b = sample_new_dataset("ds-b", "https://example.com");
    ds_b.title = "Low Similarity Dataset".to_string();
    ds_b.content_hash = NewDataset::compute_content_hash(&ds_b.title, ds_b.description.as_deref());
    // Create a vector pointing in a different direction
    let mut b_vec = vec![0.1_f32; 768];
    b_vec[0] = 0.9; // Mostly different
    ds_b.embedding = Some(b_vec);
    repo.upsert(&ds_b)
        .await
        .expect("insert ds_b should succeed");

    // Dataset C: no embedding (should be excluded from search)
    let mut ds_c = sample_new_dataset("ds-c", "https://example.com");
    ds_c.title = "No Embedding Dataset".to_string();
    ds_c.content_hash = NewDataset::compute_content_hash(&ds_c.title, ds_c.description.as_deref());
    ds_c.embedding = None;
    repo.upsert(&ds_c)
        .await
        .expect("insert ds_c should succeed");

    // Perform vector search
    let results = repo
        .search(query_vec, 10)
        .await
        .expect("search should succeed");

    // Verify results
    assert_eq!(
        results.len(),
        2,
        "should return 2 results (ds_c excluded due to NULL embedding)"
    );

    // Verify ordering by similarity (highest first)
    assert_eq!(
        results[0].dataset.original_id, "ds-a",
        "ds-a should be first (highest similarity)"
    );
    assert_eq!(
        results[1].dataset.original_id, "ds-b",
        "ds-b should be second (lower similarity)"
    );

    // Verify similarity scores are in valid range and ordered
    assert!(
        results[0].similarity_score > 0.0 && results[0].similarity_score <= 1.0,
        "similarity score should be in (0, 1] range"
    );
    assert!(
        results[0].similarity_score > results[1].similarity_score,
        "first result should have higher similarity than second"
    );
}
