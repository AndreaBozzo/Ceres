//! Integration tests for ExportService.
//!
//! These tests verify the export functionality using mock implementations.

use crate::integration::common::{MockDatasetStore, MockPortalData};
use ceres_core::export::{ExportFormat, ExportService};
use ceres_core::models::NewDataset;
use ceres_core::traits::DatasetStore;

const TEST_PORTAL_URL: &str = "https://test-portal.example.com";

/// Helper to create and store test datasets in the mock store.
async fn setup_test_datasets(store: &MockDatasetStore, count: usize) -> Vec<MockPortalData> {
    let datasets: Vec<MockPortalData> = (0..count)
        .map(|i| MockPortalData {
            id: format!("dataset-{:03}", i),
            title: format!("Test Dataset {}", i),
            description: Some(format!("Description for dataset {}", i)),
        })
        .collect();

    for data in &datasets {
        let new_dataset = NewDataset {
            original_id: data.id.clone(),
            source_portal: TEST_PORTAL_URL.to_string(),
            url: format!("{}/dataset/{}", TEST_PORTAL_URL, data.id),
            title: data.title.clone(),
            description: data.description.clone(),
            embedding: None,
            metadata: serde_json::json!({}),
            content_hash: NewDataset::compute_content_hash(
                &data.title,
                data.description.as_deref(),
            ),
        };
        store.upsert(&new_dataset).await.unwrap();
    }

    datasets
}

// =============================================================================
// JSONL Export Tests
// =============================================================================

#[tokio::test]
async fn test_export_jsonl_format() {
    // Arrange
    let store = MockDatasetStore::new();
    setup_test_datasets(&store, 2).await;
    let service = ExportService::new(store);

    // Act
    let mut output = Vec::new();
    let count = service
        .export_to_writer(&mut output, ExportFormat::Jsonl, None, None)
        .await
        .unwrap();

    // Assert
    assert_eq!(count, 2, "Should export 2 datasets");

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 2, "JSONL should have 2 lines");

    // Each line should be valid JSON
    for line in lines {
        let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(parsed.is_object(), "Each line should be a JSON object");
        assert!(parsed.get("id").is_some(), "Should have id field");
        assert!(parsed.get("title").is_some(), "Should have title field");
    }
}

#[tokio::test]
async fn test_export_jsonl_empty_store() {
    // Arrange
    let store = MockDatasetStore::new();
    let service = ExportService::new(store);

    // Act
    let mut output = Vec::new();
    let count = service
        .export_to_writer(&mut output, ExportFormat::Jsonl, None, None)
        .await
        .unwrap();

    // Assert
    assert_eq!(count, 0, "Should export 0 datasets");
    let output_str = String::from_utf8(output).unwrap();
    assert!(output_str.is_empty(), "Output should be empty");
}

// =============================================================================
// JSON Export Tests
// =============================================================================

#[tokio::test]
async fn test_export_json_format() {
    // Arrange
    let store = MockDatasetStore::new();
    setup_test_datasets(&store, 2).await;
    let service = ExportService::new(store);

    // Act
    let mut output = Vec::new();
    let count = service
        .export_to_writer(&mut output, ExportFormat::Json, None, None)
        .await
        .unwrap();

    // Assert
    assert_eq!(count, 2, "Should export 2 datasets");

    let output_str = String::from_utf8(output).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output_str).unwrap();

    assert!(parsed.is_array(), "JSON output should be an array");
    let arr = parsed.as_array().unwrap();
    assert_eq!(arr.len(), 2, "Array should have 2 elements");
}

#[tokio::test]
async fn test_export_json_empty_store() {
    // Arrange
    let store = MockDatasetStore::new();
    let service = ExportService::new(store);

    // Act
    let mut output = Vec::new();
    let count = service
        .export_to_writer(&mut output, ExportFormat::Json, None, None)
        .await
        .unwrap();

    // Assert
    assert_eq!(count, 0, "Should export 0 datasets");

    let output_str = String::from_utf8(output).unwrap();
    let parsed: serde_json::Value = serde_json::from_str(&output_str).unwrap();
    assert!(parsed.is_array(), "JSON output should be an array");
    assert!(
        parsed.as_array().unwrap().is_empty(),
        "Array should be empty"
    );
}

// =============================================================================
// CSV Export Tests
// =============================================================================

#[tokio::test]
async fn test_export_csv_format() {
    // Arrange
    let store = MockDatasetStore::new();
    setup_test_datasets(&store, 2).await;
    let service = ExportService::new(store);

    // Act
    let mut output = Vec::new();
    let count = service
        .export_to_writer(&mut output, ExportFormat::Csv, None, None)
        .await
        .unwrap();

    // Assert
    assert_eq!(count, 2, "Should export 2 datasets");

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.lines().collect();

    // Should have header + 2 data rows
    assert_eq!(lines.len(), 3, "CSV should have 3 lines (header + 2 rows)");

    // Check header
    assert!(
        lines[0].starts_with("id,original_id,source_portal,url,title,description"),
        "First line should be CSV header"
    );
}

#[tokio::test]
async fn test_export_csv_empty_store() {
    // Arrange
    let store = MockDatasetStore::new();
    let service = ExportService::new(store);

    // Act
    let mut output = Vec::new();
    let count = service
        .export_to_writer(&mut output, ExportFormat::Csv, None, None)
        .await
        .unwrap();

    // Assert
    assert_eq!(count, 0, "Should export 0 datasets");

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.lines().collect();

    // Should have only header
    assert_eq!(lines.len(), 1, "CSV should have only header line");
}

#[tokio::test]
async fn test_export_csv_escapes_special_characters() {
    // Arrange
    let store = MockDatasetStore::new();

    // Create a dataset with special characters in description
    let dataset = NewDataset {
        original_id: "special-chars".to_string(),
        source_portal: TEST_PORTAL_URL.to_string(),
        url: format!("{}/dataset/special-chars", TEST_PORTAL_URL),
        title: "Dataset with, commas".to_string(),
        description: Some("Description with \"quotes\" and\nnewlines".to_string()),
        embedding: None,
        metadata: serde_json::json!({}),
        content_hash: NewDataset::compute_content_hash(
            "Dataset with, commas",
            Some("Description with \"quotes\" and\nnewlines"),
        ),
    };
    store.upsert(&dataset).await.unwrap();

    let service = ExportService::new(store);

    // Act
    let mut output = Vec::new();
    let count = service
        .export_to_writer(&mut output, ExportFormat::Csv, None, None)
        .await
        .unwrap();

    // Assert
    assert_eq!(count, 1, "Should export 1 dataset");

    let output_str = String::from_utf8(output).unwrap();

    // Title with comma should be quoted
    assert!(
        output_str.contains("\"Dataset with, commas\""),
        "Title with comma should be escaped"
    );

    // Description with quotes should have escaped quotes
    assert!(
        output_str.contains("\"\"quotes\"\""),
        "Quotes in description should be escaped"
    );
}

// =============================================================================
// Filter and Limit Tests
// =============================================================================

#[tokio::test]
async fn test_export_with_limit() {
    // Arrange
    let store = MockDatasetStore::new();
    setup_test_datasets(&store, 10).await;
    let service = ExportService::new(store);

    // Act
    let mut output = Vec::new();
    let count = service
        .export_to_writer(&mut output, ExportFormat::Jsonl, None, Some(3))
        .await
        .unwrap();

    // Assert
    assert_eq!(count, 3, "Should export only 3 datasets due to limit");

    let output_str = String::from_utf8(output).unwrap();
    let lines: Vec<&str> = output_str.lines().collect();
    assert_eq!(lines.len(), 3, "JSONL should have 3 lines");
}

#[tokio::test]
async fn test_export_with_portal_filter() {
    // Arrange
    let store = MockDatasetStore::new();

    // Create datasets from two different portals
    let dataset1 = NewDataset {
        original_id: "dataset-a".to_string(),
        source_portal: "https://portal-a.example.com".to_string(),
        url: "https://portal-a.example.com/dataset/dataset-a".to_string(),
        title: "Portal A Dataset".to_string(),
        description: Some("From portal A".to_string()),
        embedding: None,
        metadata: serde_json::json!({}),
        content_hash: NewDataset::compute_content_hash("Portal A Dataset", Some("From portal A")),
    };

    let dataset2 = NewDataset {
        original_id: "dataset-b".to_string(),
        source_portal: "https://portal-b.example.com".to_string(),
        url: "https://portal-b.example.com/dataset/dataset-b".to_string(),
        title: "Portal B Dataset".to_string(),
        description: Some("From portal B".to_string()),
        embedding: None,
        metadata: serde_json::json!({}),
        content_hash: NewDataset::compute_content_hash("Portal B Dataset", Some("From portal B")),
    };

    store.upsert(&dataset1).await.unwrap();
    store.upsert(&dataset2).await.unwrap();

    let service = ExportService::new(store);

    // Act - filter by portal A
    let mut output = Vec::new();
    let count = service
        .export_to_writer(
            &mut output,
            ExportFormat::Jsonl,
            Some("https://portal-a.example.com"),
            None,
        )
        .await
        .unwrap();

    // Assert
    assert_eq!(count, 1, "Should export only 1 dataset from portal A");

    let output_str = String::from_utf8(output).unwrap();
    assert!(
        output_str.contains("portal-a"),
        "Output should contain portal-a data"
    );
    assert!(
        !output_str.contains("portal-b"),
        "Output should not contain portal-b data"
    );
}

#[tokio::test]
async fn test_export_with_filter_and_limit() {
    // Arrange
    let store = MockDatasetStore::new();

    // Create 5 datasets from same portal
    for i in 0..5 {
        let dataset = NewDataset {
            original_id: format!("dataset-{}", i),
            source_portal: TEST_PORTAL_URL.to_string(),
            url: format!("{}/dataset/dataset-{}", TEST_PORTAL_URL, i),
            title: format!("Dataset {}", i),
            description: Some(format!("Description {}", i)),
            embedding: None,
            metadata: serde_json::json!({}),
            content_hash: NewDataset::compute_content_hash(
                &format!("Dataset {}", i),
                Some(&format!("Description {}", i)),
            ),
        };
        store.upsert(&dataset).await.unwrap();
    }

    let service = ExportService::new(store);

    // Act - filter by portal and limit to 2
    let mut output = Vec::new();
    let count = service
        .export_to_writer(
            &mut output,
            ExportFormat::Jsonl,
            Some(TEST_PORTAL_URL),
            Some(2),
        )
        .await
        .unwrap();

    // Assert
    assert_eq!(
        count, 2,
        "Should export only 2 datasets due to filter + limit"
    );
}
