//! Integration tests for ExportService.
//!
//! These tests verify the export functionality using mock implementations.

use crate::integration::common::{MockDatasetStore, MockPortalData};
use ceres_core::export::{ExportFormat, ExportService};
use ceres_core::models::NewDataset;
use ceres_core::traits::DatasetStore;
use ceres_core::{
    ParquetExportConfig, ParquetExportService, PortalEntry, PortalType, PortalsConfig,
};
use sha2::{Digest, Sha256};

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
            record_kind: ceres_core::CatalogRecordKind::Dataset,
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

// =============================================================================
// Parquet Snapshot Manifest Tests
// =============================================================================

#[tokio::test]
async fn test_parquet_export_writes_versioned_snapshot_manifest() {
    let store = MockDatasetStore::new();
    let dataset = NewDataset {
        record_kind: ceres_core::CatalogRecordKind::Dataset,
        original_id: "transport-routes".to_string(),
        source_portal: TEST_PORTAL_URL.to_string(),
        url: format!("{TEST_PORTAL_URL}/dataset/transport-routes"),
        title: "Public transport routes".to_string(),
        description: Some("Route and stop information for public transport.".to_string()),
        embedding: None,
        metadata: serde_json::json!({"tags": [{"name": "transport"}]}),
        content_hash: NewDataset::compute_content_hash(
            "Public transport routes",
            Some("Route and stop information for public transport."),
        ),
    };
    store.upsert(&dataset).await.unwrap();

    let portals = PortalsConfig {
        portals: vec![PortalEntry {
            name: "test-portal".to_string(),
            url: TEST_PORTAL_URL.to_string(),
            portal_type: PortalType::Ckan,
            enabled: true,
            description: None,
            url_template: None,
            language: None,
            profile: None,
            sparql_endpoint: None,
            ogc_endpoint: None,
            aliases: Vec::new(),
        }],
    };
    let service = ParquetExportService::new(
        store,
        Some(portals),
        ParquetExportConfig::default().with_git_commit("abc123def"),
    );
    let output = tempfile::tempdir().unwrap();

    let result = service.export_to_directory(output.path()).await.unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.path().join("metadata.json")).unwrap())
            .unwrap();

    assert_eq!(manifest["schema_version"], "1.0.0");
    assert_eq!(manifest["snapshot_id"], result.snapshot_id);
    assert_eq!(manifest["ceres"]["git_commit"], "abc123def");
    assert_eq!(manifest["canonical_file"], "all.parquet");
    assert_eq!(manifest["row_counts"]["raw"], 1);
    assert_eq!(manifest["row_counts"]["exported"], 1);
    assert!(manifest["portal_config"]["sha256"].as_str().is_some());
    assert!(manifest.get("output_dir").is_none());

    // Duplicate detection is recorded as a documented heuristic, not canonical dedup.
    assert_eq!(
        manifest["duplicate_detection"]["method"],
        "title-exact-ci-cross-portal"
    );
    assert_eq!(manifest["duplicate_detection"]["version"], "1");
    assert_eq!(manifest["duplicate_detection"]["alias_groups"], 0);

    // Files now include all.parquet, identity.parquet, and the portal subset.
    let files = manifest["files"].as_array().unwrap();
    assert_eq!(files.len(), 3);
    assert!(
        files
            .iter()
            .any(|file| file["path"] == "identity.parquet" && file["kind"] == "identity")
    );
    assert!(output.path().join("identity.parquet").exists());
    assert!(
        files
            .iter()
            .all(|file| file["sha256"].as_str().unwrap().len() == 64)
    );
    assert!(
        files
            .iter()
            .all(|file| file["size_bytes"].as_u64().unwrap() > 0)
    );
    let canonical = files
        .iter()
        .find(|file| file["path"] == "all.parquet")
        .unwrap();
    let canonical_bytes = std::fs::read(output.path().join("all.parquet")).unwrap();
    let expected_checksum = format!("{:x}", Sha256::digest(canonical_bytes));
    assert_eq!(
        canonical["sha256"].as_str(),
        Some(expected_checksum.as_str())
    );

    let portal = &manifest["portals"][0];
    assert_eq!(portal["status"], "included");
    assert_eq!(portal["portal_type"], "ckan");
    assert_eq!(portal["file"], "data/test-portal.parquet");
}

#[tokio::test]
async fn test_parquet_export_writes_coverage_and_quality_report() {
    let store = MockDatasetStore::new();
    // Two datasets: one fully populated, one missing license/org/tags/modified.
    let rich = NewDataset {
        record_kind: ceres_core::CatalogRecordKind::Dataset,
        original_id: "rich".to_string(),
        source_portal: TEST_PORTAL_URL.to_string(),
        url: format!("{TEST_PORTAL_URL}/dataset/rich"),
        title: "Air quality measurements".to_string(),
        description: Some("Hourly air quality readings.".to_string()),
        embedding: None,
        metadata: serde_json::json!({
            "tags": [{"name": "air"}],
            "organization": {"title": "Env Agency"},
            "license_title": "CC-BY 4.0",
            "metadata_modified": "2026-01-01"
        }),
        content_hash: NewDataset::compute_content_hash(
            "Air quality measurements",
            Some("Hourly air quality readings."),
        ),
    };
    let sparse = NewDataset {
        record_kind: ceres_core::CatalogRecordKind::Dataset,
        original_id: "sparse".to_string(),
        source_portal: TEST_PORTAL_URL.to_string(),
        url: format!("{TEST_PORTAL_URL}/dataset/sparse"),
        title: "Bus timetable export".to_string(),
        description: Some("Timetable data.".to_string()),
        embedding: None,
        metadata: serde_json::json!({}),
        content_hash: NewDataset::compute_content_hash(
            "Bus timetable export",
            Some("Timetable data."),
        ),
    };
    store.upsert(&rich).await.unwrap();
    store.upsert(&sparse).await.unwrap();

    let portals = PortalsConfig {
        portals: vec![PortalEntry {
            name: "test-portal".to_string(),
            url: TEST_PORTAL_URL.to_string(),
            portal_type: PortalType::Ckan,
            enabled: true,
            description: None,
            url_template: None,
            language: Some("en".to_string()),
            profile: None,
            sparql_endpoint: None,
            ogc_endpoint: None,
            aliases: Vec::new(),
        }],
    };
    let service = ParquetExportService::new(store, Some(portals), ParquetExportConfig::default());
    let output = tempfile::tempdir().unwrap();

    let result = service.export_to_directory(output.path()).await.unwrap();

    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.path().join("reports.json")).unwrap())
            .unwrap();
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.path().join("metadata.json")).unwrap())
            .unwrap();

    // Report figures agree with the manifest (acceptance criterion).
    assert_eq!(report["schema_version"], "1.0.0");
    assert_eq!(report["snapshot_id"], manifest["snapshot_id"]);
    assert_eq!(
        report["curation"]["exported"],
        manifest["row_counts"]["exported"]
    );
    assert_eq!(report["curation"]["raw"], manifest["row_counts"]["raw"]);
    assert_eq!(
        report["curation"]["filtered"],
        manifest["row_counts"]["filtered"]
    );

    assert_eq!(report["coverage"]["total_datasets"], 2);
    assert_eq!(report["coverage"]["portals"], 1);
    assert_eq!(report["coverage"]["by_language"][0]["key"], "en");
    assert_eq!(report["coverage"]["by_language"][0]["count"], 2);
    assert_eq!(report["coverage"]["by_portal_type"][0]["key"], "ckan");

    // Completeness: description present for both, license/org/tags/modified for one.
    let fc = &report["field_completeness"];
    assert_eq!(fc["total"], 2);
    assert_eq!(fc["description"]["present"], 2);
    assert_eq!(fc["description"]["rate"], 1.0);
    assert_eq!(fc["license"]["present"], 1);
    assert_eq!(fc["license"]["rate"], 0.5);
    assert_eq!(fc["tags"]["present"], 1);
    assert_eq!(fc["modification_date"]["present"], 1);

    // Report is also surfaced on the in-memory result and a human-readable form.
    assert_eq!(result.report.coverage.total_datasets, 2);
    let report_md = std::fs::read_to_string(output.path().join("report.md")).unwrap();
    assert!(report_md.contains("# Ceres Snapshot Report"));
    assert!(report_md.contains("Field completeness"));
}

// =============================================================================
// Duplicate / Alias Semantics (#155)
// =============================================================================

/// Builds a CKAN portal entry with optional alias/mirror URLs.
fn portal_entry(name: &str, url: &str, aliases: Vec<String>) -> PortalEntry {
    PortalEntry {
        name: name.to_string(),
        url: url.to_string(),
        portal_type: PortalType::Ckan,
        enabled: true,
        description: None,
        url_template: None,
        language: Some("en".to_string()),
        profile: None,
        sparql_endpoint: None,
        ogc_endpoint: None,
        aliases,
    }
}

/// Upserts one dataset into the mock store with a content-hash fingerprint.
async fn upsert_dataset(
    store: &MockDatasetStore,
    portal: &str,
    id: &str,
    title: &str,
    description: &str,
) {
    let dataset = NewDataset {
        record_kind: ceres_core::CatalogRecordKind::Dataset,
        original_id: id.to_string(),
        source_portal: portal.to_string(),
        url: format!("{portal}/dataset/{id}"),
        title: title.to_string(),
        description: Some(description.to_string()),
        embedding: None,
        metadata: serde_json::json!({}),
        content_hash: NewDataset::compute_content_hash(title, Some(description)),
    };
    store.upsert(&dataset).await.unwrap();
}

#[tokio::test]
async fn test_duplicate_flagged_across_independent_portals() {
    let store = MockDatasetStore::new();
    upsert_dataset(
        &store,
        "https://a.example.com",
        "x",
        "Shared air quality",
        "Readings A",
    )
    .await;
    upsert_dataset(
        &store,
        "https://b.example.com",
        "y",
        "Shared air quality",
        "Readings B",
    )
    .await;

    let portals = PortalsConfig {
        portals: vec![
            portal_entry("portal-a", "https://a.example.com", vec![]),
            portal_entry("portal-b", "https://b.example.com", vec![]),
        ],
    };
    let service = ParquetExportService::new(store, Some(portals), ParquetExportConfig::default());
    let output = tempfile::tempdir().unwrap();

    let result = service.export_to_directory(output.path()).await.unwrap();

    // Same title on two genuinely independent portals -> both flagged.
    assert_eq!(result.total_duplicates, 2);
}

#[tokio::test]
async fn test_alias_folds_mirror_so_title_not_flagged_duplicate() {
    let store = MockDatasetStore::new();
    upsert_dataset(
        &store,
        "https://a.example.com",
        "x",
        "Shared air quality",
        "Readings",
    )
    .await;
    upsert_dataset(
        &store,
        "https://mirror.example.com",
        "y",
        "Shared air quality",
        "Readings via mirror",
    )
    .await;

    // portal-a declares the mirror as an alias.
    let portals = PortalsConfig {
        portals: vec![portal_entry(
            "portal-a",
            "https://a.example.com",
            vec!["https://mirror.example.com".to_string()],
        )],
    };
    let service = ParquetExportService::new(store, Some(portals), ParquetExportConfig::default());
    let output = tempfile::tempdir().unwrap();

    let result = service.export_to_directory(output.path()).await.unwrap();

    // The mirror folds onto portal-a, so the shared title is a single source.
    assert_eq!(result.total_duplicates, 0);
    assert_eq!(result.total_exported, 2);
    assert_eq!(result.portals.len(), 1);
    assert_eq!(result.portals[0].name, "portal-a");

    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.path().join("metadata.json")).unwrap())
            .unwrap();
    assert_eq!(manifest["duplicate_detection"]["alias_groups"], 1);
}

#[tokio::test]
async fn test_same_portal_repeated_title_not_flagged() {
    let store = MockDatasetStore::new();
    upsert_dataset(
        &store,
        "https://a.example.com",
        "x",
        "Repeated title here",
        "One",
    )
    .await;
    upsert_dataset(
        &store,
        "https://a.example.com",
        "y",
        "Repeated title here",
        "Two",
    )
    .await;

    let portals = PortalsConfig {
        portals: vec![portal_entry("portal-a", "https://a.example.com", vec![])],
    };
    let service = ParquetExportService::new(store, Some(portals), ParquetExportConfig::default());
    let output = tempfile::tempdir().unwrap();

    let result = service.export_to_directory(output.path()).await.unwrap();

    // A repeated title within one portal is not a cross-portal duplicate.
    assert_eq!(result.total_duplicates, 0);
}

// =============================================================================
// Snapshot Changelog (#154)
// =============================================================================

#[tokio::test]
async fn test_changelog_diffs_against_previous_snapshot() {
    const PORTAL: &str = "https://c.example.com";
    let portals = || PortalsConfig {
        portals: vec![portal_entry("portal-c", PORTAL, vec![])],
    };

    // Previous snapshot: alpha (stable), bravo (will change), delta (removed later).
    let prev_store = MockDatasetStore::new();
    upsert_dataset(
        &prev_store,
        PORTAL,
        "a",
        "Alpha dataset",
        "Stable description",
    )
    .await;
    upsert_dataset(
        &prev_store,
        PORTAL,
        "b",
        "Bravo dataset",
        "Original description",
    )
    .await;
    upsert_dataset(&prev_store, PORTAL, "d", "Delta dataset", "To be removed").await;
    let prev_dir = tempfile::tempdir().unwrap();
    ParquetExportService::new(prev_store, Some(portals()), ParquetExportConfig::default())
        .export_to_directory(prev_dir.path())
        .await
        .unwrap();

    // Current snapshot: alpha (unchanged), bravo (changed description), charlie (added).
    let curr_store = MockDatasetStore::new();
    upsert_dataset(
        &curr_store,
        PORTAL,
        "a",
        "Alpha dataset",
        "Stable description",
    )
    .await;
    upsert_dataset(
        &curr_store,
        PORTAL,
        "b",
        "Bravo dataset",
        "Updated description",
    )
    .await;
    upsert_dataset(&curr_store, PORTAL, "c", "Charlie dataset", "Newly added").await;
    let curr_dir = tempfile::tempdir().unwrap();
    let result =
        ParquetExportService::new(curr_store, Some(portals()), ParquetExportConfig::default())
            .export_to_directory_with_previous(curr_dir.path(), Some(prev_dir.path()))
            .await
            .unwrap();

    let totals = &result.changelog.totals;
    assert!(result.changelog.compared);
    assert_eq!(totals.added, 1, "charlie is new");
    assert_eq!(totals.changed, 1, "bravo's content_hash changed");
    assert_eq!(totals.removed, 1, "delta dropped out");
    assert_eq!(totals.unchanged, 1, "alpha is identical");

    let changelog: serde_json::Value =
        serde_json::from_slice(&std::fs::read(curr_dir.path().join("changelog.json")).unwrap())
            .unwrap();
    assert_eq!(changelog["compared"], true);
    assert_eq!(changelog["totals"]["added"], 1);
    assert!(changelog["previous_snapshot_id"].as_str().is_some());
    assert!(curr_dir.path().join("changelog.md").exists());
}

#[tokio::test]
async fn test_changelog_without_previous_is_baseline() {
    let store = MockDatasetStore::new();
    upsert_dataset(
        &store,
        "https://c.example.com",
        "a",
        "Alpha dataset",
        "Desc",
    )
    .await;
    let portals = PortalsConfig {
        portals: vec![portal_entry("portal-c", "https://c.example.com", vec![])],
    };
    let output = tempfile::tempdir().unwrap();

    let result = ParquetExportService::new(store, Some(portals), ParquetExportConfig::default())
        .export_to_directory(output.path())
        .await
        .unwrap();

    assert!(!result.changelog.compared);
    let changelog: serde_json::Value =
        serde_json::from_slice(&std::fs::read(output.path().join("changelog.json")).unwrap())
            .unwrap();
    assert_eq!(changelog["compared"], false);
}

#[tokio::test]
async fn test_export_csv_escapes_special_characters() {
    // Arrange
    let store = MockDatasetStore::new();

    // Create a dataset with special characters in description
    let dataset = NewDataset {
        record_kind: ceres_core::CatalogRecordKind::Dataset,
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
        record_kind: ceres_core::CatalogRecordKind::Dataset,
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
        record_kind: ceres_core::CatalogRecordKind::Dataset,
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
            record_kind: ceres_core::CatalogRecordKind::Dataset,
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
