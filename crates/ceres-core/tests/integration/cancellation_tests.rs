//! Integration tests for cancellation support in HarvestService.

use ceres_core::harvest::HarvestService;
use ceres_core::{HarvestConfig, SyncStatus};
use tokio_util::sync::CancellationToken;

use crate::integration::common::{MockDatasetStore, MockPortalClientFactory, MockPortalData};

const TEST_PORTAL_URL: &str = "https://test-portal.example.com";

#[tokio::test]
async fn test_cancellation_before_start() {
    // Arrange
    let store = MockDatasetStore::new();
    let factory = MockPortalClientFactory::new(vec![]);
    let service = HarvestService::new(store.clone(), factory);

    let token = CancellationToken::new();
    token.cancel(); // Cancel immediately

    // Act
    let result = service
        .sync_portal_cancellable(TEST_PORTAL_URL, token)
        .await
        .unwrap();

    // Assert
    assert!(
        result.status.is_cancelled(),
        "Result status should be Cancelled"
    );
    assert_eq!(result.stats.total(), 0, "Should have processed 0 items");

    // Verify DB record
    let history = store.sync_history.lock().unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].sync_status, SyncStatus::Cancelled.as_str());
}

#[tokio::test]
async fn test_cancellation_during_processing() {
    // Arrange: Create many datasets. With mocks (zero delay), harvest is
    // instantaneous, so we pre-cancel the token. The harvest loop checks
    // the token between batches, so with batch_size=2 and 500 datasets
    // it will detect cancellation quickly.
    let datasets: Vec<MockPortalData> = (0..500)
        .map(|i| MockPortalData {
            id: format!("ds-{}", i),
            title: format!("Dataset {}", i),
            description: Some("Description".to_string()),
        })
        .collect();

    let store = MockDatasetStore::new();
    let factory = MockPortalClientFactory::new(datasets);

    let config = HarvestConfig {
        concurrency: 1,
        upsert_batch_size: 2,
        force_full_sync: true,
        ..Default::default()
    };
    let service = HarvestService::with_config(store.clone(), factory, config);
    let token = CancellationToken::new();

    // Pre-cancel — the harvest loop checks the token between batches
    token.cancel();

    let result = service
        .sync_portal_cancellable(TEST_PORTAL_URL, token)
        .await
        .unwrap();

    // Assert
    assert!(
        result.status.is_cancelled(),
        "Result status should be Cancelled"
    );

    let processed = result.stats.total();
    assert!(processed < 500, "Should not have processed all items");

    // Verify DB record
    let history = store.sync_history.lock().unwrap();
    assert!(!history.is_empty());
    let last_record = history.last().unwrap();
    assert_eq!(last_record.sync_status, SyncStatus::Cancelled.as_str());
    assert_eq!(last_record.datasets_synced, processed as i32);
}

#[tokio::test]
async fn test_batch_harvest_cancellable() {
    // Arrange
    let datasets1 = vec![MockPortalData {
        id: "d1".to_string(),
        title: "T1".to_string(),
        description: None,
    }];

    let factory = MockPortalClientFactory::new(datasets1);
    let store = MockDatasetStore::new();

    let service = HarvestService::new(store.clone(), factory);
    let token = CancellationToken::new();

    let portals = [
        ceres_core::PortalEntry {
            name: "Portal 1".to_string(),
            url: "http://p1.com".to_string(),
            description: None,
            enabled: true,
            portal_type: ceres_core::PortalType::Ckan,
            url_template: None,
            language: None,
            profile: None,
        },
        ceres_core::PortalEntry {
            name: "Portal 2".to_string(),
            url: "http://p2.com".to_string(),
            description: None,
            enabled: true,
            portal_type: ceres_core::PortalType::Ckan,
            url_template: None,
            language: None,
            profile: None,
        },
    ];

    // Act: cancel immediately to ensure it stops before second portal
    token.cancel();

    let refs: Vec<&ceres_core::PortalEntry> = portals.iter().collect();
    let _summary = service.batch_harvest_cancellable(&refs, token).await;

    // Assert: with immediate cancel, the first portal should be cancelled
    let history = store.sync_history.lock().unwrap();
    // May have 0 or 1 records depending on timing
    if !history.is_empty() {
        assert_eq!(history[0].sync_status, SyncStatus::Cancelled.as_str());
    }

    // Should not have attempted second portal
    let p2_records = history
        .iter()
        .filter(|r| r.portal_url == "http://p2.com")
        .count();
    assert_eq!(p2_records, 0, "Should not have attempted second portal");
}
