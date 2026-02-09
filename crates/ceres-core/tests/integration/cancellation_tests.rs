//! Integration tests for cancellation support in HarvestService.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use ceres_core::harvest::HarvestService;
use ceres_core::traits::EmbeddingProvider;
use ceres_core::{AppError, SyncConfig, SyncStatus};
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

use crate::integration::common::{MockDatasetStore, MockPortalClientFactory, MockPortalData};

const TEST_PORTAL_URL: &str = "https://test-portal.example.com";

/// A "slow" embedding provider that delays generation to simulate work.
#[derive(Clone)]
pub struct SlowEmbeddingProvider {
    delay: Duration,
    processed_count: Arc<AtomicUsize>,
}

impl SlowEmbeddingProvider {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            processed_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl EmbeddingProvider for SlowEmbeddingProvider {
    fn name(&self) -> &'static str {
        "slow-mock"
    }

    fn dimension(&self) -> usize {
        768
    }

    async fn generate(&self, _text: &str) -> Result<Vec<f32>, AppError> {
        sleep(self.delay).await;
        self.processed_count.fetch_add(1, Ordering::Relaxed);
        Ok(vec![0.1; 768])
    }
}

#[tokio::test]
async fn test_cancellation_before_start() {
    // Arrange
    let store = MockDatasetStore::new();
    let embedding = SlowEmbeddingProvider::new(Duration::from_millis(10));
    let factory = MockPortalClientFactory::new(vec![]);
    let service = HarvestService::new(store.clone(), embedding, factory);

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
    // Arrange: Create enough datasets so that processing would normally take time
    let datasets: Vec<MockPortalData> = (0..50)
        .map(|i| MockPortalData {
            id: format!("ds-{}", i),
            title: format!("Dataset {}", i),
            description: Some("Description".to_string()),
        })
        .collect();

    let store = MockDatasetStore::new();
    let embedding = SlowEmbeddingProvider::new(Duration::from_millis(50));
    let factory = MockPortalClientFactory::new(datasets);

    // Low concurrency to ensure sequential-ish processing and easier cancellation
    let config = SyncConfig {
        concurrency: 2,
        force_full_sync: true,
        ..Default::default()
    };
    let service = HarvestService::with_config(store.clone(), embedding.clone(), factory, config);
    let token = CancellationToken::new();

    // Act: Spawn harvest and cancel it shortly after
    let token_clone = token.clone();
    let harvest_handle = tokio::spawn(async move {
        service
            .sync_portal_cancellable(TEST_PORTAL_URL, token_clone)
            .await
    });

    // Wait bit to let it start, but not finish (50 items * 50ms / 2 threads = ~1.25s total)
    sleep(Duration::from_millis(200)).await;
    token.cancel();

    let result = harvest_handle.await.unwrap().unwrap();

    // Assert
    assert!(
        result.status.is_cancelled(),
        "Result status should be Cancelled"
    );

    let processed = result.stats.total();
    assert!(processed > 0, "Should have processed some items");
    assert!(processed < 50, "Should not have processed all items"); // 50 items

    // Verify we stopped processing
    let processing_check = embedding.processed_count.load(Ordering::Relaxed);
    // Allow small margin for in-flight tasks
    assert!(
        processing_check < 50,
        "Embedding provider processed too many items"
    );

    // Verify DB record
    let history = store.sync_history.lock().unwrap();
    // Might have one record (the final cancellation)
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

    // Hack: We can't reuse factory with specific datasets for different URLs easily
    // in this mock setup without making a smarter mock factory.
    // For this test, we just want to see that if we cancel, it stops after/during the first portal.
    // So we'll use same datasets for all portals.
    let factory = MockPortalClientFactory::new(datasets1);
    let store = MockDatasetStore::new();
    let embedding = SlowEmbeddingProvider::new(Duration::from_millis(100)); // Slow enough

    let service = HarvestService::new(store.clone(), embedding, factory);
    let token = CancellationToken::new();

    let portals = [
        ceres_core::PortalEntry {
            name: "Portal 1".to_string(),
            url: "http://p1.com".to_string(),
            description: None,
            enabled: true,
            portal_type: ceres_core::PortalType::Ckan,
            url_template: None,
        },
        ceres_core::PortalEntry {
            name: "Portal 2".to_string(),
            url: "http://p2.com".to_string(),
            description: None,
            enabled: true,
            portal_type: ceres_core::PortalType::Ckan,
            url_template: None,
        },
    ];

    // Act
    let token_clone = token.clone();
    let handle = tokio::spawn(async move {
        let refs: Vec<&ceres_core::PortalEntry> = portals.iter().collect();
        service.batch_harvest_cancellable(&refs, token_clone).await
    });

    sleep(Duration::from_millis(50)).await;
    token.cancel();

    let _summary = handle.await.unwrap();

    // Assert
    // We expect the first portal to be cancelled or possibly fail depending on exact timing,
    // but the summary should reflect incomplete work.

    let history = store.sync_history.lock().unwrap();
    assert!(!history.is_empty());
    assert_eq!(history[0].sync_status, SyncStatus::Cancelled.as_str());

    // Should verify we didn't try to sync the second portal (no history for it)
    let p2_records = history
        .iter()
        .filter(|r| r.portal_url == "http://p2.com")
        .count();
    assert_eq!(p2_records, 0, "Should not have attempted second portal");
}
