//! Worker state machine tests.
//!
//! Tests the `WorkerService` job lifecycle: claim → process → complete/fail/cancel,
//! using `MockJobQueue` and mock services.

use std::time::Duration;

use ceres_core::config::PortalType;
use ceres_core::error::AppError;
use ceres_core::job::{JobStatus, WorkerConfig};
use ceres_core::pipeline::HarvestPipeline;
use ceres_core::progress::SilentReporter;
use ceres_core::traits::PortalClientFactory;
use ceres_core::worker::{SilentWorkerReporter, WorkerService};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::{
    MockDatasetStore, MockEmbeddingProvider, MockJobQueue, MockPortalClientFactory, MockPortalData,
};

const TEST_PORTAL: &str = "https://test.example.com";

// =============================================================================
// Helpers
// =============================================================================

fn test_worker_config() -> WorkerConfig {
    WorkerConfig::default()
        .with_worker_id("test-worker")
        .with_poll_interval(Duration::from_millis(10))
}

fn make_worker(
    queue: MockJobQueue,
    datasets: Vec<MockPortalData>,
) -> WorkerService<MockJobQueue, MockDatasetStore, MockEmbeddingProvider, MockPortalClientFactory> {
    let store = MockDatasetStore::new();
    let embedding = MockEmbeddingProvider::new();
    let factory = MockPortalClientFactory::new(datasets);
    let pipeline = HarvestPipeline::new(store, embedding, factory);
    WorkerService::new(queue, pipeline, test_worker_config())
}

/// Failing factory that simulates network errors during harvest.
#[derive(Clone)]
struct FailingPortalClientFactory;

impl PortalClientFactory for FailingPortalClientFactory {
    type Client = super::common::MockPortalClient;

    fn create(
        &self,
        _portal_url: &str,
        _portal_type: PortalType,
        _language: &str,
        _profile: Option<&str>,
    ) -> Result<Self::Client, AppError> {
        Err(AppError::NetworkError(
            "simulated network failure".to_string(),
        ))
    }
}

fn make_failing_worker(
    queue: MockJobQueue,
) -> WorkerService<MockJobQueue, MockDatasetStore, MockEmbeddingProvider, FailingPortalClientFactory>
{
    let store = MockDatasetStore::new();
    let embedding = MockEmbeddingProvider::new();
    let pipeline = HarvestPipeline::new(store, embedding, FailingPortalClientFactory);
    WorkerService::new(queue, pipeline, test_worker_config())
}

/// Wait for a job to reach a terminal state, with timeout.
async fn wait_for_terminal(queue: &MockJobQueue, job_id: Uuid) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = queue.job_status(job_id)
            && (status.is_terminal() || status == JobStatus::Pending)
        {
            // Pending is also a valid "done" state for retryable failures
            // (fail_job resets to Pending with next_retry_at).
            // We check if error_message is set to distinguish from initial pending.
            if status.is_terminal() {
                return;
            }
            if let Some(job) = queue.get_job_snapshot(job_id)
                && job.error_message.is_some()
            {
                return; // It was retried back to pending
            }
        }
        if tokio::time::Instant::now() > deadline {
            panic!("Timeout waiting for job {} to reach terminal state", job_id);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

// =============================================================================
// Tests
// =============================================================================

#[tokio::test]
async fn test_worker_completes_job_successfully() {
    let datasets = vec![MockPortalData {
        id: "d1".to_string(),
        title: "Test Dataset".to_string(),
        description: None,
    }];
    let queue = MockJobQueue::new();
    let (queue, job_id) = queue.with_pending_job(TEST_PORTAL);
    let worker = make_worker(queue.clone(), datasets);
    let cancel = CancellationToken::new();

    let cancel_clone = cancel.clone();
    let handle = tokio::spawn(async move {
        worker
            .run(cancel_clone, &SilentWorkerReporter, &SilentReporter)
            .await
    });

    wait_for_terminal(&queue, job_id).await;
    cancel.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(queue.job_status(job_id), Some(JobStatus::Completed));
    let job = queue.get_job_snapshot(job_id).unwrap();
    assert!(job.sync_stats.is_some());
    assert!(job.completed_at.is_some());
}

#[tokio::test]
async fn test_worker_fails_job_retryable() {
    // Job with retries remaining + retryable error (NetworkError)
    let queue = MockJobQueue::new();
    let (queue, job_id) = queue.with_pending_job_config(TEST_PORTAL, 0, 3);
    let worker = make_failing_worker(queue.clone());
    let cancel = CancellationToken::new();

    let cancel_clone = cancel.clone();
    let handle = tokio::spawn(async move {
        worker
            .run(cancel_clone, &SilentWorkerReporter, &SilentReporter)
            .await
    });

    wait_for_terminal(&queue, job_id).await;
    cancel.cancel();
    handle.await.unwrap().unwrap();

    // Job should be reset to Pending for retry
    let job = queue.get_job_snapshot(job_id).unwrap();
    assert_eq!(job.status, JobStatus::Pending);
    assert!(job.error_message.is_some());
    assert!(job.next_retry_at.is_some());
    assert_eq!(job.retry_count, 1);
}

#[tokio::test]
async fn test_worker_fails_job_permanent() {
    // Job with max retries already exhausted
    let queue = MockJobQueue::new();
    let (queue, job_id) = queue.with_pending_job_config(TEST_PORTAL, 3, 3);
    let worker = make_failing_worker(queue.clone());
    let cancel = CancellationToken::new();

    let cancel_clone = cancel.clone();
    let handle = tokio::spawn(async move {
        worker
            .run(cancel_clone, &SilentWorkerReporter, &SilentReporter)
            .await
    });

    wait_for_terminal(&queue, job_id).await;
    cancel.cancel();
    handle.await.unwrap().unwrap();

    // Job should be permanently failed
    assert_eq!(queue.job_status(job_id), Some(JobStatus::Failed));
    let job = queue.get_job_snapshot(job_id).unwrap();
    assert!(job.error_message.is_some());
    assert!(job.next_retry_at.is_none());
}

#[tokio::test]
async fn test_worker_releases_jobs_on_shutdown() {
    // Cancel immediately before the worker can claim any jobs
    let queue = MockJobQueue::new();
    let cancel = CancellationToken::new();
    cancel.cancel(); // Cancel before run

    let worker = make_worker(queue.clone(), vec![]);

    let result = worker
        .run(cancel, &SilentWorkerReporter, &SilentReporter)
        .await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_process_single_job_not_found() {
    let queue = MockJobQueue::new();
    let worker = make_worker(queue, vec![]);
    let cancel = CancellationToken::new();

    let result = worker
        .process_single_job(
            Uuid::new_v4(), // nonexistent
            cancel,
            &SilentWorkerReporter,
            &SilentReporter,
        )
        .await;

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Job not found"));
}

#[tokio::test]
async fn test_worker_handles_no_jobs_gracefully() {
    // Worker with empty queue should just poll and shut down cleanly
    let queue = MockJobQueue::new();
    let worker = make_worker(queue, vec![]);
    let cancel = CancellationToken::new();

    let cancel_clone = cancel.clone();
    let handle = tokio::spawn(async move {
        worker
            .run(cancel_clone, &SilentWorkerReporter, &SilentReporter)
            .await
    });

    // Let it poll once, then cancel
    tokio::time::sleep(Duration::from_millis(50)).await;
    cancel.cancel();

    let result = handle.await.unwrap();
    assert!(result.is_ok());
}
