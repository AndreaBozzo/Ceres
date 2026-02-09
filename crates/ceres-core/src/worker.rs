//! Worker service for processing harvest jobs from the queue.
//!
//! This module provides the [`WorkerService`] that polls for pending jobs
//! and processes them using the [`HarvestService`].
//!
//! # Architecture
//!
//! The worker follows a poll-based model:
//! ```text
//! loop {
//!     1. Check for cancellation
//!     2. Claim next available job (SELECT FOR UPDATE SKIP LOCKED)
//!     3. Process job using HarvestService
//!     4. Update job status (completed/failed/cancelled)
//!     5. If no jobs available, sleep for poll_interval
//! }
//! ```
//!
//! # Graceful Shutdown
//!
//! On cancellation token trigger:
//! - Stops claiming new jobs
//! - Allows current job to complete or cancel gracefully
//! - Releases any claimed jobs back to the queue
//!
//! # Example
//!
//! ```ignore
//! use ceres_core::worker::{WorkerService, WorkerConfig, TracingWorkerReporter};
//! use ceres_core::progress::TracingReporter;
//! use tokio_util::sync::CancellationToken;
//!
//! let worker = WorkerService::new(job_queue, harvest_service, WorkerConfig::default());
//! let cancel = CancellationToken::new();
//!
//! worker.run(cancel, &TracingWorkerReporter, &TracingReporter).await?;
//! ```

use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::SyncStats;
use crate::config::PortalType;
use crate::error::AppError;
use crate::harvest::HarvestService;
use crate::job::{HarvestJob, WorkerConfig};
use crate::job_queue::JobQueue;
use crate::progress::ProgressReporter;
use crate::traits::{DatasetStore, EmbeddingProvider, PortalClientFactory};

// =============================================================================
// Worker Events
// =============================================================================

/// Events emitted by the worker during operation.
#[derive(Debug, Clone)]
pub enum WorkerEvent<'a> {
    /// Worker started and is ready to process jobs.
    Started { worker_id: &'a str },
    /// Worker is polling for new jobs.
    Polling,
    /// Worker claimed a job.
    JobClaimed { job: &'a HarvestJob },
    /// Job processing started.
    JobStarted { job_id: Uuid, portal_url: &'a str },
    /// Job completed successfully.
    JobCompleted { job_id: Uuid, stats: &'a SyncStats },
    /// Job failed with error.
    JobFailed {
        job_id: Uuid,
        error: &'a str,
        will_retry: bool,
    },
    /// Job was cancelled.
    JobCancelled { job_id: Uuid, stats: &'a SyncStats },
    /// Worker is shutting down.
    ShuttingDown {
        worker_id: &'a str,
        jobs_released: u64,
    },
    /// Worker stopped.
    Stopped { worker_id: &'a str },
}

// =============================================================================
// Worker Reporter Trait
// =============================================================================

/// Trait for reporting worker events.
///
/// Similar to [`ProgressReporter`] but for worker-level events.
pub trait WorkerReporter: Send + Sync {
    /// Called when a worker event occurs.
    ///
    /// The default implementation does nothing (silent mode).
    fn report(&self, event: WorkerEvent<'_>) {
        let _ = event;
    }
}

/// Silent worker reporter that ignores all events.
#[derive(Debug, Default, Clone, Copy)]
pub struct SilentWorkerReporter;

impl WorkerReporter for SilentWorkerReporter {}

/// Tracing-based worker reporter for CLI/server logging.
#[derive(Debug, Default, Clone, Copy)]
pub struct TracingWorkerReporter;

impl WorkerReporter for TracingWorkerReporter {
    fn report(&self, event: WorkerEvent<'_>) {
        match event {
            WorkerEvent::Started { worker_id } => {
                info!(worker_id, "Worker started");
            }
            WorkerEvent::Polling => {
                // Debug level to avoid spam
                tracing::debug!("Polling for jobs...");
            }
            WorkerEvent::JobClaimed { job } => {
                info!(job_id = %job.id, portal = %job.portal_url, "Job claimed");
            }
            WorkerEvent::JobStarted { job_id, portal_url } => {
                info!(%job_id, portal = portal_url, "Processing job");
            }
            WorkerEvent::JobCompleted { job_id, stats } => {
                info!(
                    %job_id,
                    created = stats.created,
                    updated = stats.updated,
                    unchanged = stats.unchanged,
                    failed = stats.failed,
                    "Job completed"
                );
            }
            WorkerEvent::JobFailed {
                job_id,
                error,
                will_retry,
            } => {
                if will_retry {
                    warn!(%job_id, %error, "Job failed, will retry");
                } else {
                    error!(%job_id, %error, "Job permanently failed");
                }
            }
            WorkerEvent::JobCancelled { job_id, stats } => {
                info!(%job_id, processed = stats.total(), "Job cancelled");
            }
            WorkerEvent::ShuttingDown {
                worker_id,
                jobs_released,
            } => {
                info!(worker_id, jobs_released, "Worker shutting down");
            }
            WorkerEvent::Stopped { worker_id } => {
                info!(worker_id, "Worker stopped");
            }
        }
    }
}

// =============================================================================
// Worker Service
// =============================================================================

/// Worker service that processes jobs from the queue.
///
/// This service polls for pending jobs and processes them using the
/// [`HarvestService`]. It supports graceful shutdown via cancellation tokens.
pub struct WorkerService<Q, S, E, F>
where
    Q: JobQueue,
    S: DatasetStore,
    E: EmbeddingProvider,
    F: PortalClientFactory,
{
    queue: Q,
    harvest_service: HarvestService<S, E, F>,
    config: WorkerConfig,
}

impl<Q, S, E, F> WorkerService<Q, S, E, F>
where
    Q: JobQueue,
    S: DatasetStore,
    E: EmbeddingProvider,
    F: PortalClientFactory,
{
    /// Create a new worker service.
    pub fn new(queue: Q, harvest_service: HarvestService<S, E, F>, config: WorkerConfig) -> Self {
        Self {
            queue,
            harvest_service,
            config,
        }
    }

    /// Run the worker until cancelled.
    ///
    /// The worker will:
    /// 1. Poll for available jobs
    /// 2. Claim and process jobs
    /// 3. Handle retries on failure
    /// 4. Release jobs on graceful shutdown
    pub async fn run<WR, HR>(
        &self,
        cancel_token: CancellationToken,
        worker_reporter: &WR,
        harvest_reporter: &HR,
    ) -> Result<(), AppError>
    where
        WR: WorkerReporter,
        HR: ProgressReporter,
    {
        worker_reporter.report(WorkerEvent::Started {
            worker_id: &self.config.worker_id,
        });

        loop {
            // Check for shutdown
            if cancel_token.is_cancelled() {
                break;
            }

            // Poll for jobs
            worker_reporter.report(WorkerEvent::Polling);

            match self.queue.claim_job(&self.config.worker_id).await {
                Ok(Some(job)) => {
                    worker_reporter.report(WorkerEvent::JobClaimed { job: &job });

                    // Process the job
                    self.process_job(&job, &cancel_token, worker_reporter, harvest_reporter)
                        .await;
                }
                Ok(None) => {
                    // No jobs available, wait before polling again
                    tokio::select! {
                        _ = tokio::time::sleep(self.config.poll_interval) => {}
                        _ = cancel_token.cancelled() => break,
                    }
                }
                Err(e) => {
                    error!(error = %e, "Failed to claim job");
                    // Back off on errors
                    tokio::time::sleep(self.config.poll_interval * 2).await;
                }
            }
        }

        // Graceful shutdown: release any jobs this worker claimed
        let released = self
            .queue
            .release_worker_jobs(&self.config.worker_id)
            .await
            .unwrap_or(0);

        worker_reporter.report(WorkerEvent::ShuttingDown {
            worker_id: &self.config.worker_id,
            jobs_released: released,
        });

        worker_reporter.report(WorkerEvent::Stopped {
            worker_id: &self.config.worker_id,
        });

        Ok(())
    }

    /// Process a single job.
    async fn process_job<WR, HR>(
        &self,
        job: &HarvestJob,
        cancel_token: &CancellationToken,
        worker_reporter: &WR,
        harvest_reporter: &HR,
    ) where
        WR: WorkerReporter,
        HR: ProgressReporter,
    {
        worker_reporter.report(WorkerEvent::JobStarted {
            job_id: job.id,
            portal_url: &job.portal_url,
        });

        // Create job-specific cancellation token
        let job_cancel = cancel_token.child_token();

        // Execute the harvest with cancellation support
        // TODO: Add portal_type to HarvestJob when Socrata/DCAT support is added
        let result = self
            .harvest_service
            .sync_portal_with_progress_cancellable_with_options(
                &job.portal_url,
                job.url_template.as_deref(),
                harvest_reporter,
                job_cancel.clone(),
                job.force_full_sync,
                PortalType::default(),
            )
            .await;

        match result {
            Ok(sync_result) => {
                if sync_result.is_cancelled() {
                    // Job was cancelled
                    worker_reporter.report(WorkerEvent::JobCancelled {
                        job_id: job.id,
                        stats: &sync_result.stats,
                    });

                    if let Err(e) = self.queue.cancel_job(job.id, Some(sync_result.stats)).await {
                        error!(job_id = %job.id, error = %e, "Failed to mark job as cancelled");
                    }
                } else {
                    // Job completed successfully
                    worker_reporter.report(WorkerEvent::JobCompleted {
                        job_id: job.id,
                        stats: &sync_result.stats,
                    });

                    if let Err(e) = self.queue.complete_job(job.id, sync_result.stats).await {
                        error!(job_id = %job.id, error = %e, "Failed to mark job as completed");
                    }
                }
            }
            Err(e) => {
                let error_msg = e.to_string();
                let can_retry = job.can_retry() && e.is_retryable();

                worker_reporter.report(WorkerEvent::JobFailed {
                    job_id: job.id,
                    error: &error_msg,
                    will_retry: can_retry,
                });

                let next_retry = if can_retry {
                    Some(job.calculate_next_retry(&self.config.retry_config))
                } else {
                    None
                };

                if let Err(e) = self.queue.fail_job(job.id, &error_msg, next_retry).await {
                    error!(job_id = %job.id, error = %e, "Failed to mark job as failed");
                }
            }
        }
    }

    /// Process a single job by ID (for one-off execution, e.g., CLI).
    ///
    /// This method is useful for processing a specific job without running
    /// the full polling loop.
    pub async fn process_single_job<WR, HR>(
        &self,
        job_id: Uuid,
        cancel_token: CancellationToken,
        worker_reporter: &WR,
        harvest_reporter: &HR,
    ) -> Result<(), AppError>
    where
        WR: WorkerReporter,
        HR: ProgressReporter,
    {
        let job = self
            .queue
            .get_job(job_id)
            .await?
            .ok_or_else(|| AppError::Generic(format!("Job not found: {}", job_id)))?;

        self.process_job(&job, &cancel_token, worker_reporter, harvest_reporter)
            .await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_silent_worker_reporter() {
        let reporter = SilentWorkerReporter;
        // Should not panic
        reporter.report(WorkerEvent::Started {
            worker_id: "test-worker",
        });
        reporter.report(WorkerEvent::Polling);
        reporter.report(WorkerEvent::Stopped {
            worker_id: "test-worker",
        });
    }

    #[test]
    fn test_tracing_worker_reporter() {
        let reporter = TracingWorkerReporter;

        // Test all event variants don't panic
        reporter.report(WorkerEvent::Started {
            worker_id: "test-worker",
        });
        reporter.report(WorkerEvent::Polling);

        let stats = SyncStats {
            created: 5,
            updated: 3,
            unchanged: 10,
            failed: 1,
            skipped: 0,
        };
        reporter.report(WorkerEvent::JobCompleted {
            job_id: Uuid::new_v4(),
            stats: &stats,
        });
        reporter.report(WorkerEvent::JobFailed {
            job_id: Uuid::new_v4(),
            error: "test error",
            will_retry: true,
        });
        reporter.report(WorkerEvent::JobFailed {
            job_id: Uuid::new_v4(),
            error: "fatal error",
            will_retry: false,
        });
        reporter.report(WorkerEvent::JobCancelled {
            job_id: Uuid::new_v4(),
            stats: &stats,
        });
        reporter.report(WorkerEvent::ShuttingDown {
            worker_id: "test-worker",
            jobs_released: 2,
        });
        reporter.report(WorkerEvent::Stopped {
            worker_id: "test-worker",
        });
    }
}
