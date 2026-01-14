//! Job queue trait for abstracting job persistence.
//!
//! This module provides the [`JobQueue`] trait that abstracts job queue operations,
//! enabling different storage backends (PostgreSQL, in-memory for tests) and
//! facilitating dependency injection in the worker service.

use std::future::Future;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::SyncStats;
use crate::error::AppError;
use crate::job::{CreateJobRequest, HarvestJob, JobStatus};

/// Trait for job queue persistence operations.
///
/// This abstraction enables different storage backends (PostgreSQL, in-memory for tests)
/// and facilitates dependency injection in the worker service.
///
/// # Implementation Notes
///
/// Implementations should ensure:
/// - Atomic job claiming with `SELECT FOR UPDATE SKIP LOCKED` semantics
/// - Proper handling of retry scheduling
/// - Safe concurrent access from multiple workers
pub trait JobQueue: Send + Sync + Clone {
    /// Create a new job in the queue.
    ///
    /// Returns the created job with generated ID and timestamps.
    fn create_job(
        &self,
        request: CreateJobRequest,
    ) -> impl Future<Output = Result<HarvestJob, AppError>> + Send;

    /// Claim the next available pending job for processing.
    ///
    /// Uses `SELECT FOR UPDATE SKIP LOCKED` semantics for safe concurrent claiming.
    /// Jobs are claimed in order of:
    /// 1. Non-retry jobs first (next_retry_at IS NULL)
    /// 2. Then retry-ready jobs (next_retry_at <= NOW)
    /// 3. Oldest first within each category
    ///
    /// Returns `None` if no jobs are available.
    fn claim_job(
        &self,
        worker_id: &str,
    ) -> impl Future<Output = Result<Option<HarvestJob>, AppError>> + Send;

    /// Mark a job as completed with final statistics.
    fn complete_job(
        &self,
        job_id: Uuid,
        stats: SyncStats,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Mark a job as failed with error message.
    ///
    /// If `next_retry_at` is provided, the job is reset to pending for retry.
    /// Otherwise, the job is marked as permanently failed.
    fn fail_job(
        &self,
        job_id: Uuid,
        error: &str,
        next_retry_at: Option<DateTime<Utc>>,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Mark a job as cancelled.
    ///
    /// Optionally saves partial sync statistics.
    fn cancel_job(
        &self,
        job_id: Uuid,
        stats: Option<SyncStats>,
    ) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Get a job by ID.
    fn get_job(
        &self,
        job_id: Uuid,
    ) -> impl Future<Output = Result<Option<HarvestJob>, AppError>> + Send;

    /// List jobs with optional status filter.
    ///
    /// Results are ordered by creation time (newest first).
    fn list_jobs(
        &self,
        status: Option<JobStatus>,
        limit: usize,
    ) -> impl Future<Output = Result<Vec<HarvestJob>, AppError>> + Send;

    /// Release a job back to pending state.
    ///
    /// Used when a worker needs to give up a job (e.g., during shutdown).
    /// Only affects jobs in 'running' status.
    fn release_job(&self, job_id: Uuid) -> impl Future<Output = Result<(), AppError>> + Send;

    /// Release all jobs claimed by a specific worker.
    ///
    /// Used for graceful shutdown to return all claimed jobs to the queue.
    /// Returns the number of jobs released.
    fn release_worker_jobs(
        &self,
        worker_id: &str,
    ) -> impl Future<Output = Result<u64, AppError>> + Send;

    /// Get count of jobs by status.
    fn count_by_status(
        &self,
        status: JobStatus,
    ) -> impl Future<Output = Result<i64, AppError>> + Send;
}
