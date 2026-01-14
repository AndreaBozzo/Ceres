//! Job repository for PostgreSQL with SELECT FOR UPDATE SKIP LOCKED.
//!
//! Implements the [`JobQueue`] trait for persistent job storage with safe
//! concurrent job claiming using PostgreSQL's row-level locking.

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Pool, Postgres};
use uuid::Uuid;

use ceres_core::SyncStats;
use ceres_core::error::AppError;
use ceres_core::job::{CreateJobRequest, HarvestJob, JobStatus};
use ceres_core::job_queue::JobQueue;

/// PostgreSQL implementation of the job queue.
///
/// Uses `SELECT FOR UPDATE SKIP LOCKED` for safe concurrent job claiming,
/// ensuring that multiple workers can process jobs without conflicts.
#[derive(Clone)]
pub struct JobRepository {
    pool: Pool<Postgres>,
}

impl JobRepository {
    /// Create a new job repository with the given connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

// =============================================================================
// Helper Types for Database Mapping
// =============================================================================

/// Helper struct for deserializing job rows from the database.
#[derive(sqlx::FromRow)]
struct JobRow {
    id: Uuid,
    portal_url: String,
    portal_name: Option<String>,
    status: String,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
    retry_count: i32,
    max_retries: i32,
    next_retry_at: Option<DateTime<Utc>>,
    error_message: Option<String>,
    sync_stats: Option<sqlx::types::Json<SyncStatsJson>>,
    worker_id: Option<String>,
    force_full_sync: bool,
}

/// JSON representation of SyncStats for database storage.
#[derive(serde::Serialize, serde::Deserialize)]
struct SyncStatsJson {
    unchanged: usize,
    updated: usize,
    created: usize,
    failed: usize,
}

impl From<&SyncStats> for SyncStatsJson {
    fn from(stats: &SyncStats) -> Self {
        Self {
            unchanged: stats.unchanged,
            updated: stats.updated,
            created: stats.created,
            failed: stats.failed,
        }
    }
}

impl From<SyncStatsJson> for SyncStats {
    fn from(json: SyncStatsJson) -> Self {
        Self {
            unchanged: json.unchanged,
            updated: json.updated,
            created: json.created,
            failed: json.failed,
        }
    }
}

impl From<JobRow> for HarvestJob {
    fn from(row: JobRow) -> Self {
        Self {
            id: row.id,
            portal_url: row.portal_url,
            portal_name: row.portal_name,
            status: row.status.parse().unwrap_or(JobStatus::Pending),
            created_at: row.created_at,
            updated_at: row.updated_at,
            started_at: row.started_at,
            completed_at: row.completed_at,
            retry_count: row.retry_count as u32,
            max_retries: row.max_retries as u32,
            next_retry_at: row.next_retry_at,
            error_message: row.error_message,
            sync_stats: row.sync_stats.map(|j| j.0.into()),
            worker_id: row.worker_id,
            force_full_sync: row.force_full_sync,
        }
    }
}

// =============================================================================
// JobQueue Trait Implementation
// =============================================================================

impl JobQueue for JobRepository {
    async fn create_job(&self, request: CreateJobRequest) -> Result<HarvestJob, AppError> {
        let max_retries = request.max_retries.unwrap_or(3) as i32;

        let row: JobRow = sqlx::query_as(
            r#"
            INSERT INTO harvest_jobs (portal_url, portal_name, force_full_sync, max_retries)
            VALUES ($1, $2, $3, $4)
            RETURNING *
            "#,
        )
        .bind(&request.portal_url)
        .bind(&request.portal_name)
        .bind(request.force_full_sync)
        .bind(max_retries)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.into())
    }

    async fn claim_job(&self, worker_id: &str) -> Result<Option<HarvestJob>, AppError> {
        // Use SELECT FOR UPDATE SKIP LOCKED for safe concurrent claiming.
        // This query claims jobs that are:
        // 1. Pending with no retry scheduled (new jobs), OR
        // 2. Pending with retry time passed (retry-ready jobs)
        //
        // Priority:
        // - Non-retry jobs first (next_retry_at IS NULL)
        // - Then by creation order (oldest first)
        let row: Option<JobRow> = sqlx::query_as(
            r#"
            UPDATE harvest_jobs
            SET
                status = 'running',
                worker_id = $1,
                started_at = NOW(),
                updated_at = NOW()
            WHERE id = (
                SELECT id FROM harvest_jobs
                WHERE status = 'pending'
                  AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                ORDER BY
                    next_retry_at NULLS FIRST,
                    created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            RETURNING *
            "#,
        )
        .bind(worker_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(Into::into))
    }

    async fn complete_job(&self, job_id: Uuid, stats: SyncStats) -> Result<(), AppError> {
        let stats_json = serde_json::to_value(SyncStatsJson::from(&stats))
            .map_err(AppError::SerializationError)?;

        sqlx::query(
            r#"
            UPDATE harvest_jobs
            SET
                status = 'completed',
                completed_at = NOW(),
                updated_at = NOW(),
                sync_stats = $2,
                error_message = NULL,
                worker_id = NULL
            WHERE id = $1
            "#,
        )
        .bind(job_id)
        .bind(stats_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn fail_job(
        &self,
        job_id: Uuid,
        error: &str,
        next_retry_at: Option<DateTime<Utc>>,
    ) -> Result<(), AppError> {
        // If next_retry_at is provided, reset to pending for retry.
        // Otherwise, mark as permanently failed.
        let (new_status, should_increment) = if next_retry_at.is_some() {
            ("pending", true) // Will retry
        } else {
            ("failed", false) // Permanently failed
        };

        sqlx::query(
            r#"
            UPDATE harvest_jobs
            SET
                status = $2,
                error_message = $3,
                next_retry_at = $4,
                retry_count = CASE WHEN $5 THEN retry_count + 1 ELSE retry_count END,
                updated_at = NOW(),
                completed_at = CASE WHEN $2 = 'failed' THEN NOW() ELSE NULL END,
                worker_id = NULL,
                started_at = NULL
            WHERE id = $1
            "#,
        )
        .bind(job_id)
        .bind(new_status)
        .bind(error)
        .bind(next_retry_at)
        .bind(should_increment)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn cancel_job(&self, job_id: Uuid, stats: Option<SyncStats>) -> Result<(), AppError> {
        let stats_json = stats
            .as_ref()
            .map(|s| serde_json::to_value(SyncStatsJson::from(s)))
            .transpose()
            .map_err(AppError::SerializationError)?;

        sqlx::query(
            r#"
            UPDATE harvest_jobs
            SET
                status = 'cancelled',
                completed_at = NOW(),
                updated_at = NOW(),
                sync_stats = COALESCE($2, sync_stats),
                worker_id = NULL
            WHERE id = $1
            "#,
        )
        .bind(job_id)
        .bind(stats_json)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn get_job(&self, job_id: Uuid) -> Result<Option<HarvestJob>, AppError> {
        let row: Option<JobRow> = sqlx::query_as("SELECT * FROM harvest_jobs WHERE id = $1")
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(Into::into))
    }

    async fn list_jobs(
        &self,
        status: Option<JobStatus>,
        limit: usize,
    ) -> Result<Vec<HarvestJob>, AppError> {
        let rows: Vec<JobRow> = if let Some(s) = status {
            sqlx::query_as(
                r#"
                SELECT * FROM harvest_jobs
                WHERE status = $1
                ORDER BY created_at DESC
                LIMIT $2
                "#,
            )
            .bind(s.as_str())
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                r#"
                SELECT * FROM harvest_jobs
                ORDER BY created_at DESC
                LIMIT $1
                "#,
            )
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?
        };

        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn release_job(&self, job_id: Uuid) -> Result<(), AppError> {
        sqlx::query(
            r#"
            UPDATE harvest_jobs
            SET
                status = 'pending',
                worker_id = NULL,
                started_at = NULL,
                updated_at = NOW()
            WHERE id = $1 AND status = 'running'
            "#,
        )
        .bind(job_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn release_worker_jobs(&self, worker_id: &str) -> Result<u64, AppError> {
        let result = sqlx::query(
            r#"
            UPDATE harvest_jobs
            SET
                status = 'pending',
                worker_id = NULL,
                started_at = NULL,
                updated_at = NOW()
            WHERE worker_id = $1 AND status = 'running'
            "#,
        )
        .bind(worker_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }

    async fn count_by_status(&self, status: JobStatus) -> Result<i64, AppError> {
        let (count,): (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM harvest_jobs WHERE status = $1")
                .bind(status.as_str())
                .fetch_one(&self.pool)
                .await?;

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_stats_json_conversion() {
        let stats = SyncStats {
            unchanged: 10,
            updated: 5,
            created: 3,
            failed: 2,
        };

        let json = SyncStatsJson::from(&stats);
        assert_eq!(json.unchanged, 10);
        assert_eq!(json.updated, 5);
        assert_eq!(json.created, 3);
        assert_eq!(json.failed, 2);

        let back: SyncStats = json.into();
        assert_eq!(back.unchanged, stats.unchanged);
        assert_eq!(back.updated, stats.updated);
        assert_eq!(back.created, stats.created);
        assert_eq!(back.failed, stats.failed);
    }

    #[test]
    fn test_job_status_parsing() {
        assert_eq!("pending".parse::<JobStatus>(), Ok(JobStatus::Pending));
        assert_eq!("running".parse::<JobStatus>(), Ok(JobStatus::Running));
        assert_eq!("completed".parse::<JobStatus>(), Ok(JobStatus::Completed));
        assert_eq!("failed".parse::<JobStatus>(), Ok(JobStatus::Failed));
        assert_eq!("cancelled".parse::<JobStatus>(), Ok(JobStatus::Cancelled));
        assert!("invalid".parse::<JobStatus>().is_err());
    }
}
