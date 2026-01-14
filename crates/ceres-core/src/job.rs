//! Job queue types for persistent harvest job management.
//!
//! This module provides domain types for the harvest job queue,
//! enabling recoverable, distributed job processing with exponential backoff retry.
//!
//! # Architecture
//!
//! Jobs flow through these states:
//! ```text
//! pending → running → completed
//!              ↓
//!           failed (if retries exhausted)
//!              ↓
//!           pending (if retries available, with next_retry_at)
//! ```
//!
//! # Retry Strategy
//!
//! Uses exponential backoff with configurable delays:
//! - Attempt 1: 1 minute
//! - Attempt 2: 5 minutes
//! - Attempt 3: 30 minutes
//! - After max retries: permanently failed

use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::SyncStats;

// =============================================================================
// Job Status
// =============================================================================

/// Status of a harvest job in the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    /// Job is waiting to be processed.
    Pending,
    /// Job is currently being processed by a worker.
    Running,
    /// Job completed successfully.
    Completed,
    /// Job failed after exhausting all retries.
    Failed,
    /// Job was cancelled by user or system.
    Cancelled,
}

impl JobStatus {
    /// Returns the string representation for database storage.
    pub fn as_str(&self) -> &'static str {
        match self {
            JobStatus::Pending => "pending",
            JobStatus::Running => "running",
            JobStatus::Completed => "completed",
            JobStatus::Failed => "failed",
            JobStatus::Cancelled => "cancelled",
        }
    }

    /// Returns true if the job is in a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            JobStatus::Completed | JobStatus::Failed | JobStatus::Cancelled
        )
    }
}

/// Error type for parsing JobStatus from string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseJobStatusError(String);

impl std::fmt::Display for ParseJobStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid job status: {}", self.0)
    }
}

impl std::error::Error for ParseJobStatusError {}

impl std::str::FromStr for JobStatus {
    type Err = ParseJobStatusError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(JobStatus::Pending),
            "running" => Ok(JobStatus::Running),
            "completed" => Ok(JobStatus::Completed),
            "failed" => Ok(JobStatus::Failed),
            "cancelled" => Ok(JobStatus::Cancelled),
            _ => Err(ParseJobStatusError(s.to_string())),
        }
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// =============================================================================
// Retry Configuration
// =============================================================================

/// Configuration for job retry behavior with exponential backoff.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts.
    pub max_retries: u32,
    /// Maximum delay cap.
    pub max_delay: TimeDelta,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            max_delay: TimeDelta::minutes(60), // 1 hour cap
        }
    }
}

impl RetryConfig {
    /// Calculate delay for a given retry attempt using exponential backoff.
    ///
    /// - Attempt 1: 1 minute
    /// - Attempt 2: 5 minutes
    /// - Attempt 3: 30 minutes
    /// - Attempt 4+: 60 minutes (capped)
    pub fn delay_for_attempt(&self, attempt: u32) -> TimeDelta {
        if attempt == 0 {
            return TimeDelta::zero();
        }

        // Exponential multipliers: 1, 5, 30 (minutes)
        let minutes = match attempt {
            1 => 1,
            2 => 5,
            3 => 30,
            _ => 60, // Cap at 60 minutes for any additional retries
        };

        let delay = TimeDelta::minutes(minutes);
        std::cmp::min(delay, self.max_delay)
    }
}

// =============================================================================
// Harvest Job
// =============================================================================

/// A harvest job in the queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarvestJob {
    /// Unique job identifier.
    pub id: Uuid,

    /// Target portal URL.
    pub portal_url: String,

    /// Optional friendly portal name.
    pub portal_name: Option<String>,

    /// Current job status.
    pub status: JobStatus,

    /// When the job was created.
    pub created_at: DateTime<Utc>,

    /// When the job was last updated.
    pub updated_at: DateTime<Utc>,

    /// When the job started processing.
    pub started_at: Option<DateTime<Utc>>,

    /// When the job completed (success or failure).
    pub completed_at: Option<DateTime<Utc>>,

    /// Number of retry attempts made.
    pub retry_count: u32,

    /// Maximum retries allowed.
    pub max_retries: u32,

    /// When to attempt the next retry.
    pub next_retry_at: Option<DateTime<Utc>>,

    /// Error message if failed.
    pub error_message: Option<String>,

    /// Final sync statistics (if completed).
    pub sync_stats: Option<SyncStats>,

    /// ID of the worker processing this job.
    pub worker_id: Option<String>,

    /// Whether to force full sync (bypass incremental).
    pub force_full_sync: bool,
}

impl HarvestJob {
    /// Check if the job can be retried.
    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    /// Calculate the next retry time based on current retry count.
    pub fn calculate_next_retry(&self, config: &RetryConfig) -> DateTime<Utc> {
        let delay = config.delay_for_attempt(self.retry_count + 1);
        Utc::now() + delay
    }
}

// =============================================================================
// Job Creation Request
// =============================================================================

/// Request to create a new harvest job.
#[derive(Debug, Clone)]
pub struct CreateJobRequest {
    /// Target portal URL.
    pub portal_url: String,
    /// Optional friendly portal name.
    pub portal_name: Option<String>,
    /// Whether to force full sync.
    pub force_full_sync: bool,
    /// Maximum retries (uses default if None).
    pub max_retries: Option<u32>,
}

impl CreateJobRequest {
    /// Create a new job request for the given portal URL.
    pub fn new(portal_url: impl Into<String>) -> Self {
        Self {
            portal_url: portal_url.into(),
            portal_name: None,
            force_full_sync: false,
            max_retries: None,
        }
    }

    /// Set the portal name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.portal_name = Some(name.into());
        self
    }

    /// Force full sync (bypass incremental).
    pub fn with_full_sync(mut self) -> Self {
        self.force_full_sync = true;
        self
    }

    /// Set maximum retries.
    pub fn with_max_retries(mut self, max: u32) -> Self {
        self.max_retries = Some(max);
        self
    }
}

// =============================================================================
// Worker Configuration
// =============================================================================

/// Worker configuration.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Unique worker identifier.
    pub worker_id: String,
    /// How often to poll for new jobs.
    pub poll_interval: std::time::Duration,
    /// Retry configuration.
    pub retry_config: RetryConfig,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            worker_id: format!("worker-{}", Uuid::new_v4()),
            poll_interval: std::time::Duration::from_secs(5),
            retry_config: RetryConfig::default(),
        }
    }
}

impl WorkerConfig {
    /// Set the worker ID.
    pub fn with_worker_id(mut self, id: impl Into<String>) -> Self {
        self.worker_id = id.into();
        self
    }

    /// Set the poll interval.
    pub fn with_poll_interval(mut self, interval: std::time::Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Set the retry configuration.
    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_job_status_as_str() {
        assert_eq!(JobStatus::Pending.as_str(), "pending");
        assert_eq!(JobStatus::Running.as_str(), "running");
        assert_eq!(JobStatus::Completed.as_str(), "completed");
        assert_eq!(JobStatus::Failed.as_str(), "failed");
        assert_eq!(JobStatus::Cancelled.as_str(), "cancelled");
    }

    #[test]
    fn test_job_status_from_str() {
        assert_eq!("pending".parse::<JobStatus>(), Ok(JobStatus::Pending));
        assert_eq!("running".parse::<JobStatus>(), Ok(JobStatus::Running));
        assert_eq!("completed".parse::<JobStatus>(), Ok(JobStatus::Completed));
        assert_eq!("failed".parse::<JobStatus>(), Ok(JobStatus::Failed));
        assert_eq!("cancelled".parse::<JobStatus>(), Ok(JobStatus::Cancelled));
        assert!("unknown".parse::<JobStatus>().is_err());
    }

    #[test]
    fn test_job_status_is_terminal() {
        assert!(!JobStatus::Pending.is_terminal());
        assert!(!JobStatus::Running.is_terminal());
        assert!(JobStatus::Completed.is_terminal());
        assert!(JobStatus::Failed.is_terminal());
        assert!(JobStatus::Cancelled.is_terminal());
    }

    #[test]
    fn test_retry_config_default() {
        let config = RetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.max_delay, TimeDelta::minutes(60));
    }

    #[test]
    fn test_retry_delay_exponential() {
        let config = RetryConfig::default();

        assert_eq!(config.delay_for_attempt(0), TimeDelta::zero());
        assert_eq!(config.delay_for_attempt(1), TimeDelta::minutes(1));
        assert_eq!(config.delay_for_attempt(2), TimeDelta::minutes(5));
        assert_eq!(config.delay_for_attempt(3), TimeDelta::minutes(30));
        assert_eq!(config.delay_for_attempt(4), TimeDelta::minutes(60)); // capped
        assert_eq!(config.delay_for_attempt(10), TimeDelta::minutes(60)); // still capped
    }

    #[test]
    fn test_create_job_request_builder() {
        let request = CreateJobRequest::new("https://example.com")
            .with_name("Example Portal")
            .with_full_sync()
            .with_max_retries(5);

        assert_eq!(request.portal_url, "https://example.com");
        assert_eq!(request.portal_name, Some("Example Portal".to_string()));
        assert!(request.force_full_sync);
        assert_eq!(request.max_retries, Some(5));
    }

    #[test]
    fn test_worker_config_default() {
        let config = WorkerConfig::default();

        assert!(config.worker_id.starts_with("worker-"));
        assert_eq!(config.poll_interval, std::time::Duration::from_secs(5));
        assert_eq!(config.retry_config.max_retries, 3);
    }

    #[test]
    fn test_worker_config_builder() {
        let config = WorkerConfig::default()
            .with_worker_id("my-worker")
            .with_poll_interval(std::time::Duration::from_secs(10));

        assert_eq!(config.worker_id, "my-worker");
        assert_eq!(config.poll_interval, std::time::Duration::from_secs(10));
    }

    #[test]
    fn test_harvest_job_can_retry() {
        let mut job = HarvestJob {
            id: Uuid::new_v4(),
            portal_url: "https://example.com".to_string(),
            portal_name: None,
            status: JobStatus::Running,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            started_at: Some(Utc::now()),
            completed_at: None,
            retry_count: 0,
            max_retries: 3,
            next_retry_at: None,
            error_message: None,
            sync_stats: None,
            worker_id: Some("worker-1".to_string()),
            force_full_sync: false,
        };

        assert!(job.can_retry());

        job.retry_count = 2;
        assert!(job.can_retry());

        job.retry_count = 3;
        assert!(!job.can_retry());
    }
}
