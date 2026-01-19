//! Progress reporting for harvest operations.
//!
//! This module provides a trait-based abstraction for reporting progress during
//! harvest operations, enabling decoupled logging and UI updates.

use std::time::Duration;

use crate::circuit_breaker::CircuitState;
use crate::{BatchHarvestSummary, SyncStats};

/// Events emitted during harvesting operations.
///
/// These events provide fine-grained progress information that consumers
/// can use for logging, UI updates, or metrics collection.
#[derive(Debug, Clone)]
pub enum HarvestEvent<'a> {
    /// Batch harvest starting.
    BatchStarted {
        /// Total number of portals to harvest.
        total_portals: usize,
    },

    /// Single portal harvest starting.
    PortalStarted {
        /// Zero-based index of the current portal.
        portal_index: usize,
        /// Total number of portals in batch.
        total_portals: usize,
        /// Portal name identifier.
        portal_name: &'a str,
        /// Portal URL.
        portal_url: &'a str,
    },

    /// Found existing datasets in database for portal.
    ExistingDatasetsFound {
        /// Number of existing datasets.
        count: usize,
    },

    /// Found datasets on the portal.
    PortalDatasetsFound {
        /// Number of datasets found.
        count: usize,
    },

    /// Progress update during dataset processing.
    DatasetProcessed {
        /// Number of datasets processed so far.
        current: usize,
        /// Total number of datasets to process.
        total: usize,
        /// Counts by outcome type.
        created: usize,
        /// Number of updated datasets.
        updated: usize,
        /// Number of unchanged datasets.
        unchanged: usize,
        /// Number of failed datasets.
        failed: usize,
        /// Number of datasets skipped due to circuit breaker.
        skipped: usize,
    },

    /// Single portal harvest completed successfully.
    PortalCompleted {
        /// Zero-based index of the current portal.
        portal_index: usize,
        /// Total number of portals in batch.
        total_portals: usize,
        /// Portal name identifier.
        portal_name: &'a str,
        /// Final statistics.
        stats: &'a SyncStats,
    },

    /// Single portal harvest failed.
    PortalFailed {
        /// Zero-based index of the current portal.
        portal_index: usize,
        /// Total number of portals in batch.
        total_portals: usize,
        /// Portal name identifier.
        portal_name: &'a str,
        /// Error description.
        error: &'a str,
    },

    /// Batch harvest completed.
    BatchCompleted {
        /// Aggregated summary of all portal results.
        summary: &'a BatchHarvestSummary,
    },

    /// Single portal harvest was cancelled.
    PortalCancelled {
        /// Zero-based index of the current portal.
        portal_index: usize,
        /// Total number of portals in batch.
        total_portals: usize,
        /// Portal name identifier.
        portal_name: &'a str,
        /// Partial statistics at cancellation time.
        stats: &'a SyncStats,
    },

    /// Batch harvest was cancelled.
    BatchCancelled {
        /// Number of portals completed before cancellation.
        completed_portals: usize,
        /// Total number of portals in batch.
        total_portals: usize,
    },

    /// Circuit breaker state changed.
    CircuitBreakerStateChanged {
        /// Service name (e.g., "gemini", "ckan").
        service: &'a str,
        /// Previous state.
        from_state: CircuitState,
        /// New state.
        to_state: CircuitState,
    },

    /// Circuit breaker is open, harvest pausing/failing.
    CircuitBreakerOpen {
        /// Service name.
        service: &'a str,
        /// Time until recovery attempt.
        retry_after: Duration,
    },

    /// Dataset skipped due to circuit breaker being open.
    DatasetSkipped {
        /// Dataset ID.
        dataset_id: &'a str,
        /// Reason for skipping.
        reason: &'a str,
    },
}

/// Trait for reporting harvest progress.
///
/// Implementors can provide CLI output, server event streams, metrics,
/// or any other form of progress reporting.
///
/// The default implementation does nothing (silent mode), which is
/// appropriate for library usage where the caller doesn't need progress updates.
///
/// # Example
///
/// ```
/// use ceres_core::progress::{ProgressReporter, HarvestEvent};
///
/// struct MyReporter;
///
/// impl ProgressReporter for MyReporter {
///     fn report(&self, event: HarvestEvent<'_>) {
///         match event {
///             HarvestEvent::PortalStarted { portal_name, .. } => {
///                 println!("Starting: {}", portal_name);
///             }
///             _ => {}
///         }
///     }
/// }
/// ```
pub trait ProgressReporter: Send + Sync {
    /// Called when a harvest event occurs.
    ///
    /// The default implementation does nothing (silent mode).
    fn report(&self, event: HarvestEvent<'_>) {
        // Default: do nothing (silent mode for library usage)
        let _ = event;
    }
}

/// A no-op reporter that ignores all events.
///
/// Use this when you don't need progress reporting (library mode).
#[derive(Debug, Default, Clone, Copy)]
pub struct SilentReporter;

impl ProgressReporter for SilentReporter {}

/// A reporter that logs events using the `tracing` crate.
///
/// This is suitable for CLI applications that want structured logging.
#[derive(Debug, Default, Clone, Copy)]
pub struct TracingReporter;

impl ProgressReporter for TracingReporter {
    fn report(&self, event: HarvestEvent<'_>) {
        use tracing::{error, info};

        match event {
            HarvestEvent::BatchStarted { total_portals } => {
                info!("Starting batch harvest of {} portal(s)", total_portals);
            }
            HarvestEvent::PortalStarted {
                portal_index,
                total_portals,
                portal_name,
                portal_url,
            } => {
                info!(
                    "[Portal {}/{}] {} ({})",
                    portal_index + 1,
                    total_portals,
                    portal_name,
                    portal_url
                );
            }
            HarvestEvent::ExistingDatasetsFound { count } => {
                info!("Found {} existing dataset(s) in database", count);
            }
            HarvestEvent::PortalDatasetsFound { count } => {
                info!("Found {} dataset(s) on portal", count);
            }
            HarvestEvent::DatasetProcessed {
                current,
                total,
                created,
                updated,
                unchanged,
                failed,
                skipped,
            } => {
                let pct = (current as f64 / total as f64 * 100.0) as u8;
                if skipped > 0 {
                    info!(
                        "Progress: {}/{} ({}%) - {} new, {} updated, {} unchanged, {} failed, {} skipped",
                        current, total, pct, created, updated, unchanged, failed, skipped
                    );
                } else {
                    info!(
                        "Progress: {}/{} ({}%) - {} new, {} updated, {} unchanged, {} failed",
                        current, total, pct, created, updated, unchanged, failed
                    );
                }
            }
            HarvestEvent::PortalCompleted {
                portal_index,
                total_portals,
                portal_name,
                stats,
            } => {
                info!(
                    "[Portal {}/{}] {} completed: {} dataset(s) ({} created, {} updated, {} unchanged)",
                    portal_index + 1,
                    total_portals,
                    portal_name,
                    stats.total(),
                    stats.created,
                    stats.updated,
                    stats.unchanged
                );
            }
            HarvestEvent::PortalFailed {
                portal_index,
                total_portals,
                portal_name,
                error,
            } => {
                error!(
                    "[Portal {}/{}] {} failed: {}",
                    portal_index + 1,
                    total_portals,
                    portal_name,
                    error
                );
            }
            HarvestEvent::BatchCompleted { summary } => {
                info!(
                    "Batch complete: {} portal(s), {} dataset(s) ({} successful, {} failed)",
                    summary.total_portals(),
                    summary.total_datasets(),
                    summary.successful_count(),
                    summary.failed_count()
                );
            }
            HarvestEvent::PortalCancelled {
                portal_index,
                total_portals,
                portal_name,
                stats,
            } => {
                info!(
                    "[Portal {}/{}] {} cancelled: {} dataset(s) processed ({} created, {} updated, {} unchanged)",
                    portal_index + 1,
                    total_portals,
                    portal_name,
                    stats.total(),
                    stats.created,
                    stats.updated,
                    stats.unchanged
                );
            }
            HarvestEvent::BatchCancelled {
                completed_portals,
                total_portals,
            } => {
                info!(
                    "Batch cancelled: {}/{} portal(s) completed before cancellation",
                    completed_portals, total_portals
                );
            }
            HarvestEvent::CircuitBreakerStateChanged {
                service,
                from_state,
                to_state,
            } => {
                use tracing::warn;
                warn!(
                    "Circuit breaker '{}' state changed: {} -> {}",
                    service, from_state, to_state
                );
            }
            HarvestEvent::CircuitBreakerOpen {
                service,
                retry_after,
            } => {
                use tracing::warn;
                warn!(
                    "Circuit breaker '{}' is open. Retry after {} seconds.",
                    service,
                    retry_after.as_secs()
                );
            }
            HarvestEvent::DatasetSkipped { dataset_id, reason } => {
                use tracing::warn;
                warn!("Dataset '{}' skipped: {}", dataset_id, reason);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_silent_reporter_does_nothing() {
        let reporter = SilentReporter;
        // Should not panic
        reporter.report(HarvestEvent::BatchStarted { total_portals: 5 });
    }

    #[test]
    fn test_tracing_reporter_handles_all_events() {
        let reporter = TracingReporter;

        // Test all event variants don't panic
        reporter.report(HarvestEvent::BatchStarted { total_portals: 2 });
        reporter.report(HarvestEvent::PortalStarted {
            portal_index: 0,
            total_portals: 2,
            portal_name: "test",
            portal_url: "https://example.com",
        });
        reporter.report(HarvestEvent::ExistingDatasetsFound { count: 10 });
        reporter.report(HarvestEvent::PortalDatasetsFound { count: 20 });
        reporter.report(HarvestEvent::DatasetProcessed {
            current: 10,
            total: 20,
            created: 2,
            updated: 3,
            unchanged: 5,
            failed: 0,
            skipped: 0,
        });

        let stats = SyncStats {
            unchanged: 5,
            updated: 3,
            created: 2,
            failed: 0,
            skipped: 0,
        };
        reporter.report(HarvestEvent::PortalCompleted {
            portal_index: 0,
            total_portals: 2,
            portal_name: "test",
            stats: &stats,
        });
        reporter.report(HarvestEvent::PortalFailed {
            portal_index: 1,
            total_portals: 2,
            portal_name: "test2",
            error: "connection failed",
        });

        let summary = BatchHarvestSummary::new();
        reporter.report(HarvestEvent::BatchCompleted { summary: &summary });

        // Test cancellation events
        reporter.report(HarvestEvent::PortalCancelled {
            portal_index: 0,
            total_portals: 2,
            portal_name: "test",
            stats: &stats,
        });
        reporter.report(HarvestEvent::BatchCancelled {
            completed_portals: 1,
            total_portals: 3,
        });

        // Test circuit breaker events
        reporter.report(HarvestEvent::CircuitBreakerStateChanged {
            service: "gemini",
            from_state: CircuitState::Closed,
            to_state: CircuitState::Open,
        });
        reporter.report(HarvestEvent::CircuitBreakerOpen {
            service: "gemini",
            retry_after: Duration::from_secs(30),
        });
        reporter.report(HarvestEvent::DatasetSkipped {
            dataset_id: "test-dataset-123",
            reason: "circuit breaker open",
        });
    }

    #[test]
    fn test_default_implementations() {
        let silent = SilentReporter;
        silent.report(HarvestEvent::BatchStarted { total_portals: 1 });

        let tracing = TracingReporter;
        tracing.report(HarvestEvent::BatchStarted { total_portals: 1 });
    }
}
