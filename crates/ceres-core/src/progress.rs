//! Progress reporting for harvest operations.
//!
//! This module provides a trait-based abstraction for reporting progress during
//! harvest operations, enabling decoupled logging and UI updates.

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

        let stats = SyncStats {
            unchanged: 5,
            updated: 3,
            created: 2,
            failed: 0,
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
    }

    #[test]
    fn test_default_implementations() {
        let silent = SilentReporter;
        silent.report(HarvestEvent::BatchStarted { total_portals: 1 });

        let tracing = TracingReporter;
        tracing.report(HarvestEvent::BatchStarted { total_portals: 1 });
    }
}
