//! Sync service layer for portal synchronization logic.
//!
//! This module provides pure business logic for delta detection and sync statistics,
//! decoupled from I/O operations and CLI orchestration.

/// Outcome of processing a single dataset during sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncOutcome {
    /// Dataset content hash matches existing - no changes needed
    Unchanged,
    /// Dataset content changed - embedding regenerated
    Updated,
    /// New dataset - first time seeing this dataset
    Created,
    /// Processing failed for this dataset
    Failed,
}

/// Statistics for a portal sync operation.
#[derive(Debug, Default, Clone)]
pub struct SyncStats {
    pub unchanged: usize,
    pub updated: usize,
    pub created: usize,
    pub failed: usize,
}

impl SyncStats {
    /// Creates a new empty stats tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records an outcome, incrementing the appropriate counter.
    pub fn record(&mut self, outcome: SyncOutcome) {
        match outcome {
            SyncOutcome::Unchanged => self.unchanged += 1,
            SyncOutcome::Updated => self.updated += 1,
            SyncOutcome::Created => self.created += 1,
            SyncOutcome::Failed => self.failed += 1,
        }
    }

    /// Returns the total number of processed datasets.
    pub fn total(&self) -> usize {
        self.unchanged + self.updated + self.created + self.failed
    }

    /// Returns the number of successfully processed datasets.
    pub fn successful(&self) -> usize {
        self.unchanged + self.updated + self.created
    }
}

/// Result of delta detection for a dataset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReprocessingDecision {
    /// Whether embedding needs to be regenerated
    pub needs_embedding: bool,
    /// The outcome classification for this dataset
    pub outcome: SyncOutcome,
    /// Human-readable reason for the decision
    pub reason: &'static str,
}

impl ReprocessingDecision {
    /// Returns true if this is a legacy record update (existing record without hash).
    pub fn is_legacy(&self) -> bool {
        self.reason == "legacy record without hash"
    }
}

/// Determines if a dataset needs reprocessing based on content hash comparison.
///
/// # Arguments
/// * `existing_hash` - The stored content hash for this dataset (None if new dataset)
/// * `new_hash` - The computed content hash from the portal data
///
/// # Returns
/// A `ReprocessingDecision` indicating whether embedding regeneration is needed
/// and the classification of this sync operation.
pub fn needs_reprocessing(
    existing_hash: Option<&Option<String>>,
    new_hash: &str,
) -> ReprocessingDecision {
    match existing_hash {
        Some(Some(hash)) if hash == new_hash => {
            // Hash matches - content unchanged
            ReprocessingDecision {
                needs_embedding: false,
                outcome: SyncOutcome::Unchanged,
                reason: "content hash matches",
            }
        }
        Some(Some(_)) => {
            // Hash exists but differs - content updated
            ReprocessingDecision {
                needs_embedding: true,
                outcome: SyncOutcome::Updated,
                reason: "content hash changed",
            }
        }
        Some(None) => {
            // Exists but no hash (legacy data) - treat as update
            ReprocessingDecision {
                needs_embedding: true,
                outcome: SyncOutcome::Updated,
                reason: "legacy record without hash",
            }
        }
        None => {
            // Not in existing data - new dataset
            ReprocessingDecision {
                needs_embedding: true,
                outcome: SyncOutcome::Created,
                reason: "new dataset",
            }
        }
    }
}

// =============================================================================
// Batch Harvest Types
// =============================================================================

/// Result of harvesting a single portal in batch mode.
#[derive(Debug, Clone)]
pub struct PortalHarvestResult {
    /// Portal name identifier.
    pub portal_name: String,
    /// Portal URL.
    pub portal_url: String,
    /// Sync statistics for this portal.
    pub stats: SyncStats,
    /// Error message if harvest failed, None if successful.
    pub error: Option<String>,
}

impl PortalHarvestResult {
    /// Creates a successful harvest result.
    pub fn success(name: String, url: String, stats: SyncStats) -> Self {
        Self {
            portal_name: name,
            portal_url: url,
            stats,
            error: None,
        }
    }

    /// Creates a failed harvest result.
    pub fn failure(name: String, url: String, error: String) -> Self {
        Self {
            portal_name: name,
            portal_url: url,
            stats: SyncStats::default(),
            error: Some(error),
        }
    }

    /// Returns true if the harvest was successful.
    pub fn is_success(&self) -> bool {
        self.error.is_none()
    }
}

/// Aggregated results from batch harvesting multiple portals.
#[derive(Debug, Clone, Default)]
pub struct BatchHarvestSummary {
    /// Results for each portal.
    pub results: Vec<PortalHarvestResult>,
}

impl BatchHarvestSummary {
    /// Creates a new empty summary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a portal harvest result.
    pub fn add(&mut self, result: PortalHarvestResult) {
        self.results.push(result);
    }

    /// Returns the count of successful harvests.
    pub fn successful_count(&self) -> usize {
        self.results.iter().filter(|r| r.is_success()).count()
    }

    /// Returns the count of failed harvests.
    pub fn failed_count(&self) -> usize {
        self.results.iter().filter(|r| !r.is_success()).count()
    }

    /// Returns the total number of datasets across all successful portals.
    pub fn total_datasets(&self) -> usize {
        self.results.iter().map(|r| r.stats.total()).sum()
    }

    /// Returns the total number of portals processed.
    pub fn total_portals(&self) -> usize {
        self.results.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_stats_default() {
        let stats = SyncStats::new();
        assert_eq!(stats.unchanged, 0);
        assert_eq!(stats.updated, 0);
        assert_eq!(stats.created, 0);
        assert_eq!(stats.failed, 0);
    }

    #[test]
    fn test_sync_stats_record() {
        let mut stats = SyncStats::new();
        stats.record(SyncOutcome::Unchanged);
        stats.record(SyncOutcome::Updated);
        stats.record(SyncOutcome::Created);
        stats.record(SyncOutcome::Failed);

        assert_eq!(stats.unchanged, 1);
        assert_eq!(stats.updated, 1);
        assert_eq!(stats.created, 1);
        assert_eq!(stats.failed, 1);
    }

    #[test]
    fn test_sync_stats_total() {
        let mut stats = SyncStats::new();
        stats.unchanged = 10;
        stats.updated = 5;
        stats.created = 3;
        stats.failed = 2;

        assert_eq!(stats.total(), 20);
    }

    #[test]
    fn test_sync_stats_successful() {
        let mut stats = SyncStats::new();
        stats.unchanged = 10;
        stats.updated = 5;
        stats.created = 3;
        stats.failed = 2;

        assert_eq!(stats.successful(), 18);
    }

    #[test]
    fn test_needs_reprocessing_unchanged() {
        let hash = "abc123".to_string();
        let existing = Some(Some(hash.clone()));
        let decision = needs_reprocessing(existing.as_ref(), &hash);

        assert!(!decision.needs_embedding);
        assert_eq!(decision.outcome, SyncOutcome::Unchanged);
        assert_eq!(decision.reason, "content hash matches");
    }

    #[test]
    fn test_needs_reprocessing_updated() {
        let old_hash = "abc123".to_string();
        let new_hash = "def456";
        let existing = Some(Some(old_hash));
        let decision = needs_reprocessing(existing.as_ref(), new_hash);

        assert!(decision.needs_embedding);
        assert_eq!(decision.outcome, SyncOutcome::Updated);
        assert_eq!(decision.reason, "content hash changed");
    }

    #[test]
    fn test_needs_reprocessing_legacy() {
        let existing: Option<Option<String>> = Some(None);
        let decision = needs_reprocessing(existing.as_ref(), "new_hash");

        assert!(decision.needs_embedding);
        assert_eq!(decision.outcome, SyncOutcome::Updated);
        assert_eq!(decision.reason, "legacy record without hash");
    }

    #[test]
    fn test_needs_reprocessing_new() {
        let decision = needs_reprocessing(None, "new_hash");

        assert!(decision.needs_embedding);
        assert_eq!(decision.outcome, SyncOutcome::Created);
        assert_eq!(decision.reason, "new dataset");
    }

    #[test]
    fn test_is_legacy_true() {
        let existing: Option<Option<String>> = Some(None);
        let decision = needs_reprocessing(existing.as_ref(), "new_hash");

        assert!(decision.is_legacy());
    }

    #[test]
    fn test_is_legacy_false() {
        let decision = needs_reprocessing(None, "new_hash");
        assert!(!decision.is_legacy());

        let hash = "abc123".to_string();
        let existing = Some(Some(hash.clone()));
        let decision = needs_reprocessing(existing.as_ref(), &hash);
        assert!(!decision.is_legacy());
    }
}
