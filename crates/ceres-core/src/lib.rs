//! Ceres Core - Domain types, error handling, and configuration.

pub mod config;
pub mod error;
pub mod models;
pub mod sync;

pub use config::{
    DbConfig, HttpConfig, PortalEntry, PortalsConfig, SyncConfig, default_config_path,
    load_portals_config,
};
pub use error::AppError;
pub use models::{DatabaseStats, Dataset, NewDataset, Portal, SearchResult};
pub use sync::{
    BatchHarvestSummary, PortalHarvestResult, ReprocessingDecision, SyncOutcome, SyncStats,
    needs_reprocessing,
};
