//! Ceres Core - Domain types, error handling, and configuration.

pub mod config;
pub mod error;
pub mod models;
pub mod sync;

pub use config::{
    default_config_path, load_portals_config, DbConfig, HttpConfig, PortalEntry, PortalsConfig,
    SyncConfig,
};
pub use error::AppError;
pub use models::{DatabaseStats, Dataset, NewDataset, Portal, SearchResult};
pub use sync::{
    needs_reprocessing, BatchHarvestSummary, PortalHarvestResult, ReprocessingDecision,
    SyncOutcome, SyncStats,
};
