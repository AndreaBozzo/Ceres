//! Ceres Core - Domain types, error handling, and configuration.

pub mod config;
pub mod error;
pub mod models;

pub use config::{DbConfig, HttpConfig, SyncConfig};
pub use error::AppError;
pub use models::{DatabaseStats, Dataset, NewDataset, Portal, SearchResult};
