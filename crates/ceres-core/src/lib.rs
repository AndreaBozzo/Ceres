//! Ceres Core - Domain types and error handling
//!
//! This crate provides the core domain types used throughout the Ceres application:
//!
//! - [`models`] - Domain models and data transfer objects
//! - [`error`] - Application-wide error types
//!
//! # Overview
//!
//! The `ceres-core` crate is designed to be dependency-light and provides the
//! foundational types that other crates in the workspace depend on.

pub mod error;
pub mod models;

// Re-export commonly used items for easier access
pub use error::AppError;
pub use models::{DatabaseStats, Dataset, NewDataset, Portal, SearchResult};
