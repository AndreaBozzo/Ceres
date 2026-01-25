//! Ceres Server - REST API for Ceres semantic search
//!
//! This crate provides an HTTP API for accessing Ceres functionality:
//!
//! - **Search**: Semantic search across indexed datasets
//! - **Portals**: Portal management and statistics
//! - **Harvest**: Trigger and monitor data harvesting
//! - **Export**: Export datasets in various formats
//!
//! # API Documentation
//!
//! When running the server, interactive API documentation is available
//! at `/swagger-ui`.

pub mod config;
pub mod dto;
pub mod error;
pub mod handlers;
pub mod openapi;
pub mod router;
pub mod state;

pub use config::ServerConfig;
pub use error::ApiError;
pub use router::create_router;
pub use state::AppState;
