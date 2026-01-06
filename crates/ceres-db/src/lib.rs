//! Ceres DB - Database repository layer for PostgreSQL with pgvector
//!
//! This crate provides the repository pattern for dataset persistence
//! with pgvector support for semantic search.
//!
//! # Overview
//!
//! The main component is [`DatasetRepository`], which provides methods for:
//! - Upserting datasets
//! - Retrieving datasets by ID
//! - Semantic search using vector similarity
//! - Database statistics

mod repository;

pub use repository::{DatasetRepository, PortalSyncStatus};
