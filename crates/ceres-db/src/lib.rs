//! Ceres DB - Database repository layer for PostgreSQL with pgvector
//!
//! This crate provides the repository pattern for dataset persistence
//! with pgvector support for semantic search.
//!
//! # Overview
//!
//! The main components are:
//! - [`DatasetRepository`] - Dataset persistence with vector embeddings
//! - [`JobRepository`] - Persistent job queue for harvest tasks

mod job_repository;
mod repository;

pub use job_repository::JobRepository;
pub use repository::{DatasetRepository, PortalSyncStatus};
