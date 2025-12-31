//! Integration tests for ceres-db crate.
//!
//! This module contains integration tests that verify the repository layer
//! against a real PostgreSQL database with pgvector extension.
//!
//! Each test runs in an isolated container for complete test isolation.
//!
//! # Running Tests
//!
//! ```bash
//! # Run all integration tests (requires Docker)
//! cargo test --test integration
//! ```

mod integration {
    pub mod common;
    pub mod repository_tests;
}
