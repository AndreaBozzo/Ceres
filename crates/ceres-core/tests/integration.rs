//! Integration tests for ceres-core crate.
//!
//! This module contains integration tests that verify the core services
//! (`HarvestService`, `SearchService`) using mock implementations of
//! the underlying traits (`DatasetStore`, `EmbeddingProvider`, `PortalClientFactory`).
//!
//! Unlike ceres-db which tests against a real PostgreSQL database,
//! these tests use in-memory mocks to verify business logic in isolation.
//!
//! # Running Tests
//!
//! ```bash
//! # Run all integration tests
//! cargo test --test integration -p ceres-core
//! ```

mod integration {
    pub mod common;
    pub mod harvest_tests;
}
