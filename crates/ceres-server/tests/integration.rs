//! Integration tests for ceres-server.
//!
//! Tests handler endpoints via HTTP using the actual router, with a real
//! PostgreSQL database (testcontainers) and a mock embedding provider.
//!
//! # Requirements
//!
//! - Docker must be running (for testcontainers).
//!
//! # Running Tests
//!
//! ```bash
//! cargo test --test integration -p ceres-server
//! ```

mod integration {
    pub mod common;
    pub mod handler_tests;
}
