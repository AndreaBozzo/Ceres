//! Ceres - Semantic search engine for open data portals
//!
//! Ceres is a Rust application that harvests datasets from CKAN-based open data portals,
//! generates semantic embeddings using OpenAI, and stores them in PostgreSQL with pgvector
//! for efficient similarity search.
//!
//! # Architecture
//!
//! The application is organized into several modules:
//!
//! - [`clients`] - HTTP clients for external APIs (CKAN, OpenAI)
//! - [`storage`] - Database repository layer for PostgreSQL operations
//! - [`models`] - Domain models and data transfer objects
//! - [`error`] - Application-wide error types
//! - [`config`] - CLI configuration and command parsing
//!
//! # Quick Start
//!
//! ```no_run
//! use ceres::clients::ckan::CkanClient;
//! use ceres::clients::openai::OpenAIClient;
//! use ceres::storage::DatasetRepository;
//! use sqlx::postgres::PgPoolOptions;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Connect to database
//! let pool = PgPoolOptions::new()
//!     .max_connections(5)
//!     .connect("postgresql://localhost/ceres")
//!     .await?;
//!
//! // Initialize clients
//! let ckan = CkanClient::new("https://dati.gov.it")?;
//! let openai = OpenAIClient::new("your-api-key");
//! let repo = DatasetRepository::new(pool);
//!
//! // Fetch and index datasets
//! let ids = ckan.list_package_ids().await?;
//! for id in ids.iter().take(10) {
//!     let dataset = ckan.show_package(id).await?;
//!     let mut new_dataset = CkanClient::into_new_dataset(dataset, "https://dati.gov.it");
//!
//!     // Generate embedding
//!     let embedding = openai.get_embeddings(&new_dataset.title).await?;
//!     new_dataset.embedding = Some(pgvector::Vector::from(embedding));
//!
//!     // Store in database
//!     repo.upsert(&new_dataset).await?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Features
//!
//! - **CKAN Integration**: Harvest datasets from any CKAN-based portal
//! - **OpenAI Embeddings**: Generate semantic vectors using text-embedding-3-small
//! - **Vector Search**: Store and query embeddings using PostgreSQL pgvector
//! - **Incremental Updates**: Upsert logic prevents duplicates
//! - **Type Safety**: Compile-time SQL verification with sqlx

pub mod clients {
    //! HTTP clients for external APIs.
    //!
    //! This module provides clients for interacting with:
    //! - CKAN open data portals
    //! - OpenAI embeddings API

    pub mod ckan;
    pub mod openai;
}

pub mod config;
pub mod error;
pub mod models;

pub mod storage {
    //! Database repository layer for PostgreSQL operations.
    //!
    //! This module provides the repository pattern for dataset persistence
    //! with pgvector support for semantic search.

    mod pg;
    pub use pg::DatasetRepository;
}

// Re-export commonly used items for easier access
pub use error::AppError;
