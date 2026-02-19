//! Ceres Core - Domain types, business logic, and services.
//!
//! This crate provides the core functionality for Ceres, including:
//!
//! - **Domain models**: [`Dataset`], [`SearchResult`], [`Portal`], etc.
//! - **Business logic**: Delta detection, statistics tracking
//! - **Services**: [`HarvestService`] for portal synchronization, [`SearchService`] for semantic search, [`ExportService`] for streaming exports
//! - **Traits**: [`EmbeddingProvider`], [`DatasetStore`], [`PortalClient`] for dependency injection
//! - **Progress reporting**: [`ProgressReporter`] trait for decoupled logging/UI
//!
//! # Architecture
//!
//! This crate is designed to be reusable by different frontends (CLI, server, etc.).
//! Business logic is decoupled from I/O concerns through traits:
//!
//! # REST API (ceres-server)
//!
//! The REST API uses `utoipa` for automatic OpenAPI documentation with Swagger UI
//! at `/swagger-ui`. See `ceres_server::openapi` for the implementation.
//!
//! - [`EmbeddingProvider`] - abstracts embedding generation (e.g., Gemini API)
//! - [`DatasetStore`] - abstracts database operations (e.g., PostgreSQL)
//! - [`PortalClient`] - abstracts portal access (e.g., CKAN API)
//!
//! # Example
//!
//! ```ignore
//! use ceres_core::{HarvestService, PortalType, SearchService};
//! use ceres_core::progress::TracingReporter;
//!
//! // Create services with your implementations
//! let harvest = HarvestService::new(store, embedding, portal_factory);
//! let reporter = TracingReporter;
//! let stats = harvest
//!     .sync_portal_with_progress("https://data.gov/api/3", None, "en", &reporter, PortalType::Ckan)
//!     .await?;
//!
//! // Semantic search
//! let search = SearchService::new(store, embedding);
//! let results = search.search("climate data", 10).await?;
//! ```

pub mod circuit_breaker;
pub mod config;
pub mod error;
pub mod export;
pub mod harvest;
pub mod i18n;
pub mod job;
pub mod job_queue;
pub mod models;
pub mod parquet_export;
pub mod progress;
pub mod search;
pub mod sync;
pub mod traits;
pub mod worker;

// Circuit breaker
pub use circuit_breaker::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerError, CircuitBreakerStats, CircuitState,
};

// Configuration
pub use config::{
    DbConfig, HttpConfig, PortalEntry, PortalType, PortalsConfig, SyncConfig, default_config_path,
    load_portals_config,
};

// Error handling
pub use error::AppError;

// Internationalization
pub use i18n::LocalizedField;

// Domain models
pub use models::{DatabaseStats, Dataset, NewDataset, SearchResult};

// Sync types and business logic
pub use sync::{
    AlwaysReprocessDetector, AtomicSyncStats, BatchHarvestSummary, ContentHashDetector,
    DeltaDetector, PortalHarvestResult, ReprocessingDecision, SyncOutcome, SyncResult, SyncStats,
    SyncStatus, needs_reprocessing,
};

// Progress reporting
pub use progress::{HarvestEvent, ProgressReporter, SilentReporter, TracingReporter};

// Traits for dependency injection
pub use traits::{DatasetStore, EmbeddingProvider, PortalClient, PortalClientFactory};

// Services (generic over trait implementations)
pub use export::{ExportFormat, ExportService};
pub use harvest::HarvestService;
pub use parquet_export::{ParquetExportConfig, ParquetExportResult, ParquetExportService};
pub use search::SearchService;

// Job queue types
pub use job::{CreateJobRequest, HarvestJob, JobStatus, RetryConfig, WorkerConfig};
pub use job_queue::JobQueue;

// Worker service
pub use worker::{
    SilentWorkerReporter, TracingWorkerReporter, WorkerEvent, WorkerReporter, WorkerService,
};
