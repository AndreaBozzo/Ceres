//! Ceres Core - Domain types, business logic, and services.
//!
//! This crate provides the core functionality for Ceres, including:
//!
//! - **Domain models**: [`Dataset`], [`SearchResult`], [`Portal`], etc.
//! - **Business logic**: Delta detection, statistics tracking
//! - **Services**: [`HarvestService`] for metadata harvesting, [`EmbeddingService`] for
//!   standalone embedding, [`HarvestPipeline`] for combined harvest+embed, [`SearchService`]
//!   for semantic search, [`ExportService`] for streaming exports
//! - **Traits**: [`EmbeddingProvider`], [`DatasetStore`], [`PortalClient`] for dependency injection
//! - **Progress reporting**: [`ProgressReporter`] trait for decoupled logging/UI
//!
//! # Architecture
//!
//! Harvesting and embedding are decoupled: `HarvestService` handles metadata fetching
//! (no embedding provider needed), `EmbeddingService` handles vector generation
//! (no portal access needed), and `HarvestPipeline` composes both for the common workflow.
//!
//! # Example
//!
//! ```ignore
//! use ceres_core::{HarvestService, HarvestPipeline, PortalType, SearchService};
//! use ceres_core::progress::TracingReporter;
//!
//! // Metadata-only harvesting (no API key needed)
//! let harvest = HarvestService::new(store.clone(), portal_factory.clone());
//! let stats = harvest.sync_portal("https://data.gov/api/3").await?;
//!
//! // Combined harvest + embed
//! let pipeline = HarvestPipeline::new(store.clone(), embedding.clone(), portal_factory);
//! let (sync_result, embed_stats) = pipeline
//!     .sync_portal_with_progress("https://data.gov/api/3", None, "en", &TracingReporter, PortalType::Ckan)
//!     .await?;
//!
//! // Semantic search
//! let search = SearchService::new(store, embedding);
//! let results = search.search("climate data", 10).await?;
//! ```

pub mod circuit_breaker;
pub mod config;
pub mod embedding;
pub mod error;
pub mod export;
pub mod harvest;
pub mod i18n;
pub mod job;
pub mod job_queue;
pub mod models;
pub mod parquet_export;
pub mod pipeline;
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
    DbConfig, EmbeddingServiceConfig, HarvestConfig, HttpConfig, PortalEntry, PortalType,
    PortalsConfig, SyncConfig, default_config_path, load_portals_config,
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
pub use embedding::{EmbeddingService, EmbeddingStats};
pub use export::{ExportFormat, ExportService};
pub use harvest::HarvestService;
pub use parquet_export::{ParquetExportConfig, ParquetExportResult, ParquetExportService};
pub use pipeline::HarvestPipeline;
pub use search::SearchService;

// Job queue types
pub use job::{CreateJobRequest, HarvestJob, JobStatus, RetryConfig, WorkerConfig};
pub use job_queue::JobQueue;

// Worker service
pub use worker::{
    SilentWorkerReporter, TracingWorkerReporter, WorkerEvent, WorkerReporter, WorkerService,
};
