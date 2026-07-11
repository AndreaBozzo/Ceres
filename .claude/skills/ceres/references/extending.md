# Extending Ceres

All core components in Ceres are trait-based. You can implement custom embedding providers, portal clients, data stores, and more without modifying the core crate.

## Implementing a Custom EmbeddingProvider

The `EmbeddingProvider` trait (in `ceres-core::traits`) converts text into vector embeddings for semantic search.

```rust
pub trait EmbeddingProvider: Send + Sync + Clone {
    fn name(&self) -> &'static str;
    fn dimension(&self) -> usize;
    fn generate(&self, text: &str) -> impl Future<Output = Result<Vec<f32>, AppError>> + Send;
    fn max_batch_size(&self) -> usize { 1 }  // Override for batch support
    fn generate_batch(&self, texts: &[String]) -> impl Future<Output = Result<Vec<Vec<f32>>, AppError>> + Send;
}
```

### Example: Local Ollama Provider

```rust
use ceres_core::{AppError, traits::EmbeddingProvider};

#[derive(Clone)]
pub struct OllamaClient {
    base_url: String,
    model: String,
    client: reqwest::Client,
}

impl EmbeddingProvider for OllamaClient {
    fn name(&self) -> &'static str { "ollama" }
    fn dimension(&self) -> usize { 768 }  // Depends on model

    async fn generate(&self, text: &str) -> Result<Vec<f32>, AppError> {
        let resp = self.client
            .post(format!("{}/api/embeddings", self.base_url))
            .json(&serde_json::json!({
                "model": self.model,
                "prompt": text
            }))
            .send().await
            .map_err(|e| AppError::ClientError(e.to_string()))?;

        let body: serde_json::Value = resp.json().await
            .map_err(|e| AppError::ClientError(e.to_string()))?;

        body["embedding"].as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect())
            .ok_or_else(|| AppError::EmptyResponse)
    }
}
```

**Built-in implementations:** `OllamaClient`, `GeminiClient`, `OpenAIClient`.

**Important:** The `dimension()` must match the database column. If you change providers, re-create the embedding column with the correct dimension.

## Implementing a Custom PortalClient

The `PortalClient` trait fetches dataset metadata from open data portal APIs.

```rust
pub trait PortalClient: Send + Sync + Clone {
    type PortalData: Send;

    fn portal_type(&self) -> &'static str;
    fn base_url(&self) -> &str;
    fn list_dataset_ids(&self) -> impl Future<Output = Result<Vec<String>, AppError>> + Send;
    fn get_dataset(&self, id: &str) -> impl Future<Output = Result<Self::PortalData, AppError>> + Send;
    fn into_new_dataset(data: Self::PortalData, portal_url: &str, url_template: Option<&str>, language: &str) -> NewDataset;
    fn search_modified_since(&self, since: DateTime<Utc>) -> impl Future<Output = Result<Vec<Self::PortalData>, AppError>> + Send;
    fn search_all_datasets(&self) -> impl Future<Output = Result<Vec<Self::PortalData>, AppError>> + Send;
}
```

Key design notes:

- `PortalData` is an associated type — your raw API response type before normalization.
- `into_new_dataset` is a static method that converts portal-specific data into the normalized `NewDataset` struct. It receives `url_template` and `language` for customization.
- `search_modified_since` enables incremental sync. Return `Err` if the portal doesn't support it — Ceres will fall back to full sync.
- `search_all_datasets` is an optimization for bulk fetch. Default returns an error, falling back to `list_dataset_ids()` + `get_dataset()`.

**Built-in:** `CkanClient` and `DcatClient`.

### PortalClientFactory

Separate factory trait for creating portal clients:

```rust
pub trait PortalClientFactory: Send + Sync + Clone {
    type Client: PortalClient;
    fn create(&self, portal_url: &str, portal_type: PortalType, language: &str) -> Result<Self::Client, AppError>;
}
```

**Built-in:** `PortalClientFactoryEnum` (enum dispatch over supported portal types).

## Implementing a Custom DatasetStore

The `DatasetStore` trait handles all database operations. This is the largest trait in Ceres.

```rust
pub trait DatasetStore: Send + Sync + Clone {
    // Core CRUD
    fn get_by_id(&self, id: Uuid) -> impl Future<Output = Result<Option<Dataset>, AppError>> + Send;
    fn upsert(&self, dataset: &NewDataset) -> impl Future<Output = Result<Uuid, AppError>> + Send;
    fn batch_upsert(&self, datasets: &[NewDataset]) -> impl Future<Output = Result<Vec<Uuid>, AppError>> + Send;

    // Semantic search
    fn search(&self, query_vector: Vec<f32>, limit: usize) -> impl Future<Output = Result<Vec<SearchResult>, AppError>> + Send;

    // Delta detection
    fn get_hashes_for_portal(&self, portal_url: &str) -> impl Future<Output = Result<HashMap<String, Option<String>>, AppError>> + Send;
    fn update_timestamp_only(&self, portal_url: &str, original_id: &str) -> impl Future<Output = Result<(), AppError>> + Send;
    fn batch_update_timestamps(&self, portal_url: &str, original_ids: &[String]) -> impl Future<Output = Result<u64, AppError>> + Send;

    // Streaming export
    fn list_stream<'a>(&'a self, portal_filter: Option<&'a str>, limit: Option<usize>) -> BoxStream<'a, Result<Dataset, AppError>>;

    // Sync tracking
    fn get_last_sync_time(&self, portal_url: &str) -> impl Future<Output = Result<Option<DateTime<Utc>>, AppError>> + Send;
    fn record_sync_status(&self, portal_url: &str, sync_time: DateTime<Utc>, sync_mode: &str, sync_status: &str, datasets_synced: i32) -> impl Future<Output = Result<(), AppError>> + Send;

    // Cross-portal deduplication
    fn get_duplicate_titles(&self) -> impl Future<Output = Result<HashSet<String>, AppError>> + Send;

    // Stale detection
    fn mark_stale_datasets(&self, portal_url: &str, sync_start: DateTime<Utc>) -> impl Future<Output = Result<u64, AppError>> + Send;
    fn mark_stale_by_exclusion(&self, portal_url: &str, seen_ids: &[String]) -> impl Future<Output = Result<u64, AppError>> + Send;

    // Pending embeddings
    fn list_pending_embeddings(&self, portal_filter: Option<&str>, limit: usize) -> impl Future<Output = Result<Vec<Dataset>, AppError>> + Send;

    // Health
    fn health_check(&self) -> impl Future<Output = Result<(), AppError>> + Send;
}
```

**Built-in:** `DatasetRepository` (PostgreSQL + pgvector via SQLx).

## Implementing a Custom JobQueue

The `JobQueue` trait manages persistent harvest job scheduling.

```rust
pub trait JobQueue: Send + Sync + Clone {
    fn enqueue(&self, job: NewHarvestJob) -> impl Future<Output = Result<Uuid, AppError>> + Send;
    fn claim_next(&self, worker_id: &str) -> impl Future<Output = Result<Option<HarvestJob>, AppError>> + Send;
    fn complete(&self, job_id: Uuid, stats: SyncStats) -> impl Future<Output = Result<(), AppError>> + Send;
    fn fail(&self, job_id: Uuid, error: &str) -> impl Future<Output = Result<(), AppError>> + Send;
    fn release(&self, job_id: Uuid) -> impl Future<Output = Result<(), AppError>> + Send;
    fn list_recent(&self, limit: usize) -> impl Future<Output = Result<Vec<HarvestJob>, AppError>> + Send;
    fn cancel_pending(&self) -> impl Future<Output = Result<u64, AppError>> + Send;
}
```

`claim_next` uses `SELECT FOR UPDATE SKIP LOCKED` for safe concurrent processing.

**Built-in:** `HarvestJobRepository` (PostgreSQL).

## Progress Reporting

The `ProgressReporter` trait receives events during harvest operations:

```rust
pub trait ProgressReporter: Send + Sync {
    fn report(&self, event: HarvestEvent);
}
```

`HarvestEvent` variants include: `SyncStarted`, `IncrementalSyncAttempt`, `FallbackToFullSync`, `DatasetProcessed`, `BatchCompleted`, `SyncCompleted`, `Error`.

**Built-in:** `TracingReporter` (logs via `tracing`), `SilentReporter` (no-op).

Similarly, `WorkerReporter` receives `WorkerEvent` variants for background worker lifecycle events.

## Wiring Custom Implementations

Services are generic over their dependencies:

```rust
use ceres_core::{HarvestService, HarvestPipeline, SearchService, ExportService};
use ceres_core::embedding::EmbeddingService;

// Metadata-only harvesting (no embedding provider needed)
let harvest = HarvestService::new(
    my_store.clone(),       // impl DatasetStore
    my_factory.clone(),     // impl PortalClientFactory
);

// Standalone embedding
let embedder = EmbeddingService::new(
    my_store.clone(),       // impl DatasetStore
    my_embedding.clone(),   // impl EmbeddingProvider
);

// Combined harvest + embed pipeline
let pipeline = HarvestPipeline::new(
    my_store.clone(),
    my_embedding.clone(),
    my_factory.clone(),
);

let search = SearchService::new(
    my_store.clone(),
    my_embedding.clone(),
);

let export = ExportService::new(my_store.clone());
```

For `WorkerService`:

```rust
use ceres_core::WorkerService;

let worker = WorkerService::new(
    my_queue.clone(),       // impl JobQueue
    my_store.clone(),
    my_embedding.clone(),
    my_factory.clone(),
);
worker.run(cancel_token).await;
```

## Testing with Mocks

The `ceres-client` crate provides `MockEmbeddingClient` (behind feature flag `test-support`) for unit testing without real API calls.

For integration tests, there are common test helpers in `crates/*/tests/integration/common/` that set up a real PostgreSQL database with test fixtures.
