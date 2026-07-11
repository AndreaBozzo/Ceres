# Architecture

## Crate Dependency Graph

```
ceres-cli ──────┐
                 ├──> ceres-core <── ceres-client
ceres-server ───┘         ^
                          |
                       ceres-db
```

- **ceres-cli** and **ceres-server** are the two binaries. They depend on all other crates.
- **ceres-core** is the heart: business logic, traits, services, error types. Zero I/O.
- **ceres-client** implements external API clients (CKAN, DCAT udata REST, SPARQL-backed DCAT, Ollama, Gemini, OpenAI). Depends on ceres-core for traits and error types.
- **ceres-db** implements the `DatasetStore` and `JobQueue` traits using PostgreSQL + pgvector.

## Services

### HarvestService

Generic over `<S: DatasetStore, F: PortalClientFactory>`. No embedding provider needed — harvesting is metadata-only.

| Constructor | Purpose |
|---|---|
| `new(store, portal_factory)` | Default config |
| `with_config(store, portal_factory, config)` | Custom `HarvestConfig` |

Key methods:

- `sync_portal_with_progress(portal_url, url_template, language, portal_type, force_full_sync, dry_run, reporter, cancel)` — Syncs a single portal with streaming pipeline. Returns `SyncResult`.
- `batch_harvest_with_progress(portals, force_full_sync, dry_run, reporter, cancel)` — Syncs all enabled portals sequentially. Returns `BatchHarvestSummary`.

The streaming pipeline processes datasets in batches without loading the entire portal into memory, supporting 100k+ dataset portals with a constant memory footprint. After a successful full sync with zero failures, stale detection marks removed datasets via `mark_stale_by_exclusion()`.

Current client-factory support covers CKAN and DCAT profiles: `udata_rest` by default and `sparql` for SPARQL-backed catalogs such as `data.europa.eu`. The `PortalType` enum also models additional future portal families such as Socrata.

### EmbeddingService

Generic over `<S: DatasetStore, E: EmbeddingProvider>`.

Standalone service for generating embeddings on datasets with `embedding IS NULL`. Decoupled from harvesting — enables API-key-less metadata syncs, provider switching without re-harvesting, and backfilling after outages.

Key method: `embed_pending(portal_filter, reporter, cancel_token)` → `EmbeddingStats`.

### HarvestPipeline

Composes `HarvestService` + `EmbeddingService` for the common harvest-then-embed workflow. This is convenience composition, not a hard architectural dependency: the two services are meant to operate independently.

### SearchService

Generic over `<S: DatasetStore, E: EmbeddingProvider>`.

Simple flow: generates embedding for query text, calls `store.search(vector, limit)`, returns ranked `SearchResult` list.

### ExportService

Generic over `<S: DatasetStore>`.

Streaming export to sync or async writers:

- `export_to_writer(writer, format, portal_filter, limit)` — Sync writer (stdout, File)
- `export_to_async_writer(writer, format, portal_filter, limit)` — Async writer (Axum response body)

Formats: JSONL (default), JSON, CSV.

### ParquetExportService

Generic over `<S: DatasetStore>`.

Curated Parquet export for HuggingFace dataset publication:

- `all.parquet` (canonical complete index) plus per-portal subset files with human-readable names; subset row counts repeat rows from `all.parquet`
- `identity.parquet` slim snapshot fingerprint (`source_portal`, `original_id`, `content_hash`) for snapshot-to-snapshot diffs
- Cross-portal duplicate **flagging** via `is_duplicate` (same title across canonicalized portals); duplicates are kept, not removed
- Noise filtering (short titles, placeholder descriptions)
- Flattened schema (no nested JSON)
- Portal naming and language resolution from `portals.toml` when available
- Versioned snapshot manifest `metadata.json`: stable `snapshot_id`, UTC `generated_at`, Ceres version/commit, portal-config checksum, alias-aware duplicate-detection provenance, curation row counts, per-portal inclusion status, and SHA-256 checksums for every file
- Coverage and quality reports `reports.json` (machine-readable) and `report.md` (human-readable): coverage by portal/type/profile/language, field-completeness rates, and curation outcomes — derived from the same pass so figures agree with the manifest
- Snapshot changelogs `changelog.json` and `changelog.md` when a previous snapshot directory is supplied

### WorkerService

Generic over `<Q: JobQueue, S: DatasetStore, E: EmbeddingProvider, F: PortalClientFactory>`.

Poll-based background job processor. Polls the `harvest_jobs` queue, claims pending jobs via `SELECT FOR UPDATE SKIP LOCKED`, and runs `sync_portal_with_progress`. Supports graceful shutdown via `CancellationToken`.

### CircuitBreaker

Protects the embedding API from cascading failures:

```
CLOSED (healthy) ──[N failures]──> OPEN (rejecting) ──[timeout]──> HALF_OPEN (probing)
       ^                                                                |
       └───────────────[success_threshold successes]────────────────────┘
                                    <──[any failure]──
```

Default config:

| Parameter | Default | Env Var |
|---|---|---|
| `failure_threshold` | 5 | `CB_FAILURE_THRESHOLD` |
| `success_threshold` | 2 | `CB_SUCCESS_THRESHOLD` |
| `recovery_timeout` | 30s | `CB_RECOVERY_TIMEOUT_SECS` |
| `rate_limit_backoff_multiplier` | 2.0 | `CB_RATE_LIMIT_BACKOFF_MULTIPLIER` |
| `max_recovery_timeout` | 300s (5 min) | `CB_MAX_RECOVERY_TIMEOUT_SECS` |

On HTTP 429 (rate limit), recovery timeout is multiplied by the backoff factor, capped at `max_recovery_timeout`.

## Error Handling

`AppError` (in `ceres_core::error`) is the unified error type. Key variants:

| Variant | Retryable? | Trips Circuit? | Description |
|---|---|---|---|
| `DatabaseError(String)` | No | No | Database operation failed |
| `ClientError(String)` | Yes | Conditional | HTTP client error (trips circuit if timeout/connection) |
| `GeminiError(GeminiErrorDetails)` | Conditional | Conditional | Gemini API error with structured kind |
| `SerializationError(serde_json::Error)` | No | No | JSON parse error |
| `InvalidUrl(String)` | No | No | URL parse failure |
| `DatasetNotFound(String)` | No | No | Dataset not in DB |
| `InvalidPortalUrl(String)` | No | No | Bad portal URL |
| `EmptyResponse` | No | No | API returned empty body |
| `NetworkError(String)` | Yes | Yes | Connection/DNS failure |
| `Timeout(u64)` | Yes | Yes | Request timeout |
| `RateLimitExceeded` | Yes | Yes | Too many requests |
| `ConfigError(String)` | No | No | Config file error |
| `Generic(String)` | No | No | Catch-all |

`GeminiErrorKind` sub-classification: `Authentication` (401), `RateLimit` (429), `QuotaExceeded`, `ServerError` (5xx), `NetworkError`, `Unknown`.

- Rate limit and server errors are retryable and trip the circuit.
- Authentication and quota errors are **not** retryable and do **not** trip the circuit.

## Database Schema

PostgreSQL 16+ with pgvector extension.

### `datasets` table

```sql
CREATE TABLE datasets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_id VARCHAR NOT NULL,
    source_portal VARCHAR NOT NULL,
    url VARCHAR NOT NULL,
    title TEXT NOT NULL,
    description TEXT,
    embedding vector(768),          -- Gemini: 768d (dimension varies by provider)
    metadata JSONB DEFAULT '{}',
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    content_hash VARCHAR(64),       -- SHA-256 for delta detection
    is_stale BOOLEAN NOT NULL DEFAULT FALSE,  -- Soft-deleted from portal
    CONSTRAINT uk_portal_original_id UNIQUE (source_portal, original_id)
);
-- HNSW index for fast approximate nearest neighbor search
CREATE INDEX ON datasets USING hnsw (embedding vector_cosine_ops);
-- Partial index for pending embedding queries
CREATE INDEX idx_datasets_pending_embedding ON datasets (source_portal, last_updated_at DESC)
    WHERE embedding IS NULL AND NOT is_stale;
```

### `portal_sync_status` table

```sql
CREATE TABLE portal_sync_status (
    portal_url VARCHAR PRIMARY KEY,
    last_successful_sync TIMESTAMPTZ,
    last_sync_mode VARCHAR(20),     -- 'full' or 'incremental'
    sync_status VARCHAR(20),        -- 'completed' or 'cancelled'
    datasets_synced INTEGER DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

### `harvest_jobs` table

```sql
CREATE TABLE harvest_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    portal_url VARCHAR NOT NULL,
    portal_name VARCHAR,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending/running/completed/failed/cancelled
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    next_retry_at TIMESTAMPTZ,
    error_message TEXT,
    sync_stats JSONB,
    worker_id VARCHAR(255),
    force_full_sync BOOLEAN NOT NULL DEFAULT FALSE,
    url_template TEXT,
    language VARCHAR(10) DEFAULT 'en'
);
```

Job claiming uses `SELECT FOR UPDATE SKIP LOCKED` for safe concurrent processing.

## Project Structure

```
crates/
├── ceres-cli/src/
│   ├── main.rs              # CLI entry point, command routing
│   └── config.rs            # Clap command definitions
├── ceres-server/src/
│   ├── main.rs              # Server init, graceful shutdown, worker spawning
│   ├── router.rs            # Axum route definitions
│   ├── auth.rs              # Bearer token middleware
│   ├── openapi.rs           # Swagger UI config
│   ├── state.rs             # Shared AppState
│   ├── config.rs            # Server config
│   ├── dto/                 # Data transfer objects
│   └── handlers/            # Endpoint handlers
├── ceres-core/src/
│   ├── traits.rs            # Core trait definitions
│   ├── models.rs            # Dataset, NewDataset, SearchResult, DatabaseStats
│   ├── harvest.rs           # HarvestService (metadata-only, no embedding)
│   ├── embedding.rs         # EmbeddingService (standalone embedding)
│   ├── pipeline.rs          # HarvestPipeline (harvest + embed combined)
│   ├── search.rs            # SearchService
│   ├── export.rs            # ExportService (JSONL/JSON/CSV)
│   ├── parquet_export.rs    # ParquetExportService
│   ├── sync.rs              # SyncOutcome, SyncStats, DeltaDetector
│   ├── circuit_breaker.rs   # CircuitBreaker state machine
│   ├── job.rs               # HarvestJob, JobStatus
│   ├── job_queue.rs         # JobQueue trait
│   ├── worker.rs            # WorkerService
│   ├── config.rs            # PortalEntry, PortalsConfig, HarvestConfig
│   ├── error.rs             # AppError, GeminiErrorKind
│   └── progress.rs          # ProgressReporter, HarvestEvent
├── ceres-client/src/
│   ├── ckan.rs              # CkanClient (PortalClient impl)
│   ├── dcat.rs              # DcatClient (PortalClient impl)
│   ├── sparql.rs            # SparqlDcatClient (PortalClient impl)
│   ├── gemini.rs            # GeminiClient (EmbeddingProvider impl)
│   ├── ollama.rs            # OllamaClient (EmbeddingProvider impl)
│   ├── openai.rs            # OpenAIClient (EmbeddingProvider impl)
│   ├── provider.rs          # EmbeddingProviderEnum, factory
│   └── portal.rs            # PortalClientFactoryEnum
└── ceres-db/src/
    ├── repository.rs        # DatasetRepository (DatasetStore impl)
    └── job_repository.rs    # HarvestJobRepository (JobQueue impl)
```
