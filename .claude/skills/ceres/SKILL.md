---
name: ceres
description: Use when working with Ceres — a Rust harvest-first toolkit and public open data metadata index. Covers harvesting and synchronization, optional embedding and search, CKAN, DCAT udata, SPARQL-backed DCAT, Project Open Data data.json, Socrata Discovery, OpenDataSoft Explore, ArcGIS Hub, OGC CSW, and collection-level STAC portal support, Parquet snapshot manifests/reports/changelogs, Ollama or hosted providers, CLI commands, REST API endpoints, portal configuration, architecture, extending via traits, release workflow, and contributing to the Ceres codebase.
---

# Ceres — Harvest-First Toolkit for Open Data Portals

Ceres is centered on harvesting and synchronizing open data metadata. Embeddings, semantic search, exports, and API access are downstream capabilities layered on top of the harvested catalog.

**Repository:** https://github.com/AndreaBozzo/Ceres
**License:** Apache-2.0 | **Rust edition:** 2024 | **MSRV:** 1.95+

## Pipeline

```
Metadata:  Portal URL → PortalClient (fetch) → DeltaDetector (content_hash) → DatasetStore (upsert, no embedding)
Embedding: DatasetStore (pending) → EmbeddingProvider (vector) → DatasetStore (update embedding)
Combined:  HarvestPipeline = HarvestService + EmbeddingService
```

Harvesting and embedding are decoupled: `HarvestService` handles metadata, `EmbeddingService` handles vectors, and `HarvestPipeline` composes both when you want a combined workflow. Metadata-only harvests require no embedding provider. Each stage is trait-based, so components can be swapped or mocked independently.

## Current Product Shape

- Harvest first and keep metadata synchronized over time
- Add embeddings later only if you want semantic retrieval
- Prefer Ollama for local embedding, with Gemini and OpenAI still supported
- Support CKAN, DCAT-AP udata REST, SPARQL-backed DCAT, Project Open Data `data.json`, Socrata Discovery, OpenDataSoft Explore, ArcGIS Hub, OGC CSW, and collection-level STAC portals in the current client factory
- Publish reproducible Parquet snapshots for the public Open Data Index
- Expose search, export, and API workflows over the same harvested catalog

## Crate Map

| Crate | Purpose | Key Exports |
|---|---|---|
| `ceres-core` | Business logic, traits, services | `HarvestService`, `EmbeddingService`, `HarvestPipeline`, `SearchService`, `ExportService`, `WorkerService`, `CircuitBreaker`, traits |
| `ceres-client` | Portal clients and embedding providers | `CkanClient`, `DcatClient`, `ArcGisClient`, `OgcRecordsClient`, `StacClient`, `GeminiClient`, `OpenAIClient`, `OllamaClient`, `PortalClientFactoryEnum`, `EmbeddingProviderEnum` |
| `ceres-db` | PostgreSQL + pgvector repository | `DatasetRepository`, `HarvestJobRepository` |
| `ceres-server` | Axum REST API with Swagger UI | Routes, DTOs, bearer auth, OpenAPI/Swagger |
| `ceres-cli` | Command-line interface | `harvest`, `embed`, `search`, `export`, `stats` subcommands |

## Core Traits (`ceres-core::traits`)

```rust
pub trait EmbeddingProvider: Send + Sync + Clone {
    fn name(&self) -> &'static str;
    fn dimension(&self) -> usize;
    fn generate(&self, text: &str) -> impl Future<Output = Result<Vec<f32>, AppError>> + Send;
    fn max_batch_size(&self) -> usize { 1 }
    fn generate_batch(&self, texts: &[String]) -> impl Future<Output = Result<Vec<Vec<f32>>, AppError>> + Send;
}

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

pub trait PortalClientFactory: Send + Sync + Clone {
    type Client: PortalClient;
    fn create(&self, portal_url: &str, portal_type: PortalType, language: &str, profile: Option<DcatProfile>, sparql_endpoint: Option<&str>, ogc_endpoint: Option<&str>) -> Result<Self::Client, AppError>;
}
// DcatProfile (ceres_core::config): typed DCAT profile enum — UdataRest (canonical
// "udata_rest", alias "udata", default) and Sparql ("sparql"). Since PR #171 it is
// the single source of truth across portals.toml, CLI --profile, jobs, and dispatch.
// record_sync_status takes Option<SyncMode> + SyncStatus enums (not &str).

pub trait DatasetStore: Send + Sync + Clone {
    fn get_by_id(&self, id: Uuid) -> impl Future<Output = Result<Option<Dataset>, AppError>> + Send;
    fn get_hashes_for_portal(&self, portal_url: &str) -> impl Future<Output = Result<HashMap<String, Option<String>>, AppError>> + Send;
    fn upsert(&self, dataset: &NewDataset) -> impl Future<Output = Result<Uuid, AppError>> + Send;
    fn batch_upsert(&self, datasets: &[NewDataset]) -> impl Future<Output = Result<Vec<Uuid>, AppError>> + Send;
    fn search(&self, query_vector: Vec<f32>, limit: usize) -> impl Future<Output = Result<Vec<SearchResult>, AppError>> + Send;
    fn list_stream<'a>(&'a self, portal_filter: Option<&'a str>, limit: Option<usize>) -> BoxStream<'a, Result<Dataset, AppError>>;
    fn get_last_sync_time(&self, portal_url: &str) -> impl Future<Output = Result<Option<DateTime<Utc>>, AppError>> + Send;
    fn record_sync_status(&self, portal_url: &str, sync_time: DateTime<Utc>, sync_mode: &str, sync_status: &str, datasets_synced: i32) -> impl Future<Output = Result<(), AppError>> + Send;
    fn health_check(&self) -> impl Future<Output = Result<(), AppError>> + Send;
    // + update_timestamp_only, batch_update_timestamps, get_duplicate_titles
    // Stale detection
    fn mark_stale_datasets(&self, portal_url: &str, sync_start: DateTime<Utc>) -> impl Future<Output = Result<u64, AppError>> + Send;
    fn mark_stale_by_exclusion(&self, portal_url: &str, seen_ids: &[String]) -> impl Future<Output = Result<u64, AppError>> + Send;
    // Pending embeddings
    fn list_pending_embeddings(&self, portal_filter: Option<&str>, limit: usize) -> impl Future<Output = Result<Vec<Dataset>, AppError>> + Send;
}
```

## Key Types

| Type | Module | Purpose |
|---|---|---|
| `Dataset` | `ceres_core::models` | Complete dataset row (id, original_id, source_portal, url, title, description, embedding, metadata, timestamps, content_hash, is_stale) |
| `NewDataset` | `ceres_core::models` | Insert/update DTO. Has `compute_content_hash()` for delta detection |
| `DatasetSchema` | `ceres_core::schema` | Normalized resource/distribution metadata derived from preserved portal metadata |
| `SearchResult` | `ceres_core::models` | Dataset + similarity_score (0.0-1.0) |
| `DatabaseStats` | `ceres_core::models` | total_datasets, datasets_with_embeddings, stale_datasets, total_portals, last_update |
| `HarvestJob` | `ceres_core::job` | Queued harvest job with status, retry info, portal config |
| `JobStatus` | `ceres_core::job` | Enum: Pending, Running, Completed, Failed, Cancelled |
| `SyncStats` | `ceres_core::sync` | created, updated, unchanged, failed, skipped counts |
| `SyncOutcome` | `ceres_core::sync` | Per-dataset outcome: Created, Updated, Unchanged, Failed, Skipped |
| `BatchHarvestSummary` | `ceres_core::sync` | Aggregated results from batch harvesting multiple portals |
| `PortalEntry` | `ceres_core::config` | Portal config: name, url, type, enabled, url_template, language, profile, sparql_endpoint, ogc_endpoint |
| `AppError` | `ceres_core::error` | Error enum with `is_retryable()` and `should_trip_circuit()` |
| `EmbeddingStats` | `ceres_core::embedding` | embedded, failed, skipped, total counts from an embedding run |
| `HarvestPipeline` | `ceres_core::pipeline` | Composes HarvestService + EmbeddingService for combined harvest-then-embed |
| `CircuitBreaker` | `ceres_core::circuit_breaker` | Closed -> Open -> HalfOpen state machine |

## Quick Start

```bash
# Install
cargo install ceres-search

# Start PostgreSQL + pgvector
docker compose up db -d

# Configure
cp .env.example .env

# Run migrations
make migrate

# Harvest metadata without embeddings
ceres harvest https://dati.comune.milano.it --metadata-only

# Or harvest a DCAT portal
ceres harvest https://data.public.lu --type dcat --metadata-only

# Or harvest a SPARQL-backed DCAT catalog
ceres harvest https://data.europa.eu --type dcat --profile sparql --metadata-only

# Optional: local embeddings through Ollama
export EMBEDDING_PROVIDER=ollama
ceres embed

# Search
ceres search "trasporto pubblico" --limit 5

# Export
ceres export --format jsonl > datasets.jsonl

# Stats
ceres stats
```

## Reference Guides

| Topic | File | When to Read |
|---|---|---|
| Architecture deep-dive | `references/architecture.md` | Understanding crate graph, services, error handling, database schema |
| CLI & REST API | `references/cli-and-server.md` | Running CLI commands, calling API endpoints, env vars, deployment |
| Harvesting system | `references/harvesting.md` | Two-tier optimization, delta detection, streaming, circuit breaker |
| Extending Ceres | `references/extending.md` | Implementing custom EmbeddingProvider, PortalClient, or DatasetStore |
| Contributing | `references/contributing.md` | Dev setup, testing, CI, code style |

## Version Notes

- **Current version:** 0.5.0 ([release](https://github.com/AndreaBozzo/Ceres/releases/tag/v0.5.0), 2026-06-26)
- **crates.io package:** `ceres-search`
- Harvesting and embedding are decoupled: `--metadata-only` harvests without API key, `embed` command generates embeddings separately
- Ollama is the preferred local embedding path; Gemini and OpenAI remain available
- Current portal client factory supports CKAN, Socrata, OpenDataSoft, ArcGIS Hub, OGC CSW, collection-level STAC, and DCAT (`udata_rest` default profile plus `sparql` and `static_json` profiles)
- ArcGIS Hub clients reject empty `catalogV2` item scopes because those endpoints expose global ArcGIS search results rather than portal-owned datasets
- Stale dataset detection: datasets removed from portals are soft-marked (`is_stale`) during full syncs
- Supports Ollama, Gemini, and OpenAI embeddings
- Parquet export publishes a portable snapshot: `all.parquet` (canonical), per-portal subsets, `identity.parquet`, a versioned snapshot manifest (`metadata.json` with `snapshot_id`, provenance, alias-aware duplicate metadata, and SHA-256 checksums), coverage/quality reports (`reports.json`, `report.md`), and snapshot changelogs (`changelog.json`, `changelog.md` when `--previous` is supplied)
- v0.6.0 coverage foundations shipped: CKAN, DCAT (`udata_rest`/`sparql`/`static_json`), Socrata, OpenDataSoft, ArcGIS Hub, OGC CSW, and collection-level STAC, plus at least one opt-in live smoke test per profile (`cargo test -p ceres-client -- --ignored smoke`; most take a `CERES_*_SMOKE_URL` override, STAC uses `CERES_STAC_*_URL`, and the OGC CSW smokes hard-code their endpoints) and a reproducible metadata-only coverage validation set documented in `website/src/content/docs/PORTALS.md`
- v0.7.0 milestone focus (next): resource-level metadata depth tracked in issue #68
- HuggingFace dataset: `AndreaBozzo/ceres-open-data-index`
