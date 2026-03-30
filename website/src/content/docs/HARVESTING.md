---
title: Harvesting in Ceres
description: How Ceres harvests and synchronizes portal metadata before optional embedding and search
---

# Harvesting Architecture

Ceres is organized around harvesting first.

The primary job of the system is to pull dataset metadata from portal APIs, normalize it, and keep a local catalog synchronized over time. Embeddings are a separate stage that can run later through Ollama or a hosted provider.

Today the shipping portal clients cover:

- CKAN portals
- DCAT-AP portals that expose the udata REST JSON-LD catalog

## Core pipeline

```text
Portal API -> PortalClient -> HarvestService -> DatasetStore
									 |
									 +-> sync history
									 +-> stale detection
									 +-> pending embeddings

DatasetStore (pending) -> EmbeddingService -> vectors for search
```

## Two-Tier Optimization

![Harvesting Flow Diagram](../../assets/images/harvesting.png)

Incremental sync reduces portal calls. Delta detection reduces optional embedding work.

### Streaming Pipeline

The harvest pipeline streams datasets through processing stages instead of loading an entire catalog into memory. That keeps memory bounded even on very large portals.

Embedding is no longer part of the mandatory hot path. When enabled, it runs through a separate service and can batch texts according to provider capabilities.

### Tier 1: Incremental Sync

When a portal supports modified-since querying, Ceres fetches only datasets changed since the last successful sync. The last sync timestamp is stored in `portal_sync_status`.

On the first sync for a portal, or when `--full-sync` is passed, Ceres performs a full sync. If incremental sync is unsupported or fails, the service falls back to a full sync automatically.

This is what keeps repeated harvests operationally cheap.

### Tier 2: Delta Detection

Even when a dataset is fetched, its embeddable content may not have changed. A portal can update tags, resources, or minor metadata without changing the text that would be embedded.

Delta detection computes a SHA-256 hash of `title + description` (the `content_hash`) and compares it against the stored hash. If the hash matches, the embedding regeneration is skipped entirely.

This matters most when you run the optional embedding stage, whether locally through Ollama or through a hosted provider.

### Why Both Tiers Are Necessary

| Scenario | `metadata_modified` changed? | `content_hash` changed? | Action |
|----------|------------------------------|------------------------|--------|
| Tag added to dataset | Yes | No | Fetch metadata, skip embedding |
| Resource URL updated | Yes | No | Fetch metadata, skip embedding |
| Title rewritten | Yes | Yes | Fetch metadata, mark for embedding |
| New dataset published | N/A (new) | N/A (new) | Fetch metadata, mark for embedding |
| Nothing changed | No | N/A (not fetched) | Not fetched at all |

Without incremental sync, every run would fetch the full portal. Without delta detection, every changed record would be re-embedded even when the relevant text stayed the same.

## Sync Outcomes

Each dataset processed during a sync receives one of these outcomes:

| Outcome | Meaning | Embedding generated? |
|---------|---------|---------------------|
| `Created` | New dataset, not seen before | Marked pending |
| `Updated` | Content hash changed (title or description modified) | Marked pending |
| `Unchanged` | Content hash matches stored value | No |
| `Failed` | Error during processing | No |
| `Skipped` | Embedding step was skipped or the circuit breaker is open | No |

These are tracked via `SyncStats` and reported at the end of each sync operation.

## CLI Flags

| Flag | Tier 1 (Incremental) | Tier 2 (Delta Detection) | Use case |
|------|---------------------|-------------------------|----------|
| *(none)* | Incremental if previous sync exists | Always active | Normal operation |
| `--full-sync` | Full sync forced | Still active | Re-scan portal after known issues |
| `--dry-run` | Dry run (no writes) | Still active | Preview what would happen |
| `--metadata-only` | Same as default | Skipped (no embedding) | Harvest without API key |

Delta detection is always active regardless of flags. There is no flag to bypass it — if you need to force full re-embedding, delete the stored content hashes from the database.

## Metadata-only mode is the normal harvest path

`--metadata-only` is not a degraded mode. It is the cleanest way to operate Ceres when your immediate goal is harvesting and synchronization.

Use it when you want to:

- build the catalog before choosing an embedding provider
- run fully locally without any vector generation
- separate crawl operations from search operations
- backfill vectors later with `ceres embed`

## Database Tracking

The `portal_sync_status` table tracks sync history per portal:

| Column | Type | Purpose |
|--------|------|---------|
| `portal_url` | `VARCHAR` (PK) | Portal identifier |
| `last_successful_sync` | `TIMESTAMPTZ` | Timestamp used for next incremental sync |
| `last_sync_mode` | `VARCHAR(20)` | `"full"` or `"incremental"` |
| `sync_status` | `VARCHAR(20)` | `"completed"` or `"cancelled"` |
| `datasets_synced` | `INTEGER` | Number of datasets processed |
| `updated_at` | `TIMESTAMPTZ` | When this record was last updated |

The `last_successful_sync` value is set to the sync start time (not end time), ensuring no datasets are missed between syncs.

Content hashes are stored in the `datasets` table in the `content_hash` column (`VARCHAR(64)`, nullable for backward compatibility with records indexed before delta detection was added).

## Optional embedding stage

Embeddings are processed later by `EmbeddingService`:

- `HarvestService` writes datasets and tracks changes
- `EmbeddingService` reads pending rows and generates vectors
- `HarvestPipeline` composes both when you want the combined workflow

This split lets you harvest regardless of embedding availability and makes Ollama a practical local-first default.

## Circuit Breaker

When embeddings are enabled, the embedding provider is protected by a circuit breaker to avoid cascading failures:

![Circuit Breaker Diagram](../../assets/images/circuitbreaker.png)

*Closed, Open, Half-Open states with adaptive recovery timeout on rate limits.*

- **Closed**: requests flow normally
- **Open**: all embedding requests are rejected immediately, datasets are recorded as `Skipped`
- **Half-Open**: requests are allowed to probe recovery; 2 successes close the circuit, any failure reopens it

On HTTP 429, the recovery timeout is multiplied by a backoff factor, up to a configured maximum.

## Stale Dataset Detection

After a successful full sync with zero failures and zero skipped datasets, Ceres marks datasets that no longer exist on the portal as **stale**. This uses an efficient exclusion-based approach: all datasets whose `original_id` is NOT in the set of IDs seen during the sync are marked `is_stale = TRUE`.

Stale datasets are:
- **Excluded from semantic search** (`WHERE NOT is_stale`)
- **Excluded from pending embeddings** (via partial index)
- **Not deleted** — soft-marked so they can be recovered if the portal re-publishes them

Stale detection only runs on full syncs because incremental syncs fetch only modified datasets and cannot definitively determine which datasets have been removed.

## Protocol-specific behavior

The exact harvest behavior depends on the portal client:

- CKAN clients can use modified-since filters and adaptive page sizing
- DCAT udata clients stream paginated JSON-LD catalog pages and resolve multilingual fields according to the configured language

## Adaptive Page Size

The CKAN client uses adaptive page size reduction to handle portals that truncate or timeout on large responses:

- **Initial page size**: 1000 rows
- **On Timeout or NetworkError**: quarters the page size (1000 → 250 → 62 → 15 → 10)
- **Minimum page size**: 10 rows
- **On other errors** (rate limits, client errors): no reduction, error propagated normally

This converges faster than halving and handles portals with resource-heavy datasets at specific offsets.

## Related Source Files

- Harvesting service: [`crates/ceres-core/src/harvest.rs`](../crates/ceres-core/src/harvest.rs)
- Embedding service: [`crates/ceres-core/src/embedding.rs`](../crates/ceres-core/src/embedding.rs)
- Harvest pipeline: [`crates/ceres-core/src/pipeline.rs`](../crates/ceres-core/src/pipeline.rs)
- Delta detection logic: [`crates/ceres-core/src/sync.rs`](../crates/ceres-core/src/sync.rs) (`needs_reprocessing` function)
- Content hash computation: [`crates/ceres-core/src/models.rs`](../crates/ceres-core/src/models.rs) (`NewDataset::compute_content_hash`)
- Circuit breaker: [`crates/ceres-core/src/circuit_breaker.rs`](../crates/ceres-core/src/circuit_breaker.rs)
- CKAN client: [`crates/ceres-client/src/ckan.rs`](../crates/ceres-client/src/ckan.rs) (`search_modified_since`, adaptive page size)
- DCAT client: [`crates/ceres-client/src/dcat.rs`](../crates/ceres-client/src/dcat.rs)
- DB schema: [`migrations/`](../migrations/)
