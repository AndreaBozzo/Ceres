# Harvesting System

Ceres is organized around harvesting first. The system fetches and synchronizes metadata independently from embedding, then optionally runs vector generation later.

## Two-Tier Optimization

```
Portal URL
  │
  ├─ Tier 1: Incremental Sync ──> Only fetch datasets modified since last sync
  │                                 (saves portal API calls)
  │
  └─ Tier 2: Delta Detection ───> Only mark rows pending for embedding when content changed
                                   (saves optional embedding work)
```

### Tier 1: Incremental Sync

When the portal supports it, Ceres fetches only datasets changed since the last successful sync. The timestamp is stored in the `portal_sync_status` table.

- **First sync** for a portal: full sync (fetches all datasets)
- **Subsequent syncs**: incremental (only modified datasets)
- **Fallback**: if incremental fails (portal doesn't support the filter), auto-fallback to full sync
- **Force full**: `--full-sync` flag bypasses incremental

**Savings:** Instead of fetching all 50,000 datasets on every run, only the handful that changed are fetched.

### Tier 2: Delta Detection

Even when a dataset is fetched, the embeddable content may not have actually changed. Tags, resources, or minor metadata edits can change source timestamps without changing the text that would be embedded.

Delta detection computes a SHA-256 hash of `title + description` (the `content_hash`) via `NewDataset::compute_content_hash()` and compares it against the stored hash. If the hash matches, embedding regeneration is skipped.

**Savings:** This prevents unnecessary embedding work during later `embed` passes or combined workflows.

### Why Both Tiers

| Scenario | metadata_modified changed? | content_hash changed? | Action |
|----------|---------------------------|-----------------------|--------|
| Tag added | Yes | No | Fetch metadata, skip embedding |
| Resource URL updated | Yes | No | Fetch metadata, skip embedding |
| Title rewritten | Yes | Yes | Fetch metadata, mark pending |
| New dataset | N/A (new) | N/A (new) | Fetch metadata, mark pending |
| Nothing changed | No (not fetched) | N/A | Not fetched at all |

## Sync Outcomes

Each dataset processed during a sync receives one of these outcomes (tracked via `SyncStats`):

| Outcome | Meaning | Embedding generated? |
|---------|---------|---------------------|
| `Created` | New dataset, not seen before | Marked pending |
| `Updated` | Content hash changed | Marked pending |
| `Unchanged` | Content hash matches stored value | No |
| `Failed` | Error during processing | No |
| `Skipped` | Circuit breaker is open | No |

## Streaming Pipeline

The harvest pipeline streams datasets through processing stages instead of loading all into memory. This enables harvesting portals with 100k+ datasets with constant memory.

When embeddings are enabled, batched provider calls further improve throughput.

## CLI Flags Interaction

| Flag | Tier 1 (Incremental) | Tier 2 (Delta Detection) | Use case |
|------|---------------------|-------------------------|----------|
| *(none)* | Incremental if previous sync exists | Always active | Normal operation |
| `--full-sync` | Full sync forced | Still active | Re-scan portal after known issues |
| `--dry-run` | Dry run (no writes) | Still active | Preview what would happen |
| `--metadata-only` | Same as default | Skipped (no embedding) | Harvest without API key |

Delta detection is always active. To force full re-embedding, delete the `content_hash` values from the database.

## Database Tracking

### `portal_sync_status` table

| Column | Type | Purpose |
|--------|------|---------|
| `portal_url` | `VARCHAR` (PK) | Portal identifier |
| `last_successful_sync` | `TIMESTAMPTZ` | Used for next incremental sync |
| `last_sync_mode` | `VARCHAR(20)` | `"full"` or `"incremental"` |
| `sync_status` | `VARCHAR(20)` | `"completed"` or `"cancelled"` |
| `datasets_synced` | `INTEGER` | Number of datasets processed |

The `last_successful_sync` is set to the sync **start** time (not end time), ensuring no datasets are missed between syncs.

Content hashes are stored in the `datasets.content_hash` column (VARCHAR(64), nullable for backward compatibility).

## Circuit Breaker

The embedding API is protected by a circuit breaker:

```
CLOSED ──[5 failures]──> OPEN ──[30s timeout]──> HALF_OPEN
   ^                                                  |
   └──────[2 successes]──────────────────────────────┘
                              <──[any failure]──
```

On 429 (rate limit), recovery timeout is doubled (up to 5 min max). See `references/architecture.md` for full config.

When the circuit is open, datasets are recorded as `Skipped` — they'll be retried on the next harvest run.

## Metadata-Only Mode

With `--metadata-only`, harvesting fetches and stores metadata without generating embeddings. No embedding API key is needed. Datasets are stored with `embedding = NULL`; existing embeddings are preserved via SQL `COALESCE`.

Use the standalone `ceres embed` command afterwards to generate embeddings for pending datasets.

This is the recommended path when you want to keep harvesting and embedding operationally separate.

## Stale Dataset Detection

After a successful full sync (zero failures, zero skipped), Ceres marks datasets that no longer exist on the portal as stale using `mark_stale_by_exclusion()`. This is an efficient exclusion-based approach: all datasets whose `original_id` is NOT in the set of IDs seen during the sync are marked `is_stale = TRUE`.

Stale datasets are:
- **Excluded from semantic search** (WHERE `NOT is_stale`)
- **Excluded from pending embeddings** (partial index filters them out)
- **Not deleted** — soft-marked so they can be recovered if re-published

Stale detection only runs on full syncs because incremental syncs only fetch modified datasets and cannot definitively determine which datasets have been removed.

The `StaleDetected { count }` harvest event reports how many datasets were newly marked stale.

## Adaptive Page Size

The CKAN client uses adaptive page size reduction to handle portals that truncate or timeout on large responses:

- **Initial page size**: 1000 rows
- **On Timeout or NetworkError**: quarters the page size (1000 → 250 → 62 → 15 → 10)
- **Minimum page size**: 10 rows
- **On other errors** (rate limits, client errors): no reduction, error propagated

This converges faster than halving (5 vs 8 iterations) and handles portals with resource-heavy datasets at specific offsets.

## Batch Harvesting

`HarvestService::batch_harvest_with_progress()` processes all enabled portals from `portals.toml` sequentially, producing a `BatchHarvestSummary` with per-portal results. The CLI and server both use this for bulk harvesting.

Current production-facing portal support in the factory is CKAN, DCAT udata REST, SPARQL-backed DCAT, static Project Open Data `data.json`, Socrata, OpenDataSoft, and ArcGIS Hub. For ad-hoc SPARQL DCAT harvests, use `--type dcat --profile sparql`; for config-driven harvests, set `profile = "sparql"` and optionally `sparql_endpoint`. ArcGIS Hub sites whose injected `catalogV2` has an empty item scope are rejected because their search endpoint returns global ArcGIS content instead of datasets belonging to that portal.

## Graceful Shutdown

Harvest operations accept a `CancellationToken`. When cancelled:
- Current batch completes
- Partial progress is saved to the database (sync status = "cancelled")
- Worker releases its in-progress job back to the queue

## Embedding Providers

| Provider | Model | Dimensions | Batch Size |
|----------|-------|------------|------------|
| Ollama | `nomic-embed-text` | 768 | native batch endpoint |
| Ollama | `mxbai-embed-large` | 1024 | native batch endpoint |
| Gemini (default) | `gemini-embedding-001` | 768 | 100 |
| OpenAI | `text-embedding-3-small` | 1536 | 2048 |
| OpenAI | `text-embedding-3-large` | 3072 | 2048 |

Set via `EMBEDDING_PROVIDER` env var (`ollama`, `gemini`, or `openai`). Override model with `EMBEDDING_MODEL`.

## DeltaDetector Trait

For advanced use cases, delta detection is abstracted via the `DeltaDetector` trait:

- `ContentHashDetector` (default) — SHA-256 content hash comparison
- `AlwaysReprocessDetector` — always regenerates embeddings (useful for full rebuilds)

Custom implementations can be plugged in for timestamp-based or other detection strategies.
