---
name: ceres-publish-dataset
description: Use when publishing a HuggingFace dataset export for the Ceres Open Data Index. Covers the full workflow — data quality checks, Parquet export, snapshot manifest, coverage/quality reports, snapshot changelog, README dataset card update, versioning, and push to HuggingFace.
---

# Ceres Publish Dataset — HuggingFace Export

Recurring workflow to export the Ceres open data index to [HuggingFace](https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index) as a curated Parquet dataset. Ceres v0.5.0 is the baseline for the reproducible snapshot contract: manifest, integrity checks, coverage/quality reports, alias-aware duplicate provenance, identity index, and snapshot changelogs.

**Tracking issue:** https://github.com/AndreaBozzo/Ceres/issues/89

## Checklist

1. Run data quality checks (noise, duplicates, new portals)
2. Export: `ceres export --format parquet --output ~/ceres-open-data-index`
3. Remove any stale per-portal parquet files from previous exports (e.g. renamed portals)
4. Update `README.md` dataset card (counts, portal table, snapshot date, bias notes)
5. Version the snapshot (commit + tag)
6. Push to HuggingFace
7. (Optional) Regenerate HuggingFace Space visualization

## Export Command

```bash
ceres export --format parquet --output ~/ceres-open-data-index
```

The export streams all non-stale datasets from PostgreSQL, applies curation, and writes:
- `all.parquet` — Canonical complete flattened dataset
- `data/<portal-name>.parquet` — Per-portal subsets (repeat rows from `all.parquet`; never sum into the canonical total)
- `identity.parquet` — Slim per-record fingerprint (`source_portal`, `original_id`, `content_hash`) used to diff snapshots; one row per `all.parquet` row
- `metadata.json` — Versioned snapshot manifest: stable `snapshot_id`, UTC `generated_at`, Ceres version/commit, portal-config checksum, `duplicate_detection` provenance (method/version/alias_groups), curation row counts, per-portal inclusion status, and SHA-256 checksums for every file
- `reports.json` — Machine-readable coverage and quality report: coverage by portal/type/profile/language, field-completeness rates (description, license, organization, tags, modification date), and curation outcomes (raw, exported, filtered, duplicate-flagged, duplicate-detection method/version, excluded portals)
- `report.md` — Human-readable summary of `reports.json` for the dataset card / release notes
- `changelog.json` / `changelog.md` — Snapshot-to-snapshot diff (added/changed/removed/unchanged, with per-portal summaries), written only when `--previous <DIR>` points at a prior snapshot; otherwise a zeroed baseline changelog with `compared: false`

Verify the SHA-256 checksums in `metadata.json` before publishing a copied or mirrored snapshot. `reports.json` is derived from the same export pass, so its figures agree with the manifest.

### Diffing against the previous snapshot

To publish a changelog, point `--previous` at the prior published snapshot directory:

```bash
ceres export --format parquet --output ~/ceres-open-data-index --previous ~/ceres-open-data-index
```

The diff is keyed by the stable identity (`source_portal` + `original_id`) read from each snapshot's `identity.parquet`. "Changed" means the `content_hash` (SHA-256 of title+description) differs; source modification timestamps are not used because portal coverage of them is incomplete.

Portal names are resolved from `~/.config/ceres/portals.toml`. Portals not in the config fall back to hostname-based naming (e.g. `https://data.gov.ro` becomes `data-gov-ro`).

The export takes ~30-40 minutes for 900k+ datasets due to JSONB flattening.

## Export Schema (14 columns)

| Column | Type | Description |
|--------|------|-------------|
| `original_id` | string | Dataset ID from source portal |
| `source_portal` | string | Portal base URL |
| `portal_name` | string | Human-readable portal name |
| `url` | string | Direct URL to dataset page |
| `title` | string | Dataset title |
| `description` | string | Dataset description (nullable) |
| `tags` | string | Comma-separated tag names (nullable) |
| `organization` | string | Publishing organization (nullable) |
| `license` | string | License title or identifier (nullable) |
| `metadata_created` | string | Original creation date ISO 8601 (nullable) |
| `metadata_modified` | string | Last modification date ISO 8601 (nullable) |
| `first_seen_at` | string | When Ceres first indexed this dataset (RFC 3339) |
| `language` | string | Primary language code (nullable) |
| `is_duplicate` | boolean | Heuristic signal: same title (case-insensitive) appears on another portal. Not canonical deduplication |

## Curation Rules (applied automatically during export)

- **Noise filter**: Removes datasets where title < 5 chars, description is empty, or title contains "test"/"prova"/"esempio" (case-insensitive substring match)
- **Duplicate flag** (heuristic, not canonical dedup): Same title (case-insensitive) across different portals sets `is_duplicate=true`. Duplicates are kept, not removed. Portals may declare `aliases = [...]` in `portals.toml`; aliased/mirror URLs are folded onto their canonical portal first, so a mirror is not counted as an independent source. The matching rule and version are recorded in `metadata.json` under `duplicate_detection`. Core SQL: `SELECT LOWER(title) FROM datasets GROUP BY LOWER(title) HAVING COUNT(DISTINCT <canonicalized source_portal>) > 1`
- **Metadata flattening**: Tags from `metadata.tags[].name`, organization from `metadata.organization.title`, license from `metadata.license_title`

## HuggingFace Dataset Repository

- **Location:** `~/ceres-open-data-index`
- **Remote:** `https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index`
- **Branch:** `main`
- **LFS:** `*.parquet` tracked via Git-LFS (configured in `.gitattributes`)

### Commit and push workflow

```bash
cd ~/ceres-open-data-index
git add all.parquet identity.parquet data/ metadata.json reports.json report.md changelog.json changelog.md README.md
git commit -m "vN: Month Year export (X portals, Yk datasets)"
git tag vN
git push origin main
git push origin vN
```

## README Dataset Card — What to Update

The README at `~/ceres-open-data-index/README.md` uses HuggingFace dataset card format with YAML frontmatter. Update these sections:

1. **YAML frontmatter**: `language` list (add new language codes), `size_categories`, `tags`
2. **Opening line**: Total datasets count, portal count, country count
3. **Files section**: Row count, file count
4. **Portal table** (`### Splits by portal`): Add new portals, update all counts, sort by count descending
5. **Curation section**: Noise filtering count, duplicate flagging count
6. **Update frequency**: Snapshot date
7. **Known biases**: Geographic skew percentages, language distribution, portal selection notes
8. **Citation block**: Snapshot date, portal count

Curation counts and per-portal totals come from `metadata.json`; coverage breakdowns (by type/profile/language) and field-completeness rates come from `reports.json` (or the rendered `report.md`). Both are generated by the export.

## Export History

| Version | Date | Exported | Filtered | Duplicates | Portals | Countries |
|---------|------|----------|----------|------------|---------|-----------|
| v1 | 2026-02-12 | 230,315 | 4,693 | 58,364 | 23 | 8 + intl |
| v2 | 2026-02-25 | 349,836 | 6,532 | 59,062 | 25 | 9 + intl |
| v3 | 2026-03-30 | 890,143 | 10,331 | 75,527 | 32 | 13 + intl |

## Key Files

| File | Purpose |
|------|---------|
| `~/.config/ceres/portals.toml` | Portal config — controls name resolution during export |
| `~/ceres-open-data-index/README.md` | HuggingFace dataset card |
| `~/ceres-open-data-index/metadata.json` | Versioned snapshot manifest (generated, do not edit) |
| `~/ceres-open-data-index/reports.json` | Coverage and quality report (generated, do not edit) |
| `~/ceres-open-data-index/report.md` | Human-readable coverage/quality summary (generated) |
| `crates/ceres-core/src/parquet_export.rs` | Export logic (noise filter, duplicate flagging, flattening, manifest, reports) |
| `crates/ceres-db/src/repository.rs` | SQL queries (duplicate detection, dataset streaming) |

## HuggingFace Space (Optional)

The visualization dashboard at `https://huggingface.co/spaces/AndreaBozzo/Ceres` is a static Plotly site generated from the exported data.

- **Location:** `~/Documenti/Ceres(huggingfacespace)`
- **Generator:** `~/Documenti/open-data-galaxy/galaxy_constellation.py`
- Regenerate after pushing the dataset, then push the Space repo separately

## Pre-export Data Quality Checks

```bash
# Overall stats
ceres stats

# Check noise candidates in DB (optional)
docker exec ceres_db psql -U ceres_user -d ceres_db -c "
  SELECT COUNT(*) FILTER (WHERE LENGTH(title) < 5) as tiny_titles,
         COUNT(*) FILTER (WHERE description IS NULL OR TRIM(description) = '') as empty_desc,
         COUNT(*) FILTER (WHERE LOWER(title) LIKE '%test%' OR LOWER(title) LIKE '%prova%' OR LOWER(title) LIKE '%esempio%') as noise_titles
  FROM datasets WHERE NOT is_stale;
"
```
