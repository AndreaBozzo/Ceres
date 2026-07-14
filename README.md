<div align="center">
  <img src="website/src/assets/images/logo.png" alt="Ceres Logo" width="800" height="auto"/>
  <h1>Ceres</h1>
  <p><strong>Harvest-first toolkit for open data portals — one synchronized catalog from 300+ portals and 2M+ datasets</strong></p>
  <p>
    <a href="https://crates.io/crates/ceres-search"><img src="https://img.shields.io/crates/v/ceres-search.svg" alt="crates.io"></a>
    <a href="https://github.com/AndreaBozzo/Ceres/actions"><img src="https://github.com/AndreaBozzo/Ceres/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="https://github.com/AndreaBozzo/Ceres/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License"></a>
    <a href="https://discord.gg/fztdKSPXSz"><img src="https://img.shields.io/discord/1469399961987711161?color=5865F2&logo=discord&logoColor=white&label=Discord" alt="Discord"></a>
    <a href="https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index"><img src="https://img.shields.io/badge/%F0%9F%A4%97%20Dataset-Open%20Data%20Index-yellow" alt="HuggingFace Dataset"></a>
  </p>
  <p>
    <a href="https://learnceres.pages.dev">Website</a> •
    <a href="#quick-start">Quick Start</a> •
    <a href="#supported-portals">Supported Portals</a> •
    <a href="#the-open-data-index">The Index</a> •
    <a href="#rest-api">REST API</a>
  </p>
</div>

---

Open data is fragmented across thousands of portals speaking different APIs — CKAN, DCAT, SPARQL, `data.json`, Socrata, OGC CSW, and STAC. Ceres harvests them into **one synchronized PostgreSQL catalog**: incremental sync, delta detection, stale tracking, bounded memory on multi-million-dataset sources.

Everything else is layered on top, and optional: local embeddings, semantic search, a REST API, and reproducible Parquet snapshots published as the public **[Ceres Open Data Index](https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index)**.

> Named after the Roman goddess of harvest and agriculture.

## At a Glance

- **120+ portals** harvested and kept in sync — national portals (data.gov, data.europa.eu, data.slovensko.sk, govdata.de), cities (Milano, NYC, Zurich), and agencies
- **2M+ datasets** in the live catalog and published snapshots
- **9 harvest paths** shipped, including OGC CSW 2.0.2 and collection-level STAC alongside the major open-data portal families
- **Metadata-only by default** — no embedding provider, API key, or GPU required to build and maintain a catalog
- **Local-first embeddings** via Ollama when you want semantic search; Gemini and OpenAI supported
- **Reproducible exports** — Parquet snapshots with versioned manifests, SHA-256 checksums, coverage/quality reports, and changelogs

## Why Harvest-First

Most open data tooling starts at search. In practice the hard part comes earlier:

- Portals expose different APIs, capabilities, and reliability profiles
- Large catalogs need incremental syncs and bounded memory usage
- Removed datasets must be detected without deleting history
- Embedding is optional, slower, and operationally distinct — it should never block harvesting

Ceres splits the system accordingly:

1. **Harvest and normalize metadata** — `HarvestService`, always available
2. **Optionally embed and search** — `EmbeddingService`, added later if useful

## Supported Portals

| Type | Flag | Serves | Examples |
|---|---|---|---|
| CKAN | `--type ckan` | CKAN action API portals | dati.comune.milano.it, data.ontario.ca, data.gov.kg |
| DCAT udata REST | `--type dcat` (default profile) | udata-flavored DCAT-AP portals | data.gouv.fr, data.public.lu |
| DCAT SPARQL | `--type dcat --profile sparql` | SPARQL-backed DCAT-AP catalogs | data.europa.eu, data.slovensko.sk |
| Project Open Data | `--type dcat --profile static_json` | Static DCAT-US `data.json` catalogs | data.va.gov, census.gov, justice.gov |
| Socrata | `--type socrata` | Socrata Discovery API catalogs | data.cityofnewyork.us, data.wa.gov |
| OpenDataSoft | `--type opendatasoft` | OpenDataSoft Explore API v2.1 catalogs | opendata.paris.fr, data.economie.gouv.fr |
| ArcGIS Hub | `--type arcgis` | ArcGIS Hub Search API catalogs | opendata.dc.gov, opendata.gis.utah.gov |
| OGC Records | `--type ogc_records` | CSW 2.0.2 / GeoNetwork catalogs | EMODnet, Copernicus Marine |
| STAC | `--type stac` | Collection-level STAC APIs | Copernicus Data Space, Canada DataCube |

All clients stream page-by-page, preserve the complete source metadata for downstream use, and share the same sync machinery (incremental sync, content-hash delta detection, stale marking).

## The Open Data Index

Ceres is both a toolkit and a working pipeline. The catalog it maintains is published as the **[Ceres Open Data Index](https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index)** on Hugging Face: a curated, deduplicated Parquet snapshot of open dataset metadata across national, regional, and city portals worldwide.

Each snapshot ships a verifiable contract:

- `all.parquet` — the canonical complete index (per-portal subsets under `data/`)
- `metadata.json` — versioned manifest: snapshot ID, provenance, per-portal counts, SHA-256 checksums
- `reports.json` / `report.md` — coverage by portal/type/language and field-completeness rates
- `changelog.json` / `changelog.md` — dataset-level diff against the previous snapshot

## Quick Start

### Prerequisites

- Rust 1.95+
- Docker and Docker Compose
- Optional for embeddings: [Ollama](https://ollama.com) locally, or Gemini/OpenAI credentials

### 1. Clone and start the database

```bash
git clone https://github.com/AndreaBozzo/Ceres.git
cd Ceres
docker compose up db -d
cp .env.example .env
make migrate
```

### 2. Harvest metadata first

```bash
# Single CKAN portal
cargo run --bin ceres -- harvest https://dati.comune.milano.it --metadata-only

# DCAT portal
cargo run --bin ceres -- harvest https://data.public.lu --type dcat --metadata-only

# Socrata portal (no app token required for public reads)
cargo run --bin ceres -- harvest https://data.cityofnewyork.us --type socrata --metadata-only

# OpenDataSoft portal (no API key required for public reads)
cargo run --bin ceres -- harvest https://opendata.paris.fr --type opendatasoft --metadata-only

# ArcGIS Hub portal (no credentials required)
cargo run --bin ceres -- harvest https://opendata.dc.gov --type arcgis --metadata-only

# STAC API (one row per Collection; Items are never harvested)
cargo run --bin ceres -- harvest https://stac.dataspace.copernicus.eu/v1/ --type stac --metadata-only

# All enabled portals from config
cargo run --bin ceres -- harvest --config examples/portals.toml --metadata-only
```

### 3. Add embeddings only if you want semantic search

With Ollama locally:

```bash
ollama serve
ollama pull nomic-embed-text

export EMBEDDING_PROVIDER=ollama
cargo run --bin ceres -- embed
```

### 4. Search or export

```bash
cargo run --bin ceres -- search "public transport" --limit 5
cargo run --bin ceres -- export --format jsonl > datasets.jsonl
cargo run --bin ceres -- stats
```

## Configuration

### Embeddings are optional

If you only want harvesting, run with `--metadata-only` and skip embedding configuration entirely.

If you want embeddings later, set:

```bash
EMBEDDING_PROVIDER=ollama
OLLAMA_ENDPOINT=http://localhost:11434
EMBEDDING_MODEL=nomic-embed-text
```

Hosted providers are still supported:

```bash
EMBEDDING_PROVIDER=gemini
GEMINI_API_KEY=...

# or

EMBEDDING_PROVIDER=openai
OPENAI_API_KEY=...
EMBEDDING_MODEL=text-embedding-3-small
```

### Portal configuration

Batch harvesting uses `portals.toml`:

```toml
[[portals]]
name = "milano"
url = "https://dati.comune.milano.it"
type = "ckan"
language = "it"

[[portals]]
name = "nyc"
url = "https://data.cityofnewyork.us"
type = "socrata"
language = "en"

[[portals]]
name = "europa"
url = "https://data.europa.eu"
type = "dcat"
profile = "sparql"
language = "en"
```

See `examples/portals.toml` for a larger configuration set.

Socrata public catalogs can be harvested without credentials. For higher
Discovery API rate limits, set `SOCRATA_APP_TOKEN`; Ceres sends it through the
`X-App-Token` header and never stores it in portal configuration.

OpenDataSoft public catalogs also work anonymously. For higher Explore API
quotas, set `ODS_API_KEY`; Ceres sends it as an `Authorization: Apikey ...`
header and never stores it in portal configuration.

### Harvest resilience / HTTP tuning

Harvesting is hardened for slow or rate-limiting portals: the CKAN client grows
its per-request timeout (and shrinks the page size) when a page times out, and
the DCAT client waits out `429` rate limits with an escalating per-page cooldown
instead of stopping with partial results. You can tune the HTTP behavior with
environment variables (defaults shown):

```bash
CERES_HTTP_TIMEOUT_SECS=60     # base per-request timeout (raise for very slow portals)
CERES_HTTP_MAX_RETRIES=3       # transient-error retry attempts
CERES_HTTP_RETRY_BASE_MS=500   # base backoff delay
```

### US data.gov

`catalog.data.gov` no longer serves the `/api/3/action` CKAN API; it relocated to
`api.gsa.gov` and requires an API key. To harvest it, set `DATA_GOV_API_KEY`
(get a free key at <https://api.data.gov/signup/>):

```bash
DATA_GOV_API_KEY=your-api-data-gov-key
ceres harvest https://catalog.data.gov --type ckan --metadata-only
```

Datasets are still attributed to `catalog.data.gov`. Without the key, data.gov
requests return `403`.

## Usage

### Harvest

```bash
# All enabled portals from config
ceres harvest

# Ad-hoc harvests by portal type
ceres harvest https://dati.comune.milano.it
ceres harvest https://data.public.lu --type dcat
ceres harvest https://data.europa.eu --type dcat --profile sparql
ceres harvest https://www.data.va.gov/data.json --type dcat --profile static_json --metadata-only
ceres harvest https://data.cityofnewyork.us --type socrata --metadata-only
ceres harvest https://opendata.paris.fr --type opendatasoft --metadata-only
ceres harvest https://opendata.dc.gov --type arcgis --metadata-only

# Named portal from config
ceres harvest --portal milano --config examples/portals.toml

# Force full sync
ceres harvest --portal milano --full-sync

# Dry run
ceres harvest --portal milano --dry-run --metadata-only
```

### Embed

```bash
# Embed everything pending
ceres embed

# Only one portal
ceres embed --portal https://dati.comune.milano.it
```

### Search

```bash
ceres search "air quality monitoring" --limit 10
```

### Export

```bash
ceres export --format jsonl > datasets.jsonl
ceres export --format csv > datasets.csv
ceres export --format parquet --output ./ceres-export
```

### Published snapshot contract

A Parquet export produces one portable snapshot directory:

- `all.parquet` is the **canonical complete index**.
- `data/<portal>.parquet` files are convenience subsets and intentionally repeat
  rows from `all.parquet`; do not add their row counts to the canonical total.
- `metadata.json` is a versioned manifest with the snapshot ID, UTC generation
  time, Ceres version and commit, portal-config checksum, curation counts,
  per-portal inclusion status, and SHA-256 checksums for every Parquet file.
- `reports.json` is a versioned coverage and quality report: dataset coverage by
  portal, portal type/profile, and language; field-completeness rates for
  description, license, organization, tags, and modification date; and curation
  outcomes (raw, exported, filtered, duplicate-flagged, and excluded portals). It
  is derived from the same export pass, so its figures match `metadata.json`.
- `report.md` is a human-readable summary of `reports.json` for the dataset card
  and release notes.

Verify the checksums in `metadata.json` before consuming a copied or mirrored
snapshot. A library caller that does not supply build metadata records the Git
commit as `unknown`; the `ceres` CLI populates it automatically.

### Stats

```bash
ceres stats
```

## Harvesting Model

The harvesting pipeline is built around a few operational principles:

- Incremental sync when the source supports it
- Full-sync fallback when incremental fetch is not available or fails
- Content-hash delta detection so unchanged datasets are not re-embedded
- Streaming page-by-page processing to keep memory bounded
- Stale dataset marking instead of hard deletion

Embedding is fully decoupled from harvesting:

- `HarvestService` stores and updates metadata
- `EmbeddingService` processes datasets with missing embeddings
- `HarvestPipeline` composes both when you want the combined flow

That separation is what makes local-first embedding practical and keeps harvest jobs usable even when no embedder is configured.

## REST API

Start the server:

```bash
cargo run --bin ceres-server
```

Available endpoints:

- `GET /api/v1/health`
- `GET /api/v1/stats`
- `GET /api/v1/search`
- `GET /api/v1/portals`
- `GET /api/v1/portals/{name}/stats`
- `GET /api/v1/harvest/status`
- `GET /api/v1/datasets/{id}`
- `GET /api/v1/datasets/{id}/schema`
- `POST /api/v1/portals/{name}/harvest`
- `POST /api/v1/harvest`
- `GET /api/v1/export`
- `GET /swagger-ui`

Set `CERES_ADMIN_TOKEN` to enable protected write endpoints.

Server-triggered harvest jobs use the matching `portals.toml` entry for both
`POST /api/v1/portals/{name}/harvest` and `POST /api/v1/harvest`: portal
`type`, DCAT `profile`, language, URL template, and optional
`sparql_endpoint` are copied onto each durable job before the worker runs it.

## Agentic Usage (Claude Code and friends)

Ceres ships with agent support in-repo:

- [`AGENTS.md`](AGENTS.md) — contributor guide for coding agents (project map, conventions, validation commands); `CLAUDE.md` points Claude Code at it
- [`.claude/skills/ceres`](.claude/skills/ceres/SKILL.md) — a Claude Code skill covering architecture, traits, CLI/REST usage, and extension points; picked up automatically when you open the repo with Claude Code
- [`.claude/skills/ceres-publish-dataset`](.claude/skills/ceres-publish-dataset/SKILL.md) — the maintainer workflow for publishing Open Data Index snapshots to Hugging Face

## Roadmap

- **Now (v0.5.0)** — trustable published index: versioned snapshot manifests, integrity checksums, coverage/quality reports, alias-aware duplicate semantics, snapshot changelogs; Socrata Discovery and static `data.json` harvest paths
- **Next (v0.6.0)** — validate OGC CSW and collection-level STAC coverage at scale, finish the reproducible metadata-only portal set, and exercise job-based harvesting for every client
- **Later (v0.7.0)** — resource-level metadata depth ([#68](https://github.com/AndreaBozzo/Ceres/issues/68))

## Related Projects

- [AndreaBozzo/ceres-open-data-index](https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index) — published dataset snapshots on Hugging Face
- [learnceres.pages.dev](https://learnceres.pages.dev) — documentation website

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md), [`AGENTS.md`](AGENTS.md), and the crate-level docs for development setup, tests, and release workflow. This project follows a [Code of Conduct](CODE_OF_CONDUCT.md); security issues are handled per the [security policy](SECURITY.md). Come talk to us on [Discord](https://discord.gg/fztdKSPXSz).

## License

Apache-2.0. See `LICENSE`.
