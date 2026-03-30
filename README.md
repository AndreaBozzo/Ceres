<div align="center">
  <img src="website/src/assets/images/logo.png" alt="Ceres Logo" width="800" height="auto"/>
  <h1>Ceres</h1>
  <p><strong>Harvest-first toolkit for open data portals</strong></p>
  <p>
    <a href="https://crates.io/crates/ceres-search"><img src="https://img.shields.io/crates/v/ceres-search.svg" alt="crates.io"></a>
    <a href="https://github.com/AndreaBozzo/Ceres/actions"><img src="https://github.com/AndreaBozzo/Ceres/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="https://github.com/AndreaBozzo/Ceres/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License"></a>
    <a href="https://discord.gg/fztdKSPXSz"><img src="https://img.shields.io/discord/1469399961987711161?color=5865F2&logo=discord&logoColor=white&label=Discord" alt="Discord"></a>
    <a href="https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index"><img src="https://img.shields.io/badge/%F0%9F%A4%97%20Dataset-Open%20Data%20Index-yellow" alt="HuggingFace Dataset"></a>
  </p>
  <p>
    <a href="#quick-start">Quick Start</a> •
    <a href="#what-ceres-does">What Ceres Does</a> •
    <a href="#usage">Usage</a> •
    <a href="#rest-api">REST API</a>
  </p>
</div>

---

Ceres is a Rust toolkit for harvesting metadata from open data portals and keeping that catalog synchronized over time.

Harvesting is the center of the project. Embeddings, semantic search, exports, and the REST API build on top of the harvested catalog when you want them.

> Named after the Roman goddess of harvest and agriculture.

## What Ceres Does

- Harvests dataset metadata from portal APIs into PostgreSQL with incremental sync, delta detection, and stale dataset tracking
- Works in metadata-only mode, so you can build and maintain a local catalog without any embedding provider configured
- Adds embeddings later through a separate pipeline, with local Ollama as the recommended zero-cost path and OpenAI or Gemini still available
- Exposes optional search, export, and API layers once your catalog is populated

## Why This Shape

Most open data tooling focuses on search first. In practice, the hard part is getting a reliable, repeatable harvesting pipeline:

- Portals expose different APIs and quality levels
- Large catalogs need incremental syncs and bounded memory usage
- Removed datasets need to be detected without deleting history
- Embedding should not be coupled to harvesting, because it is optional, slower, and operationally distinct

Ceres addresses that by splitting the system into two stages:

1. Harvest and normalize metadata
2. Optionally embed and search that catalog

## Current Scope

- Harvesting: CKAN and DCAT-AP udata REST portals
- Embeddings: Ollama locally, or Gemini/OpenAI if you prefer hosted providers
- Search: semantic search over datasets that already have embeddings
- Export: JSONL, JSON, CSV, and curated Parquet
- Operations: CLI, REST API, database-backed harvest jobs, graceful shutdown, protected admin endpoints

## Key Capabilities

- Harvest-first workflow with optional embedding and search
- Streaming harvest pipeline for large portals
- Incremental sync plus content-hash delta detection
- Metadata-only mode with no embedding dependency
- Standalone `embed` command for backfills and provider switches
- Local Ollama embedding support with native batching
- Recoverable job queue for API-triggered harvests
- Soft stale detection for datasets removed upstream
- Batch harvesting through `portals.toml`
- Export pipeline for downstream analytics and HuggingFace publishing

## Supported Portal Types

Today, the shipped portal clients cover:

- `ckan`
- `dcat` for udata-flavored DCAT-AP portals such as `data.public.lu` and `data.gouv.fr`

The codebase already models additional portal types such as `socrata`, but they are not yet implemented in the current client factory.

## Quick Start

### Prerequisites

- Rust 1.88+
- Docker and Docker Compose
- PostgreSQL 16+ with `pgvector` when running outside Docker
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

# Single DCAT portal
cargo run --bin ceres -- harvest https://data.public.lu --type dcat --metadata-only

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
name = "luxembourg"
url = "https://data.public.lu"
type = "dcat"
language = "fr"
enabled = false
```

See `examples/portals.toml` for a larger configuration set.

## Usage

### Harvest

```bash
# All enabled portals from config
ceres harvest

# Ad-hoc CKAN harvest
ceres harvest https://dati.comune.milano.it

# Ad-hoc DCAT harvest
ceres harvest https://data.public.lu --type dcat

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
- `POST /api/v1/portals/{name}/harvest`
- `POST /api/v1/harvest`
- `GET /api/v1/export`
- `GET /swagger-ui`

Set `CERES_ADMIN_TOKEN` to enable protected write endpoints.

## Website Docs

The website lives in `website/` and documents the same harvest-first model:

- harvesting architecture
- optional embeddings and costs
- contributing and security notes

## Related Projects

- [Ceres-Claude-Skill](https://github.com/AndreaBozzo/Ceres-Claude-Skill) for Claude Code and Claude custom skill support
- [AndreaBozzo/ceres-open-data-index](https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index) for published dataset snapshots

## Version

Current version: `0.3.5`

## Contributing

See `website/src/content/docs/CONTRIBUTING.md` and the crate-level docs for development setup, tests, and release workflow.

## License

Apache-2.0. See `LICENSE`.