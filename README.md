<div align="center">
  <img src="docs/assets/images/logo.jpeg" alt="Ceres Logo" width="800" height='auto'/>
  <h1>Ceres</h1>
  <p><strong>Semantic search engine for open data portals</strong></p>
  <p>
    <a href="https://crates.io/crates/ceres-search"><img src="https://img.shields.io/crates/v/ceres-search.svg" alt="crates.io"></a>
    <a href="https://github.com/AndreaBozzo/Ceres/actions"><img src="https://github.com/AndreaBozzo/Ceres/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="https://github.com/AndreaBozzo/Ceres/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License"></a>
    <a href="https://discord.gg/fztdKSPXSz"><img src="https://img.shields.io/discord/1469399961987711161?color=5865F2&logo=discord&logoColor=white&label=Discord" alt="Discord"></a>
    <a href="https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index"><img src="https://img.shields.io/badge/%F0%9F%A4%97%20Dataset-230k%20datasets-yellow" alt="HuggingFace Dataset"></a>
  </p>
  <p>
    <a href="#quick-start">Quick Start</a> •
    <a href="#features">Features</a> •
    <a href="#usage">Usage</a> •
    <a href="#roadmap">Roadmap</a>
  </p>
</div>

---

Ceres harvests metadata from CKAN open data portals and indexes them with vector embeddings, enabling semantic search across fragmented data sources.

> *Named after the Roman goddess of harvest and agriculture.*

## Why Ceres?

<div align="center">
  <img src="docs/assets/images/open_data_galaxy.gif" alt="Open Data Galaxy — ML-generated visualization" width="800"/>
  <br/>
  <sub>354,000+ datasets from 25 portals, embedded with <a href="https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2">all-MiniLM-L6-v2</a>, projected to 3D via UMAP, and clustered with HDBSCAN. Each color is a portal — nearby points are semantically similar.</sub>
</div>

Open data portals are everywhere, but finding the right dataset is still painful:

- **Keyword search fails**: "public transport" won't find "mobility data" or "bus schedules"
- **Portals are fragmented**: Italy alone has 20+ regional portals with different interfaces
- **No cross-portal search**: You can't query Milano and Roma datasets together

Ceres solves this by creating a unified semantic index. Search by *meaning*, not just keywords.

```
$ ceres search "trasporto pubblico" --limit 3

Found 3 matching datasets:

1. [████████░░] [78%] TPL - Percorsi linee di superficie
   📍 https://dati.comune.milano.it
   🔗 https://dati.comune.milano.it/dataset/ds534-tpl-percorsi-linee-di-superficie

2. [████████░░] [76%] TPL - Fermate linee di superficie
   📍 https://dati.comune.milano.it
   🔗 https://dati.comune.milano.it/dataset/ds535-tpl-fermate-linee-di-superficie

3. [███████░░░] [72%] Mobilità: flussi veicolari rilevati dai spire
   📍 https://dati.comune.milano.it
   🔗 https://dati.comune.milano.it/dataset/ds418-mobilita-flussi-veicolari
```

## Features

- **CKAN Harvester** — Fetch datasets from any CKAN-compatible portal
- **Multi-portal Batch Harvest** — Configure multiple portals in `portals.toml` and harvest them all at once
- **Delta Detection** — Only regenerate embeddings for changed datasets (99.8% API cost savings). See [Harvesting Architecture](docs/HARVESTING.md)
- **Persistent Jobs** — Recoverable database-backed job queue with automatic retries and exponential backoff
- **Graceful Shutdown** — Safely interrupt harvesting to ensure data consistency and release in-progress jobs back to the queue
- **Real-time Progress** — Live progress reporting during harvest with batch timestamp updates
- **Semantic Search** — Find datasets by meaning using Gemini embeddings
- **Pluggable Embeddings** — Switchable embedding backend via trait (Gemini, OpenAI)
- **Multi-format Export** — Export to JSON, JSON Lines, CSV, or Parquet

## Pre-configured Portals

Ceres comes with 25 verified CKAN portals ready to use, covering 354,000+ datasets:

| Portal | Region | Datasets |
|--------|--------|----------|
| Australia | Australia | ~109,440 |
| Italy (National) | Italy | ~70,141 |
| Ukraine | Ukraine | ~39,790 |
| HDX (Humanitarian) | Global | ~26,654 |
| NRW | Germany | ~22,849 |
| Ireland | Ireland | ~21,855 |
| Switzerland | Switzerland | ~14,559 |
| Toscana | Italy | ~12,886 |
| Tokyo | Japan | ~9,707 |
| Marche | Italy | ~5,440 |
| Romania | Romania | ~5,038 |
| Chile | Chile | ~2,897 |
| Aragón | Spain | ~2,881 |
| Emilia-Romagna | Italy | ~2,871 |
| Milano | Italy | ~2,580 |
| Puglia | Italy | ~1,801 |
| Trentino | Italy | ~1,388 |
| Umbria | Italy | ~457 |
| Lazio | Italy | ~407 |
| Roma | Italy | ~365 |
| Campania | Italy | ~332 |
| Sicilia | Italy | ~186 |
| Genova | Italy | ~171 |
| Liguria | Italy | ~124 |
| Napoli | Italy | ~33 |

See [`examples/portals.toml`](examples/portals.toml) for the full configuration. Want to add more? Check [issue #19](https://github.com/AndreaBozzo/Ceres/issues/19).

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Rust (async with Tokio) |
| Database | PostgreSQL 16+ with pgvector |
| Embeddings | Google Gemini gemini-embedding-001 |
| Portal Protocol | CKAN API v3 |
| REST API | Axum with OpenAPI/Swagger UI |

## Quick Start

### Prerequisites

- Rust 1.87+
- Docker & Docker Compose
- Google Gemini API key ([get one free](https://aistudio.google.com/apikey))

### Installation

```bash
# Install from crates.io
cargo install ceres-search

# Or build from source
git clone https://github.com/AndreaBozzo/Ceres.git
cd Ceres
cargo build --release
```

### Setup

```bash
# Start PostgreSQL with pgvector
docker-compose up -d

# Configure environment
cp .env.example .env
# Edit .env with your Gemini API key

# Run database migrations
make migrate
```

> **Tip**: Run `make help` to see all available Makefile shortcuts.

## Usage

### Harvest datasets from a CKAN portal

```bash
ceres harvest https://dati.comune.milano.it
```

> **Tip**: Running a harvest command for the first time without a config generates a pre-configured `portals.toml` automatically.

### Search indexed datasets

```bash
ceres search "trasporto pubblico" --limit 10
```

### Export datasets

```bash
# JSON Lines (default)
ceres export > datasets.jsonl

# JSON array
ceres export --format json > datasets.json

# CSV
ceres export --format csv > datasets.csv

# Filter by portal
ceres export --portal https://dati.comune.milano.it
```

### View statistics

```bash
ceres stats
```

## CLI Reference

```
ceres <COMMAND>

Commands:
  harvest  Harvest datasets from a CKAN portal or batch harvest from portals.toml
  search   Search indexed datasets using semantic similarity
  export   Export indexed datasets to various formats
  stats    Show database statistics
  help     Print help information

Environment Variables:
  DATABASE_URL     PostgreSQL connection string
  GEMINI_API_KEY   Google Gemini API key for embeddings
```

## REST API

Start the server:

```bash
ceres-server
```

Available endpoints:
- `GET  /api/v1/health` — Health check
- `GET  /api/v1/stats` — Database statistics
- `GET  /api/v1/search` — Semantic search
- `GET  /api/v1/portals` — List configured portals
- `GET  /api/v1/portals/:name/stats` — Portal-specific statistics
- `POST /api/v1/portals/:name/harvest` — Trigger harvest for a portal
- `POST /api/v1/harvest` — Trigger harvest for all portals
- `GET  /api/v1/harvest/status` — Check harvest job status
- `GET  /api/v1/export` — Export datasets
- `GET  /api/v1/datasets/:id` — Get dataset by ID
- `GET  /swagger-ui` — Interactive API docs

Server environment variables:
```
PORT                   Server port (default: 3000)
HOST                   Server host (default: 0.0.0.0)
EMBEDDING_PROVIDER     Embedding backend: gemini or openai (default: gemini)
EMBEDDING_MODEL        Model name (uses provider default if unset)
PORTALS_CONFIG         Path to portals.toml (optional)
CORS_ALLOWED_ORIGINS   Comma-separated allowed origins (default: *)
RATE_LIMIT_RPS         Requests per second per IP (default: 10)
RATE_LIMIT_BURST       Burst size for rate limiting (default: 30)
```

## Architecture

<div align="center">
  <img src="docs/assets/images/Ceres_architecture.png" alt="Ceres Architecture Diagram" width="100%" />
  <br/>
  <sub>High-level architecture of Ceres components and data flow.</sub>
</div>

### Harvesting Internals

<div align="center">
  <img src="docs/assets/images/harvesting.png" alt="Harvesting Flow Diagram" width="900" />
  <br/>
  <sub>Two-tier optimization flow: incremental sync + delta detection.</sub>
</div>

<div align="center">
  <img src="docs/assets/images/circuitbreaker.png" alt="Circuit Breaker Diagram" width="900" />
  <br/>
  <sub>Circuit breaker states and recovery behavior for embedding requests.</sub>
</div>

## Roadmap

For past releases, see the [CHANGELOG](CHANGELOG.md).

### v0.3.0 — Extensibility
- Streaming harvest for large portals ([#85](https://github.com/AndreaBozzo/Ceres/issues/85))
- Local embeddings via Ollama ([#79](https://github.com/AndreaBozzo/Ceres/issues/79))
- Abstract PortalClient for Socrata / DCAT-AP support ([#61](https://github.com/AndreaBozzo/Ceres/issues/61))
- Authentication middleware ([#72](https://github.com/AndreaBozzo/Ceres/issues/72))
- Docker image ([#74](https://github.com/AndreaBozzo/Ceres/issues/74))
- Delta detection improvements ([#51](https://github.com/AndreaBozzo/Ceres/issues/51), [#53](https://github.com/AndreaBozzo/Ceres/issues/53))


### v0.4.0 Multi-Tenancy, DCAT
- Authentication middleware ([#72](https://github.com/AndreaBozzo/Ceres/issues/72))
- Schema-level search ([#68](https://github.com/AndreaBozzo/Ceres/issues/68))
- DCAT Client
- More.

### Backlog
- Schema-level search ([#68](https://github.com/AndreaBozzo/Ceres/issues/68))
- Standalone library support ([#35](https://github.com/AndreaBozzo/Ceres/issues/35))
- data.europa.eu integration

## Related Projects

- **[databricks-ceres-pipeline](https://github.com/AndreaBozzo/databricks-ceres-pipeline)** — A Databricks medallion architecture pipeline that provides batch analytics, ML features, and dashboards on top of the same open data index.

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](docs/CONTRIBUTING.md) for setup instructions and guidelines.

## License

Apache-2.0 — see [LICENSE](LICENSE).

---

<div align="center">
  <sub>Built with <a href="https://github.com/pgvector/pgvector">pgvector</a>, <a href="https://ai.google.dev/">Google Gemini</a>, and <a href="https://ckan.org/">CKAN</a>.</sub>
</div>
