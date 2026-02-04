<div align="center">
  <img src="docs/assets/images/logo.jpeg" alt="Ceres Logo" width="800" height='auto'/>
  <h1>Ceres</h1>
  <p><strong>Semantic search engine for open data portals</strong></p>
  <p>
    <a href="https://crates.io/crates/ceres-search"><img src="https://img.shields.io/crates/v/ceres-search.svg" alt="crates.io"></a>
    <a href="https://github.com/AndreaBozzo/Ceres/actions"><img src="https://github.com/AndreaBozzo/Ceres/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
    <a href="https://github.com/AndreaBozzo/Ceres/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License"></a>
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

Open data portals are everywhere, but finding the right dataset is still painful:

- **Keyword search fails**: "public transport" won't find "mobility data" or "bus schedules"
- **Portals are fragmented**: Italy alone has 20+ regional portals with different interfaces
- **No cross-portal search**: You can't query Milano and Roma datasets together

Ceres solves this by creating a unified semantic index. Search by *meaning*, not just keywords.

```
$ ceres harvest

INFO [Portal 1/5] milano (https://dati.comune.milano.it)
INFO Found 2575 dataset(s) on portal
INFO Progress: 2575/2575 (100%) - 0 new, 0 updated, 2575 unchanged, 0 failed
INFO [Portal 1/5] milano completed: 2575 dataset(s)

INFO [Portal 2/5] sicilia (https://dati.regione.sicilia.it)
INFO Found 186 dataset(s) on portal
INFO [Portal 2/5] sicilia completed: 186 dataset(s)

INFO [Portal 3/5] trentino (https://dati.trentino.it)
INFO Found 1388 dataset(s) on portal
INFO [Portal 3/5] trentino completed: 1388 dataset(s) (1388 created)

INFO [Portal 4/5] aragon (https://opendata.aragon.es/ckan)
INFO Found 2879 dataset(s) on portal
INFO [Portal 4/5] aragon completed: 2879 dataset(s) (2879 created)

INFO [Portal 5/5] nrw (https://ckan.open.nrw.de)
INFO Found 10926 dataset(s) on portal
INFO [Portal 5/5] nrw completed: 10926 dataset(s) (10926 created)

═══════════════════════════════════════════════════════
BATCH HARVEST COMPLETE
═══════════════════════════════════════════════════════
  Portals processed:   5
  Successful:          5
  Failed:              0
  Total datasets:      17954
═══════════════════════════════════════════════════════
```

```
$ ceres search "trasporto pubblico" --limit 5

🔍 Search Results for: "trasporto pubblico"

Found 5 matching datasets:

1. [████████░░] [78%] TPL - Percorsi linee di superficie
   📍 https://dati.comune.milano.it
   🔗 https://dati.comune.milano.it/dataset/ds534-tpl-percorsi-linee-di-superficie
   📝 Il dataset contiene i tracciati delle linee di trasporto pubblico di superficie...

2. [████████░░] [76%] TPL - Fermate linee di superficie
   📍 https://dati.comune.milano.it
   🔗 https://dati.comune.milano.it/dataset/ds535-tpl-fermate-linee-di-superficie
   📝 Il dataset contiene le fermate delle linee di trasporto pubblico di superficie...

3. [███████░░░] [72%] Mobilità: flussi veicolari rilevati dai spire
   📍 https://dati.comune.milano.it
   🔗 https://dati.comune.milano.it/dataset/ds418-mobilita-flussi-veicolari
   📝 Dati sul traffico veicolare rilevati dalle spire elettromagnetiche...
```

```
$ ceres stats

📊 Database Statistics

  Total datasets:        17954
  With embeddings:       17954
  Unique portals:        5
  Last update:           2025-12-31 15:09:48 UTC
```

## Features

- **CKAN Harvester** — Fetch datasets from any CKAN-compatible portal
- **Multi-portal Batch Harvest** — Configure multiple portals in `portals.toml` and harvest them all at once
- **Delta Detection** — Only regenerate embeddings for changed datasets (99.8% API cost savings). See [Harvesting Architecture](docs/HARVESTING.md)
- **Persistent Jobs** — Recoverable database-backed job queue with automatic retries and exponential backoff
- **Graceful Shutdown** — Safely interrupt harvesting to ensure data consistency and release in-progress jobs back to the queue
- **Real-time Progress** — Live progress reporting during harvest with batch timestamp updates
- **Semantic Search** — Find datasets by meaning using Gemini embeddings
- **Multi-format Export** — Export to JSON, JSON Lines, or CSV
- **Database Statistics** — Monitor indexed datasets and portals

## Pre-configured Portals

Ceres comes with verified CKAN portals ready to use:

| Portal | Region | Datasets |
|--------|--------|----------|
| Milano | Italy | ~2,575 |
| Sicilia | Italy | ~186 |
| Trentino | Italy | ~1,388 |
| Aragón | Spain | ~2,879 |
| NRW | Germany | ~10,926 |

See [`examples/portals.toml`](examples/portals.toml) for the full list. Want to add more? Check [issue #19](https://github.com/AndreaBozzo/Ceres/issues/19).

## Cost-Effectiveness

API costs, based on the Gemini embedding model, are almost negligible, making the solution extremely efficient even for personal projects or those with limited budgets.

The main cost is for the initial creation of vector embeddings. Below is a cost breakdown for a large catalog.

### Cost Analysis for Initial Indexing

This scenario estimates the one-time cost to index a catalog of 50,000 datasets.

| Metric | Detail |
|--------------------------------|--------------------------------------------------------------------------------|
| **Cost per 1M Input Tokens** | ~$0.15 USD (Standard rate for Google's `text-embedding-004` model) |
| **Estimated Tokens per Dataset** | 500 tokens (A generous estimate for title, description, and tags) |
| **Total Tokens** | `50,000 datasets * 500 tokens/dataset = 25,000,000 tokens` |
| **Total Initial Cost** | `(25,000,000 / 1,000,000) * $0.15 =` **$3.75** |

As shown, the initial cost to index a substantial number of datasets is just a few dollars. Monthly maintenance for incremental updates would be even lower, typically amounting to a few cents.

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Rust (async with Tokio) |
| Database | PostgreSQL 16+ with pgvector |
| Embeddings | Google Gemini text-embedding-004 |
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

# Or manually with psql if you prefer
# psql postgresql://ceres_user:password@localhost:5432/ceres_db \
#   -f migrations/202511290001_init.sql
```

> **💡 Tip**: This project includes a Makefile with convenient shortcuts. Run `make help` to see all available commands.

## Usage

### Harvest datasets from a CKAN portal

```bash
ceres harvest https://dati.comune.milano.it
```
> **💡 Tip**: Running the first harvest commands fails and generates a pre-configured portals.toml if you didn't set it up before, now you can just run it again!

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
- `GET /api/v1/search` — Semantic search
- `POST /api/v1/harvest` — Trigger harvesting
- `GET /api/v1/export` — Export datasets
- `GET /api/v1/stats` — Database statistics
- `GET /api/v1/portals` — List portals
- `GET /swagger-ui` — Interactive API docs

Server environment variables:
```
PORT                  Server port (default: 3000)
HOST                  Server host (default: 0.0.0.0)
CORS_ALLOWED_ORIGINS  Comma-separated allowed origins
RATE_LIMIT_RPS        Requests per second limit
```

## Development

The project includes a Makefile with convenient shortcuts for common development tasks:

```bash
# Start development environment (starts PostgreSQL with docker-compose)
make dev

# Run database migrations
make migrate

# Build the project
make build

# Build in release mode
make release

# Run tests
make test

# Format code
make fmt

# Run lints
make clippy

# See all available commands
make help
```

## Architecture

![Ceres Architecture Diagram](docs/assets/images/Ceres_architecture.png)

## Roadmap

### v0.0.1 — Initial Release ✅
- CKAN harvester with concurrent processing
- Gemini embeddings (text-embedding-004, 768 dimensions)
- CLI with harvest, search, export, stats commands
- PostgreSQL + pgvector backend
- Multi-format export (JSON, JSONL, CSV)

### v0.1 — Enhancements ✅
- Portals configuration from `portals.toml`
- Delta detection
- Improved error handling and retry logic

### v0.2 — Multi-portal & API ✅
- Incremental harvesting (time-based metadata filtering)
- REST API
- Graceful shutdown
- Persistent jobs
- Streaming export/search

### Future
- Multilingual embeddings (E5-multilingual)
- Cross-language search
- data.europa.eu integration
- Socrata support
- DCAT-AP harvester (EU portals)
- Switchable embedding providers
- Schema-level search
- Data quality scoring
- Multi-tenant
## Contributing

Contributions are welcome! This project is in early stages, so there's plenty of room to shape its direction.

```bash
# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run -- harvest https://dati.comune.milano.it
```

See [CONTRIBUTING.md](docs/CONTRIBUTING.md) for guidelines.

## License

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Acknowledgments

- [pgvector](https://github.com/pgvector/pgvector) — vector similarity for Postgres
- [Google Gemini](https://ai.google.dev/) — embeddings API
- [CKAN](https://ckan.org/) — the open source data portal platform