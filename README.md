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
  <sub>300,000 datasets from 20 portals, embedded with <a href="https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2">all-MiniLM-L6-v2</a>, projected to 3D via UMAP, and clustered with HDBSCAN. Each color is a portal — nearby points are semantically similar, GIF shows a 15k sample.</sub>
  
</div>


Open data portals are everywhere, but finding the right dataset is still painful:

- **Keyword search fails**: "public transport" won't find "mobility data" or "bus schedules"
- **Portals are fragmented**: Italy alone has 20+ regional portals with different interfaces
- **No cross-portal search**: You can't query Milano and Roma datasets together

Ceres solves this by creating a unified semantic index. Search by *meaning*, not just keywords.

```
$ ceres harvest

INFO [Portal 1/12] milano (https://dati.comune.milano.it)
INFO Found 2575 dataset(s) on portal
INFO [Portal 1/12] milano completed: 2575 dataset(s)

...

═══════════════════════════════════════════════════════
BATCH HARVEST COMPLETE
═══════════════════════════════════════════════════════
  Portals processed:   12
  Successful:          12
  Failed:              0
  Total datasets:      106657
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

  Total datasets:        106657
  With embeddings:       106657
  Unique portals:        12
  Last update:           2026-02-06 15:31:06.250326 UTC
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

![Ceres Architecture Diagram](docs/assets/images/Ceres_architecture.png)

## Roadmap

For past releases, see the [CHANGELOG](CHANGELOG.md).

### v0.3.0 — Extensibility & Production Readiness
- Streaming harvest for large portals ([#85](https://github.com/AndreaBozzo/Ceres/issues/85))
- Local embeddings via Ollama ([#79](https://github.com/AndreaBozzo/Ceres/issues/79))
- Abstract PortalClient for Socrata / DCAT-AP support ([#61](https://github.com/AndreaBozzo/Ceres/issues/61))
- Authentication middleware ([#72](https://github.com/AndreaBozzo/Ceres/issues/72))
- Docker image ([#74](https://github.com/AndreaBozzo/Ceres/issues/74))
- Delta detection improvements ([#51](https://github.com/AndreaBozzo/Ceres/issues/51), [#53](https://github.com/AndreaBozzo/Ceres/issues/53))
- Integration test suite ([#29](https://github.com/AndreaBozzo/Ceres/issues/29))

### Backlog
- Schema-level search ([#68](https://github.com/AndreaBozzo/Ceres/issues/68))
- Standalone library support ([#35](https://github.com/AndreaBozzo/Ceres/issues/35))
- Cross-language semantic search
- data.europa.eu integration

## Related Projects

- **[databricks-ceres-pipeline](https://github.com/AndreaBozzo/databricks-ceres-pipeline)** — A Databricks medallion architecture pipeline that provides batch analytics, ML features, and dashboards on top of the same open data index.

## Contributing

Contributions are welcome! See [CONTRIBUTING.md](docs/CONTRIBUTING.md) for setup instructions and guidelines.

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