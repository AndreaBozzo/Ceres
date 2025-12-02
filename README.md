<div align="center">
  <img src="assets/images/logo.jpeg" alt="Ceres Logo" width="800"/>
  <h1>Ceres</h1>
  <p><strong>Semantic search engine for open data portals</strong></p>
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

```bash
$ ceres search "air quality monitoring stations"

🔍 Search Results for: "air quality monitoring stations"

Found 3 matching datasets:

1. [██████████] [91%] Centraline qualità aria
   📍 https://dati.comune.milano.it
   🔗 https://dati.comune.milano.it/dataset/centraline-qualita-aria

2. [████████░░] [87%] Stazioni monitoraggio atmosferico
   📍 https://dati.arpalombardia.it
   🔗 https://dati.arpalombardia.it/dataset/stazioni-monitoraggio

3. [████████░░] [84%] Air quality sensor network
   📍 https://dati.emilia-romagna.it
   🔗 https://dati.emilia-romagna.it/dataset/air-quality-sensors
```

## Features

- **CKAN Harvester** — Fetch datasets from any CKAN-compatible portal
- **Semantic Search** — Find datasets by meaning using OpenAI embeddings
- **Multi-format Export** — Export to JSON, JSON Lines, or CSV
- **Database Statistics** — Monitor indexed datasets and portals

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Rust (async with Tokio) |
| Database | PostgreSQL 16+ with pgvector |
| Embeddings | OpenAI text-embedding-3-small |
| Portal Protocol | CKAN API v3 |

## Quick Start

### Prerequisites

- Rust 1.85+
- Docker & Docker Compose
- OpenAI API key

### Setup

```bash
# Clone and enter directory
git clone https://github.com/AndreaBozzo/Ceres.git
cd Ceres

# Start PostgreSQL with pgvector
docker-compose up -d

# Run database migrations
psql postgresql://ceres_user:password@localhost:5432/ceres_db \
  -f migrations/202511290001_init.sql

# Configure environment
cp .env.example .env
# Edit .env with your OpenAI API key

# Build
cargo build --release
```

## Usage

### Harvest datasets from a CKAN portal

```bash
ceres harvest https://dati.comune.milano.it
```

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
  harvest  Harvest datasets from a CKAN portal
  search   Search indexed datasets using semantic similarity
  export   Export indexed datasets to various formats
  stats    Show database statistics
  help     Print help information

Environment Variables:
  DATABASE_URL     PostgreSQL connection string
  OPENAI_API_KEY   OpenAI API key for embeddings
```

## Architecture

![Ceres Architecture Diagram](assets/images/Ceres_architecture.png)

## Roadmap

### v0.0.1 — Initial Release ✅
- CKAN harvester
- OpenAI embeddings (text-embedding-3-small)
- CLI with harvest, search, export, stats commands
- PostgreSQL + pgvector backend

### v0.1 — Enhancements
- Portals configuration from `portals.toml`
- Incremental/delta harvesting
- Improved error handling and retry logic

### v0.2 — Multi-portal & API
- REST API
- Socrata support
- DCAT-AP harvester (EU portals)

### v0.3 — European scale
- Multilingual embeddings (E5-multilingual)
- Cross-language search
- data.europa.eu integration

### Future
- Local embedding models (no OpenAI dependency)
- Schema-level search
- Data quality scoring

## Contributing

Contributions are welcome! This project is in early stages, so there's plenty of room to shape its direction.

```bash
# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run -- harvest https://dati.comune.milano.it
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

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
- [async-openai](https://github.com/64bit/async-openai) — OpenAI Rust client
- [CKAN](https://ckan.org/) — the open source data portal platform