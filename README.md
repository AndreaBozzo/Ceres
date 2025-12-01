<div align="center">
  <img src="assets/images/logo.jpeg" alt="Ceres Logo" width="800"/>
  <h1>Ceres</h1>
</div>

**Semantic search engine for open data portals.**

Ceres harvests metadata from government open data portals (CKAN, Socrata, DCAT) and indexes them with vector embeddings, enabling semantic search across fragmented data sources.

> *Named after the Roman goddess of harvest and agriculture.*

## General Overview

Open data portals are everywhere, but finding the right dataset is still painful:

- **Keyword search fails**: "public transport" won't find "mobility data" or "bus schedules"
- **Portals are fragmented**: Italy alone has 20+ regional portals with different interfaces
- **No cross-portal search**: You can't query Milano and Roma datasets together

Ceres solves this by creating a unified semantic index. Search by *meaning*, not just keywords.

```bash
$ ceres search "air quality monitoring stations"

1. [0.91] Centraline qualità aria - Comune di Milano
2. [0.87] Stazioni monitoraggio atmosferico - ARPA Lombardia  
3. [0.84] Air quality sensor network - Regione Emilia-Romagna
```

## Status

🚧 **Early development** — not yet ready for production use.

- [x] Database schema with pgvector
- [x] Repository pattern for datasets
- [x] CKAN
- [x] OpenAI embeddings integration
- [ ] CLI interface, separated from Business Logic
- [ ] REST API
- [ ] Harvest command implementation

## Tech Stack

- **Rust** — async runtime with Tokio
- **PostgreSQL + pgvector** — metadata storage and vector similarity search
- **OpenAI API** — text-embedding-3-small for semantic vectors
- **CKAN/DCAT** — standard protocols for open data harvesting

## Quick Start

### Prerequisites

- Rust 1.75+
- PostgreSQL 16+ with pgvector extension
- OpenAI API key

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/ceres.git
cd ceres

# Start PostgreSQL with pgvector
docker-compose up -d

# Run migrations
psql $DATABASE_URL -f migrations/202511290001_init.sql

# Configure environment
cp .env.example .env
# Edit .env with your OpenAI API key

# Build and run
cargo build --release
./target/release/ceres --help
```

### Usage

```bash
# Harvest a CKAN portal
ceres harvest https://dati.comune.milano.it

# Search indexed datasets
ceres search "trasporto pubblico" --limit 10

# Export to JSON Lines
ceres export --format jsonl > datasets.jsonl
```

## Configuration

Portals are configured via `portals.toml`:

```toml
[[portals]]
name = "Milano Open Data"
url = "https://dati.comune.milano.it"
type = "ckan"

[[portals]]
name = "dati.gov.it"
url = "https://dati.gov.it"
type = "ckan"
schedule = "0 3 * * *"  # Daily at 3 AM
```

## Architecture

![Ceres Architecture Diagram](assets/images/Ceres_architecture.png)

## Roadmap

### v0.1 — MVP (in progress)
- CKAN harvester for Italian portals
- OpenAI embeddings
- Basic CLI search
- Single PostgreSQL backend

### v0.2 — Multi-portal
- Socrata support (Lombardia)
- DCAT-AP harvester (EU portals)
- Incremental/delta harvesting
- REST API

### v0.3 — European scale
- Multilingual embeddings (E5-multilingual)
- Cross-language search (query in Italian, find German datasets)
- data.europa.eu integration

### Future
- Local embedding models (no OpenAI dependency)
- Schema-level search ("find datasets with postal codes")
- Data quality scoring
- Knowledge graph linking (ISTAT codes, ATECO, etc.)

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