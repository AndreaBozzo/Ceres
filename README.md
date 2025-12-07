<div align="center">
  <img src="docs/assets/images/logo.jpeg" alt="Ceres Logo" width="800"/>
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

```
$ ceres harvest https://dati.comune.milano.it

INFO ceres: Connecting to database...
INFO ceres: Starting harvest for: https://dati.comune.milano.it
INFO ceres: Fetching package list...
INFO ceres: Found 2575 datasets. Starting concurrent processing...
INFO ceres: [1/2575] ✓ Indexed: Catalogo CSV dei dataset (b74f2145-bb06-4891-87b2-32ffaccb3689)
INFO ceres: [2/2575] ✓ Indexed: Biblioteche: numero dei prestiti... (d46f6782-a1ef-4e0a-9a8f-af0dd30037f2)
...
INFO ceres: Harvesting complete: 2575 successful, 0 failed
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

  Total datasets:        2575
  With embeddings:       2575
  Unique portals:        1
  Last update:           2025-12-02 17:34:19 UTC
```

## Features

- **CKAN Harvester** — Fetch datasets from any CKAN-compatible portal
- **Semantic Search** — Find datasets by meaning using Gemini embeddings
- **Multi-format Export** — Export to JSON, JSON Lines, or CSV
- **Database Statistics** — Monitor indexed datasets and portals

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

## Quick Start

### Prerequisites

- Rust 1.85+
- Docker & Docker Compose
- Google Gemini API key ([get one free](https://aistudio.google.com/apikey))

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
# Edit .env with your Gemini API key

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
  GEMINI_API_KEY   Google Gemini API key for embeddings
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
- Switchable embedding providers
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
- [Google Gemini](https://ai.google.dev/) — embeddings API
- [CKAN](https://ckan.org/) — the open source data portal platform