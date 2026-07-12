# CLI & REST API

## CLI (`ceres`)

Install: `cargo install ceres-search`

### `ceres harvest` — Harvest datasets from configured or ad-hoc portals

```bash
ceres harvest                               # Harvest all enabled portals from config
ceres harvest https://dati.comune.milano.it # Harvest single URL (backward compatible)
ceres harvest https://data.public.lu --type dcat
ceres harvest https://data.europa.eu --type dcat --profile sparql
ceres harvest --portal milano               # Harvest portal by name from config
ceres harvest --config ~/custom.toml        # Use custom config file
ceres harvest --full-sync                   # Force full sync (ignore incremental)
ceres harvest --dry-run                     # Preview without DB writes or embedding calls
```

| Flag | Short | Description |
|---|---|---|
| `<URL>` | | Single portal URL (positional, backward compatible) |
| `--type <TYPE>` | | Portal type for ad-hoc URL (`ckan`, `dcat`, `socrata`, `opendatasoft`) |
| `--profile <PROFILE>` | | DCAT profile for ad-hoc DCAT URLs, currently `sparql` for SPARQL-backed catalogs |
| `--portal <NAME>` | `-p` | Named portal from config file |
| `--config <PATH>` | `-c` | Custom portals.toml path |
| `--full-sync` | | Force full sync even if incremental is available |
| `--dry-run` | | Preview what would happen without DB writes |
| `--metadata-only` | | Harvest metadata only, no embedding (no API key needed) |

Metadata-only harvesting is the normal operational mode when you want to build or refresh the catalog without touching embeddings.

### `ceres embed` — Generate embeddings for pending datasets

```bash
ceres embed                              # Embed all pending datasets
ceres embed --portal https://dati.gov.it # Embed pending from one portal
```

| Flag | Short | Description |
|---|---|---|
| `--portal <URL>` | `-p` | Only embed pending datasets from this portal |

Processes datasets with `embedding IS NULL`. Can run independently from harvest.

Common use cases:

- backfill embeddings after a metadata-only harvest
- switch providers without re-harvesting
- isolate embedding runs from crawl operations

### `ceres search` — Semantic search across indexed datasets

```bash
ceres search "trasporto pubblico" --limit 10
ceres search "air quality monitoring" --limit 5
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `<QUERY>` | | (required) | Search query text |
| `--limit` | `-l` | 10 | Maximum results |

Output: ranked results with similarity score (0-100%), portal name, and dataset URL.

### `ceres export` — Export indexed datasets

```bash
ceres export --format jsonl > datasets.jsonl
ceres export --format json --portal https://dati.gov.it
ceres export --format csv > datasets.csv
ceres export --format parquet --output ./ceres-export
ceres export --format parquet --output ./ceres-export --previous ./previous-export
```

| Flag | Short | Default | Description |
|---|---|---|---|
| `--format` | `-f` | jsonl | Output format: `jsonl`, `json`, `csv`, `parquet` |
| `--portal` | `-p` | | Filter by source portal URL |
| `--limit` | `-l` | | Maximum datasets to export |
| `--output` | `-o` | | Output directory (required for parquet) |
| `--config` | `-c` | | Custom portals.toml (for portal name resolution in parquet) |
| `--previous` | | | Previous snapshot directory to diff against; parquet only, writes snapshot changelogs |

A parquet export writes a portable snapshot directory:

- `all.parquet` — canonical complete index
- `data/<portal-name>.parquet` — per-portal subsets (repeat rows from `all.parquet`; do not sum into the canonical total)
- `identity.parquet` — slim per-record fingerprint used for snapshot-to-snapshot diffs
- `metadata.json` — versioned snapshot manifest (`snapshot_id`, provenance, row counts, alias-aware duplicate metadata, per-portal status, SHA-256 checksums)
- `reports.json` / `report.md` — coverage and quality reports derived from the same pass (figures match the manifest)
- `changelog.json` / `changelog.md` — added/changed/removed/unchanged counts when `--previous` points at a comparable prior snapshot

### `ceres stats` — Show database statistics

```bash
ceres stats
```

Output: total datasets, datasets with embeddings, stale datasets, unique portals, last update timestamp.

---

## REST API (`ceres-server`)

Start: `ceres-server` (reads from `.env` and env vars)

Base URL: `http://{HOST}:{PORT}` (default `http://127.0.0.1:3000`)

Swagger UI: `http://127.0.0.1:3000/swagger-ui`

### Public Endpoints (no auth)

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/health` | Health check (DB connectivity) |
| GET | `/api/v1/stats` | Database statistics |
| GET | `/api/v1/search?q=...&limit=10` | Semantic search |
| GET | `/api/v1/datasets/:id` | Get dataset by UUID |
| GET | `/api/v1/datasets/:id/schema` | Get normalized resource/distribution schema derived from harvested metadata |
| GET | `/api/v1/portals` | List configured portals |
| GET | `/api/v1/portals/:name/stats` | Portal-specific statistics |
| GET | `/api/v1/harvest/status` | Check harvest job status |

### Protected Endpoints (Bearer token)

| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/v1/portals/:name/harvest` | Trigger harvest for one portal |
| POST | `/api/v1/harvest` | Trigger harvest for all portals |
| GET | `/api/v1/export` | Export datasets |

Authentication: `Authorization: Bearer <CERES_ADMIN_TOKEN>`. If `CERES_ADMIN_TOKEN` is unset, admin endpoints return 403 Forbidden.

### Example Requests

```bash
# Search
curl "http://localhost:3000/api/v1/search?q=trasporto+pubblico&limit=5"

# Trigger harvest for a portal
curl -X POST http://localhost:3000/api/v1/portals/milano/harvest \
  -H "Authorization: Bearer $CERES_ADMIN_TOKEN"

# Trigger harvest for all portals
curl -X POST http://localhost:3000/api/v1/harvest \
  -H "Authorization: Bearer $CERES_ADMIN_TOKEN"

# Check harvest status
curl http://localhost:3000/api/v1/harvest/status

# Export as JSONL
curl http://localhost:3000/api/v1/export \
  -H "Authorization: Bearer $CERES_ADMIN_TOKEN" > datasets.jsonl
```

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | (required) | PostgreSQL connection URL |
| `DB_MAX_CONNECTIONS` | 10 | Connection pool size |
| `EMBEDDING_PROVIDER` | `gemini` | `gemini`, `openai`, or `ollama` |
| `GEMINI_API_KEY` | | Google Gemini API key (required for embedding with gemini) |
| `OPENAI_API_KEY` | | OpenAI API key (required for embedding with openai) |
| `EMBEDDING_MODEL` | | Provider-specific model override |
| `OLLAMA_ENDPOINT` | `http://localhost:11434` | Ollama API endpoint |
| `DATA_GOV_API_KEY` | | Required for `catalog.data.gov`, which routes CKAN API traffic through `api.gsa.gov` |
| `HOST` | `127.0.0.1` | Server bind address |
| `PORT` | `3000` | Server port |
| `CORS_ALLOWED_ORIGINS` | `*` | Comma-separated origins or `*` |
| `RATE_LIMIT_RPS` | `10` | Requests per second per IP |
| `RATE_LIMIT_BURST` | `30` | Burst size for rate limiter |
| `PORTALS_CONFIG` | `~/.config/ceres/portals.toml` | Portal configuration file path |
| `CERES_ADMIN_TOKEN` | | Bearer token for admin endpoints |
| `RUST_LOG` | `info` | Log level (tracing) |
| `CB_FAILURE_THRESHOLD` | `5` | Circuit breaker failure threshold |
| `CB_RECOVERY_TIMEOUT_SECS` | `30` | Circuit breaker recovery timeout |
| `CB_SUCCESS_THRESHOLD` | `2` | Circuit breaker success threshold |
| `CB_RATE_LIMIT_BACKOFF_MULTIPLIER` | `2.0` | Backoff multiplier on 429 |
| `CB_MAX_RECOVERY_TIMEOUT_SECS` | `300` | Max circuit breaker timeout |
| `CERES_HTTP_TIMEOUT_SECS` | `60` | Base per-request portal HTTP timeout |
| `CERES_HTTP_MAX_RETRIES` | `3` | Transient-error retry attempts |
| `CERES_HTTP_RETRY_BASE_MS` | `500` | Base retry backoff delay in milliseconds |

---

## Portal Configuration (`portals.toml`)

Default location: `~/.config/ceres/portals.toml`. Auto-generated on first run.

```toml
[[portals]]
name = "milano"
url = "https://dati.comune.milano.it"
type = "ckan"
enabled = true
language = "it"

[[portals]]
name = "hdx"
url = "https://data.humdata.org"
type = "ckan"
enabled = true
language = "en"
url_template = "https://data.humdata.org/dataset/{name}"

[[portals]]
name = "luxembourg"
url = "https://data.public.lu"
type = "dcat"
language = "fr"
enabled = false

[[portals]]
name = "eu-open-data"
url = "https://data.europa.eu"
type = "dcat"
profile = "sparql"
language = "en"
enabled = false
```

Fields:

| Field | Required | Default | Description |
|---|---|---|---|
| `name` | Yes | | Portal identifier (used with `--portal` flag) |
| `url` | Yes | | Portal API base URL |
| `type` | No | `ckan` | Portal type (`ckan`, `dcat`, `socrata`, `opendatasoft`, `arcgis`, `ogc_records`) |
| `profile` | No | | DCAT profile selector, e.g. `sparql`; omitted DCAT defaults to udata REST |
| `sparql_endpoint` | No | | Override SPARQL endpoint for `profile = "sparql"` portals |
| `ogc_endpoint` | No | | CSW service URL for `type = "ogc_records"` portals when it differs from `url` |
| `enabled` | No | `true` | Include in batch harvests |
| `language` | No | `en` | Preferred language for multilingual fields |
| `url_template` | No | | Custom URL template with `{id}` and `{name}` placeholders |
| `description` | No | | Human-readable description |

---

## Deployment

### Docker Compose (development)

```bash
docker compose up -d        # Start PostgreSQL + pgAdmin
make migrate                # Run database migrations
cargo run -- harvest        # Run CLI
cargo run --bin ceres-server  # Start server
```

### Docker (production)

```bash
docker build -t ceres-server .
docker run -d \
  --env-file .env \
  -p 3000:3000 \
  ceres-server
```

The Dockerfile uses a multi-stage build (build in Rust image, run in minimal Debian image).

For local-first setups, pair the server or CLI with a local Ollama instance and keep harvesting and embedding as separate operational steps.
