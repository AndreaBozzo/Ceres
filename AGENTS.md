# Ceres contributor guide

Ceres is a harvest-first Rust toolkit for open data portal metadata: portal
clients stream dataset metadata into PostgreSQL with incremental sync, delta
detection, and stale tracking; embeddings, semantic search, the REST API, and
Parquet snapshot exports are optional layers on top.

## Project map

- `crates/ceres-core`: domain models, harvesting pipeline, configuration, and traits (`PortalClient`, `EmbeddingProvider`, `DatasetStore`).
- `crates/ceres-client`: portal clients (CKAN, DCAT udata REST, SPARQL DCAT, Project Open Data `data.json`, Socrata Discovery, OpenDataSoft Explore) and embedding providers (Ollama, Gemini, OpenAI).
- `crates/ceres-db`: PostgreSQL/pgvector persistence.
- `crates/ceres-server`: Axum HTTP API and OpenAPI definitions.
- `crates/ceres-cli`: command-line entry point (`harvest`, `embed`, `search`, `export`, `stats`).
- `migrations/`: ordered PostgreSQL migrations.
- `examples/portals.toml`: curated portal configuration set.
- `website/`: Astro/Starlight documentation site (deployed to learnceres.pages.dev).
- `.claude/skills/ceres`: in-repo Claude Code skill — architecture, traits, CLI/REST reference, and extension guides under `references/`. Read it before deep-diving the codebase; keep it in sync when changing public APIs, traits, or CLI/REST surfaces.
- `.claude/skills/ceres-publish-dataset`: maintainer workflow for publishing Open Data Index snapshots to Hugging Face.

## Working conventions

- Rust edition is 2024, MSRV 1.95; keep formatting compatible with `cargo fmt --all`.
- Do not commit `.env`, generated harvest output, `target/`, or local agent state (`.claude/*` other than `skills/` is gitignored).
- Preserve full portal metadata when adding harvest paths: API features such as resource-schema extraction depend on it.
- Treat portal language as a display preference, not a reason to omit datasets, unless a caller explicitly requests filtering.
- Keep network smoke tests ignored or opt-in; unit tests must be deterministic and offline.
- New portal clients implement `PortalClient` in `ceres-client` and register in `PortalClientFactoryEnum`; follow the streaming, page-by-page pattern of the existing clients so memory stays bounded on large catalogs.
- User-facing scale claims (portal/dataset counts) live in `README.md` and `website/`; keep them round ("120+ portals", "2M+ datasets") so they age gracefully as coverage grows.

## Validation

Run the smallest relevant checks first, then the workspace checks when practical:

```powershell
cargo fmt --all -- --check
cargo test -p ceres-client -p ceres-core
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Database and server integration tests use Testcontainers and require a running Docker daemon. The local stack can be started with `docker compose up -d`.

The website builds with `npm ci && npm run build` inside `website/`.
