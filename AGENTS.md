# Ceres contributor guide

## Project map

- `crates/ceres-core`: domain models, harvesting pipeline, configuration, and traits.
- `crates/ceres-client`: CKAN, DCAT, and SPARQL portal clients.
- `crates/ceres-db`: PostgreSQL/pgvector persistence.
- `crates/ceres-server`: Axum HTTP API and OpenAPI definitions.
- `crates/ceres-cli`: command-line entry point.
- `migrations/`: ordered PostgreSQL migrations.
- `website/`: Astro documentation site.

## Working conventions

- Rust edition is 2024; keep formatting compatible with `cargo fmt --all`.
- Do not commit `.env`, generated harvest output, `target/`, or local agent state.
- Preserve full portal metadata when adding harvest paths: API features such as resource-schema extraction depend on it.
- Treat portal language as a display preference, not a reason to omit datasets, unless a caller explicitly requests filtering.
- Keep network smoke tests ignored or opt-in; unit tests must be deterministic and offline.

## Validation

Run the smallest relevant checks first, then the workspace checks when practical:

```powershell
cargo fmt --all -- --check
cargo test -p ceres-client -p ceres-core
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Database and server integration tests use Testcontainers and require a running Docker daemon. The local stack can be started with `docker compose up -d`.
