# Contributing

## Development Setup

**Requirements:** Rust 1.88+, PostgreSQL 16+ with pgvector, Docker (recommended).

```bash
# Clone
git clone https://github.com/AndreaBozzo/Ceres.git
cd Ceres

# Start PostgreSQL + pgvector
docker compose up db -d

# Configure
cp .env.example .env
# Edit .env only if you want hosted embeddings; for local-first setups use Ollama or harvest with --metadata-only

# Run migrations
make migrate

# Build
cargo build

# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run -- harvest https://dati.comune.milano.it
```

## Makefile Targets

| Target | Description |
|---|---|
| `make help` | Show available targets |
| `make build` | Build the project |
| `make release` | Build in release mode |
| `make test` | Run tests |
| `make fmt` | Format code |
| `make fmt-check` | Check formatting |
| `make clippy` | Run clippy lints |
| `make clean` | Clean build artifacts |
| `make docker-build` | Build Docker image |
| `make docker-up` | Start services with docker compose |
| `make docker-down` | Stop services |
| `make migrate` | Run database migrations |
| `make all` | Run fmt, clippy, and tests |

## Testing

### Unit Tests

Inline `#[cfg(test)]` modules in each source file. Run with `cargo test`.

### Integration Tests

Located in `crates/*/tests/integration/`. Require a running PostgreSQL database (set `DATABASE_URL`).

The `ceres-client` crate provides `MockEmbeddingClient` (feature `test-support`) for testing without real API calls.

## CI Pipeline (GitHub Actions)

1. **Format check:** `cargo fmt --check`
2. **Clippy:** `cargo clippy --all-targets --all-features -- -D warnings`
3. **Unit tests:** `cargo test`
4. **Integration tests:** with PostgreSQL service container
5. **Dependency audit:** `cargo deny check`

## Code Style

- Trait-based abstraction for all external dependencies
- `AppError` (not `anyhow`) in library code — explicit error types
- `impl Future<Output = ...> + Send` for async trait methods (not `async_trait` macro)
- Builder pattern with `with_*` methods for constructors
- `tracing` for all logging (not `println!` or `log`)
- No `unwrap()` in library code (OK in tests and CLI)
- Conventional commits for changelog generation

## Releasing

Automated via GitHub Actions + [git-cliff](https://git-cliff.org):

1. Go to **Actions > Prepare Release > Run workflow**
2. Enter version (e.g., `0.5.0`)
3. Workflow generates `CHANGELOG.md` and updates `Cargo.toml`
4. After workflow: `git pull && git tag vX.Y.Z && git push origin vX.Y.Z`
5. The tag-triggered Release workflow validates version/changelog, runs fmt/clippy/build/tests/security audit, publishes Docker and crates.io artifacts, and creates the GitHub Release

Only stable versions: `vX.Y.Z`.

## Areas for Contribution

- **v0.6.0 portal coverage:** DCAT profile cleanup (#165), Project Open Data `data.json` (#156), Socrata (#166), OpenDataSoft (#167), ArcGIS Hub (#168), opt-in smoke checks (#169)
- **v0.7.0 resource metadata:** enrich resource/distribution metadata and schema exposure (#68)
- **Backlog:** REST Parquet export endpoint (#98), multi-tenancy (#91), configurable delta detection (#51)
- **Embedding providers:** more local or self-hosted models beyond Ollama
- **HNSW tuning:** Optimize pgvector index parameters for production scale
- **CLI improvements:** Progress bars, interactive mode
- **Documentation:** Guides, examples, API docs
- **Tests:** Increase coverage, especially integration tests
