# Contributing to Ceres

Thank you for considering contributing to Ceres! This document provides guidelines for contributing to the project.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/yourusername/ceres.git`
3. Create a feature branch: `git checkout -b feature/your-feature-name`
4. Make your changes
5. Run tests: `cargo test`
6. Commit your changes: `git commit -m "Add your feature"`
7. Push to your fork: `git push origin feature/your-feature-name`
8. Open a Pull Request

## Development Setup

```bash
# Start PostgreSQL with pgvector
docker-compose up -d

# Run migrations
psql $DATABASE_URL -f migrations/202511290001_init.sql

# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run
```

## Code Style

- Follow Rust standard formatting: `cargo fmt`
- Ensure clippy passes: `cargo clippy -- -D warnings`
- Write tests for new functionality
- Document public APIs with doc comments

## Commit Messages

- Use clear, descriptive commit messages
- Start with a verb in imperative mood: "Add", "Fix", "Update", "Remove"
- Reference issues when applicable: "Fix #123"

## Pull Request Process

1. Update documentation for any changed functionality
2. Ensure all tests pass
3. Update the README.md if needed
4. Your PR will be reviewed by maintainers
5. Address any requested changes
6. Once approved, your PR will be merged

## Areas for Contribution

- **Harvesters**: Add support for new portal types (Socrata, DCAT-AP)
- **Embeddings**: Implement alternative embedding providers
- **CLI**: Improve user experience and add new commands
- **Documentation**: Improve guides, examples, and API docs
- **Tests**: Increase test coverage
- **Bug fixes**: Fix issues listed in GitHub Issues

## Releasing

Releases are automated via GitHub Actions using [git-cliff](https://git-cliff.org) for changelog generation.

### Automated Release Process

1. Go to **Actions > Prepare Release > Run workflow**
2. Enter the version number (e.g., `0.2.0`)
3. Optionally check "Dry run" to preview changes without committing
4. The workflow will:
   - Generate `CHANGELOG.md` from conventional commits
   - Update version in `Cargo.toml`
   - Commit and push the changes

5. After the workflow completes, create and push the release tag:
   ```bash
   git pull && git tag vX.Y.Z && git push origin vX.Y.Z
   ```

6. Monitor the [Actions tab](https://github.com/AndreaBozzo/Ceres/actions) for the release workflow.

### Local Changelog Preview

To preview the changelog locally before releasing:

```bash
# Install git-cliff
cargo install git-cliff

# Preview unreleased changes
git-cliff --unreleased

# Preview full changelog with a hypothetical new version
git-cliff --tag v0.2.0
```

### Version Format

Only stable versions are supported: `vX.Y.Z` (e.g., v0.1.0, v1.0.0).

## Questions?

Feel free to open an issue for questions or discussions about contributing.

## Code of Conduct

Please be respectful and constructive in all interactions. We want Ceres to be a welcoming project for all contributors.
