<div align="center">
  <img src="website/src/assets/images/logo.png" alt="Ceres logo" width="800" height="auto"/>
  <h1>Ceres</h1>
  <p><strong>Harvest open data portal metadata into one PostgreSQL catalog.</strong></p>

  [![crates.io](https://img.shields.io/crates/v/ceres-search.svg)](https://crates.io/crates/ceres-search)
  [![CI](https://github.com/AndreaBozzo/Ceres/actions/workflows/ci.yml/badge.svg)](https://github.com/AndreaBozzo/Ceres/actions)
  [![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](https://github.com/AndreaBozzo/Ceres/blob/master/LICENSE)
  [![Hugging Face dataset](https://img.shields.io/badge/%F0%9F%A4%97%20Dataset-Open%20Data%20Index-yellow)](https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index)

  [Website](https://learnceres.pages.dev) · [Supported portals](https://learnceres.pages.dev/portals/) · [Harvesting](https://learnceres.pages.dev/harvesting/) · [REST API](https://learnceres.pages.dev/api/) · [Open Data Index](https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index)

</div>

Ceres reads dataset metadata from open data portals and keeps it in sync in PostgreSQL: incremental fetches, stale marking instead of deletion, and bounded memory on catalogs with millions of records. It speaks ten portal APIs: CKAN, DCAT (udata REST and SPARQL), Project Open Data `data.json`, Socrata, OpenDataSoft, ArcGIS Hub, OGC CSW, STAC and SDMX.

Embeddings, semantic search, a REST API and Parquet snapshots are optional layers on top. Building a catalog needs no API key and no GPU.

## Harvest a portal

Rust 1.95+ and Docker:

```bash
git clone https://github.com/AndreaBozzo/Ceres.git && cd Ceres
docker compose up db -d && cp .env.example .env && make migrate

cargo run --bin ceres -- harvest https://dati.comune.milano.it --metadata-only
cargo run --bin ceres -- harvest https://opendata.paris.fr --type opendatasoft --metadata-only
cargo run --bin ceres -- stats
```

For many portals, list them in `portals.toml` and run `ceres harvest` with no URL. Each portal fails on its own, and the run ends with a per-portal summary:

```toml
[[portals]]
name = "milano"
url = "https://dati.comune.milano.it"
type = "ckan"
```

[`examples/portals.toml`](examples/portals.toml) has 300+ entries, and [Supported portals](https://learnceres.pages.dev/portals/) covers each type's flags and keys.

## Search it, if you want

```bash
ollama pull nomic-embed-text
EMBEDDING_PROVIDER=ollama cargo run --bin ceres -- embed
cargo run --bin ceres -- search "public transport" --limit 5
```

Gemini and OpenAI also work as embedding providers. `ceres-server` serves the same catalog over a REST API with OpenAPI docs.

## The Open Data Index

The catalog is published on Hugging Face as the [Ceres Open Data Index](https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index): 2M+ dataset records from 300+ portals, in Parquet, with a checksummed manifest, a coverage and quality report, and a changelog against the previous snapshot.

Each snapshot is dated by its export. A scheduled harvest refreshes a few portals between snapshots; the rest were last harvested by hand, so a snapshot's date is not the date of every record.

## What you can count on

- **Harvesting never waits on embeddings.** The metadata path runs with no provider configured.
- **Partial runs say so.** A catalog read only in part is recorded as `partial`, and a batch with failed portals exits with code 2.
- **Removals are kept.** Datasets that disappear from a portal are marked stale, not deleted.
- **Source metadata is kept whole.** Normalized resources sit next to the raw portal record.
- **Snapshots are verifiable.** Every Parquet file has a SHA-256 checksum in the manifest.

## Status

Maintenance mode after v0.7.0: fixes and security updates land, new portal families are not planned. Known limits:

- Change detection compares title and description only. A license or resource change with the same text is not written on re-harvest. [Harvesting](https://learnceres.pages.dev/harvesting/#tier-2-delta-detection) explains the workaround.
- Some portals cap pagination or block cloud runners, so their harvests can be partial. The run reports which ones.

## Learn more

- [Harvesting](https://learnceres.pages.dev/harvesting/): sync tiers, batch runs, exit codes, HTTP tuning
- [Deployment](https://learnceres.pages.dev/deployment/): the container image and unattended scheduled jobs
- [REST API](https://learnceres.pages.dev/api/): endpoints, the schema contract and metadata redaction
- [Embeddings and costs](https://learnceres.pages.dev/cost/): provider setup
- [`AGENTS.md`](AGENTS.md) and the in-repo [Claude Code skill](.claude/skills/ceres/SKILL.md) for working on the code with an agent
- Downstream uses of the index: [databricks-ceres-pipeline](https://github.com/AndreaBozzo/databricks-ceres-pipeline) and [ceres-discovery-agent](https://github.com/AndreaBozzo/ceres-discovery-agent)
- [Changelog](CHANGELOG.md), [contributing](CONTRIBUTING.md), [security policy](SECURITY.md) and [Discord](https://discord.gg/fztdKSPXSz)

## License

[Apache-2.0](LICENSE).
