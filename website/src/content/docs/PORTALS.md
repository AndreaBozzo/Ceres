---
title: Supported portals
description: The portal APIs Ceres can harvest today, how to configure each one, and where coverage is headed next
---

# Supported portals

Ceres currently harvests **120+ portals** into a single synchronized catalog of
**2M+ datasets** — national portals, EU aggregators, US federal agencies, and
city open data sites. Seven harvest paths are shipped today, and every one of
them shares the same sync machinery: incremental sync, content-hash delta
detection, streaming page-by-page processing, and stale dataset marking.

| Type | Selector | Serves | Example portals |
|---|---|---|---|
| CKAN | `--type ckan` | CKAN action API portals | dati.comune.milano.it, catalog.data.gov, dati.gov.it |
| DCAT udata REST | `--type dcat` (default profile) | udata-flavored DCAT-AP portals | data.gouv.fr, data.public.lu |
| DCAT SPARQL | `--type dcat --profile sparql` | SPARQL-backed DCAT-AP catalogs | data.europa.eu |
| Project Open Data | `--type dcat --profile static_json` | Static DCAT-US `data.json` catalogs | data.va.gov, census.gov, justice.gov |
| Socrata | `--type socrata` | Socrata Discovery API catalogs | data.cityofnewyork.us, data.wa.gov |
| OpenDataSoft | `--type opendatasoft` | OpenDataSoft Explore API v2.1 catalogs | opendata.paris.fr, data.economie.gouv.fr |
| ArcGIS Hub | `--type arcgis` | ArcGIS Hub Search API catalogs | opendata.dc.gov, opendata.gis.utah.gov |

Every client preserves the complete source metadata payload, so downstream
features like resource-schema extraction keep working no matter which portal a
dataset came from.

## Quick examples

```bash
# CKAN
ceres harvest https://dati.comune.milano.it --metadata-only

# DCAT udata REST
ceres harvest https://data.public.lu --type dcat --metadata-only

# SPARQL-backed DCAT (e.g. the EU aggregator)
ceres harvest https://data.europa.eu --type dcat --profile sparql --metadata-only

# Static Project Open Data / DCAT-US data.json
ceres harvest https://www.data.va.gov/data.json --type dcat --profile static_json --metadata-only

# Socrata Discovery API
ceres harvest https://data.cityofnewyork.us --type socrata --metadata-only

# OpenDataSoft Explore API
ceres harvest https://opendata.paris.fr --type opendatasoft --metadata-only

# ArcGIS Hub Search API
ceres harvest https://opendata.dc.gov --type arcgis --metadata-only
```

Or configure them once in `portals.toml` and harvest in batch:

```toml
[[portals]]
name = "milano"
url = "https://dati.comune.milano.it"
type = "ckan"
language = "it"

[[portals]]
name = "nyc"
url = "https://data.cityofnewyork.us"
type = "socrata"
language = "en"

[[portals]]
name = "europa"
url = "https://data.europa.eu"
type = "dcat"
profile = "sparql"
language = "en"
```

See [`examples/portals.toml`](https://github.com/AndreaBozzo/Ceres/blob/master/examples/portals.toml)
for a larger, curated configuration set.

## Portal-specific notes

### CKAN

- Modified-since filtering for incremental syncs
- Adaptive page sizing: on timeouts the page size is quartered (1000 → 250 → 62 → 15 → 10) and the per-request timeout grows, which handles portals that choke on large responses
- `catalog.data.gov` relocated its CKAN API to `api.gsa.gov` and requires `DATA_GOV_API_KEY` (free key at [api.data.gov/signup](https://api.data.gov/signup/))

### DCAT udata REST

- Streams paginated JSON-LD catalog pages
- Resolves multilingual fields according to the configured portal language
- Waits out `429` rate limits with an escalating per-page cooldown instead of stopping with partial results

### DCAT SPARQL

- Pages through the catalog with `LIMIT`/`OFFSET` queries and deduplicates by dataset URI
- Localized title/description selection: requested language → sibling language → English → untagged
- Endpoint defaults to `{url}/sparql`; override with `sparql_endpoint`

### Project Open Data (`data.json`)

- Handles static DCAT-US catalogs published as one JSON document (US federal agencies)
- Hard 256 MiB response limit by default; override with `CERES_STATIC_JSON_MAX_BYTES`
- Incremental runs filter the catalog locally using `modified`

### Socrata Discovery API

- Harvests the paginated `/api/catalog/v1` endpoint, scoped to the portal domain and limited to dataset assets
- Uses `updatedAt` ordering for incremental synchronization
- HTML descriptions are converted to plain text for search and embedding; the original HTML stays in the raw metadata
- No credentials required for public reads; set `SOCRATA_APP_TOKEN` for higher rate limits

### OpenDataSoft Explore API

- Harvests the paginated `/api/explore/v2.1/catalog/datasets` endpoint (100 datasets per page, the API maximum)
- Normalizes title, description, themes, keywords, license, publisher, and `modified` from `metas.default`; the complete catalog entry, including `fields` schema hints, stays in the raw metadata
- Catalogs deeper than the API's 10,000-row pagination window (e.g. the `data.opendatasoft.com` federation hub) are walked with a keyset cursor on `modified`
- Incremental sync filters server-side with an ODSQL `where=modified >= date'...'` clause
- No credentials required for public reads; set `ODS_API_KEY` for higher quotas

### ArcGIS Hub Search API

- Harvests the paginated `/api/search/v1/collections/dataset/items` endpoint (100 items per page, the API maximum)
- Normalizes title, description (falling back to the item snippet), categories, tags, license, publisher, and `modified`; the complete catalog feature, including geometry and service metadata, stays in the raw metadata
- Catalog entries mix hosted services (Feature/Image/Map Services) and file items (CSV, shapefile): the normalized dataset URL is always the Hub landing page — a service endpoint is a queryable API, not a file download
- Catalogs deeper than the API's 10,000-row result window are walked with a keyset cursor on `modified`; incremental sync filters server-side with `filter=modified >= <epoch millis>`
- Hub sites with an empty `catalogV2` item scope are rejected because their search endpoint returns global ArcGIS content rather than datasets belonging to that portal
- No credentials required for public reads

## The published index

The catalog Ceres maintains is published as the
[Ceres Open Data Index](https://huggingface.co/datasets/AndreaBozzo/ceres-open-data-index)
on Hugging Face: curated Parquet snapshots with versioned manifests, SHA-256
checksums, coverage/quality reports, and snapshot-to-snapshot changelogs.

## What's next

Coverage keeps expanding. With **OpenDataSoft** and **ArcGIS Hub** shipped, the
[v0.6.0 milestone](https://github.com/AndreaBozzo/Ceres/milestones) continues
with job-based harvesting for every supported client and newly validated
portals at scale.

Want a portal that none of the current clients cover? The client layer is
trait-based and designed for extension — see
[Contributing](/contributing/) or open an issue.
