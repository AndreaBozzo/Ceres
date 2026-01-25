//! OpenAPI documentation configuration.

use utoipa::OpenApi;

use crate::dto::{
    DatasetResponse, ExportQuery, HarvestJobResponse, HarvestStatusResponse, HealthResponse,
    PortalInfoResponse, PortalStatsResponse, SearchQuery, SearchResponse, SearchResultDto,
    StatsResponse, SyncStatsDto, TriggerHarvestRequest,
};
use crate::handlers::{datasets, export, harvest, health, portals, search, stats};

/// OpenAPI documentation for the Ceres API.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Ceres API",
        version = "1.0.0",
        description = "Semantic search engine for open data portals.

Ceres indexes datasets from open data portals (CKAN, Socrata, DCAT) and provides
semantic search capabilities using vector embeddings.

## Features

- **Semantic Search**: Find datasets using natural language queries
- **Portal Management**: Configure and monitor data sources
- **Harvest Operations**: Trigger and track data synchronization
- **Export**: Download datasets in JSON, JSONL, or CSV format

## Quick Start

1. Check server health: `GET /api/v1/health`
2. Search for datasets: `GET /api/v1/search?q=air+quality`
3. View statistics: `GET /api/v1/stats`
",
        contact(
            name = "Andrea Bozzo",
            url = "https://github.com/AndreaBozzo/Ceres"
        ),
        license(
            name = "Apache-2.0",
            url = "https://www.apache.org/licenses/LICENSE-2.0"
        )
    ),
    servers(
        (url = "http://localhost:3000", description = "Local development server")
    ),
    paths(
        health::health_check,
        stats::get_stats,
        search::search,
        portals::list_portals,
        portals::get_portal_stats,
        portals::trigger_portal_harvest,
        harvest::trigger_harvest_all,
        harvest::get_harvest_status,
        export::export_datasets,
        datasets::get_dataset_by_id,
    ),
    components(
        schemas(
            // Request types
            SearchQuery,
            ExportQuery,
            TriggerHarvestRequest,
            // Response types
            HealthResponse,
            StatsResponse,
            SearchResponse,
            SearchResultDto,
            PortalInfoResponse,
            PortalStatsResponse,
            HarvestJobResponse,
            HarvestStatusResponse,
            SyncStatsDto,
            DatasetResponse,
        )
    ),
    tags(
        (name = "system", description = "System health and statistics"),
        (name = "search", description = "Semantic search operations"),
        (name = "portals", description = "Portal management and monitoring"),
        (name = "harvest", description = "Data harvesting operations"),
        (name = "export", description = "Data export operations"),
        (name = "datasets", description = "Dataset retrieval"),
    )
)]
pub struct ApiDoc;
