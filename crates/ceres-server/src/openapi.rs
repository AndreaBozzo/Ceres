//! OpenAPI documentation configuration.

use utoipa::OpenApi;

use crate::dto::{
    DatasetResourceDto, DatasetResponse, DatasetSchemaResponse, ExportQuery, HarvestJobResponse,
    HarvestStatusResponse, HealthResponse, PortalInfoResponse, PortalStatsResponse,
    ResourceFieldDto, SearchQuery, SearchResponse, SearchResultDto, ServiceStatus, StatsResponse,
    SyncStatsDto, TriggerHarvestRequest,
};
use crate::handlers::{datasets, export, harvest, health, portals, search, stats};

/// OpenAPI documentation for the Ceres API.
#[derive(OpenApi)]
#[openapi(
    info(
        title = "Ceres API",
        version = "1.0.0",
        description = "Semantic search engine for open data portals.

Ceres indexes datasets from open data portals (CKAN, Socrata, DCAT, OpenDataSoft, ArcGIS Hub) and provides
semantic search capabilities using vector embeddings.

## Dataset metadata contracts

`GET /api/v1/datasets/{id}` includes source-specific raw `metadata`. Its shape
is portal-specific and best-effort. Use `GET /api/v1/datasets/{id}/schema` as
the supported public contract for normalized resources and distributions.

## Features

- **Semantic Search**: Find datasets using natural language queries
- **Portal Management**: Configure and monitor data sources
- **Harvest Operations**: Trigger and track data synchronization
- **Export**: Download datasets in JSON, JSONL, or CSV format
- **Resource schema**: Consume normalized resources and column fields across portal families

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
        datasets::get_dataset_schema,
    ),
    components(
        schemas(
            // Request types
            SearchQuery,
            ExportQuery,
            TriggerHarvestRequest,
            // Response types
            HealthResponse,
            ServiceStatus,
            StatsResponse,
            SearchResponse,
            SearchResultDto,
            PortalInfoResponse,
            PortalStatsResponse,
            HarvestJobResponse,
            HarvestStatusResponse,
            SyncStatsDto,
            DatasetResponse,
            DatasetSchemaResponse,
            DatasetResourceDto,
            ResourceFieldDto,
        )
    ),
    tags(
        (name = "system", description = "System health and statistics"),
        (name = "search", description = "Semantic search operations"),
        (name = "portals", description = "Portal management and monitoring"),
        (name = "harvest", description = "Data harvesting operations"),
        (name = "export", description = "Data export operations"),
        (name = "datasets", description = "Dataset retrieval"),
    ),
    modifiers(&SecurityAddon)
)]
pub struct ApiDoc;

/// Adds Bearer token security scheme to the OpenAPI spec.
struct SecurityAddon;

impl utoipa::Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "bearer",
                utoipa::openapi::security::SecurityScheme::Http(
                    utoipa::openapi::security::HttpBuilder::new()
                        .scheme(utoipa::openapi::security::HttpAuthScheme::Bearer)
                        .bearer_format("token")
                        .description(Some(
                            "Admin API key. Set via CERES_ADMIN_TOKEN environment variable.",
                        ))
                        .build(),
                ),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;
    use utoipa::OpenApi;

    use super::ApiDoc;

    #[test]
    fn dataset_schema_openapi_contract_has_required_nullable_fields_and_example() {
        let document = serde_json::to_value(ApiDoc::openapi()).expect("serialize OpenAPI");
        let schemas = &document["components"]["schemas"];

        assert_required_includes(
            &schemas["DatasetSchemaResponse"],
            &["id", "original_id", "source_portal", "resources"],
        );
        assert_required_includes(
            &schemas["DatasetResourceDto"],
            &[
                "name",
                "format",
                "media_type",
                "url",
                "description",
                "fields",
            ],
        );
        assert_required_includes(
            &schemas["ResourceFieldDto"],
            &["name", "type", "description"],
        );

        for property in ["name", "format", "media_type", "url", "description"] {
            assert_nullable(&schemas["DatasetResourceDto"]["properties"][property]);
        }
        for property in ["type", "description"] {
            assert_nullable(&schemas["ResourceFieldDto"]["properties"][property]);
        }

        let example = &schemas["DatasetSchemaResponse"]["example"];
        assert_eq!(example["resources"][0]["format"], "CSV");
        assert_eq!(example["resources"][0]["fields"][0]["name"], "station_id");
    }

    fn assert_required_includes(schema: &Value, expected: &[&str]) {
        let required = schema["required"]
            .as_array()
            .expect("schema must declare required properties");

        for property in expected {
            assert!(
                required
                    .iter()
                    .any(|value| value.as_str() == Some(property)),
                "required properties do not include {property}: {required:?}"
            );
        }
    }

    fn assert_nullable(schema: &Value) {
        let nullable_flag = schema["nullable"].as_bool() == Some(true);
        let nullable_type = schema["type"]
            .as_array()
            .is_some_and(|types| types.iter().any(|value| value == "null"));
        let nullable_union = schema["oneOf"]
            .as_array()
            .is_some_and(|variants| variants.iter().any(|value| value["type"] == "null"));

        assert!(
            nullable_flag || nullable_type || nullable_union,
            "property is not nullable: {schema}"
        );
    }
}
