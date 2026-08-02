---
title: REST API
description: Run the Ceres server and consume its stable normalized resource schema
---

Ceres exposes public read endpoints and token-protected administrative
endpoints through an Axum server. Start it with:

```bash
cargo run --bin ceres-server
```

The interactive Swagger UI is available at `/swagger-ui`, and the generated
OpenAPI document is served at `/api-docs/openapi.json`.

## Raw metadata vs. normalized resources

The two dataset endpoints serve different purposes:

| Endpoint | Intended use | Contract |
|---|---|---|
| `GET /api/v1/datasets/{id}` | Dataset details and source-specific raw metadata for inspection or debugging | The nested `metadata` shape is source-specific and best-effort. Do not build cross-portal resource integrations against it. |
| `GET /api/v1/datasets/{id}/schema` | Resources, distributions, download or service URLs, and column fields | This is the supported public resource contract. Existing fields will not be removed, renamed, or change type without a versioned API change. Consumers should ignore additive fields they do not recognize. |

Raw metadata varies because Ceres preserves what each portal family exposes.
The schema endpoint derives one normalized response from those CKAN resources,
DCAT distributions, Socrata columns, OpenDataSoft fields, ArcGIS services, OGC
online resources, and STAC assets.

## Schema response

```json
{
  "id": "2f1c1b44-6957-4c61-8823-3d77e91b024a",
  "original_id": "air-quality-2024",
  "source_portal": "https://data.example.org",
  "resources": [
    {
      "name": "Air quality observations",
      "format": "CSV",
      "media_type": "text/csv",
      "url": "https://data.example.org/download/air-quality.csv",
      "description": "Hourly station observations",
      "fields": [
        {
          "name": "station_id",
          "type": "string",
          "description": "Monitoring station identifier"
        }
      ]
    }
  ]
}
```

All documented keys are present in every response. Their nullability is fixed
as follows:

| Object | Field | Type | Nullable |
|---|---|---|---|
| schema | `id` | UUID string | No |
| schema | `original_id` | string | No |
| schema | `source_portal` | string | No |
| schema | `resources` | array of resources | No; empty when no resource can be normalized |
| resource | `name` | string | Yes |
| resource | `format` | string | Yes |
| resource | `media_type` | string | Yes |
| resource | `url` | string | Yes |
| resource | `description` | string | Yes |
| resource | `fields` | array of fields | No; empty when no inline column schema is available |
| field | `name` | string | No |
| field | `type` | string | Yes |
| field | `description` | string | Yes |

Nullable values are serialized as explicit `null` values, not omitted keys.
An empty `resources` array means the harvested record did not expose a usable
resource or distribution through its currently supported normalization path;
it does not mean the dataset itself is empty.

## Other endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/v1/health` | Database health |
| GET | `/api/v1/stats` | Catalog statistics |
| GET | `/api/v1/search?q=...&limit=10` | Semantic search |
| GET | `/api/v1/portals` | Configured portals |
| GET | `/api/v1/portals/{name}/stats` | Portal statistics |
| GET | `/api/v1/harvest/status` | Harvest job status |
| POST | `/api/v1/portals/{name}/harvest` | Trigger one configured portal; Bearer token required |
| POST | `/api/v1/harvest` | Trigger all enabled portals; Bearer token required |
| GET | `/api/v1/export` | Stream an export; Bearer token required |

Set `CERES_ADMIN_TOKEN` to enable protected endpoints and send it as
`Authorization: Bearer <token>`.
