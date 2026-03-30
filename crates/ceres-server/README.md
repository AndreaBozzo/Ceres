# ceres-server

**The API gateway for Ceres.**

`ceres-server` exposes Ceres functionality through a REST API, enabling programmatic access to harvesting, portal management, search, export, and job status operations.

### What it provides
* **REST API**: HTTP endpoints for search, harvest, export, stats, datasets, and configured portals.
* **Bearer Token Auth**: Admin endpoints protected with configurable API key authentication.
* **OpenAPI Documentation**: Interactive Swagger UI at `/swagger-ui`.
* **Rate Limiting**: Configurable request throttling per IP.
* **CORS Support**: Configurable cross-origin resource sharing.
* **Docker Ready**: Production multi-stage Docker image included.

### Deployment role
* **Public read surface**: health, stats, search, dataset, and portal endpoints.
* **Protected write surface**: harvest triggers and exports behind `CERES_ADMIN_TOKEN`.
* **Job-aware operations**: integrates with the persistent harvest queue and status reporting.
