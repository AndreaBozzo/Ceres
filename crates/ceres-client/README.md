# ceres-client

**The external communication layer for Ceres.**

This crate handles connections to external systems used by Ceres: portal APIs for harvesting and embedding providers for optional vector generation.

OGC catalogue records are harvested through CSW 2.0.2 with
`type = "ogc_records"`; use `ogc_endpoint` when the CSW service URL differs
from the logical portal URL.

### What it provides
* **Portal Clients**: CKAN, DCAT profiles, Socrata, OpenDataSoft, ArcGIS Hub, and OGC CSW 2.0.2.
* **Embedding Providers**: Ollama, Gemini, and OpenAI via the `EmbeddingProvider` trait.
* **HTTP Handling**: Robust request handling with retries.

### Current focus
* **Harvest compatibility**: normalize heterogeneous portal responses into the shared `NewDataset` model.
* **Local-first embedding**: Ollama support keeps embedding optional and self-hostable.
* **Provider dispatch**: enum-based factories bridge runtime configuration with async traits.
