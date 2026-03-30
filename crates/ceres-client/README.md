# ceres-client

**The external communication layer for Ceres.**

This crate handles connections to external systems used by Ceres: portal APIs for harvesting and embedding providers for optional vector generation.

### What it provides
* **Portal Clients**: CKAN harvesting plus current DCAT udata support.
* **Embedding Providers**: Ollama, Gemini, and OpenAI via the `EmbeddingProvider` trait.
* **HTTP Handling**: Robust request handling with retries.

### Current focus
* **Harvest compatibility**: normalize heterogeneous portal responses into the shared `NewDataset` model.
* **Local-first embedding**: Ollama support keeps embedding optional and self-hostable.
* **Provider dispatch**: enum-based factories bridge runtime configuration with async traits.
