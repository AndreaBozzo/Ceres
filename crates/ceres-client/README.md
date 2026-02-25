# ceres-client

**The external communication layer for Ceres.**

This crate handles connections to external services: CKAN portals for data harvesting and embedding providers for vector generation.

### What it provides
* **CKAN Client**: Fetches metadata from CKAN open data portals with multilingual and custom URL template support.
* **Embedding Providers**: Pluggable backends (Gemini, OpenAI) via the `EmbeddingProvider` trait, with batched API calls.
* **HTTP Handling**: Robust request handling with retries.
