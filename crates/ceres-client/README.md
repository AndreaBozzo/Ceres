# ceres-client

**The external communication layer for Ceres.**

This crate handles connections to external services: CKAN portals for data harvesting and Google Gemini for embeddings.

### What it provides
* **CKAN Client**: Fetches metadata from CKAN open data portals.
* **Gemini Client**: Generates text embeddings via Google's API.
* **HTTP Handling**: Robust request handling with retries.
