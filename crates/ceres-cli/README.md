# ceres-cli

**The command-line interface for Ceres.**

`ceres-cli` is the main operator interface for Ceres. It is built around harvesting first, with standalone embedding, search, export, and stats commands on top of the same local catalog.

### What you can do
* **Harvest**: Sync datasets from configured or ad-hoc portals, including CKAN and current DCAT udata support.
* **Embed**: Generate vectors later with Ollama, Gemini, or OpenAI.
* **Search**: Run semantic search once datasets have embeddings.
* **Export**: Export data in JSON, JSONL, CSV, or Parquet format.
* **Stats**: View database statistics.

### Operational model
* **Metadata-first**: `ceres harvest --metadata-only` keeps harvesting independent from embedding.
* **Provider flexibility**: `ceres embed` backfills vectors without re-harvesting.
* **Batch workflows**: `portals.toml` drives multi-portal harvest and export flows.
