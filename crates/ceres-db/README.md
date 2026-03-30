# ceres-db

**The storage layer for Ceres.**

This crate implements persistence for the harvested catalog, sync tracking, pending-embedding queues, and search storage using PostgreSQL with pgvector.

### What it does
* **Stores Data**: Persists harvested datasets, metadata, embeddings, and stale flags.
* **Sync Tracking**: Records portal sync history and status.
* **Job Persistence**: Stores database-backed harvest jobs for worker-driven execution.
* **Vector Search**: Uses pgvector for similarity queries once embeddings exist.
* **Repository Pattern**: Implements `DatasetStore` and `JobQueue` for the rest of the workspace.
