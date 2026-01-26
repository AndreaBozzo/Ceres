# ceres-db

**The storage layer for Ceres.**

This crate handles data persistence using PostgreSQL with pgvector for semantic search capabilities.

### What it does
* **Stores Data**: Persists harvested datasets and embeddings.
* **Vector Search**: Uses pgvector for fast semantic similarity queries.
* **Repository Pattern**: Clean data access interface for other crates.
