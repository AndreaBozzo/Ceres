# ceres-core

**The foundation of Ceres.**

This crate provides the core building blocks: types, errors, configuration, and domain traits used throughout the project. It is database-agnostic — all storage and embedding concerns are abstracted behind traits.

### What it provides
* **Core Types & Errors**: Fundamental data structures and error definitions.
* **Configuration**: Environment settings and sync configuration.
* **Domain Traits**: `DatasetStore`, `EmbeddingProvider`, `PortalClient` abstractions.
* **Services**: HarvestService, DeltaDetector, CircuitBreaker.
