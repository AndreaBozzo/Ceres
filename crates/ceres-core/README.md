# ceres-core

**The foundation of Ceres.**

This crate provides the core business logic of Ceres: harvesting, synchronization, optional embedding, search, export, and worker orchestration. It is database-agnostic and provider-agnostic; storage and external integrations are abstracted behind traits.

### What it provides
* **Core Types & Errors**: Fundamental data structures and error definitions.
* **Configuration**: Environment settings and sync configuration.
* **Domain Traits**: `DatasetStore`, `EmbeddingProvider`, `PortalClient` abstractions.
* **Services**: `HarvestService`, `EmbeddingService`, `HarvestPipeline`, `SearchService`, `ExportService`, `WorkerService`.

### Architectural role
* **Harvest-first flow**: metadata synchronization is independent from vector generation.
* **Trait-based composition**: portal clients, embedding providers, stores, and job queues remain swappable.
* **Operational resilience**: delta detection, stale marking, and circuit-breaker logic live here.
