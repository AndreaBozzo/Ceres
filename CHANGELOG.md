# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2026-03-06


### Added

- Add Ceres-Claude-Skill project link to related projects section
- Ceres Landing Page Overhaul with Astro
- Decouple metadata harvesting from embedding + stale dataset detection


### Documentation

- Enhance documentation for metadata-only harvesting and stale dataset detection, API keys are completely optional


### Fixed

- **CI**: Added a fallback to changelog generation


### Miscellaneous

- **CI**: Added logo path to release workflow, for next release
- Update version numbers to 0.3.1 and adjust Rust version to 1.88 in configuration files
## [0.3.0] - 2026-02-26


### Added

- **docs**: Add new CKAN portals for Emilia-Romagna, Rome, and Puglia to portals.toml, also added dati.gov.it commented
- Support custom URL templates for portals with non-standard frontends
- Abstract PortalClient for multiple portal providers
- Add new verified portals for Italy, Romania, Chile, and Ukraine
- Stream harvest pipeline to reduce memory on large portals
- Add multilingual support for portals, including language preference in harvest jobs and datasets
- Enhance multilingual support by adding description fallback and new test cases, this fully enables nested multilingual portals like Swiss
- Add Parquet export functionality
- **portal**: Add Asia-Pacific portals for Tokyo and Australia to the configuration
- Add dry run flag to preview harvesting without DB writes and embedding calls
- **docs**: Enhance README and HARVESTING documentation with architecture and flow diagrams
- Implement batch upsert functionality for datasets and enhance logging in harvest service
- **server**: Add Bearer token auth middleware
- Production config and Docker image (#71, #74)


### Changed

- Extract DeltaDetector as explicit service
- **arch**: Remove sqlx/pgvector from ceres-core
- Extract duplicated code across CLI, server, client, and core


### Documentation

- Add Discord badge to README
- Update Ceres architecture image
- Update README with new API endpoints and embedding model details
- Add Open Data Galaxy visualization to README
- Update README with new dataset and portal statistics
- Update gif description
- Update documentation for dataset listing methods and CORS origins example
- Revamped readme to make it shorter, references related projects
- Update dataset statistics and improve visualization description in README
- Update README with enhanced dataset statistics and improved portal configuration details
- Update release notes for v0.3.0 and add v0.4.0 features
- Moved refs
- Update logo, old one moved to Dbx pipeline folder
- Update documentation for 0.3.0 and improve workflow for building docker image
- Update dataset count in README to reflect current total of 350k datasets


### Fixed

- Update Discord badge link in README
- Update Discord link in README badge
- Cleanup
- Resolve 5 code quality issues from milestone 0.3.0


### Miscellaneous

- Audit and update stale TODOs, fix .env.example and embedding model default
- Remove duped CHANGELOG.md file, git-cliff needs changelog.md to be present in main folder
- Add inline TODO comments from monthly tech debt review
- Added blank issue template
- Comment out server configuration in compose.yml as its optional
- Update open_data_galaxy.gif image asset
- Clarified git description
- Minor docs update + simplified docker release


### Performance

- Batch embedding API calls during harvest
## [0.2.2] - 2026-02-06


### Added

- Abstract EmbeddingProvider trait for multiple backends


### Dependencies

- **deps**: Bump bytes from 1.11.0 to 1.11.1
- **deps**: Bump git2 from 0.20.3 to 0.20.4
- **deps**: Update num-conv, time, time-core, and time-macros to latest versions


### Documentation

- Logo height set to auto
- Moved social docs to docs folder
- Add harvesting architecture documentation


### Fixed

- MAJOR upgrade gemini to 001
## [0.2.0] - 2026-01-27


### Added

- **core**: Add real-time progress reporting and batch timestamp updates
- **portals**: Add verified European CKAN portals
- **tests**: Add integration tests for HarvestService and core mock implementations
- Implement incremental harvesting with full sync option and database tracking
- Add cancellation support for sync operations with status tracking
- Implement automated pre-release workflow with changelog generation
- Implement job queue for persistent harvest job management
- Add per-call override for force full sync in portal synchronization
- Add initial ceres-server crate to the workspace
- Implement circuit breaker pattern for API resilience
- Implement streaming export service for datasets
- Add get_by_id method to DatasetStore trait and its implementations
- REST API


### CI/CD

- Add PR labeler workflow
- **ci**: Update actions/checkout and actions/cache versions in CI workflows


### Changed

- **cli**: Version output with git commit and build info
- **core**: Extract business logic from CLI to ceres-core
- Improve mutex handling in circuit breaker to recover from eventual poison


### Dependencies

- **deps**: Update dependencies in Cargo.toml and Cargo.lock
- **deps**: Bump rsa from 0.9.9 to 0.9.10


### Documentation

- Update Ceres architecture diagram
- Fixed minor warning for cargo doc
- Update README to reflect changes in delta detection, persistent jobs, and graceful shutdown features
- Update roadmap version for portal type support in CKAN client
- Update README files to enhance documentation for REST API


### Fixed

- **cli**: Avoid UTF-8 panic in text truncation
- **ui**: Improve similarity bar for low scores
- **docs**: Update Rust version requirement and enhance future roadmap details, also updated TODOs in the codebase
- **CI**: Replace actions/cache with rust-cache for improved cargo caching
- Enable support for force full sync in job processing
- Update GitHub Pages workflow to allow PR deployments and improve concurrency handling, this will help testing index on potential PRs
- **release**: Preserve path in internal crate dependencies


### Miscellaneous

- Update to Rust Edition 2024
- Removed redundant docker command
- Bump tokio to 1.49.0
- Update minimum supported Rust version to 1.87 for clippy.toml
- Add permissions section to CI and release workflows
- Add ceres-server package to Cargo.lock and updated todo
- Replace cargo-audit with cargo-deny for security audits and add deny.toml configuration
- Add publishing steps for ceres-search and ceres-server with indexing delays


### Testing

- **db**: Add integration tests for DatasetRepository
## [0.1.1] - 2025-12-28


### Added

- Update Ceres architecture diagram image with drawio
- Implement structured error handling for Gemini API with classification and detailed messages
- Add changelog
- Delta harvesting with content hash tracking
- Add portals.toml for multi-portal batch harvesting
- Implement automated release workflow and update versioning to 0.1.0


### Changed

- Improve code quality
- Extract sync logic into service layer


### Documentation

- Update CKAN and Gemini API documentation for clarity and security practices
- Add cost-effectiveness section to README.md highlighting API efficiency
- Add cost analysis section to README.md detailing initial indexing expenses
- Translate Italian docstrings to English in models.rs
- Re-add incremental harvesting to readme.md
- Add TODOs for future enhancements and performance improvements across modules
- Update README for v0.1.0
- Add crates.io badges and installation instructions


### Fixed

- Phased out OpenAI references as we use only gemini in this iteration of Ceres
- Update error documentation to reflect Gemini API instead of OpenAI
- Update Gemini API request to include API key in headers ( Google API docs )
- **ci**: Dry-run only ceres-core to avoid dependency resolution
- Rename ceres-cli to ceres-search (crates.io name conflict)


### Testing

- Improve coverage for sync and config modules
## [0.0.1] - 2025-12-02


### Added

- Update package metadata and add Makefile for project management
- Implement OpenAIClient with embedding creation functionality
- Restructure clients module and implement main function with database connection and command handling
- Enhance CkanClient with robust error handling and dataset retrieval methods
- Update dependencies and enhance CKAN and OpenAI clients with improved functionality and error handling
- Enhance error handling in main function and CKAN client initialization, added tests inline into ckan
- Enhance OpenAIClient with detailed documentation and text sanitization logic, added unit tests for client creation and text processing
- Enhance CKAN and OpenAI clients with improved error handling, detailed documentation, and unit tests for dataset operations
- Update README to reflect CKAN and OpenAI embeddings integration completion
- Remove multi-portal configuration from status checklist in README as its planned for later
- Implement semantic search functionality and enhance database statistics retrieval, 1/6 presumably
- Clarify CLI interface status in README to indicate separation from Business Logic
- Add dataset export functionality with support for JSON, JSONL, and CSV formats
- Enhance README with quick start guide, usage examples, and updated features
- Add retry mechanism for CKAN API requests and enhance error handling
- Add Cargo audit configuration for vulnerability management
- Add cargo-audit installation and security audit step to CI workflow
- Migrate from OpenAI to Google Gemini API for embeddings
- Update README with detailed harvest command examples and improve search results display
- Add examples to CKAN API response and error handling documentation
- Update Ceres architecture diagram with google api


### Changed

- Improve code formatting and organization in CKAN and OpenAI clients
- Migrate to workspace structure with multiple crates
- Fmt
- Fmt


### Documentation

- Add important reminders about local .env file usage
- Update section title from "Ceres?" to "General Overview" for clarity
- Remove emoji from title for consistency


### Fixed

- Add Clone derive to Dataset and fix pgvector query mapping
- Update database URL and repository links in configuration files
- Improve error messages for OpenAI API key issues and format code for clarity
- Improve error messages by removing emoji and enhancing clarity


### Miscellaneous

- Update logo image asset
- Replace logo image with JPEG format and remove PNG version

