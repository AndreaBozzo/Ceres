//! Ceres Client - HTTP clients for external APIs
//!
//! This crate provides HTTP clients for interacting with:
//!
//! - [`ckan`] - CKAN open data portals
//! - [`portal`] - Unified portal client factory (enum dispatch over portal types)
//! - [`gemini`] - Google Gemini embeddings API
//! - [`openai`] - OpenAI embeddings API
//! - [`ollama`] - Ollama local embeddings
//!
//! # Overview
//!
//! The clients handle authentication, request building, response parsing,
//! and error handling for their respective APIs.
//!
//! # Portal Clients
//!
//! Multiple portal types are supported via [`PortalClientEnum`]:
//!
//! | Portal Type | Status | API |
//! |-------------|--------|-----|
//! | CKAN | Supported | [CKAN API](https://docs.ckan.org/en/2.9/api/) |
//! | DCAT-AP (udata REST) | Supported | [DCAT-AP](https://joinup.ec.europa.eu/collection/semantic-interoperability-community-semic/solution/dcat-application-profile-data-portals-europe) |
//! | DCAT-AP (SPARQL) | Supported | SPARQL endpoint with DCAT vocabulary |
//! | DCAT-US (`data.json`) | Supported | Project Open Data static catalog |
//! | Socrata | Supported | [Discovery API](https://dev.socrata.com/docs/other/discovery) |
//! | OpenDataSoft | Supported | Explore API v2.1 (`/api/explore/v2.1/catalog/datasets`) |
//! | ArcGIS Hub | Supported | Hub Search API (`/api/search/v1/collections/dataset/items`) |
//! | OGC Records | Supported | CSW 2.0.2 catalog service |
//! | STAC | Supported | Collection-level STAC API harvesting |
//!
//! # Embedding Providers
//!
//! Multiple embedding providers are supported:
//!
//! | Provider | Model | Dimensions |
//! |----------|-------|------------|
//! | Gemini | gemini-embedding-001 | 768 |
//! | OpenAI | text-embedding-3-small | 1536 |
//! | OpenAI | text-embedding-3-large | 3072 |
//! | Ollama | nomic-embed-text | 768 |
//! | Ollama | mxbai-embed-large | 1024 |

pub mod arcgis;
pub mod ckan;
pub mod datajson;
pub mod dcat;
pub mod gemini;
pub mod ogc_records;
pub mod ollama;
pub mod openai;
pub mod opendatasoft;
pub mod portal;
pub mod provider;
pub mod socrata;
pub mod sparql;
pub mod stac;

// Re-export main client types
pub use arcgis::{ArcGisClient, ArcGisDataset};
pub use ckan::{CkanClient, CkanClientFactory};
pub use datajson::{DataJsonClient, DataJsonDataset};
pub use dcat::DcatClient;
pub use gemini::GeminiClient;
pub use ogc_records::{OgcRecord, OgcRecordsClient};
pub use ollama::OllamaClient;
pub use openai::OpenAIClient;
pub use opendatasoft::{OpenDataSoftClient, OpenDataSoftDataset};
pub use portal::{PortalClientEnum, PortalClientFactoryEnum, PortalDataEnum};
#[cfg(feature = "test-support")]
pub use provider::MockEmbeddingClient;
pub use provider::{EmbeddingConfig, EmbeddingProviderEnum};
pub use socrata::{SocrataClient, SocrataDataset};
pub use sparql::SparqlDcatClient;
pub use stac::{StacClient, StacCollection};
