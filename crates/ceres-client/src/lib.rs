//! Ceres Client - HTTP clients for external APIs
//!
//! This crate provides HTTP clients for interacting with:
//!
//! - [`ckan`] - CKAN open data portals
//! - [`portal`] - Unified portal client factory (enum dispatch over portal types)
//! - [`gemini`] - Google Gemini embeddings API
//! - [`openai`] - OpenAI embeddings API
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
//! | Socrata | Planned | [Socrata API](https://dev.socrata.com/) |
//! | DCAT-AP | Planned | [DCAT-AP](https://joinup.ec.europa.eu/collection/semantic-interoperability-community-semic/solution/dcat-application-profile-data-portals-europe) |
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

pub mod ckan;
pub mod gemini;
pub mod openai;
pub mod portal;
pub mod provider;

// Re-export main client types
pub use ckan::{CkanClient, CkanClientFactory};
pub use gemini::GeminiClient;
pub use openai::OpenAIClient;
pub use portal::{PortalClientEnum, PortalClientFactoryEnum, PortalDataEnum};
pub use provider::EmbeddingProviderEnum;
