//! Ceres Client - HTTP clients for external APIs
//!
//! This crate provides HTTP clients for interacting with:
//!
//! - [`ckan`] - CKAN open data portals
//! - [`gemini`] - Google Gemini embeddings API
//! - [`openai`] - OpenAI embeddings API
//!
//! # Overview
//!
//! The clients handle authentication, request building, response parsing,
//! and error handling for their respective APIs.
//!
//! # Embedding Providers
//!
//! Multiple embedding providers are supported:
//!
//! | Provider | Model | Dimensions |
//! |----------|-------|------------|
//! | Gemini | text-embedding-004 | 768 |
//! | OpenAI | text-embedding-3-small | 1536 |
//! | OpenAI | text-embedding-3-large | 3072 |

pub mod ckan;
pub mod gemini;
pub mod openai;
pub mod provider;

// Re-export main client types
pub use ckan::{CkanClient, CkanClientFactory};
pub use gemini::GeminiClient;
pub use openai::OpenAIClient;
pub use provider::EmbeddingProviderEnum;
