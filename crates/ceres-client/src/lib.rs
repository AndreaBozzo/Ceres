//! Ceres Client - HTTP clients for external APIs
//!
//! This crate provides HTTP clients for interacting with:
//!
//! - [`ckan`] - CKAN open data portals
//! - [`openai`] - OpenAI embeddings API
//!
//! # Overview
//!
//! The clients handle authentication, request building, response parsing,
//! and error handling for their respective APIs.

pub mod ckan;
pub mod openai;

// Re-export main client types
pub use ckan::CkanClient;
pub use openai::OpenAIClient;
