//! Ceres Client - HTTP clients for external APIs
//!
//! This crate provides HTTP clients for interacting with:
//!
//! - [`ckan`] - CKAN open data portals
//! - [`gemini`] - Google Gemini embeddings API
//!
//! # Overview
//!
//! The clients handle authentication, request building, response parsing,
//! and error handling for their respective APIs.

pub mod ckan;
pub mod gemini;

// Re-export main client types
pub use ckan::{CkanClient, CkanClientFactory};
pub use gemini::GeminiClient;
