//! Ollama embeddings client.
//!
//! Generates embeddings locally via [Ollama](https://ollama.com), enabling
//! fully offline operation with zero API costs.
//!
//! # Supported models
//!
//! | Model | Dimensions | Notes |
//! |-------|-----------|-------|
//! | `nomic-embed-text` | 768 | Default, matches Gemini dimension |
//! | `mxbai-embed-large` | 1024 | Higher quality |
//! | `snowflake-arctic-embed` | 1024 | Strong retrieval performance |
//! | `all-minilm` | 384 | Smallest/fastest |
//!
//! # Examples
//!
//! ```no_run
//! use ceres_client::OllamaClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = OllamaClient::new()?;
//! let embedding = client.get_embeddings("Hello, world!").await?;
//! println!("Embedding dimension: {}", embedding.len()); // 768
//! # Ok(())
//! # }
//! ```

use ceres_core::error::AppError;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Default Ollama API endpoint.
const DEFAULT_ENDPOINT: &str = "http://localhost:11434";

/// Default embedding model.
const DEFAULT_MODEL: &str = "nomic-embed-text";

/// Timeout for Ollama requests (CPU inference can be slow).
const TIMEOUT_SECS: u64 = 120;

/// Returns the embedding dimension for a known Ollama model.
///
/// For unknown models, returns 768 (the most common dimension).
pub fn model_dimension(model: &str) -> usize {
    match model {
        "nomic-embed-text" => 768,
        "mxbai-embed-large" | "snowflake-arctic-embed" => 1024,
        "all-minilm" => 384,
        _ => {
            tracing::warn!(
                model,
                "Unknown Ollama model dimension, defaulting to 768. \
                 Set EMBEDDING_MODEL to a known model or verify dimension matches your database."
            );
            768
        }
    }
}

/// HTTP client for generating embeddings via a local Ollama instance.
///
/// Ollama runs embedding models locally with zero per-request cost,
/// making it ideal for bulk embedding, development, and self-hosted deployments.
#[derive(Clone)]
pub struct OllamaClient {
    client: Client,
    model: String,
    endpoint: String,
    dim: usize,
}

/// Request body for Ollama embed API.
#[derive(Serialize)]
struct EmbedRequest<'a> {
    model: &'a str,
    input: Vec<&'a str>,
}

/// Response from Ollama embed API.
#[derive(Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

/// Error response from Ollama API.
#[derive(Deserialize)]
struct OllamaErrorResponse {
    error: String,
}

impl OllamaClient {
    /// Creates a new Ollama client with default settings.
    ///
    /// Uses `nomic-embed-text` model at `http://localhost:11434`.
    pub fn new() -> Result<Self, AppError> {
        Self::with_config(DEFAULT_MODEL, None)
    }

    /// Creates a new Ollama client with a specific model.
    pub fn with_model(model: &str) -> Result<Self, AppError> {
        Self::with_config(model, None)
    }

    /// Creates a new Ollama client with full configuration.
    ///
    /// # Arguments
    ///
    /// * `model` - Ollama model name (e.g., `nomic-embed-text`)
    /// * `endpoint` - Custom Ollama API endpoint (default: `http://localhost:11434`)
    pub fn with_config(model: &str, endpoint: Option<&str>) -> Result<Self, AppError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(TIMEOUT_SECS))
            .build()
            .map_err(|e| AppError::ClientError(e.to_string()))?;

        let endpoint = endpoint.unwrap_or(DEFAULT_ENDPOINT).to_string();
        let dim = model_dimension(model);

        Ok(Self {
            client,
            model: model.to_string(),
            endpoint,
            dim,
        })
    }

    /// Returns the model being used.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Generates an embedding for a single text.
    pub async fn get_embeddings(&self, text: &str) -> Result<Vec<f32>, AppError> {
        let embeddings = self.get_embeddings_batch(&[text]).await?;
        embeddings.into_iter().next().ok_or(AppError::EmptyResponse)
    }

    /// Generates embeddings for multiple texts in a single API call.
    ///
    /// Ollama's `/api/embed` endpoint supports native batching.
    pub async fn get_embeddings_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, AppError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let url = format!("{}/api/embed", self.endpoint);
        let request_body = EmbedRequest {
            model: &self.model,
            input: texts.to_vec(),
        };

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| self.map_connection_error(e))?;

        let status = response.status();

        if !status.is_success() {
            let status_code = status.as_u16();
            let error_text = response.text().await.unwrap_or_default();
            return Err(self.map_api_error(status_code, &error_text));
        }

        let embed_response: EmbedResponse = response.json().await.map_err(|e| {
            AppError::ClientError(format!("Failed to parse Ollama response: {}", e))
        })?;

        Ok(embed_response.embeddings)
    }

    /// Maps reqwest connection/transport errors to AppError.
    fn map_connection_error(&self, err: reqwest::Error) -> AppError {
        if err.is_timeout() {
            AppError::Timeout(TIMEOUT_SECS)
        } else if err.is_connect() {
            AppError::NetworkError(format!(
                "Cannot connect to Ollama at {}. Is it running? Try: ollama serve",
                self.endpoint
            ))
        } else {
            AppError::ClientError(format!("Ollama request failed: {}", err))
        }
    }

    /// Maps Ollama HTTP error responses to AppError.
    fn map_api_error(&self, status_code: u16, error_text: &str) -> AppError {
        let message = serde_json::from_str::<OllamaErrorResponse>(error_text)
            .map(|e| e.error)
            .unwrap_or_else(|_| format!("HTTP {}: {}", status_code, error_text));

        if status_code == 404 || message.contains("not found") {
            return AppError::ClientError(format!(
                "Ollama model '{}' not found. Try: ollama pull {}",
                self.model, self.model
            ));
        }

        AppError::ClientError(format!("Ollama error: {}", message))
    }
}

// =============================================================================
// Trait Implementation: EmbeddingProvider
// =============================================================================

impl ceres_core::traits::EmbeddingProvider for OllamaClient {
    fn name(&self) -> &'static str {
        "ollama"
    }

    fn dimension(&self) -> usize {
        self.dim
    }

    fn max_batch_size(&self) -> usize {
        512
    }

    async fn generate(&self, text: &str) -> Result<Vec<f32>, AppError> {
        self.get_embeddings(text).await
    }

    async fn generate_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, AppError> {
        let text_refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
        self.get_embeddings_batch(&text_refs).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_dimension() {
        assert_eq!(model_dimension("nomic-embed-text"), 768);
        assert_eq!(model_dimension("mxbai-embed-large"), 1024);
        assert_eq!(model_dimension("snowflake-arctic-embed"), 1024);
        assert_eq!(model_dimension("all-minilm"), 384);
        assert_eq!(model_dimension("unknown-model"), 768); // default
    }

    #[test]
    fn test_new_client() {
        let client = OllamaClient::new();
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.model(), "nomic-embed-text");
        assert_eq!(client.dim, 768);
        assert_eq!(client.endpoint, "http://localhost:11434");
    }

    #[test]
    fn test_client_with_model() {
        let client = OllamaClient::with_model("mxbai-embed-large").unwrap();
        assert_eq!(client.model(), "mxbai-embed-large");
        assert_eq!(client.dim, 1024);
    }

    #[test]
    fn test_client_with_config() {
        let client =
            OllamaClient::with_config("nomic-embed-text", Some("http://myhost:11434")).unwrap();
        assert_eq!(client.endpoint, "http://myhost:11434");
        assert_eq!(client.model(), "nomic-embed-text");
    }

    #[test]
    fn test_request_serialization() {
        let request = EmbedRequest {
            model: "nomic-embed-text",
            input: vec!["Hello world", "Test input"],
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("nomic-embed-text"));
        assert!(json.contains("Hello world"));
        assert!(json.contains("Test input"));
    }

    #[test]
    fn test_trait_implementation() {
        use ceres_core::traits::EmbeddingProvider;

        let client = OllamaClient::new().unwrap();
        assert_eq!(client.name(), "ollama");
        assert_eq!(client.dimension(), 768);
        assert_eq!(client.max_batch_size(), 512);
    }

    #[test]
    fn test_map_api_error_model_not_found() {
        let client = OllamaClient::new().unwrap();

        let err = client.map_api_error(404, r#"{"error":"model \"nomic-embed-text\" not found"}"#);
        let msg = err.to_string();
        assert!(msg.contains("not found"));
        assert!(msg.contains("ollama pull"));
    }

    #[test]
    fn test_map_api_error_generic() {
        let client = OllamaClient::new().unwrap();

        let err = client.map_api_error(500, r#"{"error":"internal server error"}"#);
        let msg = err.to_string();
        assert!(msg.contains("internal server error"));
    }
}
