//! OpenAI embeddings client.
//!
//! Supports OpenAI's text embedding models:
//! - `text-embedding-3-small` (1536 dimensions, recommended)
//! - `text-embedding-3-large` (3072 dimensions, higher quality)
//! - `text-embedding-ada-002` (1536 dimensions, legacy)
//!
//! # Examples
//!
//! ```no_run
//! use ceres_client::OpenAIClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let client = OpenAIClient::new("sk-your-api-key")?;
//! let embedding = client.get_embeddings("Hello, world!").await?;
//! println!("Embedding dimension: {}", embedding.len()); // 1536
//! # Ok(())
//! # }
//! ```

use ceres_core::HttpConfig;
use ceres_core::error::AppError;
use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Known OpenAI embedding models and their dimensions.
pub fn model_dimension(model: &str) -> usize {
    match model {
        "text-embedding-3-small" => 1536,
        "text-embedding-3-large" => 3072,
        "text-embedding-ada-002" => 1536,
        _ => 1536, // default to small model dimension
    }
}

/// HTTP client for interacting with OpenAI's Embeddings API.
///
/// This client generates text embeddings using OpenAI's embedding models.
/// The default model is `text-embedding-3-small` which produces 1536-dimensional vectors.
#[derive(Clone)]
pub struct OpenAIClient {
    client: Client,
    api_key: String,
    model: String,
    endpoint: String,
    dim: usize,
    timeout_secs: u64,
}

/// Request body for OpenAI embedding API
#[derive(Serialize)]
struct EmbeddingRequest<'a> {
    model: &'a str,
    input: Vec<&'a str>,
}

/// Response from OpenAI embedding API
#[derive(Deserialize)]
struct EmbeddingResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
    #[allow(dead_code)]
    index: usize,
}

/// Error response from OpenAI API
#[derive(Deserialize)]
struct OpenAIError {
    error: OpenAIErrorDetail,
}

#[derive(Deserialize)]
struct OpenAIErrorDetail {
    message: String,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    error_type: Option<String>,
    #[allow(dead_code)]
    code: Option<String>,
}

impl OpenAIClient {
    /// Creates a new OpenAI client with the specified API key.
    ///
    /// Uses the default model `text-embedding-3-small` (1536 dimensions).
    pub fn new(api_key: &str) -> Result<Self, AppError> {
        Self::with_model(api_key, "text-embedding-3-small")
    }

    /// Creates a new OpenAI client with a specific model.
    ///
    /// # Arguments
    ///
    /// * `api_key` - OpenAI API key (starts with `sk-`)
    /// * `model` - Model name (e.g., `text-embedding-3-small`, `text-embedding-3-large`)
    pub fn with_model(api_key: &str, model: &str) -> Result<Self, AppError> {
        Self::with_config(api_key, model, None)
    }

    /// Creates a new OpenAI client with full configuration.
    ///
    /// # Arguments
    ///
    /// * `api_key` - OpenAI API key
    /// * `model` - Model name
    /// * `endpoint` - Custom API endpoint (for Azure OpenAI or proxies)
    pub fn with_config(
        api_key: &str,
        model: &str,
        endpoint: Option<&str>,
    ) -> Result<Self, AppError> {
        let http_config = HttpConfig::default();
        let client = Client::builder()
            .timeout(http_config.timeout)
            .build()
            .map_err(|e| AppError::ClientError(e.to_string()))?;

        let endpoint = endpoint
            .unwrap_or("https://api.openai.com/v1/embeddings")
            .to_string();
        let dim = model_dimension(model);
        let timeout_secs = http_config.timeout.as_secs();

        Ok(Self {
            client,
            api_key: api_key.to_string(),
            model: model.to_string(),
            endpoint,
            dim,
            timeout_secs,
        })
    }

    /// Returns the model being used.
    pub fn model(&self) -> &str {
        &self.model
    }

    /// Generates text embeddings for a single text.
    ///
    /// # Arguments
    ///
    /// * `text` - The input text to generate embeddings for
    ///
    /// # Returns
    ///
    /// A vector of floating-point values representing the text embedding.
    pub async fn get_embeddings(&self, text: &str) -> Result<Vec<f32>, AppError> {
        let embeddings = self.get_embeddings_batch(&[text]).await?;
        embeddings.into_iter().next().ok_or(AppError::EmptyResponse)
    }

    /// Generates text embeddings for multiple texts in a single API call.
    ///
    /// OpenAI's API supports batch embedding, making this more efficient
    /// than calling `get_embeddings` multiple times.
    ///
    /// # Arguments
    ///
    /// * `texts` - Slice of texts to embed
    ///
    /// # Returns
    ///
    /// A vector of embedding vectors, one per input text, in the same order.
    pub async fn get_embeddings_batch(&self, texts: &[&str]) -> Result<Vec<Vec<f32>>, AppError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }

        let request_body = EmbeddingRequest {
            model: &self.model,
            input: texts.to_vec(),
        };

        let response = self
            .client
            .post(&self.endpoint)
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    AppError::Timeout(self.timeout_secs)
                } else if e.is_connect() {
                    AppError::NetworkError(format!("Cannot connect to OpenAI: {}", e))
                } else {
                    AppError::ClientError(e.to_string())
                }
            })?;

        let status = response.status();

        if !status.is_success() {
            let status_code = status.as_u16();
            let error_text = response.text().await.unwrap_or_default();

            // Try to parse as structured OpenAI error
            let message = if let Ok(openai_error) = serde_json::from_str::<OpenAIError>(&error_text)
            {
                openai_error.error.message
            } else {
                format!("HTTP {}: {}", status_code, error_text)
            };

            // Map common HTTP status codes to appropriate errors
            return match status_code {
                401 => Err(AppError::ClientError(format!(
                    "OpenAI authentication failed: {}. Check your OPENAI_API_KEY.",
                    message
                ))),
                429 => Err(AppError::RateLimitExceeded),
                _ => Err(AppError::ClientError(format!("OpenAI error: {}", message))),
            };
        }

        let embedding_response: EmbeddingResponse = response.json().await.map_err(|e| {
            AppError::ClientError(format!("Failed to parse OpenAI response: {}", e))
        })?;

        // Sort by index to ensure correct order
        let mut data = embedding_response.data;
        data.sort_by_key(|d| d.index);

        Ok(data.into_iter().map(|d| d.embedding).collect())
    }
}

// =============================================================================
// Trait Implementation: EmbeddingProvider
// =============================================================================

impl ceres_core::traits::EmbeddingProvider for OpenAIClient {
    fn name(&self) -> &'static str {
        "openai"
    }

    fn dimension(&self) -> usize {
        self.dim
    }

    async fn generate(&self, text: &str) -> Result<Vec<f32>, AppError> {
        self.get_embeddings(text).await
    }

    async fn generate_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, AppError> {
        // Convert &[String] to Vec<&str> for the API
        let text_refs: Vec<&str> = texts.iter().map(|s| s.as_str()).collect();
        self.get_embeddings_batch(&text_refs).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_model_dimension() {
        assert_eq!(model_dimension("text-embedding-3-small"), 1536);
        assert_eq!(model_dimension("text-embedding-3-large"), 3072);
        assert_eq!(model_dimension("text-embedding-ada-002"), 1536);
        assert_eq!(model_dimension("unknown-model"), 1536); // default
    }

    #[test]
    fn test_new_client() {
        let client = OpenAIClient::new("sk-test-api-key");
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.model(), "text-embedding-3-small");
        assert_eq!(client.dim, 1536);
    }

    #[test]
    fn test_client_with_model() {
        let client = OpenAIClient::with_model("sk-test", "text-embedding-3-large").unwrap();
        assert_eq!(client.model(), "text-embedding-3-large");
        assert_eq!(client.dim, 3072);
    }

    #[test]
    fn test_request_serialization() {
        let request = EmbeddingRequest {
            model: "text-embedding-3-small",
            input: vec!["Hello world", "Test input"],
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("text-embedding-3-small"));
        assert!(json.contains("Hello world"));
        assert!(json.contains("Test input"));
    }

    #[test]
    fn test_trait_implementation() {
        use ceres_core::traits::EmbeddingProvider;

        let client = OpenAIClient::new("sk-test").unwrap();
        assert_eq!(client.name(), "openai");
        assert_eq!(client.dimension(), 1536);
    }
}
