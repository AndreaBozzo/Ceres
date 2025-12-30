//! Google Gemini embeddings client.
//!
//! # Future Extensions
//!
//! TODO: Implement switchable embedding providers (roadmap v0.3+)
//! Consider creating an `EmbeddingProvider` trait:
//! ```ignore
//! #[async_trait]
//! pub trait EmbeddingProvider: Send + Sync {
//!     fn dimension(&self) -> usize;
//!     async fn embed(&self, text: &str) -> Result<Vec<f32>, AppError>;
//! }
//! ```
//!
//! Potential providers to support:
//! - OpenAI text-embedding-3-small/large
//! - Cohere embed-multilingual-v3.0
//! - E5-multilingual (local, for cross-language search)
//! - Ollama (local embeddings)

use ceres_core::HttpConfig;
use ceres_core::error::{AppError, GeminiErrorDetails, GeminiErrorKind};
use reqwest::Client;
use serde::{Deserialize, Serialize};

/// HTTP client for interacting with Google's Gemini Embeddings API.
///
/// This client provides methods to generate text embeddings using Google's
/// text-embedding-004 model. Embeddings are vector representations of text
/// that can be used for semantic search, clustering, and similarity comparisons.
///
/// # Security
///
/// The API key is securely transmitted via the `x-goog-api-key` HTTP header,
/// not in the URL, to prevent accidental exposure in logs and proxies.
///
/// # Examples
///
/// ```no_run
/// use ceres_client::GeminiClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = GeminiClient::new("your-api-key")?;
/// let embedding = client.get_embeddings("Hello, world!").await?;
/// println!("Embedding dimension: {}", embedding.len()); // 768
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct GeminiClient {
    client: Client,
    api_key: String,
}

/// Request body for Gemini embedding API
#[derive(Serialize)]
struct EmbeddingRequest {
    model: String,
    content: Content,
}

#[derive(Serialize)]
struct Content {
    parts: Vec<Part>,
}

#[derive(Serialize)]
struct Part {
    text: String,
}

/// Response from Gemini embedding API
#[derive(Deserialize)]
struct EmbeddingResponse {
    embedding: EmbeddingData,
}

#[derive(Deserialize)]
struct EmbeddingData {
    values: Vec<f32>,
}

/// Error response from Gemini API
#[derive(Deserialize)]
struct GeminiError {
    error: GeminiErrorDetail,
}

#[derive(Deserialize)]
struct GeminiErrorDetail {
    message: String,
    #[allow(dead_code)]
    status: Option<String>,
}

/// Classify Gemini API error based on status code and message
fn classify_gemini_error(status_code: u16, message: &str) -> GeminiErrorKind {
    match status_code {
        401 => GeminiErrorKind::Authentication,
        429 => {
            // Check if it's quota exceeded or rate limit
            if message.contains("insufficient_quota") || message.contains("quota") {
                GeminiErrorKind::QuotaExceeded
            } else {
                GeminiErrorKind::RateLimit
            }
        }
        500..=599 => GeminiErrorKind::ServerError,
        _ => {
            // Check message content for specific error types
            if message.contains("API key") || message.contains("Unauthorized") {
                GeminiErrorKind::Authentication
            } else if message.contains("rate") {
                GeminiErrorKind::RateLimit
            } else if message.contains("quota") {
                GeminiErrorKind::QuotaExceeded
            } else {
                GeminiErrorKind::Unknown
            }
        }
    }
}

impl GeminiClient {
    /// Creates a new Gemini client with the specified API key.
    pub fn new(api_key: &str) -> Result<Self, AppError> {
        let http_config = HttpConfig::default();
        let client = Client::builder()
            .timeout(http_config.timeout)
            .build()
            .map_err(|e| AppError::ClientError(e.to_string()))?;

        Ok(Self {
            client,
            api_key: api_key.to_string(),
        })
    }

    /// Generates text embeddings using Google's text-embedding-004 model.
    ///
    /// This method converts input text into a 768-dimensional vector representation
    /// that captures semantic meaning.
    ///
    /// # Arguments
    ///
    /// * `text` - The input text to generate embeddings for
    ///
    /// # Returns
    ///
    /// A vector of 768 floating-point values representing the text embedding.
    ///
    /// # Errors
    ///
    /// Returns `AppError::ClientError` if the HTTP request fails.
    /// Returns `AppError::Generic` if the API returns an error.
    pub async fn get_embeddings(&self, text: &str) -> Result<Vec<f32>, AppError> {
        // Sanitize text - replace newlines with spaces
        let sanitized_text = text.replace('\n', " ");

        // TODO(config): Make API endpoint configurable via GEMINI_API_ENDPOINT env var
        // Useful for: (1) Proxy servers, (2) Self-hosted alternatives, (3) Testing
        let url = "https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:embedContent";

        // TODO(config): Make embedding model configurable via GEMINI_EMBEDDING_MODEL env var
        // Different models offer different cost/quality tradeoffs:
        // - text-embedding-004 (current): 768 dimensions
        // - Future models may have different dimensions - handle dynamically
        let request_body = EmbeddingRequest {
            model: "models/text-embedding-004".to_string(),
            content: Content {
                parts: vec![Part {
                    text: sanitized_text,
                }],
            },
        };

        let response = self
            .client
            .post(url)
            .header("x-goog-api-key", self.api_key.clone())
            .json(&request_body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    AppError::Timeout(30)
                } else if e.is_connect() {
                    AppError::GeminiError(GeminiErrorDetails::new(
                        GeminiErrorKind::NetworkError,
                        format!("Connection failed: {}", e),
                        0, // No HTTP status for connection failures
                    ))
                } else {
                    AppError::ClientError(e.to_string())
                }
            })?;

        let status = response.status();

        if !status.is_success() {
            let status_code = status.as_u16();
            let error_text = response.text().await.unwrap_or_default();

            // Try to parse as structured Gemini error
            let message = if let Ok(gemini_error) = serde_json::from_str::<GeminiError>(&error_text)
            {
                gemini_error.error.message
            } else {
                format!("HTTP {}: {}", status_code, error_text)
            };

            // Classify the error
            let kind = classify_gemini_error(status_code, &message);

            // Return structured error
            return Err(AppError::GeminiError(GeminiErrorDetails::new(
                kind,
                message,
                status_code,
            )));
        }

        let embedding_response: EmbeddingResponse = response
            .json()
            .await
            .map_err(|e| AppError::ClientError(format!("Failed to parse response: {}", e)))?;

        Ok(embedding_response.embedding.values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_client() {
        let client = GeminiClient::new("test-api-key");
        assert!(client.is_ok());
    }

    #[test]
    fn test_text_sanitization() {
        let text_with_newlines = "Line 1\nLine 2\nLine 3";
        let sanitized = text_with_newlines.replace('\n', " ");
        assert_eq!(sanitized, "Line 1 Line 2 Line 3");
    }

    #[test]
    fn test_request_serialization() {
        let request = EmbeddingRequest {
            model: "models/text-embedding-004".to_string(),
            content: Content {
                parts: vec![Part {
                    text: "Hello world".to_string(),
                }],
            },
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("text-embedding-004"));
        assert!(json.contains("Hello world"));
    }

    #[test]
    fn test_classify_gemini_error_auth() {
        let kind = classify_gemini_error(401, "Invalid API key");
        assert_eq!(kind, GeminiErrorKind::Authentication);
    }

    #[test]
    fn test_classify_gemini_error_auth_from_message() {
        let kind = classify_gemini_error(400, "API key not valid");
        assert_eq!(kind, GeminiErrorKind::Authentication);
    }

    #[test]
    fn test_classify_gemini_error_rate_limit() {
        let kind = classify_gemini_error(429, "Rate limit exceeded");
        assert_eq!(kind, GeminiErrorKind::RateLimit);
    }

    #[test]
    fn test_classify_gemini_error_quota() {
        let kind = classify_gemini_error(429, "insufficient_quota");
        assert_eq!(kind, GeminiErrorKind::QuotaExceeded);
    }

    #[test]
    fn test_classify_gemini_error_server() {
        let kind = classify_gemini_error(500, "Internal server error");
        assert_eq!(kind, GeminiErrorKind::ServerError);
    }

    #[test]
    fn test_classify_gemini_error_server_503() {
        let kind = classify_gemini_error(503, "Service unavailable");
        assert_eq!(kind, GeminiErrorKind::ServerError);
    }

    #[test]
    fn test_classify_gemini_error_unknown() {
        let kind = classify_gemini_error(400, "Bad request");
        assert_eq!(kind, GeminiErrorKind::Unknown);
    }
}
