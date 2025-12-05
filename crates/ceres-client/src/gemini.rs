use ceres_core::error::AppError;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// HTTP client for interacting with Google's Gemini Embeddings API.
///
/// This client provides methods to generate text embeddings using Google's
/// text-embedding-004 model. Embeddings are vector representations of text
/// that can be used for semantic search, clustering, and similarity comparisons.
///
/// # Examples
///
/// ```no_run
/// use ceres_client::GeminiClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = GeminiClient::new("your-api-key");
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

impl GeminiClient {
    /// Creates a new Gemini client with the specified API key.
    ///
    /// # Arguments
    ///
    /// * `api_key` - Your Google AI API key
    ///
    /// # Returns
    ///
    /// A configured `GeminiClient` instance.
    pub fn new(api_key: &str) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");

        Self {
            client,
            api_key: api_key.to_string(),
        }
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

        let url = format!(
            "https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:embedContent?key={}",
            self.api_key
        );

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
            .post(&url)
            .json(&request_body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    AppError::Timeout(30)
                } else if e.is_connect() {
                    AppError::NetworkError(format!("Connection failed: {}", e))
                } else {
                    AppError::ClientError(e.to_string())
                }
            })?;

        let status = response.status();

        if !status.is_success() {
            let error_text = response.text().await.unwrap_or_default();

            // Try to parse as Gemini error
            if let Ok(gemini_error) = serde_json::from_str::<GeminiError>(&error_text) {
                let msg = gemini_error.error.message;
                if status.as_u16() == 401 || msg.contains("API key") {
                    // TODO: Implement proper handling for Gemini instead of old OpenAiError
                    return Err(AppError::GeminiError(
                        "401 Unauthorized - Invalid API key".to_string(),
                    ));
                } else if status.as_u16() == 429 {
                    return Err(AppError::RateLimitExceeded);
                }
                return Err(AppError::Generic(format!("Gemini API error: {}", msg)));
            }

            return Err(AppError::Generic(format!(
                "Gemini API error: HTTP {}",
                status
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
        let _client = GeminiClient::new("test-api-key");
        // Just verify we can create a client without panicking
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
}
