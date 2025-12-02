use ceres_core::error::AppError;
use async_openai::{
    Client,
    config::OpenAIConfig,
    types::{CreateEmbeddingRequestArgs, EmbeddingInput},
};

/// HTTP client for interacting with OpenAI's Embeddings API.
///
/// This client provides methods to generate text embeddings using OpenAI's
/// embedding models. Embeddings are vector representations of text that can
/// be used for semantic search, clustering, and similarity comparisons.
///
/// # Examples
///
/// ```no_run
/// use ceres_client::OpenAIClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = OpenAIClient::new("your-api-key");
/// let embedding = client.get_embeddings("Hello, world!").await?;
/// println!("Embedding dimension: {}", embedding.len());
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct OpenAIClient {
    client: Client<OpenAIConfig>,
}

impl OpenAIClient {
    /// Creates a new OpenAI client with the specified API key.
    ///
    /// # Arguments
    ///
    /// * `api_key` - Your OpenAI API key
    ///
    /// # Returns
    ///
    /// A configured `OpenAIClient` instance.
    pub fn new(api_key: &str) -> Self {
        let config = OpenAIConfig::new().with_api_key(api_key.to_string());
        let client = Client::with_config(config);
        Self { client }
    }

    /// Generates text embeddings using OpenAI's text-embedding-3-small model.
    ///
    /// This method converts input text into a 1536-dimensional vector representation
    /// that captures semantic meaning.
    ///
    /// # Arguments
    ///
    /// * `text` - The input text to generate embeddings for
    ///
    /// # Returns
    ///
    /// A vector of 1536 floating-point values representing the text embedding.
    ///
    /// # Errors
    ///
    /// Returns `AppError::Generic` if the embedding request cannot be built.
    /// Returns `AppError::OpenAiError` if the API call fails.
    pub async fn get_embeddings(&self, text: &str) -> Result<Vec<f32>, AppError> {
        // OpenAI recommends replacing newlines with spaces for better results
        let sanitized_text = text.replace('\n', " ");

        let request = CreateEmbeddingRequestArgs::default()
            .model("text-embedding-3-small")
            .input(EmbeddingInput::String(sanitized_text))
            .build()
            .map_err(|e| AppError::Generic(format!("Failed to build embedding request: {}", e)))?;

        let response = self
            .client
            .embeddings()
            .create(request)
            .await
            .map_err(|e| AppError::OpenAiError(e.to_string()))?;

        let embedding = response
            .data
            .first()
            .ok_or_else(|| AppError::Generic("No embedding data returned".to_string()))?
            .embedding
            .clone();

        Ok(embedding)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_client() {
        let _client = OpenAIClient::new("test-api-key");
        // Just verify we can create a client without panicking
    }

    #[test]
    fn test_text_sanitization() {
        let text_with_newlines = "Line 1\nLine 2\nLine 3";
        let sanitized = text_with_newlines.replace('\n', " ");
        assert_eq!(sanitized, "Line 1 Line 2 Line 3");
    }
}
