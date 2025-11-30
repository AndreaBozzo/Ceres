use crate::error::AppError;
use async_openai::{
    Client,
    config::OpenAIConfig,
    types::embeddings::{CreateEmbeddingRequestArgs, EmbeddingInput},
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
/// use ceres::clients::openai::OpenAIClient;
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ceres::clients::openai::OpenAIClient;
    ///
    /// let client = OpenAIClient::new("sk-...");
    /// ```
    pub fn new(api_key: &str) -> Self {
        let config = OpenAIConfig::new().with_api_key(api_key.to_string());
        let client = Client::with_config(config);
        Self { client }
    }

    /// Generates text embeddings using OpenAI's text-embedding-3-small model.
    ///
    /// This method converts input text into a 1536-dimensional vector representation
    /// that captures semantic meaning. The embedding can be used for similarity search,
    /// clustering, or other machine learning tasks.
    ///
    /// The text is automatically sanitized by replacing newlines with spaces,
    /// as recommended by OpenAI for better embedding quality.
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
    /// Returns `AppError::Generic` if the response contains no embedding data.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ceres::clients::openai::OpenAIClient;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = OpenAIClient::new("your-api-key");
    /// let embedding = client.get_embeddings("Semantic search for open data").await?;
    /// assert_eq!(embedding.len(), 1536);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_embeddings(&self, text: &str) -> Result<Vec<f32>, AppError> {
        // OpenAI reccomends replacing newlines with spaces for better results
        let sanitized_text = text.replace("\n", " ");

        let request = CreateEmbeddingRequestArgs::default()
            .model("text-embedding-3-small")
            .input(EmbeddingInput::String(sanitized_text))
            .build()
            .map_err(|e| AppError::Generic(format!("Failed to build embedding request: {}", e)))?;

        let response = self.client.embeddings().create(request).await?;

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
    fn test_new_client_with_empty_key() {
        let _client = OpenAIClient::new("");
        // Client creation should succeed even with empty key
        // The API call will fail later if the key is invalid
    }

    #[test]
    fn test_text_sanitization() {
        // Test that newlines would be replaced (we can't test the actual method
        // without making API calls, but we can test the logic)
        let text_with_newlines = "Line 1\nLine 2\nLine 3";
        let sanitized = text_with_newlines.replace("\n", " ");
        assert_eq!(sanitized, "Line 1 Line 2 Line 3");
    }

    #[test]
    fn test_multi_newline_sanitization() {
        let text = "First\n\nSecond\n\n\nThird";
        let sanitized = text.replace("\n", " ");
        assert_eq!(sanitized, "First  Second   Third");
    }

    // Note: Integration tests that make actual API calls would require:
    // 1. A valid API key
    // 2. Network access
    // 3. Handling of API rate limits and costs
    // These should be in separate integration tests with proper test fixtures
}
