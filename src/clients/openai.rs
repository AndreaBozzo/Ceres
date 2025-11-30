use crate::error::AppError;
use async_openai::{
    Client,
    config::OpenAIConfig,
    types::embeddings::{CreateEmbeddingRequestArgs, EmbeddingInput},
};

#[derive(Clone)]
pub struct OpenAIClient {
    client: Client<OpenAIConfig>,
}

impl OpenAIClient {
    pub fn new(api_key: &str) -> Self {
        let config = OpenAIConfig::new().with_api_key(api_key.to_string());
        let client = Client::with_config(config);
        Self { client }
    }

    /// Create embeddings for the given input text.
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
