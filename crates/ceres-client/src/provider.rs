//! Embedding provider factory and dynamic dispatch.
//!
//! This module provides a unified interface for working with different
//! embedding providers through the [`EmbeddingProviderEnum`] enum.
//!
//! # Why an Enum Instead of `dyn Trait`?
//!
//! The [`EmbeddingProvider`] trait uses `impl Future` return types (RPITIT),
//! which makes it not object-safe. We use an enum to provide dynamic dispatch
//! while maintaining the ergonomic async trait syntax.
//!
//! # Usage
//!
//! ```no_run
//! use ceres_client::provider::EmbeddingProviderEnum;
//! use ceres_core::traits::EmbeddingProvider;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create provider based on configuration
//! let provider = EmbeddingProviderEnum::gemini("your-api-key")?;
//!
//! // Use the provider generically
//! println!("Using {} provider ({} dimensions)", provider.name(), provider.dimension());
//! let embedding = provider.generate("Hello world").await?;
//! # Ok(())
//! # }
//! ```

use anyhow::Context;
use ceres_core::config::EmbeddingProviderType;
use ceres_core::error::AppError;
use ceres_core::traits::EmbeddingProvider;

use crate::{GeminiClient, OpenAIClient};

/// Configuration needed to create an embedding provider.
///
/// This struct extracts the embedding-related fields shared between
/// CLI and server configurations, avoiding duplication of the factory logic.
pub struct EmbeddingConfig {
    pub provider: String,
    pub gemini_api_key: Option<String>,
    pub openai_api_key: Option<String>,
    pub embedding_model: Option<String>,
}

/// Unified embedding provider that wraps concrete implementations.
///
/// This enum allows runtime selection of embedding providers while
/// implementing the `EmbeddingProvider` trait.
#[derive(Clone)]
pub enum EmbeddingProviderEnum {
    /// Google Gemini embedding provider (768 dimensions).
    Gemini(GeminiClient),
    /// OpenAI embedding provider (1536 or 3072 dimensions).
    OpenAI(OpenAIClient),
}

impl EmbeddingProviderEnum {
    /// Creates a Gemini embedding provider.
    ///
    /// # Arguments
    ///
    /// * `api_key` - Google Gemini API key
    pub fn gemini(api_key: &str) -> Result<Self, AppError> {
        Ok(Self::Gemini(GeminiClient::new(api_key)?))
    }

    /// Creates an OpenAI embedding provider with the default model.
    ///
    /// Uses `text-embedding-3-small` (1536 dimensions).
    ///
    /// # Arguments
    ///
    /// * `api_key` - OpenAI API key (starts with `sk-`)
    pub fn openai(api_key: &str) -> Result<Self, AppError> {
        Ok(Self::OpenAI(OpenAIClient::new(api_key)?))
    }

    /// Creates an OpenAI embedding provider with a specific model.
    ///
    /// # Arguments
    ///
    /// * `api_key` - OpenAI API key
    /// * `model` - Model name (e.g., `text-embedding-3-large`)
    pub fn openai_with_model(api_key: &str, model: &str) -> Result<Self, AppError> {
        Ok(Self::OpenAI(OpenAIClient::with_model(api_key, model)?))
    }

    /// Creates an embedding provider from configuration.
    ///
    /// Parses the provider type and initializes the appropriate client
    /// with the given API key and optional model override.
    pub fn from_config(config: &EmbeddingConfig) -> anyhow::Result<Self> {
        let provider_type: EmbeddingProviderType = config
            .provider
            .parse()
            .context("Invalid embedding provider")?;

        match provider_type {
            EmbeddingProviderType::Gemini => {
                let api_key = config.gemini_api_key.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("GEMINI_API_KEY required when using gemini provider")
                })?;
                Self::gemini(api_key).context("Failed to initialize Gemini client")
            }
            EmbeddingProviderType::OpenAI => {
                let api_key = config.openai_api_key.as_ref().ok_or_else(|| {
                    anyhow::anyhow!("OPENAI_API_KEY required when using openai provider")
                })?;

                if let Some(model) = &config.embedding_model {
                    Self::openai_with_model(api_key, model)
                        .context("Failed to initialize OpenAI client")
                } else {
                    Self::openai(api_key).context("Failed to initialize OpenAI client")
                }
            }
        }
    }
}

impl EmbeddingProvider for EmbeddingProviderEnum {
    fn name(&self) -> &'static str {
        match self {
            Self::Gemini(c) => c.name(),
            Self::OpenAI(c) => c.name(),
        }
    }

    fn dimension(&self) -> usize {
        match self {
            Self::Gemini(c) => c.dimension(),
            Self::OpenAI(c) => c.dimension(),
        }
    }

    fn max_batch_size(&self) -> usize {
        match self {
            Self::Gemini(c) => c.max_batch_size(),
            Self::OpenAI(c) => c.max_batch_size(),
        }
    }

    async fn generate(&self, text: &str) -> Result<Vec<f32>, AppError> {
        match self {
            Self::Gemini(c) => c.generate(text).await,
            Self::OpenAI(c) => c.generate(text).await,
        }
    }

    async fn generate_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, AppError> {
        match self {
            Self::Gemini(c) => c.generate_batch(texts).await,
            Self::OpenAI(c) => c.generate_batch(texts).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gemini_provider_creation() {
        let provider = EmbeddingProviderEnum::gemini("test-key");
        assert!(provider.is_ok());
        let provider = provider.unwrap();
        assert_eq!(provider.name(), "gemini");
        assert_eq!(provider.dimension(), 768);
    }

    #[test]
    fn test_openai_provider_creation() {
        let provider = EmbeddingProviderEnum::openai("sk-test");
        assert!(provider.is_ok());
        let provider = provider.unwrap();
        assert_eq!(provider.name(), "openai");
        assert_eq!(provider.dimension(), 1536);
    }

    #[test]
    fn test_openai_large_model() {
        let provider =
            EmbeddingProviderEnum::openai_with_model("sk-test", "text-embedding-3-large");
        assert!(provider.is_ok());
        let provider = provider.unwrap();
        assert_eq!(provider.dimension(), 3072);
    }

    fn base_config(provider: &str) -> EmbeddingConfig {
        EmbeddingConfig {
            provider: provider.to_string(),
            gemini_api_key: None,
            openai_api_key: None,
            embedding_model: None,
        }
    }

    #[test]
    fn test_from_config_gemini() {
        let mut config = base_config("gemini");
        config.gemini_api_key = Some("test-key".to_string());
        let provider = EmbeddingProviderEnum::from_config(&config).unwrap();
        assert!(matches!(provider, EmbeddingProviderEnum::Gemini(_)));
    }

    #[test]
    fn test_from_config_openai_default_model() {
        let mut config = base_config("openai");
        config.openai_api_key = Some("sk-test".to_string());
        let provider = EmbeddingProviderEnum::from_config(&config).unwrap();
        assert!(matches!(provider, EmbeddingProviderEnum::OpenAI(_)));
        assert_eq!(provider.dimension(), 1536);
    }

    #[test]
    fn test_from_config_openai_custom_model() {
        let mut config = base_config("openai");
        config.openai_api_key = Some("sk-test".to_string());
        config.embedding_model = Some("text-embedding-3-large".to_string());
        let provider = EmbeddingProviderEnum::from_config(&config).unwrap();
        assert_eq!(provider.dimension(), 3072);
    }

    #[test]
    fn test_from_config_invalid_provider() {
        let config = base_config("invalid");
        assert!(EmbeddingProviderEnum::from_config(&config).is_err());
    }

    #[test]
    fn test_from_config_missing_gemini_key() {
        let config = base_config("gemini");
        assert!(EmbeddingProviderEnum::from_config(&config).is_err());
    }

    #[test]
    fn test_from_config_missing_openai_key() {
        let config = base_config("openai");
        assert!(EmbeddingProviderEnum::from_config(&config).is_err());
    }
}
