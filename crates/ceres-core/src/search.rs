//! Search service for semantic dataset queries.
//!
//! This module provides a high-level service for performing semantic searches
//! across the dataset index using vector embeddings.

use crate::traits::{DatasetStore, EmbeddingProvider};
use crate::{AppError, SearchResult};

/// Service for semantic search operations.
///
/// This service encapsulates the search business logic, coordinating between
/// the embedding provider and the dataset store.
///
/// # Type Parameters
///
/// * `S` - Dataset store implementation (e.g., `DatasetRepository`)
/// * `E` - Embedding provider implementation (e.g., `GeminiClient`)
///
/// # Example
///
/// ```ignore
/// use ceres_core::search::SearchService;
///
/// let search_service = SearchService::new(repo, gemini);
/// let results = search_service.search("climate data", 10).await?;
///
/// for result in results {
///     println!("{}: {:.2}", result.dataset.title, result.similarity_score);
/// }
/// ```
pub struct SearchService<S, E>
where
    S: DatasetStore,
    E: EmbeddingProvider,
{
    store: S,
    embedding: E,
}

impl<S, E> Clone for SearchService<S, E>
where
    S: DatasetStore + Clone,
    E: EmbeddingProvider + Clone,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            embedding: self.embedding.clone(),
        }
    }
}

impl<S, E> SearchService<S, E>
where
    S: DatasetStore,
    E: EmbeddingProvider,
{
    /// Creates a new search service.
    ///
    /// # Arguments
    ///
    /// * `store` - Dataset store for vector search queries
    /// * `embedding` - Embedding provider for generating query embeddings
    pub fn new(store: S, embedding: E) -> Self {
        Self { store, embedding }
    }

    /// Performs semantic search and returns ranked results.
    ///
    /// This method:
    /// 1. Generates an embedding vector from the query text
    /// 2. Searches the database using cosine similarity
    /// 3. Returns results ordered by similarity (highest first)
    ///
    /// # Arguments
    ///
    /// * `query` - The search query text
    /// * `limit` - Maximum number of results to return
    ///
    /// # Returns
    ///
    /// A vector of [`SearchResult`], ordered by similarity score (highest first).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The embedding generation fails (API error, network error, etc.)
    /// - The database query fails
    pub async fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>, AppError> {
        let query_vector = self.embedding.generate(query).await?;
        self.store.search(query_vector, limit).await
    }
}

#[cfg(test)]
mod tests {
    // Tests require concrete implementations - see integration tests
}
