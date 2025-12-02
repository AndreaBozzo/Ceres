use thiserror::Error;

/// Application-wide error types.
///
/// This enum represents all possible errors that can occur in the Ceres application.
/// It uses the `thiserror` crate for ergonomic error handling and automatic conversion
/// from underlying library errors.
///
/// # Error Conversion
///
/// Most errors automatically convert from their source types using the `#[from]` attribute:
/// - `sqlx::Error` → `AppError::DatabaseError`
/// - `reqwest::Error` → `AppError::ClientError`
/// - `async_openai::error::OpenAIError` → `AppError::OpenAiError`
/// - `serde_json::Error` → `AppError::SerializationError`
/// - `url::ParseError` → `AppError::InvalidUrl`
///
/// # Examples
///
/// ```no_run
/// use ceres_core::error::AppError;
///
/// fn example() -> Result<(), AppError> {
///     // Errors automatically convert
///     Err(AppError::Generic("Something went wrong".to_string()))
/// }
/// ```
#[derive(Error, Debug)]
pub enum AppError {
    /// Database operation failed.
    ///
    /// This error wraps all errors from SQLx database operations, including
    /// connection failures, query errors, and constraint violations.
    #[error("Database error: {0}")]
    DatabaseError(#[from] sqlx::Error),

    /// HTTP client request failed.
    ///
    /// This error occurs when HTTP requests fail due to network issues,
    /// timeouts, or server errors.
    #[error("API Client error: {0}")]
    ClientError(String),

    /// OpenAI API call failed.
    ///
    /// This error occurs when OpenAI API calls fail, including
    /// authentication failures, rate limiting, and API errors.
    #[error("OpenAI error: {0}")]
    OpenAiError(String),

    /// JSON serialization or deserialization failed.
    ///
    /// This error occurs when converting between Rust types and JSON,
    /// typically when parsing API responses or preparing database values.
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    /// URL parsing failed.
    ///
    /// This error occurs when attempting to parse an invalid URL string,
    /// typically when constructing API endpoints or validating portal URLs.
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),

    /// Dataset not found in the database.
    ///
    /// This error indicates that a requested dataset does not exist.
    #[error("Dataset not found: {0}")]
    DatasetNotFound(String),

    /// Invalid CKAN portal URL provided.
    ///
    /// This error occurs when the provided CKAN portal URL is malformed
    /// or cannot be used to construct valid API endpoints.
    #[error("Invalid CKAN portal URL: {0}")]
    InvalidPortalUrl(String),

    /// API response contained no data.
    ///
    /// This error occurs when an API returns a successful status but
    /// the response body is empty or missing expected data.
    #[error("Empty response from API")]
    EmptyResponse,

    /// Network or connection error.
    ///
    /// This error occurs when a network request fails due to connectivity issues,
    /// DNS resolution failures, or the remote server being unreachable.
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Request timeout.
    ///
    /// This error occurs when a request takes longer than the configured timeout.
    #[error("Request timed out after {0} seconds")]
    Timeout(u64),

    /// Rate limit exceeded.
    ///
    /// This error occurs when too many requests are made in a short period.
    #[error("Rate limit exceeded. Please wait and try again.")]
    RateLimitExceeded,

    /// Generic application error for cases not covered by specific variants.
    ///
    /// Use this sparingly - prefer creating specific error variants
    /// for better error handling and debugging.
    #[error("Error: {0}")]
    Generic(String),
}

impl AppError {
    /// Returns a user-friendly error message suitable for CLI output.
    pub fn user_message(&self) -> String {
        match self {
            AppError::DatabaseError(e) => {
                if e.to_string().contains("connection") {
                    "❌ Cannot connect to database. Is PostgreSQL running?\n   Try: docker-compose up -d".to_string()
                } else {
                    format!("❌ Database error: {}", e)
                }
            }
            AppError::ClientError(msg) => {
                if msg.contains("timeout") || msg.contains("timed out") {
                    "❌ Request timed out. The portal may be slow or unreachable.\n   Try again later or check the portal URL.".to_string()
                } else if msg.contains("connect") {
                    format!("❌ Cannot connect to portal: {}\n   Check your internet connection and the portal URL.", msg)
                } else {
                    format!("❌ API error: {}", msg)
                }
            }
            AppError::OpenAiError(msg) => {
                if msg.contains("401")
                    || msg.contains("Unauthorized")
                    || msg.contains("invalid_api_key")
                {
                    "❌ Invalid OpenAI API key.\n   Check your OPENAI_API_KEY environment variable."
                        .to_string()
                } else if msg.contains("429") || msg.contains("rate") {
                    "❌ OpenAI rate limit reached.\n   Wait a moment and try again, or reduce concurrency.".to_string()
                } else if msg.contains("insufficient_quota") {
                    "❌ OpenAI quota exceeded.\n   Check your OpenAI account billing.".to_string()
                } else {
                    format!("❌ OpenAI error: {}", msg)
                }
            }
            AppError::InvalidPortalUrl(url) => {
                format!(
                    "❌ Invalid portal URL: {}\n   Example: https://dati.comune.milano.it",
                    url
                )
            }
            AppError::NetworkError(msg) => {
                format!(
                    "❌ Network error: {}\n   Check your internet connection.",
                    msg
                )
            }
            AppError::Timeout(secs) => {
                format!("❌ Request timed out after {} seconds.\n   The server may be overloaded. Try again later.", secs)
            }
            AppError::RateLimitExceeded => {
                "❌ Too many requests. Please wait a moment and try again.".to_string()
            }
            AppError::EmptyResponse => {
                "❌ The API returned no data. The portal may be temporarily unavailable."
                    .to_string()
            }
            _ => format!("❌ {}", self),
        }
    }

    /// Returns true if this error is retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            AppError::NetworkError(_)
                | AppError::Timeout(_)
                | AppError::RateLimitExceeded
                | AppError::ClientError(_)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = AppError::DatasetNotFound("test-id".to_string());
        assert_eq!(err.to_string(), "Dataset not found: test-id");
    }

    #[test]
    fn test_generic_error() {
        let err = AppError::Generic("Something went wrong".to_string());
        assert_eq!(err.to_string(), "Error: Something went wrong");
    }

    #[test]
    fn test_empty_response_error() {
        let err = AppError::EmptyResponse;
        assert_eq!(err.to_string(), "Empty response from API");
    }

    #[test]
    fn test_invalid_portal_url() {
        let err = AppError::InvalidPortalUrl("not a url".to_string());
        assert!(err.to_string().contains("Invalid CKAN portal URL"));
    }

    #[test]
    fn test_error_from_serde() {
        let json = "{ invalid json }";
        let result: Result<serde_json::Value, _> = serde_json::from_str(json);
        let serde_err = result.unwrap_err();
        let app_err: AppError = serde_err.into();
        assert!(matches!(app_err, AppError::SerializationError(_)));
    }

    #[test]
    fn test_user_message_database_connection() {
        let err = AppError::DatabaseError(sqlx::Error::PoolTimedOut);
        let msg = err.user_message();
        assert!(msg.contains("Database error"));
    }

    #[test]
    fn test_user_message_openai_auth() {
        let err = AppError::OpenAiError("401 Unauthorized".to_string());
        let msg = err.user_message();
        assert!(msg.contains("Invalid OpenAI API key"));
    }

    #[test]
    fn test_user_message_rate_limit() {
        let err = AppError::OpenAiError("429 rate limit".to_string());
        let msg = err.user_message();
        assert!(msg.contains("rate limit"));
    }

    #[test]
    fn test_is_retryable() {
        assert!(AppError::NetworkError("timeout".to_string()).is_retryable());
        assert!(AppError::Timeout(30).is_retryable());
        assert!(AppError::RateLimitExceeded.is_retryable());
        assert!(!AppError::InvalidPortalUrl("bad".to_string()).is_retryable());
    }

    #[test]
    fn test_timeout_error() {
        let err = AppError::Timeout(30);
        assert_eq!(err.to_string(), "Request timed out after 30 seconds");
    }
}
