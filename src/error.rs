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
/// use ceres::error::AppError;
///
/// fn example() -> Result<(), AppError> {
///     // Errors automatically convert
///     let url = reqwest::Url::parse("invalid")?;
///     Ok(())
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
    /// This error wraps all errors from the `reqwest` HTTP client, including
    /// network failures, timeout errors, and HTTP status errors.
    #[error("API Client error: {0}")]
    ClientError(#[from] reqwest::Error),

    /// OpenAI API call failed.
    ///
    /// This error wraps all errors from the OpenAI client, including
    /// authentication failures, rate limiting, and API errors.
    #[error("OpenAI error: {0}")]
    OpenAiError(#[from] async_openai::error::OpenAIError),

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
    InvalidUrl(#[from] url::ParseError),

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

    /// Generic application error for cases not covered by specific variants.
    ///
    /// Use this sparingly - prefer creating specific error variants
    /// for better error handling and debugging.
    #[error("Error: {0}")]
    Generic(String),
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
    fn test_error_from_url_parse() {
        let parse_err = url::Url::parse("not a valid url").unwrap_err();
        let app_err: AppError = parse_err.into();
        assert!(matches!(app_err, AppError::InvalidUrl(_)));
    }

    #[test]
    fn test_error_from_serde() {
        let json = "{ invalid json }";
        let result: Result<serde_json::Value, _> = serde_json::from_str(json);
        let serde_err = result.unwrap_err();
        let app_err: AppError = serde_err.into();
        assert!(matches!(app_err, AppError::SerializationError(_)));
    }
}
