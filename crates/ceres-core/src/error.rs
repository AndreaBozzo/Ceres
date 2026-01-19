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
///
/// # Gemini Error Classification
///
/// Gemini API errors are classified into specific categories for better error handling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GeminiErrorKind {
    /// Authentication failure (401, invalid API key)
    Authentication,
    /// Rate limit exceeded (429)
    RateLimit,
    /// Quota exceeded (insufficient_quota)
    QuotaExceeded,
    /// Server error (5xx)
    ServerError,
    /// Network/connection error
    NetworkError,
    /// Unknown or unclassified error
    Unknown,
}

/// Structured error details from Gemini API
#[derive(Debug, Clone)]
pub struct GeminiErrorDetails {
    /// The specific error category
    pub kind: GeminiErrorKind,
    /// Human-readable error message from the API
    pub message: String,
    /// HTTP status code
    pub status_code: u16,
}

impl GeminiErrorDetails {
    /// Create a new GeminiErrorDetails
    pub fn new(kind: GeminiErrorKind, message: String, status_code: u16) -> Self {
        Self {
            kind,
            message,
            status_code,
        }
    }
}

impl std::fmt::Display for GeminiErrorDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Gemini API error (HTTP {}): {}",
            self.status_code, self.message
        )
    }
}

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

    /// Gemini API call failed.
    ///
    /// This error occurs when Gemini API calls fail, including
    /// authentication failures, rate limiting, and API errors.
    /// Contains structured error information for better error handling.
    #[error("Gemini error: {0}")]
    GeminiError(GeminiErrorDetails),

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

    /// Configuration file error.
    ///
    /// This error occurs when reading or parsing the configuration file fails,
    /// such as when the portals.toml file is malformed or contains invalid values.
    #[error("Configuration error: {0}")]
    ConfigError(String),

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
                    "Cannot connect to database. Is PostgreSQL running?\n   Try: docker-compose up -d".to_string()
                } else {
                    format!("Database error: {}", e)
                }
            }
            AppError::ClientError(msg) => {
                if msg.contains("timeout") || msg.contains("timed out") {
                    "Request timed out. The portal may be slow or unreachable.\n   Try again later or check the portal URL.".to_string()
                } else if msg.contains("connect") {
                    format!("Cannot connect to portal: {}\n   Check your internet connection and the portal URL.", msg)
                } else {
                    format!("API error: {}", msg)
                }
            }
            AppError::GeminiError(details) => match details.kind {
                GeminiErrorKind::Authentication => {
                    "Invalid Gemini API key.\n   Check your GEMINI_API_KEY environment variable."
                        .to_string()
                }
                GeminiErrorKind::RateLimit => {
                    "Gemini rate limit reached.\n   Wait a moment and try again, or reduce concurrency."
                        .to_string()
                }
                GeminiErrorKind::QuotaExceeded => {
                    "Gemini quota exceeded.\n   Check your Google account billing.".to_string()
                }
                GeminiErrorKind::ServerError => {
                    format!(
                        "Gemini server error (HTTP {}).\n   Please try again later.",
                        details.status_code
                    )
                }
                GeminiErrorKind::NetworkError => {
                    format!(
                        "Network error connecting to Gemini: {}\n   Check your internet connection.",
                        details.message
                    )
                }
                GeminiErrorKind::Unknown => {
                    format!("Gemini error: {}", details.message)
                }
            },
            AppError::InvalidPortalUrl(url) => {
                format!(
                    "Invalid portal URL: {}\n   Example: https://dati.comune.milano.it",
                    url
                )
            }
            AppError::NetworkError(msg) => {
                format!("Network error: {}\n   Check your internet connection.", msg)
            }
            AppError::Timeout(secs) => {
                format!("Request timed out after {} seconds.\n   The server may be overloaded. Try again later.", secs)
            }
            AppError::RateLimitExceeded => {
                "Too many requests. Please wait a moment and try again.".to_string()
            }
            AppError::EmptyResponse => {
                "The API returned no data. The portal may be temporarily unavailable.".to_string()
            }
            AppError::ConfigError(msg) => {
                format!(
                    "Configuration error: {}\n   Check your configuration file.",
                    msg
                )
            }
            _ => self.to_string(),
        }
    }

    /// Returns true if this error is retryable.
    ///
    /// # Examples
    ///
    /// ```
    /// use ceres_core::error::AppError;
    ///
    /// // Network errors are retryable
    /// let err = AppError::NetworkError("connection reset".to_string());
    /// assert!(err.is_retryable());
    ///
    /// // Rate limits are retryable (after a delay)
    /// let err = AppError::RateLimitExceeded;
    /// assert!(err.is_retryable());
    ///
    /// // Dataset not found is NOT retryable
    /// let err = AppError::DatasetNotFound("test".to_string());
    /// assert!(!err.is_retryable());
    /// ```
    pub fn is_retryable(&self) -> bool {
        match self {
            AppError::NetworkError(_)
            | AppError::Timeout(_)
            | AppError::RateLimitExceeded
            | AppError::ClientError(_) => true,
            AppError::GeminiError(details) => matches!(
                details.kind,
                GeminiErrorKind::RateLimit
                    | GeminiErrorKind::NetworkError
                    | GeminiErrorKind::ServerError
            ),
            _ => false,
        }
    }

    /// Returns true if this error should trip the circuit breaker.
    ///
    /// Transient errors (network issues, timeouts, rate limits, server errors)
    /// should trip the circuit breaker. Non-transient errors (authentication,
    /// quota exceeded, invalid data) should NOT trip the circuit.
    ///
    /// # Examples
    ///
    /// ```
    /// use ceres_core::error::{AppError, GeminiErrorKind, GeminiErrorDetails};
    ///
    /// // Network errors trip the circuit
    /// let err = AppError::NetworkError("connection reset".to_string());
    /// assert!(err.should_trip_circuit());
    ///
    /// // Authentication errors do NOT trip the circuit
    /// let err = AppError::GeminiError(GeminiErrorDetails::new(
    ///     GeminiErrorKind::Authentication,
    ///     "Invalid API key".to_string(),
    ///     401,
    /// ));
    /// assert!(!err.should_trip_circuit());
    /// ```
    pub fn should_trip_circuit(&self) -> bool {
        match self {
            // Transient errors - should trip circuit
            AppError::NetworkError(_) | AppError::Timeout(_) | AppError::RateLimitExceeded => true,

            // Client errors are often transient (timeouts, connection issues)
            AppError::ClientError(msg) => {
                msg.contains("timeout")
                    || msg.contains("timed out")
                    || msg.contains("connect")
                    || msg.contains("connection")
            }

            // Gemini errors - only transient ones
            AppError::GeminiError(details) => matches!(
                details.kind,
                GeminiErrorKind::RateLimit
                    | GeminiErrorKind::NetworkError
                    | GeminiErrorKind::ServerError
            ),

            // Non-transient errors - should NOT trip circuit
            // Authentication, quota, validation errors indicate configuration
            // problems, not temporary service issues
            AppError::DatabaseError(_)
            | AppError::SerializationError(_)
            | AppError::InvalidUrl(_)
            | AppError::DatasetNotFound(_)
            | AppError::InvalidPortalUrl(_)
            | AppError::EmptyResponse
            | AppError::ConfigError(_)
            | AppError::Generic(_) => false,
        }
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
    fn test_user_message_gemini_auth() {
        let details = GeminiErrorDetails::new(
            GeminiErrorKind::Authentication,
            "Invalid API key".to_string(),
            401,
        );
        let err = AppError::GeminiError(details);
        let msg = err.user_message();
        assert!(msg.contains("Invalid Gemini API key"));
        assert!(msg.contains("GEMINI_API_KEY"));
    }

    #[test]
    fn test_user_message_gemini_rate_limit() {
        let details = GeminiErrorDetails::new(
            GeminiErrorKind::RateLimit,
            "Rate limit exceeded".to_string(),
            429,
        );
        let err = AppError::GeminiError(details);
        let msg = err.user_message();
        assert!(msg.contains("rate limit"));
    }

    #[test]
    fn test_user_message_gemini_quota() {
        let details = GeminiErrorDetails::new(
            GeminiErrorKind::QuotaExceeded,
            "Insufficient quota".to_string(),
            429,
        );
        let err = AppError::GeminiError(details);
        let msg = err.user_message();
        assert!(msg.contains("quota exceeded"));
        assert!(msg.contains("Google account billing"));
    }

    #[test]
    fn test_gemini_error_display() {
        let details = GeminiErrorDetails::new(
            GeminiErrorKind::Authentication,
            "Invalid API key".to_string(),
            401,
        );
        let err = AppError::GeminiError(details);
        assert!(err.to_string().contains("Gemini error"));
        assert!(err.to_string().contains("401"));
    }

    #[test]
    fn test_gemini_error_retryable() {
        let rate_limit = AppError::GeminiError(GeminiErrorDetails::new(
            GeminiErrorKind::RateLimit,
            "Rate limit".to_string(),
            429,
        ));
        assert!(rate_limit.is_retryable());

        let auth_error = AppError::GeminiError(GeminiErrorDetails::new(
            GeminiErrorKind::Authentication,
            "Invalid key".to_string(),
            401,
        ));
        assert!(!auth_error.is_retryable());

        let server_error = AppError::GeminiError(GeminiErrorDetails::new(
            GeminiErrorKind::ServerError,
            "Internal server error".to_string(),
            500,
        ));
        assert!(server_error.is_retryable());
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
        // PoolTimedOut message contains "connection", so it triggers the connection error branch
        let err = AppError::DatabaseError(sqlx::Error::PoolTimedOut);
        let msg = err.user_message();
        assert!(msg.contains("Cannot connect to database") || msg.contains("Database error"));
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

    #[test]
    fn test_should_trip_circuit_transient_errors() {
        // Transient errors should trip the circuit
        assert!(AppError::NetworkError("connection reset".to_string()).should_trip_circuit());
        assert!(AppError::Timeout(30).should_trip_circuit());
        assert!(AppError::RateLimitExceeded.should_trip_circuit());
    }

    #[test]
    fn test_should_trip_circuit_client_errors() {
        // Client errors with transient keywords should trip
        assert!(AppError::ClientError("connection refused".to_string()).should_trip_circuit());
        assert!(AppError::ClientError("request timed out".to_string()).should_trip_circuit());

        // Client errors without transient keywords should NOT trip
        assert!(!AppError::ClientError("invalid json".to_string()).should_trip_circuit());
    }

    #[test]
    fn test_should_trip_circuit_gemini_errors() {
        // Rate limit should trip
        let rate_limit = AppError::GeminiError(GeminiErrorDetails::new(
            GeminiErrorKind::RateLimit,
            "Rate limit exceeded".to_string(),
            429,
        ));
        assert!(rate_limit.should_trip_circuit());

        // Server error should trip
        let server_error = AppError::GeminiError(GeminiErrorDetails::new(
            GeminiErrorKind::ServerError,
            "Internal server error".to_string(),
            500,
        ));
        assert!(server_error.should_trip_circuit());

        // Network error should trip
        let network_error = AppError::GeminiError(GeminiErrorDetails::new(
            GeminiErrorKind::NetworkError,
            "Connection failed".to_string(),
            0,
        ));
        assert!(network_error.should_trip_circuit());

        // Authentication should NOT trip
        let auth_error = AppError::GeminiError(GeminiErrorDetails::new(
            GeminiErrorKind::Authentication,
            "Invalid API key".to_string(),
            401,
        ));
        assert!(!auth_error.should_trip_circuit());

        // Quota exceeded should NOT trip
        let quota_error = AppError::GeminiError(GeminiErrorDetails::new(
            GeminiErrorKind::QuotaExceeded,
            "Insufficient quota".to_string(),
            429,
        ));
        assert!(!quota_error.should_trip_circuit());
    }

    #[test]
    fn test_should_trip_circuit_non_transient_errors() {
        // These should NOT trip the circuit
        assert!(!AppError::InvalidPortalUrl("bad url".to_string()).should_trip_circuit());
        assert!(!AppError::DatasetNotFound("missing".to_string()).should_trip_circuit());
        assert!(!AppError::InvalidUrl("bad".to_string()).should_trip_circuit());
        assert!(!AppError::EmptyResponse.should_trip_circuit());
        assert!(!AppError::ConfigError("bad config".to_string()).should_trip_circuit());
        assert!(!AppError::Generic("something".to_string()).should_trip_circuit());
    }
}
