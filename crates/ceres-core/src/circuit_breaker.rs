//! Circuit breaker pattern for API resilience.
//!
//! This module implements a circuit breaker to protect against cascading failures
//! when external APIs (Gemini, CKAN) experience issues.
//!
//! # Circuit States
//!
//! ```text
//! CLOSED (healthy) --[N failures]--> OPEN (rejecting) --[timeout]--> HALF_OPEN (probing)
//!                                                                         |
//!                                       <--[failure]--                    |
//!                                                                         |
//! CLOSED <---------------------------[success]----------------------------+
//! ```
//!
//! # Example
//!
//! ```ignore
//! use ceres_core::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
//!
//! let cb = CircuitBreaker::new("gemini", CircuitBreakerConfig::default());
//!
//! // Wrap API calls
//! match cb.call(|| client.get_embeddings(text)).await {
//!     Ok(embedding) => { /* success */ }
//!     Err(CircuitBreakerError::Open { .. }) => { /* circuit is open, skip */ }
//!     Err(CircuitBreakerError::Inner(e)) => { /* actual API error */ }
//! }
//! ```

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::error::{AppError, GeminiErrorKind};

/// Current state of the circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed - requests flow normally.
    Closed,
    /// Circuit is open - requests are rejected immediately.
    Open,
    /// Circuit is half-open - limited requests allowed to test recovery.
    HalfOpen,
}

impl std::fmt::Display for CircuitState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitState::Closed => write!(f, "closed"),
            CircuitState::Open => write!(f, "open"),
            CircuitState::HalfOpen => write!(f, "half-open"),
        }
    }
}

/// Configuration for circuit breaker behavior.
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of consecutive failures before opening the circuit.
    pub failure_threshold: u32,

    /// Number of successful requests in half-open state to close the circuit.
    pub success_threshold: u32,

    /// Time to wait before transitioning from Open to Half-Open.
    pub recovery_timeout: Duration,

    /// When rate limit (429) is detected, multiply recovery_timeout by this factor.
    pub rate_limit_backoff_multiplier: f32,

    /// Maximum recovery timeout after rate limit backoffs.
    pub max_recovery_timeout: Duration,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            recovery_timeout: Duration::from_secs(30),
            rate_limit_backoff_multiplier: 2.0,
            max_recovery_timeout: Duration::from_secs(300), // 5 minutes
        }
    }
}

impl CircuitBreakerConfig {
    /// Creates config from environment variables with fallback to defaults.
    pub fn from_env() -> Self {
        Self {
            failure_threshold: std::env::var("CB_FAILURE_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(5),
            success_threshold: std::env::var("CB_SUCCESS_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),
            recovery_timeout: Duration::from_secs(
                std::env::var("CB_RECOVERY_TIMEOUT_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(30),
            ),
            ..Default::default()
        }
    }
}

/// Internal state tracking for the circuit breaker.
#[derive(Debug)]
struct CircuitBreakerInner {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    last_failure_time: Option<Instant>,
    last_error_message: Option<String>,
    current_recovery_timeout: Duration,
}

impl CircuitBreakerInner {
    fn new(config: &CircuitBreakerConfig) -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure_time: None,
            last_error_message: None,
            current_recovery_timeout: config.recovery_timeout,
        }
    }
}

/// Statistics about circuit breaker state for monitoring.
#[derive(Debug, Clone)]
pub struct CircuitBreakerStats {
    /// Name of the circuit breaker.
    pub name: String,
    /// Current state.
    pub state: CircuitState,
    /// Number of consecutive failures.
    pub failure_count: u32,
    /// Number of consecutive successes (in half-open state).
    pub success_count: u32,
    /// Last error message if any.
    pub last_error: Option<String>,
    /// Time until circuit may transition to half-open (if currently open).
    pub time_until_half_open: Option<Duration>,
}

/// Error type for circuit breaker operations.
#[derive(Debug)]
pub enum CircuitBreakerError {
    /// Circuit is open - request was rejected without calling the service.
    Open {
        /// Name of the circuit breaker.
        name: String,
        /// Time until the circuit may transition to half-open.
        retry_after: Duration,
    },
    /// The inner operation failed.
    Inner(AppError),
}

impl std::fmt::Display for CircuitBreakerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::Open { name, retry_after } => {
                write!(
                    f,
                    "Circuit breaker '{}' is open. Retry after {} seconds.",
                    name,
                    retry_after.as_secs()
                )
            }
            CircuitBreakerError::Inner(e) => write!(f, "{}", e),
        }
    }
}

impl std::error::Error for CircuitBreakerError {}

/// Thread-safe circuit breaker for protecting external API calls.
#[derive(Clone)]
pub struct CircuitBreaker {
    name: String,
    config: CircuitBreakerConfig,
    inner: Arc<Mutex<CircuitBreakerInner>>,
}

impl CircuitBreaker {
    /// Creates a new circuit breaker with the given name and configuration.
    pub fn new(name: impl Into<String>, config: CircuitBreakerConfig) -> Self {
        let inner = CircuitBreakerInner::new(&config);
        Self {
            name: name.into(),
            config,
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Returns the name of this circuit breaker.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the current state of the circuit.
    ///
    /// Note: This also handles lazy state transitions from Open to HalfOpen
    /// when the recovery timeout has elapsed.
    pub fn state(&self) -> CircuitState {
        let mut inner = self.inner.lock().unwrap();
        self.maybe_transition_to_half_open(&mut inner);
        inner.state
    }

    /// Returns circuit breaker statistics for monitoring.
    pub fn stats(&self) -> CircuitBreakerStats {
        let mut inner = self.inner.lock().unwrap();
        self.maybe_transition_to_half_open(&mut inner);

        let time_until_half_open = if inner.state == CircuitState::Open {
            inner.last_failure_time.map(|t| {
                let elapsed = t.elapsed();
                if elapsed < inner.current_recovery_timeout {
                    inner.current_recovery_timeout - elapsed
                } else {
                    Duration::ZERO
                }
            })
        } else {
            None
        };

        CircuitBreakerStats {
            name: self.name.clone(),
            state: inner.state,
            failure_count: inner.failure_count,
            success_count: inner.success_count,
            last_error: inner.last_error_message.clone(),
            time_until_half_open,
        }
    }

    /// Executes the given operation through the circuit breaker.
    ///
    /// - If circuit is Closed: executes operation, tracks success/failure
    /// - If circuit is Open: returns `CircuitBreakerError::Open` immediately
    /// - If circuit is HalfOpen: executes operation, transitions based on result
    pub async fn call<F, T, Fut>(&self, operation: F) -> Result<T, CircuitBreakerError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, AppError>>,
    {
        // Check if we should allow the request
        {
            let mut inner = self.inner.lock().unwrap();
            self.maybe_transition_to_half_open(&mut inner);

            if inner.state == CircuitState::Open {
                let retry_after = inner
                    .last_failure_time
                    .map(|t| {
                        let elapsed = t.elapsed();
                        if elapsed < inner.current_recovery_timeout {
                            inner.current_recovery_timeout - elapsed
                        } else {
                            Duration::ZERO
                        }
                    })
                    .unwrap_or(inner.current_recovery_timeout);

                return Err(CircuitBreakerError::Open {
                    name: self.name.clone(),
                    retry_after,
                });
            }
        }

        // Execute the operation
        let result = operation().await;

        // Record the result
        match &result {
            Ok(_) => self.record_success(),
            Err(e) => {
                if e.should_trip_circuit() {
                    self.record_failure(e);
                }
            }
        }

        result.map_err(CircuitBreakerError::Inner)
    }

    /// Records a successful operation.
    pub fn record_success(&self) {
        let mut inner = self.inner.lock().unwrap();

        match inner.state {
            CircuitState::HalfOpen => {
                inner.success_count += 1;
                if inner.success_count >= self.config.success_threshold {
                    // Transition to Closed
                    tracing::info!(
                        circuit = %self.name,
                        "Circuit breaker closing after {} successful probes",
                        inner.success_count
                    );
                    inner.state = CircuitState::Closed;
                    inner.failure_count = 0;
                    inner.success_count = 0;
                    inner.last_error_message = None;
                    // Reset recovery timeout
                    inner.current_recovery_timeout = self.config.recovery_timeout;
                }
            }
            CircuitState::Closed => {
                // Reset failure count on success
                inner.failure_count = 0;
            }
            CircuitState::Open => {
                // Should not happen, but ignore
            }
        }
    }

    /// Records a failed operation.
    pub fn record_failure(&self, error: &AppError) {
        let mut inner = self.inner.lock().unwrap();

        // Check if this is a rate limit error
        let is_rate_limit = matches!(error, AppError::RateLimitExceeded)
            || matches!(
                error,
                AppError::GeminiError(details) if details.kind == GeminiErrorKind::RateLimit
            );

        match inner.state {
            CircuitState::Closed => {
                inner.failure_count += 1;
                inner.last_failure_time = Some(Instant::now());
                inner.last_error_message = Some(error.to_string());

                if inner.failure_count >= self.config.failure_threshold {
                    // Transition to Open
                    tracing::warn!(
                        circuit = %self.name,
                        failures = inner.failure_count,
                        error = %error,
                        "Circuit breaker opening after {} consecutive failures",
                        inner.failure_count
                    );
                    inner.state = CircuitState::Open;

                    // Apply rate limit backoff if applicable
                    if is_rate_limit {
                        inner.current_recovery_timeout = std::cmp::min(
                            Duration::from_secs_f32(
                                inner.current_recovery_timeout.as_secs_f32()
                                    * self.config.rate_limit_backoff_multiplier,
                            ),
                            self.config.max_recovery_timeout,
                        );
                        tracing::info!(
                            circuit = %self.name,
                            recovery_timeout_secs = inner.current_recovery_timeout.as_secs(),
                            "Extended recovery timeout due to rate limit"
                        );
                    }
                }
            }
            CircuitState::HalfOpen => {
                // Probe failed, go back to Open
                tracing::warn!(
                    circuit = %self.name,
                    error = %error,
                    "Circuit breaker probe failed, returning to open state"
                );
                inner.state = CircuitState::Open;
                inner.last_failure_time = Some(Instant::now());
                inner.last_error_message = Some(error.to_string());
                inner.success_count = 0;

                // Apply rate limit backoff if applicable
                if is_rate_limit {
                    inner.current_recovery_timeout = std::cmp::min(
                        Duration::from_secs_f32(
                            inner.current_recovery_timeout.as_secs_f32()
                                * self.config.rate_limit_backoff_multiplier,
                        ),
                        self.config.max_recovery_timeout,
                    );
                }
            }
            CircuitState::Open => {
                // Already open, just update last error
                inner.last_error_message = Some(error.to_string());
            }
        }
    }

    /// Manually resets the circuit breaker to closed state.
    pub fn reset(&self) {
        let mut inner = self.inner.lock().unwrap();
        tracing::info!(circuit = %self.name, "Circuit breaker manually reset");
        inner.state = CircuitState::Closed;
        inner.failure_count = 0;
        inner.success_count = 0;
        inner.last_failure_time = None;
        inner.last_error_message = None;
        inner.current_recovery_timeout = self.config.recovery_timeout;
    }

    /// Check if we should transition from Open to HalfOpen.
    fn maybe_transition_to_half_open(&self, inner: &mut CircuitBreakerInner) {
        if inner.state == CircuitState::Open {
            if let Some(last_failure) = inner.last_failure_time {
                if last_failure.elapsed() >= inner.current_recovery_timeout {
                    tracing::info!(
                        circuit = %self.name,
                        "Circuit breaker transitioning to half-open state"
                    );
                    inner.state = CircuitState::HalfOpen;
                    inner.success_count = 0;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::GeminiErrorDetails;

    #[test]
    fn test_circuit_starts_closed() {
        let cb = CircuitBreaker::new("test", CircuitBreakerConfig::default());
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_opens_after_threshold_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        for _ in 0..3 {
            cb.record_failure(&AppError::NetworkError("test".into()));
        }

        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_circuit_stays_closed_below_threshold() {
        let config = CircuitBreakerConfig {
            failure_threshold: 5,
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        for _ in 0..4 {
            cb.record_failure(&AppError::NetworkError("test".into()));
        }

        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_success_resets_failure_count() {
        let config = CircuitBreakerConfig {
            failure_threshold: 5,
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        // 4 failures
        for _ in 0..4 {
            cb.record_failure(&AppError::NetworkError("test".into()));
        }

        // 1 success resets count
        cb.record_success();

        // 4 more failures should NOT open circuit
        for _ in 0..4 {
            cb.record_failure(&AppError::NetworkError("test".into()));
        }

        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_transitions_to_half_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_millis(10),
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        cb.record_failure(&AppError::NetworkError("test".into()));
        assert_eq!(cb.state(), CircuitState::Open);

        std::thread::sleep(Duration::from_millis(20));

        assert_eq!(cb.state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_half_open_closes_on_success() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 2,
            recovery_timeout: Duration::from_millis(1),
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        // Open the circuit
        cb.record_failure(&AppError::NetworkError("test".into()));
        std::thread::sleep(Duration::from_millis(5));

        // Should be half-open now
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // 2 successes should close it
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_half_open_reopens_on_failure() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 2,
            recovery_timeout: Duration::from_millis(1),
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        // Open the circuit
        cb.record_failure(&AppError::NetworkError("test".into()));
        std::thread::sleep(Duration::from_millis(5));

        // Should be half-open now
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // Failure should go back to open
        cb.record_failure(&AppError::NetworkError("test".into()));
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_rate_limit_extends_recovery_timeout() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_secs(30),
            rate_limit_backoff_multiplier: 2.0,
            max_recovery_timeout: Duration::from_secs(300),
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        let rate_limit_error = AppError::RateLimitExceeded;
        cb.record_failure(&rate_limit_error);

        let stats = cb.stats();
        assert_eq!(stats.state, CircuitState::Open);
        // Recovery timeout should be extended to 60 seconds
        assert!(stats.time_until_half_open.unwrap() > Duration::from_secs(55));
    }

    #[test]
    fn test_rate_limit_backoff_capped_at_max() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            success_threshold: 1,
            recovery_timeout: Duration::from_secs(200),
            rate_limit_backoff_multiplier: 2.0,
            max_recovery_timeout: Duration::from_secs(300),
        };
        let cb = CircuitBreaker::new("test", config);

        // This would be 400s but should be capped at 300s
        cb.record_failure(&AppError::RateLimitExceeded);

        let stats = cb.stats();
        assert!(stats.time_until_half_open.unwrap() <= Duration::from_secs(300));
    }

    #[test]
    fn test_manual_reset() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_secs(300),
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        cb.record_failure(&AppError::NetworkError("test".into()));
        assert_eq!(cb.state(), CircuitState::Open);

        cb.reset();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_stats() {
        let cb = CircuitBreaker::new("gemini", CircuitBreakerConfig::default());

        let stats = cb.stats();
        assert_eq!(stats.name, "gemini");
        assert_eq!(stats.state, CircuitState::Closed);
        assert_eq!(stats.failure_count, 0);
        assert!(stats.last_error.is_none());
        assert!(stats.time_until_half_open.is_none());
    }

    #[test]
    fn test_gemini_rate_limit_error_trips_circuit() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        let gemini_error = AppError::GeminiError(GeminiErrorDetails::new(
            GeminiErrorKind::RateLimit,
            "Rate limit exceeded".into(),
            429,
        ));

        cb.record_failure(&gemini_error);
        assert_eq!(cb.state(), CircuitState::Open);
    }

    #[test]
    fn test_non_tripping_errors_dont_affect_circuit() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        // Authentication errors should not trip the circuit
        let auth_error = AppError::GeminiError(GeminiErrorDetails::new(
            GeminiErrorKind::Authentication,
            "Invalid API key".into(),
            401,
        ));

        // This error doesn't trip the circuit because should_trip_circuit returns false
        if auth_error.should_trip_circuit() {
            cb.record_failure(&auth_error);
        }

        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_call_returns_open_error_when_circuit_open() {
        let config = CircuitBreakerConfig {
            failure_threshold: 1,
            recovery_timeout: Duration::from_secs(60),
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);
        cb.record_failure(&AppError::NetworkError("test".into()));

        let result = cb
            .call(|| async { Ok::<_, AppError>("should not execute".to_string()) })
            .await;

        assert!(matches!(result, Err(CircuitBreakerError::Open { .. })));
    }

    #[tokio::test]
    async fn test_call_executes_when_closed() {
        let cb = CircuitBreaker::new("test", CircuitBreakerConfig::default());

        let result = cb
            .call(|| async { Ok::<_, AppError>("success".to_string()) })
            .await;

        assert_eq!(result.unwrap(), "success");
    }

    #[tokio::test]
    async fn test_call_records_failure() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            ..Default::default()
        };
        let cb = CircuitBreaker::new("test", config);

        let _ = cb
            .call(|| async { Err::<String, _>(AppError::NetworkError("fail".into())) })
            .await;

        let stats = cb.stats();
        assert_eq!(stats.failure_count, 1);
    }
}
