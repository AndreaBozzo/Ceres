use std::time::Duration;

/// Database connection pool configuration.
pub struct DbConfig {
    pub max_connections: u32,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self { max_connections: 5 }
    }
}

/// HTTP client configuration for external API calls.
pub struct HttpConfig {
    pub timeout: Duration,
    pub max_retries: u32,
    pub retry_base_delay: Duration,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_base_delay: Duration::from_millis(500),
        }
    }
}

/// Portal synchronization configuration.
pub struct SyncConfig {
    pub concurrency: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self { concurrency: 10 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_config_defaults() {
        let config = DbConfig::default();
        assert_eq!(config.max_connections, 5);
    }

    #[test]
    fn test_http_config_defaults() {
        let config = HttpConfig::default();
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_base_delay, Duration::from_millis(500));
    }

    #[test]
    fn test_sync_config_defaults() {
        let config = SyncConfig::default();
        assert_eq!(config.concurrency, 10);
    }
}
