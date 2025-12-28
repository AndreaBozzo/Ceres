//! Configuration types for Ceres components.
//!
//! # Configuration Improvements
//!
//! TODO(config): Make all configuration values environment-configurable
//! Currently all defaults are hardcoded. Should support:
//! - `DB_MAX_CONNECTIONS` for database pool size
//! - `SYNC_CONCURRENCY` for parallel dataset processing
//! - `HTTP_TIMEOUT` for API request timeout
//! - `HTTP_MAX_RETRIES` for retry attempts
//!
//! Consider using the `config` crate for layered configuration:
//! defaults -> config file -> environment variables -> CLI args

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::error::AppError;

/// Database connection pool configuration.
///
/// TODO(config): Support environment variable `DB_MAX_CONNECTIONS`
/// Default of 5 may be insufficient for high-concurrency scenarios.
pub struct DbConfig {
    pub max_connections: u32,
}

impl Default for DbConfig {
    fn default() -> Self {
        // TODO(config): Read from DB_MAX_CONNECTIONS env var
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
///
/// TODO(config): Support CLI arg `--concurrency` and env var `SYNC_CONCURRENCY`
/// Optimal value depends on portal rate limits and system resources.
/// Consider auto-tuning based on API response times.
pub struct SyncConfig {
    pub concurrency: usize,
}

impl Default for SyncConfig {
    fn default() -> Self {
        // TODO(config): Read from SYNC_CONCURRENCY env var
        Self { concurrency: 10 }
    }
}

// =============================================================================
// Portal Configuration (portals.toml)
// =============================================================================

/// Default portal type when not specified in configuration.
fn default_portal_type() -> String {
    "ckan".to_string()
}

/// Default enabled status when not specified in configuration.
fn default_enabled() -> bool {
    true
}

/// Root configuration structure for portals.toml.
///
/// This structure represents the entire configuration file containing
/// an array of portal definitions.
///
/// # Example
///
/// ```toml
/// [[portals]]
/// name = "dati-gov-it"
/// url = "https://dati.gov.it"
/// type = "ckan"
/// description = "Italian national open data portal"
///
/// [[portals]]
/// name = "milano"
/// url = "https://dati.comune.milano.it"
/// enabled = true
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortalsConfig {
    /// Array of portal configurations.
    pub portals: Vec<PortalEntry>,
}

impl PortalsConfig {
    /// Returns only enabled portals.
    ///
    /// Portals with `enabled = false` are excluded from batch harvesting.
    pub fn enabled_portals(&self) -> Vec<&PortalEntry> {
        self.portals.iter().filter(|p| p.enabled).collect()
    }

    /// Find a portal by name (case-insensitive).
    ///
    /// # Arguments
    /// * `name` - The portal name to search for.
    ///
    /// # Returns
    /// The matching portal entry, or None if not found.
    pub fn find_by_name(&self, name: &str) -> Option<&PortalEntry> {
        self.portals
            .iter()
            .find(|p| p.name.eq_ignore_ascii_case(name))
    }
}

/// A single portal entry in the configuration file.
///
/// Each portal entry defines a CKAN portal to harvest, including
/// its URL, type, and whether it's enabled for batch harvesting.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortalEntry {
    /// Human-readable portal name.
    ///
    /// Used for `--portal <name>` lookup and logging.
    pub name: String,

    /// Base URL of the CKAN portal.
    ///
    /// Example: "https://dati.comune.milano.it"
    pub url: String,

    /// Portal type: "ckan", "socrata", or "dcat".
    ///
    /// Defaults to "ckan" if not specified.
    #[serde(rename = "type", default = "default_portal_type")]
    pub portal_type: String,

    /// Whether this portal is enabled for batch harvesting.
    ///
    /// Defaults to `true` if not specified.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Optional description of the portal.
    pub description: Option<String>,
}

/// Default configuration file name.
pub const CONFIG_FILE_NAME: &str = "portals.toml";

/// Returns the default configuration directory path.
///
/// Uses XDG Base Directory specification: `~/.config/ceres/`
pub fn default_config_dir() -> Option<PathBuf> {
    dirs::config_dir().map(|p| p.join("ceres"))
}

/// Returns the default configuration file path.
///
/// Path: `~/.config/ceres/portals.toml`
pub fn default_config_path() -> Option<PathBuf> {
    default_config_dir().map(|p| p.join(CONFIG_FILE_NAME))
}

/// Default template content for a new portals.toml file.
///
/// Includes pre-configured Italian open data portals so users can
/// immediately run `ceres harvest` without manual configuration.
const DEFAULT_CONFIG_TEMPLATE: &str = r#"# Ceres Portal Configuration
#
# Usage:
#   ceres harvest                 # Harvest all enabled portals
#   ceres harvest --portal milano # Harvest specific portal by name
#   ceres harvest https://...     # Harvest single URL (ignores this file)
#
# Set enabled = false to skip a portal during batch harvest.

# City of Milan open data
[[portals]]
name = "milano"
url = "https://dati.comune.milano.it"
type = "ckan"
description = "Open data del Comune di Milano"

# Sicily Region open data
[[portals]]
name = "sicilia"
url = "https://dati.regione.sicilia.it"
type = "ckan"
description = "Open data della Regione Siciliana"
"#;

/// Load portal configuration from a TOML file.
///
/// # Arguments
/// * `path` - Optional custom path. If `None`, uses default XDG path.
///
/// # Returns
/// * `Ok(Some(config))` - Configuration loaded successfully
/// * `Ok(None)` - No configuration file found (not an error for backward compatibility)
/// * `Err(e)` - Configuration file exists but is invalid
///
/// # Behavior
/// If no configuration file exists at the default path, a template file
/// is automatically created to help users get started.
pub fn load_portals_config(path: Option<PathBuf>) -> Result<Option<PortalsConfig>, AppError> {
    let using_default_path = path.is_none();
    let config_path = match path {
        Some(p) => p,
        None => match default_config_path() {
            Some(p) => p,
            None => return Ok(None),
        },
    };

    if !config_path.exists() {
        // Auto-create template if using default path
        if using_default_path {
            match create_default_config(&config_path) {
                Ok(()) => {
                    // Template created successfully - read it and return the config
                    // This allows the user to immediately harvest without re-running
                    tracing::info!(
                        "Config file created at {}. Starting harvest with default portals...",
                        config_path.display()
                    );
                    // Continue to read the newly created file below
                }
                Err(e) => {
                    // Log warning but don't fail - user might not have write permissions
                    tracing::warn!("Could not create default config template: {}", e);
                    return Ok(None);
                }
            }
        } else {
            // Custom path specified but doesn't exist - that's an error
            return Err(AppError::ConfigError(format!(
                "Config file not found: {}",
                config_path.display()
            )));
        }
    }

    let content = std::fs::read_to_string(&config_path).map_err(|e| {
        AppError::ConfigError(format!(
            "Failed to read config file '{}': {}",
            config_path.display(),
            e
        ))
    })?;

    let config: PortalsConfig = toml::from_str(&content).map_err(|e| {
        AppError::ConfigError(format!(
            "Invalid TOML in '{}': {}",
            config_path.display(),
            e
        ))
    })?;

    Ok(Some(config))
}

/// Create a default configuration file with a template.
///
/// Creates the parent directory if it doesn't exist.
///
/// # Arguments
/// * `path` - The path where the config file should be created.
fn create_default_config(path: &Path) -> std::io::Result<()> {
    // Create parent directory if needed
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    std::fs::write(path, DEFAULT_CONFIG_TEMPLATE)?;
    tracing::info!("Created default config template at: {}", path.display());

    Ok(())
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

    // =========================================================================
    // Portal Configuration Tests
    // =========================================================================

    #[test]
    fn test_portals_config_deserialize() {
        let toml = r#"
[[portals]]
name = "test-portal"
url = "https://example.com"
type = "ckan"
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.portals.len(), 1);
        assert_eq!(config.portals[0].name, "test-portal");
        assert_eq!(config.portals[0].url, "https://example.com");
        assert_eq!(config.portals[0].portal_type, "ckan");
        assert!(config.portals[0].enabled); // default
        assert!(config.portals[0].description.is_none());
    }

    #[test]
    fn test_portals_config_defaults() {
        let toml = r#"
[[portals]]
name = "minimal"
url = "https://example.com"
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.portals[0].portal_type, "ckan"); // default type
        assert!(config.portals[0].enabled); // default enabled
    }

    #[test]
    fn test_portals_config_enabled_filter() {
        let toml = r#"
[[portals]]
name = "enabled-portal"
url = "https://a.com"

[[portals]]
name = "disabled-portal"
url = "https://b.com"
enabled = false
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();
        let enabled = config.enabled_portals();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].name, "enabled-portal");
    }

    #[test]
    fn test_portals_config_find_by_name() {
        let toml = r#"
[[portals]]
name = "Milano"
url = "https://dati.comune.milano.it"
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();

        // Case-insensitive search
        assert!(config.find_by_name("milano").is_some());
        assert!(config.find_by_name("MILANO").is_some());
        assert!(config.find_by_name("Milano").is_some());

        // Not found
        assert!(config.find_by_name("roma").is_none());
    }

    #[test]
    fn test_portals_config_with_description() {
        let toml = r#"
[[portals]]
name = "test"
url = "https://example.com"
description = "A test portal"
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();
        assert_eq!(
            config.portals[0].description,
            Some("A test portal".to_string())
        );
    }

    #[test]
    fn test_portals_config_multiple_portals() {
        let toml = r#"
[[portals]]
name = "portal-1"
url = "https://a.com"

[[portals]]
name = "portal-2"
url = "https://b.com"

[[portals]]
name = "portal-3"
url = "https://c.com"
enabled = false
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.portals.len(), 3);
        assert_eq!(config.enabled_portals().len(), 2);
    }

    #[test]
    fn test_default_config_path() {
        // This test just verifies the function doesn't panic
        // Actual path depends on the platform
        let path = default_config_path();
        if let Some(p) = path {
            assert!(p.ends_with("portals.toml"));
        }
    }
}
