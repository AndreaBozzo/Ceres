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
use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use crate::circuit_breaker::CircuitBreakerConfig;
use crate::error::AppError;

// =============================================================================
// Embedding Provider Configuration
// =============================================================================

/// Embedding provider type.
///
/// Determines which embedding API to use for generating text embeddings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EmbeddingProviderType {
    /// Google Gemini gemini-embedding-001 (768 dimensions).
    #[default]
    Gemini,
    /// OpenAI text-embedding-3-small (1536d) or text-embedding-3-large (3072d).
    OpenAI,
}

impl fmt::Display for EmbeddingProviderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Gemini => write!(f, "gemini"),
            Self::OpenAI => write!(f, "openai"),
        }
    }
}

impl FromStr for EmbeddingProviderType {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "gemini" => Ok(Self::Gemini),
            "openai" => Ok(Self::OpenAI),
            _ => Err(AppError::ConfigError(format!(
                "Unknown embedding provider: '{}'. Valid options: gemini, openai",
                s
            ))),
        }
    }
}

/// Gemini embedding provider configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeminiEmbeddingConfig {
    /// Gemini model name.
    #[serde(default = "default_gemini_model")]
    pub model: String,
}

fn default_gemini_model() -> String {
    "gemini-embedding-001".to_string()
}

impl Default for GeminiEmbeddingConfig {
    fn default() -> Self {
        Self {
            model: default_gemini_model(),
        }
    }
}

/// OpenAI embedding provider configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenAIEmbeddingConfig {
    /// OpenAI model name.
    #[serde(default = "default_openai_model")]
    pub model: String,
    /// Custom API endpoint (for Azure OpenAI or proxies).
    pub endpoint: Option<String>,
}

fn default_openai_model() -> String {
    "text-embedding-3-small".to_string()
}

impl Default for OpenAIEmbeddingConfig {
    fn default() -> Self {
        Self {
            model: default_openai_model(),
            endpoint: None,
        }
    }
}

/// Returns the embedding dimension for a given provider and model.
///
/// # Arguments
///
/// * `provider` - The embedding provider type
/// * `model` - The model name (optional, uses default if None)
pub fn embedding_dimension(provider: EmbeddingProviderType, model: Option<&str>) -> usize {
    match provider {
        EmbeddingProviderType::Gemini => 768, // gemini-embedding-001 with output_dimensionality=768
        EmbeddingProviderType::OpenAI => match model.unwrap_or("text-embedding-3-small") {
            "text-embedding-3-large" => 3072,
            _ => 1536, // text-embedding-3-small and ada-002
        },
    }
}

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
#[derive(Clone)]
pub struct SyncConfig {
    /// Number of concurrent dataset processing tasks.
    pub concurrency: usize,
    /// Maximum number of texts per embedding API batch call.
    /// The actual batch size is `min(this, provider.max_batch_size())`.
    pub embedding_batch_size: usize,
    /// Force full sync even if incremental sync is available.
    pub force_full_sync: bool,
    /// Circuit breaker configuration for API resilience.
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for SyncConfig {
    fn default() -> Self {
        // TODO(config): Read from SYNC_CONCURRENCY env var
        Self {
            concurrency: 10,
            embedding_batch_size: 64,
            force_full_sync: false,
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

impl SyncConfig {
    /// Creates a new SyncConfig with force_full_sync enabled.
    pub fn with_full_sync(mut self) -> Self {
        self.force_full_sync = true;
        self
    }

    /// Creates a new SyncConfig with a custom embedding batch size.
    pub fn with_embedding_batch_size(mut self, size: usize) -> Self {
        self.embedding_batch_size = size.max(1);
        self
    }

    /// Creates a new SyncConfig with custom circuit breaker configuration.
    pub fn with_circuit_breaker(mut self, config: CircuitBreakerConfig) -> Self {
        self.circuit_breaker = config;
        self
    }
}

// =============================================================================
// Portal Configuration (portals.toml)
// =============================================================================

/// Portal type identifier.
///
/// Determines which portal API client to use for harvesting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PortalType {
    /// CKAN open data portal (default).
    #[default]
    Ckan,
    /// Socrata open data portal (US cities: NYC, Chicago, SF).
    Socrata,
    /// DCAT-AP / SPARQL endpoint (EU portals, data.europa.eu).
    Dcat,
}

impl fmt::Display for PortalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ckan => write!(f, "ckan"),
            Self::Socrata => write!(f, "socrata"),
            Self::Dcat => write!(f, "dcat"),
        }
    }
}

impl FromStr for PortalType {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "ckan" => Ok(Self::Ckan),
            "socrata" => Ok(Self::Socrata),
            "dcat" => Ok(Self::Dcat),
            _ => Err(AppError::ConfigError(format!(
                "Unknown portal type: '{}'. Valid options: ckan, socrata, dcat",
                s
            ))),
        }
    }
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
    /// Example: "<https://dati.comune.milano.it>"
    pub url: String,

    /// Portal type: ckan, socrata, or dcat.
    ///
    /// Defaults to `Ckan` if not specified.
    #[serde(rename = "type", default)]
    pub portal_type: PortalType,

    /// Whether this portal is enabled for batch harvesting.
    ///
    /// Defaults to `true` if not specified.
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    /// Optional description of the portal.
    pub description: Option<String>,

    /// Optional URL template for dataset landing pages.
    ///
    /// Supports placeholders:
    /// - `{id}` — dataset UUID from the CKAN API
    /// - `{name}` — dataset slug/name
    ///
    /// If not set, defaults to `{portal_url}/dataset/{name}`.
    pub url_template: Option<String>,

    /// Preferred language for multilingual portals (e.g., `"en"`, `"de"`, `"fr"`).
    ///
    /// Some portals return title and description as language-keyed objects.
    /// This field controls which language is selected when resolving those fields.
    /// Defaults to `"en"` when not specified.
    pub language: Option<String>,
}

impl PortalEntry {
    /// Returns the preferred language, defaulting to `"en"`.
    pub fn language(&self) -> &str {
        self.language.as_deref().unwrap_or("en")
    }
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
# Use url_template for portals with non-standard frontends:
#   url_template = "https://example.com/dataset?id={id}"
#   Placeholders: {id} = dataset UUID, {name} = dataset slug

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
        assert_eq!(config.portals[0].portal_type, PortalType::Ckan);
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
        assert_eq!(config.portals[0].portal_type, PortalType::Ckan); // default type
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

    // =========================================================================
    // load_portals_config() tests with real files
    // =========================================================================

    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_portals_config_valid_file() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[[portals]]
name = "test"
url = "https://test.com"
"#
        )
        .unwrap();

        let config = load_portals_config(Some(file.path().to_path_buf()))
            .unwrap()
            .unwrap();

        assert_eq!(config.portals.len(), 1);
        assert_eq!(config.portals[0].name, "test");
        assert_eq!(config.portals[0].url, "https://test.com");
    }

    #[test]
    fn test_load_portals_config_custom_path_not_found() {
        let result = load_portals_config(Some("/nonexistent/path/to/config.toml".into()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, AppError::ConfigError(_)));
    }

    #[test]
    fn test_load_portals_config_invalid_toml() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "this is not valid toml {{{{").unwrap();

        let result = load_portals_config(Some(file.path().to_path_buf()));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, AppError::ConfigError(_)));
    }

    #[test]
    fn test_load_portals_config_multiple_portals_with_enabled_filter() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[[portals]]
name = "enabled-portal"
url = "https://a.com"

[[portals]]
name = "disabled-portal"
url = "https://b.com"
enabled = false

[[portals]]
name = "another-enabled"
url = "https://c.com"
enabled = true
"#
        )
        .unwrap();

        let config = load_portals_config(Some(file.path().to_path_buf()))
            .unwrap()
            .unwrap();

        assert_eq!(config.portals.len(), 3);
        assert_eq!(config.enabled_portals().len(), 2);
    }

    #[test]
    fn test_load_portals_config_with_all_fields() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(
            file,
            r#"
[[portals]]
name = "full-config"
url = "https://example.com"
type = "ckan"
enabled = true
description = "A fully configured portal"
"#
        )
        .unwrap();

        let config = load_portals_config(Some(file.path().to_path_buf()))
            .unwrap()
            .unwrap();

        let portal = &config.portals[0];
        assert_eq!(portal.name, "full-config");
        assert_eq!(portal.url, "https://example.com");
        assert_eq!(portal.portal_type, PortalType::Ckan);
        assert!(portal.enabled);
        assert_eq!(
            portal.description,
            Some("A fully configured portal".to_string())
        );
    }

    #[test]
    fn test_load_portals_config_empty_portals_array() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "portals = []").unwrap();

        let config = load_portals_config(Some(file.path().to_path_buf()))
            .unwrap()
            .unwrap();

        assert!(config.portals.is_empty());
        assert!(config.enabled_portals().is_empty());
    }

    // =========================================================================
    // Embedding Provider Configuration Tests
    // =========================================================================

    #[test]
    fn test_embedding_provider_type_from_str() {
        assert_eq!(
            "gemini".parse::<EmbeddingProviderType>().unwrap(),
            EmbeddingProviderType::Gemini
        );
        assert_eq!(
            "openai".parse::<EmbeddingProviderType>().unwrap(),
            EmbeddingProviderType::OpenAI
        );
        assert_eq!(
            "GEMINI".parse::<EmbeddingProviderType>().unwrap(),
            EmbeddingProviderType::Gemini
        );
        assert_eq!(
            "OpenAI".parse::<EmbeddingProviderType>().unwrap(),
            EmbeddingProviderType::OpenAI
        );
    }

    #[test]
    fn test_embedding_provider_type_invalid() {
        let result = "invalid".parse::<EmbeddingProviderType>();
        assert!(result.is_err());
    }

    #[test]
    fn test_embedding_provider_type_display() {
        assert_eq!(EmbeddingProviderType::Gemini.to_string(), "gemini");
        assert_eq!(EmbeddingProviderType::OpenAI.to_string(), "openai");
    }

    #[test]
    fn test_embedding_dimension() {
        // Gemini is always 768
        assert_eq!(
            embedding_dimension(EmbeddingProviderType::Gemini, None),
            768
        );
        assert_eq!(
            embedding_dimension(EmbeddingProviderType::Gemini, Some("gemini-embedding-001")),
            768
        );

        // OpenAI defaults to 1536
        assert_eq!(
            embedding_dimension(EmbeddingProviderType::OpenAI, None),
            1536
        );
        assert_eq!(
            embedding_dimension(
                EmbeddingProviderType::OpenAI,
                Some("text-embedding-3-small")
            ),
            1536
        );
        assert_eq!(
            embedding_dimension(
                EmbeddingProviderType::OpenAI,
                Some("text-embedding-3-large")
            ),
            3072
        );
    }

    #[test]
    fn test_gemini_embedding_config_default() {
        let config = GeminiEmbeddingConfig::default();
        assert_eq!(config.model, "gemini-embedding-001");
    }

    #[test]
    fn test_openai_embedding_config_default() {
        let config = OpenAIEmbeddingConfig::default();
        assert_eq!(config.model, "text-embedding-3-small");
        assert!(config.endpoint.is_none());
    }
}
