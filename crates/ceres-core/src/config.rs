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
    /// Ollama local embedding (default: nomic-embed-text, 768 dimensions).
    Ollama,
}

impl fmt::Display for EmbeddingProviderType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Gemini => write!(f, "gemini"),
            Self::OpenAI => write!(f, "openai"),
            Self::Ollama => write!(f, "ollama"),
        }
    }
}

impl FromStr for EmbeddingProviderType {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "gemini" => Ok(Self::Gemini),
            "openai" => Ok(Self::OpenAI),
            "ollama" => Ok(Self::Ollama),
            _ => Err(AppError::ConfigError(format!(
                "Unknown embedding provider: '{}'. Valid options: gemini, openai, ollama",
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

/// Ollama embedding provider configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OllamaEmbeddingConfig {
    /// Ollama model name.
    #[serde(default = "default_ollama_model")]
    pub model: String,
    /// Ollama API endpoint.
    #[serde(default = "default_ollama_endpoint")]
    pub endpoint: String,
}

fn default_ollama_model() -> String {
    "nomic-embed-text".to_string()
}

fn default_ollama_endpoint() -> String {
    "http://localhost:11434".to_string()
}

impl Default for OllamaEmbeddingConfig {
    fn default() -> Self {
        Self {
            model: default_ollama_model(),
            endpoint: default_ollama_endpoint(),
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
        EmbeddingProviderType::Ollama => {
            // Normalize tagged model identifiers like "snowflake-arctic-embed:335m"
            let normalized = model
                .map(|m| m.split(':').next().unwrap_or(m))
                .unwrap_or("nomic-embed-text");

            match normalized {
                "mxbai-embed-large" | "snowflake-arctic-embed" => 1024,
                "all-minilm" => 384,
                _ => 768, // nomic-embed-text and unknown models
            }
        }
    }
}

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
            // 60s base. Some healthy-but-slow portals (e.g. govdata.de) take well
            // over 30s to serve a full `package_search` page; the harvest clients
            // additionally scale this up adaptively when a page times out.
            timeout: Duration::from_secs(60),
            max_retries: 3,
            retry_base_delay: Duration::from_millis(500),
        }
    }
}

impl HttpConfig {
    /// Builds an [`HttpConfig`] from environment variables, falling back to the
    /// defaults for any var that is unset or unparseable.
    ///
    /// Recognized variables (all optional):
    /// - `CERES_HTTP_TIMEOUT_SECS` — per-request timeout in seconds
    /// - `CERES_HTTP_MAX_RETRIES` — transient-error retry attempts
    /// - `CERES_HTTP_RETRY_BASE_MS` — base backoff delay in milliseconds
    ///
    /// This is the knob for harvesting slow portals at scale without code
    /// changes (e.g. `CERES_HTTP_TIMEOUT_SECS=120` for very slow catalogs).
    pub fn from_env() -> Self {
        fn env_parse<T: FromStr>(key: &str) -> Option<T> {
            std::env::var(key).ok()?.trim().parse().ok()
        }

        let default = Self::default();
        Self {
            timeout: env_parse::<u64>("CERES_HTTP_TIMEOUT_SECS")
                .map(Duration::from_secs)
                .unwrap_or(default.timeout),
            max_retries: env_parse::<u32>("CERES_HTTP_MAX_RETRIES").unwrap_or(default.max_retries),
            retry_base_delay: env_parse::<u64>("CERES_HTTP_RETRY_BASE_MS")
                .map(Duration::from_millis)
                .unwrap_or(default.retry_base_delay),
        }
    }
}

/// Harvest-only configuration (metadata fetching + delta detection).
///
/// Used by [`crate::HarvestService`] when running without an embedding provider.
/// Contains only the settings relevant to portal fetching and persistence.
#[derive(Clone)]
pub struct HarvestConfig {
    /// Number of concurrent dataset processing tasks.
    pub concurrency: usize,
    /// Maximum number of datasets per DB upsert batch.
    pub upsert_batch_size: usize,
    /// Force full sync even if incremental sync is available.
    pub force_full_sync: bool,
    /// Preview mode: fetch and compare datasets without writing to DB.
    pub dry_run: bool,
}

impl Default for HarvestConfig {
    fn default() -> Self {
        Self {
            concurrency: 10,
            upsert_batch_size: 500,
            force_full_sync: false,
            dry_run: false,
        }
    }
}

impl HarvestConfig {
    /// Creates a new HarvestConfig with force_full_sync enabled.
    pub fn with_full_sync(mut self) -> Self {
        self.force_full_sync = true;
        self
    }

    /// Creates a new HarvestConfig with dry_run enabled.
    pub fn with_dry_run(mut self) -> Self {
        self.dry_run = true;
        self
    }
}

/// Embedding service configuration.
///
/// Used by [`crate::EmbeddingService`] for standalone embedding passes.
/// Contains only the settings relevant to embedding API calls.
#[derive(Clone)]
pub struct EmbeddingServiceConfig {
    /// Maximum number of texts per embedding API batch call.
    /// The actual batch size is `min(this, provider.max_batch_size())`.
    pub batch_size: usize,
    /// Circuit breaker configuration for embedding API resilience.
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for EmbeddingServiceConfig {
    fn default() -> Self {
        Self {
            batch_size: 64,
            circuit_breaker: CircuitBreakerConfig::default(),
        }
    }
}

/// Portal synchronization configuration (combined harvest + embed).
///
/// This is a convenience type that composes [`HarvestConfig`] and
/// [`EmbeddingServiceConfig`] for the common harvest-then-embed workflow.
/// Used by [`crate::HarvestPipeline`] and kept for backward compatibility.
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
    /// Maximum number of datasets per DB upsert batch.
    pub upsert_batch_size: usize,
    /// Force full sync even if incremental sync is available.
    pub force_full_sync: bool,
    /// Preview mode: fetch and compare datasets without writing to DB or calling embedding API.
    pub dry_run: bool,
    /// Circuit breaker configuration for API resilience.
    pub circuit_breaker: CircuitBreakerConfig,
}

impl Default for SyncConfig {
    fn default() -> Self {
        // TODO(config): Read from SYNC_CONCURRENCY env var
        Self {
            concurrency: 10,
            embedding_batch_size: 64,
            upsert_batch_size: 500,
            force_full_sync: false,
            dry_run: false,
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

    /// Creates a new SyncConfig with dry_run enabled.
    pub fn with_dry_run(mut self) -> Self {
        self.dry_run = true;
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

    /// Extracts the harvest-only configuration.
    pub fn harvest_config(&self) -> HarvestConfig {
        HarvestConfig {
            concurrency: self.concurrency,
            upsert_batch_size: self.upsert_batch_size,
            force_full_sync: self.force_full_sync,
            dry_run: self.dry_run,
        }
    }

    /// Extracts the embedding service configuration.
    pub fn embedding_service_config(&self) -> EmbeddingServiceConfig {
        EmbeddingServiceConfig {
            batch_size: self.embedding_batch_size,
            circuit_breaker: self.circuit_breaker.clone(),
        }
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
    /// OpenDataSoft portal (Explore API v2.1; common in European and municipal portals).
    OpenDataSoft,
}

impl fmt::Display for PortalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ckan => write!(f, "ckan"),
            Self::Socrata => write!(f, "socrata"),
            Self::Dcat => write!(f, "dcat"),
            Self::OpenDataSoft => write!(f, "opendatasoft"),
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
            "opendatasoft" => Ok(Self::OpenDataSoft),
            _ => Err(AppError::ConfigError(format!(
                "Unknown portal type: '{}'. Valid options: ckan, socrata, dcat, opendatasoft",
                s
            ))),
        }
    }
}

/// DCAT transport profile identifier.
///
/// Selects which client implementation handles a `type = "dcat"` portal.
/// This is the single source of truth for profile semantics: `portals.toml`,
/// CLI `--profile`, harvest job requests, and factory dispatch all parse into
/// and match on this enum. Every profile preserves full source metadata.
///
/// Deserialization delegates to [`FromStr`], so every input path shares the
/// same parsing rules (canonical names, the `udata` alias, case-insensitive)
/// and the same error message listing supported values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DcatProfile {
    /// udata REST JSON-LD catalog endpoint (default when omitted).
    #[default]
    UdataRest,
    /// SPARQL endpoint returning DCAT-AP metadata.
    Sparql,
    /// Static Project Open Data / DCAT-US `data.json` catalog.
    StaticJson,
}

impl<'de> Deserialize<'de> for DcatProfile {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

impl DcatProfile {
    /// Human-readable list of accepted profile names, for error messages.
    pub const SUPPORTED: &'static str =
        "'udata_rest' (alias: 'udata'), 'sparql', 'static_json' (alias: 'data_json')";

    /// Returns the canonical string representation (used in config files,
    /// the harvest job table, and API responses).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::UdataRest => "udata_rest",
            Self::Sparql => "sparql",
            Self::StaticJson => "static_json",
        }
    }
}

impl fmt::Display for DcatProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for DcatProfile {
    type Err = AppError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "udata_rest" | "udata" => Ok(Self::UdataRest),
            "sparql" => Ok(Self::Sparql),
            "static_json" | "data_json" => Ok(Self::StaticJson),
            _ => Err(AppError::ConfigError(format!(
                "Unknown DCAT profile: '{}'. Supported profiles: {}",
                s,
                Self::SUPPORTED
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

    /// Validates every portal entry (see [`PortalEntry::validate`]).
    ///
    /// Returns the first validation error encountered.
    pub fn validate(&self) -> Result<(), AppError> {
        self.portals.iter().try_for_each(PortalEntry::validate)
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

    /// Optional DCAT profile for sub-dispatch.
    ///
    /// Only valid when `type = "dcat"` (validated by [`PortalEntry::validate`]).
    /// See [`DcatProfile`] for the supported profiles; defaults to
    /// [`DcatProfile::UdataRest`] when omitted.
    #[serde(default)]
    pub profile: Option<DcatProfile>,

    /// Optional custom SPARQL endpoint URL.
    ///
    /// Only meaningful when `profile = "sparql"`. Overrides the default
    /// convention of `{url}/sparql`. Use this when the SPARQL endpoint
    /// lives on a different domain than the portal (e.g., Norway's
    /// `data.norge.no` portal uses `sparql.fellesdatakatalog.digdir.no`).
    #[serde(default)]
    pub sparql_endpoint: Option<String>,

    /// Alternate or mirror base URLs that are the *same logical portal* as `url`.
    ///
    /// Datasets harvested under any of these URLs are folded onto `url` for
    /// cross-portal duplicate detection and snapshot identity, so a mirror or
    /// renamed endpoint is not counted as an independent source (which would
    /// otherwise inflate cross-portal duplicate flags). This is config-declared
    /// and does not perform entity resolution across genuinely distinct portals.
    #[serde(default)]
    pub aliases: Vec<String>,
}

impl PortalEntry {
    /// Returns the preferred language, defaulting to `"en"`.
    pub fn language(&self) -> &str {
        self.language.as_deref().unwrap_or("en")
    }

    /// Returns the DCAT profile, if set.
    pub fn profile(&self) -> Option<DcatProfile> {
        self.profile
    }

    /// Returns the custom SPARQL endpoint URL, if set.
    pub fn sparql_endpoint(&self) -> Option<&str> {
        self.sparql_endpoint.as_deref()
    }

    /// Returns the declared alias/mirror base URLs for this portal.
    pub fn aliases(&self) -> &[String] {
        &self.aliases
    }

    /// Validates profile-specific settings for this portal entry.
    ///
    /// Rules:
    /// - `profile` is only valid when `type = "dcat"`
    /// - `sparql_endpoint` is only valid when `profile = "sparql"`
    pub fn validate(&self) -> Result<(), AppError> {
        if self.profile.is_some() && self.portal_type != PortalType::Dcat {
            return Err(AppError::ConfigError(format!(
                "Portal '{}': 'profile' is only valid for type = \"dcat\" portals (found type = \"{}\")",
                self.name, self.portal_type
            )));
        }
        if self.sparql_endpoint.is_some() && self.profile != Some(DcatProfile::Sparql) {
            return Err(AppError::ConfigError(format!(
                "Portal '{}': 'sparql_endpoint' is only valid with profile = \"sparql\"",
                self.name
            )));
        }
        Ok(())
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

    config.validate()?;

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
        assert_eq!(config.timeout, Duration::from_secs(60));
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.retry_base_delay, Duration::from_millis(500));
    }

    #[test]
    fn test_sync_config_defaults() {
        let config = SyncConfig::default();
        assert_eq!(config.concurrency, 10);
        assert_eq!(config.upsert_batch_size, 500);
    }

    #[test]
    fn test_sync_config_harvest_config_upsert_batch_size() {
        let config = SyncConfig::default();
        let harvest = config.harvest_config();
        assert_eq!(harvest.upsert_batch_size, 500);
        assert_ne!(harvest.upsert_batch_size, config.embedding_batch_size);
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
    fn test_portals_config_dcat_profile() {
        let toml = r#"
[[portals]]
name = "eu-sparql"
url = "https://data.europa.eu"
type = "dcat"
profile = "sparql"
language = "en"
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();
        let portal = &config.portals[0];
        assert_eq!(portal.portal_type, PortalType::Dcat);
        assert_eq!(portal.profile(), Some(DcatProfile::Sparql));
        assert_eq!(portal.language(), "en");
        assert!(portal.validate().is_ok());
    }

    #[test]
    fn test_portals_config_profile_defaults_none() {
        let toml = r#"
[[portals]]
name = "luxembourg"
url = "https://data.public.lu"
type = "dcat"
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.portals[0].profile(), None);
    }

    #[test]
    fn test_dcat_profile_from_str() {
        assert_eq!(
            "udata_rest".parse::<DcatProfile>().unwrap(),
            DcatProfile::UdataRest
        );
        assert_eq!(
            "udata".parse::<DcatProfile>().unwrap(),
            DcatProfile::UdataRest
        );
        assert_eq!(
            "sparql".parse::<DcatProfile>().unwrap(),
            DcatProfile::Sparql
        );
        assert_eq!(
            "SPARQL".parse::<DcatProfile>().unwrap(),
            DcatProfile::Sparql
        );
        assert_eq!(
            "static_json".parse::<DcatProfile>().unwrap(),
            DcatProfile::StaticJson
        );
        assert_eq!(
            "data_json".parse::<DcatProfile>().unwrap(),
            DcatProfile::StaticJson
        );

        let err = "spqarql".parse::<DcatProfile>().unwrap_err();
        assert!(err.to_string().contains("spqarql"));
        assert!(err.to_string().contains("udata_rest"));
    }

    #[test]
    fn test_portal_type_opendatasoft_parse_display_deserialize() {
        assert_eq!(
            "opendatasoft".parse::<PortalType>().unwrap(),
            PortalType::OpenDataSoft
        );
        assert_eq!(
            "OpenDataSoft".parse::<PortalType>().unwrap(),
            PortalType::OpenDataSoft
        );
        assert_eq!(PortalType::OpenDataSoft.to_string(), "opendatasoft");

        let toml = r#"
[[portals]]
name = "paris"
url = "https://opendata.paris.fr"
type = "opendatasoft"
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.portals[0].portal_type, PortalType::OpenDataSoft);
        assert!(config.portals[0].validate().is_ok());

        let err = "odsx".parse::<PortalType>().unwrap_err();
        assert!(err.to_string().contains("opendatasoft"));
    }

    #[test]
    fn test_dcat_profile_canonical_roundtrip() {
        for profile in [
            DcatProfile::UdataRest,
            DcatProfile::Sparql,
            DcatProfile::StaticJson,
        ] {
            assert_eq!(profile.as_str().parse::<DcatProfile>().unwrap(), profile);
        }
        assert_eq!(DcatProfile::default(), DcatProfile::UdataRest);
    }

    #[test]
    fn test_portals_config_udata_alias_accepted() {
        let toml = r#"
[[portals]]
name = "luxembourg"
url = "https://data.public.lu"
type = "dcat"
profile = "udata"
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.portals[0].profile(), Some(DcatProfile::UdataRest));
    }

    #[test]
    fn test_portals_config_rejects_unknown_profile() {
        let toml = r#"
[[portals]]
name = "bad"
url = "https://example.org"
type = "dcat"
profile = "spqarql"
"#;
        let err = toml::from_str::<PortalsConfig>(toml).unwrap_err();
        assert!(err.to_string().contains("Unknown DCAT profile"));
        assert!(err.to_string().contains("udata_rest"));
    }

    #[test]
    fn test_portals_config_profile_is_case_insensitive() {
        // TOML deserialization delegates to FromStr, so it accepts the same
        // case-insensitive spellings as the CLI --profile flag.
        let toml = r#"
[[portals]]
name = "eu"
url = "https://data.europa.eu"
type = "dcat"
profile = "SPARQL"
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();
        assert_eq!(config.portals[0].profile(), Some(DcatProfile::Sparql));
    }

    #[test]
    fn test_dcat_profile_serializes_to_canonical_name() {
        assert_eq!(
            serde_json::to_string(&DcatProfile::UdataRest).unwrap(),
            "\"udata_rest\""
        );
        assert_eq!(
            serde_json::to_string(&DcatProfile::Sparql).unwrap(),
            "\"sparql\""
        );
        assert_eq!(
            serde_json::to_string(&DcatProfile::StaticJson).unwrap(),
            "\"static_json\""
        );
    }

    #[test]
    fn test_portal_entry_validate_rejects_profile_on_non_dcat() {
        let toml = r#"
[[portals]]
name = "bad-ckan"
url = "https://example.org"
type = "ckan"
profile = "sparql"
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("bad-ckan"));
        assert!(err.to_string().contains("dcat"));
    }

    #[test]
    fn test_portal_entry_validate_rejects_sparql_endpoint_without_sparql_profile() {
        let toml = r#"
[[portals]]
name = "bad-endpoint"
url = "https://example.org"
type = "dcat"
sparql_endpoint = "https://example.org/sparql"
"#;
        let config: PortalsConfig = toml::from_str(toml).unwrap();
        let err = config.validate().unwrap_err();
        assert!(err.to_string().contains("bad-endpoint"));
        assert!(err.to_string().contains("sparql_endpoint"));
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
        assert_eq!(
            "ollama".parse::<EmbeddingProviderType>().unwrap(),
            EmbeddingProviderType::Ollama
        );
        assert_eq!(
            "OLLAMA".parse::<EmbeddingProviderType>().unwrap(),
            EmbeddingProviderType::Ollama
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
        assert_eq!(EmbeddingProviderType::Ollama.to_string(), "ollama");
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

    #[test]
    fn test_ollama_embedding_config_default() {
        let config = OllamaEmbeddingConfig::default();
        assert_eq!(config.model, "nomic-embed-text");
        assert_eq!(config.endpoint, "http://localhost:11434");
    }

    #[test]
    fn test_embedding_dimension_ollama() {
        assert_eq!(
            embedding_dimension(EmbeddingProviderType::Ollama, None),
            768
        );
        assert_eq!(
            embedding_dimension(EmbeddingProviderType::Ollama, Some("nomic-embed-text")),
            768
        );
        assert_eq!(
            embedding_dimension(EmbeddingProviderType::Ollama, Some("mxbai-embed-large")),
            1024
        );
        assert_eq!(
            embedding_dimension(EmbeddingProviderType::Ollama, Some("all-minilm")),
            384
        );
        assert_eq!(
            embedding_dimension(EmbeddingProviderType::Ollama, Some("unknown-model")),
            768
        );
        // Tagged model identifiers
        assert_eq!(
            embedding_dimension(
                EmbeddingProviderType::Ollama,
                Some("snowflake-arctic-embed:335m")
            ),
            1024
        );
        assert_eq!(
            embedding_dimension(
                EmbeddingProviderType::Ollama,
                Some("nomic-embed-text:latest")
            ),
            768
        );
    }
}
