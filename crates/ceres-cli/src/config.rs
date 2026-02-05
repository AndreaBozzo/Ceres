use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;
use std::sync::LazyLock;

static VERSION_INFO: LazyLock<String> = LazyLock::new(|| {
    let version = env!("CARGO_PKG_VERSION");

    // Use VERGEN_GIT_SHA for the commit hash (with safe slicing)
    let commit = option_env!("VERGEN_GIT_SHA")
        .map(|s| s.chars().take(7).collect::<String>())
        .unwrap_or_else(|| "unknown".to_string());

    let built = option_env!("VERGEN_BUILD_DATE").unwrap_or("unknown"); // YYYY-MM-DD
    let target = option_env!("VERGEN_CARGO_TARGET_TRIPLE").unwrap_or("unknown");
    let rustc = option_env!("VERGEN_RUSTC_SEMVER").unwrap_or("unknown");

    format!("{version}\ncommit: {commit}\nbuilt: {built}\ntarget: {target}\nrustc: {rustc}")
});

pub fn version_info() -> &'static str {
    &VERSION_INFO
}

/// CLI configuration parsed from command line arguments and environment variables
#[derive(Parser, Debug)]
#[command(name = "ceres")]
#[command(
    author,
    version = version_info(),
    about = "Semantic search engine for open data portals"
)]
#[command(after_help = "Examples:
  ceres harvest https://dati.comune.milano.it
  ceres search \"air quality monitoring\" --limit 5
  ceres export --format jsonl > datasets.jsonl
  ceres stats

Embedding providers:
  EMBEDDING_PROVIDER=gemini (default) - Google Gemini (768 dimensions)
  EMBEDDING_PROVIDER=openai           - OpenAI (1536 or 3072 dimensions)")]
pub struct Config {
    /// PostgreSQL database connection URL
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// Embedding provider to use: gemini (default) or openai
    #[arg(long, env = "EMBEDDING_PROVIDER", default_value = "gemini")]
    pub embedding_provider: String,

    /// Google Gemini API key (required when embedding_provider=gemini)
    #[arg(long, env = "GEMINI_API_KEY")]
    pub gemini_api_key: Option<String>,

    /// OpenAI API key (required when embedding_provider=openai)
    #[arg(long, env = "OPENAI_API_KEY")]
    pub openai_api_key: Option<String>,

    /// Embedding model name (provider-specific, uses default if not set)
    #[arg(long, env = "EMBEDDING_MODEL")]
    pub embedding_model: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

/// Available CLI commands
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Harvest datasets from CKAN portals
    #[command(after_help = "Examples:
  ceres harvest                               # Harvest all enabled portals from config
  ceres harvest https://dati.comune.milano.it # Harvest single URL (backward compatible)
  ceres harvest --portal milano               # Harvest portal by name from config
  ceres harvest --config ~/custom.toml        # Use custom config file
  ceres harvest --full-sync                   # Force full sync even if incremental is available")]
    Harvest {
        /// URL of a single CKAN portal to harvest (backward compatible)
        #[arg(value_name = "URL")]
        portal_url: Option<String>,

        /// Harvest a specific portal by name from config file
        #[arg(short, long, value_name = "NAME", conflicts_with = "portal_url")]
        portal: Option<String>,

        /// Custom path to portals.toml configuration file
        #[arg(short, long, value_name = "PATH")]
        config: Option<PathBuf>,

        /// Force full sync even if incremental sync is available
        #[arg(long)]
        full_sync: bool,
    },
    /// Search indexed datasets using semantic similarity
    #[command(after_help = "Example: ceres search \"trasporto pubblico\" --limit 10")]
    Search {
        /// Search query text
        query: String,
        /// Maximum number of results to return
        #[arg(short, long, default_value = "10")]
        limit: usize,
    },
    /// Export indexed datasets to various formats
    #[command(after_help = "Examples:
  ceres export --format jsonl > datasets.jsonl
  ceres export --format json --portal https://dati.gov.it")]
    Export {
        /// Output format for exported data
        #[arg(short, long, default_value = "jsonl")]
        format: ExportFormat,
        /// Filter by source portal URL
        #[arg(short, long)]
        portal: Option<String>,
        /// Maximum number of datasets to export
        #[arg(short, long)]
        limit: Option<usize>,
    },
    /// Show database statistics
    Stats,
}

/// Supported export formats
#[derive(Debug, Clone, ValueEnum)]
pub enum ExportFormat {
    /// JSON Lines format (one JSON object per line)
    Jsonl,
    /// Standard JSON array format
    Json,
    /// CSV format (comma-separated values)
    Csv,
}

#[cfg(test)]
mod tests {
    use super::version_info;

    #[test]
    fn test_version_info_contains_expected_fields() {
        let info = version_info();
        assert!(info.contains("commit:"));
        assert!(info.contains("built:"));
        assert!(info.contains("target:"));
        assert!(info.contains("rustc:"));
    }
}
