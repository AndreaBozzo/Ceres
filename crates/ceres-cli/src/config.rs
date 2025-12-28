use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

/// CLI configuration parsed from command line arguments and environment variables
#[derive(Parser, Debug)]
#[command(name = "ceres")]
#[command(
    author,
    version,
    about = "Semantic search engine for open data portals"
)]
#[command(after_help = "Examples:
  ceres harvest https://dati.comune.milano.it
  ceres search \"air quality monitoring\" --limit 5
  ceres export --format jsonl > datasets.jsonl
  ceres stats")]
pub struct Config {
    /// PostgreSQL database connection URL
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// Google Gemini API key for generating embeddings
    #[arg(long, env = "GEMINI_API_KEY")]
    pub gemini_api_key: String,

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
  ceres harvest --config ~/custom.toml        # Use custom config file")]
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
