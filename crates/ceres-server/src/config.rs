use clap::Parser;
use std::path::PathBuf;

/// Server configuration parsed from command line arguments and environment variables
#[derive(Parser, Debug)]
#[command(name = "ceres-server")]
#[command(author, version, about = "REST API server for Ceres semantic search")]
pub struct ServerConfig {
    /// PostgreSQL database connection URL
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// Google Gemini API key for generating embeddings
    #[arg(long, env = "GEMINI_API_KEY")]
    pub gemini_api_key: String,

    /// Server port to listen on
    #[arg(short, long, env = "PORT", default_value = "3000")]
    pub port: u16,

    /// Server host to bind to
    #[arg(long, env = "HOST", default_value = "0.0.0.0")]
    pub host: String,

    /// Path to portals.toml configuration file
    #[arg(long, env = "PORTALS_CONFIG")]
    pub portals_config: Option<PathBuf>,

    /// Allowed CORS origins (comma-separated). Use "*" for any origin (dev only).
    /// Example: "https://example.com,https://app.example.com"
    #[arg(long, env = "CORS_ALLOWED_ORIGINS", default_value = "*")]
    pub cors_origins: String,

    /// Rate limit: maximum requests per second per IP
    #[arg(long, env = "RATE_LIMIT_RPS", default_value = "10")]
    pub rate_limit_rps: u32,

    /// Rate limit: burst size (max requests allowed in a burst)
    #[arg(long, env = "RATE_LIMIT_BURST", default_value = "30")]
    pub rate_limit_burst: u32,
}
