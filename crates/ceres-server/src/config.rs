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
}
