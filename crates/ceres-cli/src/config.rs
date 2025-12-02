use clap::{Parser, Subcommand};

/// CLI configuration parsed from command line arguments and environment variables
#[derive(Parser, Debug)]
#[command(name = "ceres")]
#[command(author, version, about = "Semantic search engine for open data portals")]
pub struct Config {
    /// PostgreSQL database connection URL
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// OpenAI API key for generating embeddings
    #[arg(long, env = "OPENAI_API_KEY")]
    pub openai_api_key: String,

    #[command(subcommand)]
    pub command: Command,
}

/// Available CLI commands
#[derive(Subcommand, Debug)]
pub enum Command {
    /// Harvest datasets from a CKAN portal
    Harvest {
        /// URL of the CKAN portal to harvest
        portal_url: String,
    },
    /// Search indexed datasets using semantic similarity
    Search {
        /// Search query text
        query: String,
        /// Maximum number of results to return
        #[arg(long, default_value = "10")]
        limit: usize,
    },
    /// Show database statistics
    Stats,
}
