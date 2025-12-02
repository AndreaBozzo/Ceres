use anyhow::Context;
use clap::Parser;
use dotenvy::dotenv;
use futures::stream::{self, StreamExt};
use pgvector::Vector;
use sqlx::postgres::PgPoolOptions;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use ceres_cli::{Command, Config};
use ceres_client::{CkanClient, OpenAIClient};
use ceres_db::DatasetRepository;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from .env file
    dotenv().ok();

    // Setup logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // Parse command line arguments
    let config = Config::parse();

    // Database connection
    info!("Connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .context("Failed to connect to database")?;

    // Initialize services
    let repo = DatasetRepository::new(pool);
    let openai_client = OpenAIClient::new(&config.openai_api_key);

    // Execute command
    match config.command {
        Command::Harvest { portal_url } => {
            harvest(&repo, &openai_client, &portal_url).await?;
        }
        Command::Search { query, limit } => {
            search(&repo, &openai_client, &query, limit).await?;
        }
        Command::Stats => {
            show_stats(&repo).await?;
        }
    }

    Ok(())
}

/// Harvest datasets from a CKAN portal
async fn harvest(
    repo: &DatasetRepository,
    openai_client: &OpenAIClient,
    portal_url: &str,
) -> anyhow::Result<()> {
    info!("Starting harvest for: {}", portal_url);

    // Initialize CKAN client
    let ckan = CkanClient::new(portal_url).context("Invalid CKAN portal URL")?;

    // Fetch package list
    info!("Fetching package list...");
    let ids = ckan.list_package_ids().await?;
    info!(
        "Found {} datasets. Starting concurrent processing...",
        ids.len()
    );

    // Process datasets concurrently (10 at a time)
    let total = ids.len();
    let results: Vec<_> = stream::iter(ids.into_iter().enumerate())
        .map(|(i, id)| {
            let ckan = ckan.clone();
            let openai = openai_client.clone();
            let repo = repo.clone();
            let portal_url = portal_url.to_string();

            async move {
                // Fetch dataset details
                let ckan_data = match ckan.show_package(&id).await {
                    Ok(data) => data,
                    Err(e) => {
                        error!("[{}/{}] Failed to fetch {}: {}", i + 1, total, id, e);
                        return Err(e);
                    }
                };

                // Convert to internal model
                let mut new_dataset = CkanClient::into_new_dataset(ckan_data, &portal_url);

                // Generate embedding from title and description
                let combined_text = format!(
                    "{} {}",
                    new_dataset.title,
                    new_dataset.description.as_deref().unwrap_or_default()
                );

                if !combined_text.trim().is_empty() {
                    match openai.get_embeddings(&combined_text).await {
                        Ok(emb) => {
                            new_dataset.embedding = Some(Vector::from(emb));
                        }
                        Err(e) => {
                            error!(
                                "[{}/{}] Failed to generate embedding for {}: {}",
                                i + 1,
                                total,
                                id,
                                e
                            );
                        }
                    }
                }

                // Upsert to database
                match repo.upsert(&new_dataset).await {
                    Ok(uuid) => {
                        info!(
                            "[{}/{}] ✓ Indexed: {} ({})",
                            i + 1,
                            total,
                            new_dataset.title,
                            uuid
                        );
                        Ok(())
                    }
                    Err(e) => {
                        error!("[{}/{}] Failed to save {}: {}", i + 1, total, id, e);
                        Err(e)
                    }
                }
            }
        })
        .buffer_unordered(10)
        .collect()
        .await;

    // Summary
    let successful = results.iter().filter(|r| r.is_ok()).count();
    let failed = results.iter().filter(|r| r.is_err()).count();
    info!(
        "Harvesting complete: {} successful, {} failed out of {} total",
        successful, failed, total
    );

    Ok(())
}

/// Search for datasets using semantic similarity
async fn search(
    repo: &DatasetRepository,
    openai_client: &OpenAIClient,
    query: &str,
    limit: usize,
) -> anyhow::Result<()> {
    info!("Searching for: '{}' (limit: {})", query, limit);

    // Generate query embedding
    let vector = openai_client.get_embeddings(query).await?;
    let query_vector = Vector::from(vector);

    // Search in repository
    let results = repo.search(query_vector, limit).await?;

    // Output results
    if results.is_empty() {
        println!("No results found.");
    } else {
        println!("\nFound {} results:\n", results.len());
        for (i, result) in results.iter().enumerate() {
            println!(
                "{}. [{:.2}] {} - {}",
                i + 1,
                result.similarity_score,
                result.dataset.title,
                result.dataset.source_portal
            );
            if let Some(desc) = &result.dataset.description {
                let truncated = if desc.len() > 100 {
                    format!("{}...", &desc[..100])
                } else {
                    desc.clone()
                };
                println!("   {}", truncated);
            }
            println!();
        }
    }

    Ok(())
}

/// Show database statistics
async fn show_stats(repo: &DatasetRepository) -> anyhow::Result<()> {
    let stats = repo.get_stats().await?;

    println!("\n📊 Database Statistics\n");
    println!("  Total datasets:        {}", stats.total_datasets);
    println!("  With embeddings:       {}", stats.datasets_with_embeddings);
    println!("  Unique portals:        {}", stats.total_portals);
    if let Some(last_update) = stats.last_update {
        println!("  Last update:           {}", last_update);
    }
    println!();

    Ok(())
}
