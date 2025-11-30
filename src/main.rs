use anyhow::Context;
use clap::Parser;
use dotenvy::dotenv;
use futures::stream::{self, StreamExt};
use pgvector::Vector;
use sqlx::postgres::PgPoolOptions;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use ceres::clients::ckan::CkanClient;
use ceres::clients::openai::OpenAIClient;
use ceres::config::{Command, Config};
use ceres::storage::DatasetRepository;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from .env file
    dotenv().ok();

    // Setup Logging (could be improved with more configuration options)
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // Parse command line arguments (as above, could be improved)
    let config = Config::parse();

    // Db connection
    info!("Connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .context("Failed to connect to database")?;

    // Services
    let repo = DatasetRepository::new(pool);
    let openai_client = OpenAIClient::new(&config.openai_api_key);

    // Commands
    match config.command {
        Command::Harvest { portal_url } => {
            info!("Starting harvest for: {}", portal_url);

            // 1. Inizializza il client CKAN
            let ckan = CkanClient::new(&portal_url).context("Invalid CKAN portal URL")?;

            // 2. Scarica la lista degli ID (molto veloce)
            info!("Fetching package list...");
            let ids = ckan.list_package_ids().await?;
            info!(
                "Found {} datasets. Starting concurrent processing...",
                ids.len()
            );

            // 3. Process datasets concurrently (10 at a time)
            let total = ids.len();
            let results: Vec<_> = stream::iter(ids.into_iter().enumerate())
                .map(|(i, id)| {
                    let ckan = ckan.clone();
                    let openai = openai_client.clone();
                    let repo = repo.clone();
                    let portal_url = portal_url.clone();

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
                                    error!("[{}/{}] Failed to generate embedding for {}: {}", i + 1, total, id, e);
                                }
                            }
                        }

                        // Upsert to database
                        match repo.upsert(&new_dataset).await {
                            Ok(uuid) => {
                                info!(
                                    "[{}/{}] ✓ Indexed: {} ({})",
                                    i + 1, total, new_dataset.title, uuid
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
                .buffer_unordered(10) // Process 10 datasets concurrently
                .collect()
                .await;

            // Summary
            let successful = results.iter().filter(|r| r.is_ok()).count();
            let failed = results.iter().filter(|r| r.is_err()).count();
            info!(
                "Harvesting complete: {} successful, {} failed out of {} total",
                successful, failed, total
            );
        }
        Command::Search { query, limit } => {
            info!("Searching for: '{}' (limit: {})", query, limit);

            // Query conversion to embeddings
            let vector = openai_client.get_embeddings(&query).await?;

            // TODO: Search implementation in repository
            // let results = repo.search(vector, limit).await?;
            // println!("Found {} results.", results.len());

            println!(
                "Vector generated successfully (len: {}). Search implementation pending in repository.",
                vector.len()
            );
        }
    }

    Ok(())
}
