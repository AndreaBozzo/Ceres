use clap::Parser;
use dotenvy::dotenv;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use ceres::clients::ckan::CkanClient;
use ceres::clients::openai::OpenAIClient;
use ceres::config::{Command, Config};
use ceres::storage::DatasetRepository;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// Load environment variables from .env file
    dotenv().ok();

    /// Setup Logging (could be improved with more configuration options)
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    /// Parse command line arguments (as above, could be improved)
    let config = Config::parse();

    /// Db connection
    info!("Connecting to database...");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&config.database_url)
        .await
        .expect("Failed to connect to database");

    /// Services
    let repo = DatasetRepository::new(pool);
    let openai_client = OpenAIClient::new(&config.openai_api_key);

    /// Commands
    match config.command {
        Command::Harvest { portal_url } => {
            info!("Starting harvest for: {}", portal_url);

            // 1. Inizializza il client CKAN
            let ckan = CkanClient::new(&portal_url).expect("Invalid CKAN configuration");

            // 2. Scarica la lista degli ID (molto veloce)
            info!("Fetching package list...");
            let ids = ckan.list_package_ids().await?;
            info!("Found {} datasets. Starting processing...", ids.len());

            // 3. Itera (in un caso reale useresti buffer_unordered per parallelizzare)
            for (i, id) in ids.iter().enumerate() {
                // Logica semplice sequenziale per ora
                match ckan.show_package(id).await {
                    Ok(ckan_data) => {
                        // Conversione
                        let mut new_dataset = CkanClient::into_new_dataset(ckan_data, &portal_url);

                        // TODO: Qui è dove chiameresti OpenAI per l'embedding
                        // if let Ok(emb) = openai_client.get_embeddings(&new_dataset.title).await {
                        //    new_dataset.embedding = Some(emb);
                        // }

                        // Upsert nel DB
                        match repo.upsert(&new_dataset).await {
                            Ok(_) => {
                                info!("[{}/{}] Indexed: {}", i + 1, ids.len(), new_dataset.title)
                            }
                            Err(e) => error!("Failed to save {}: {}", id, e),
                        }
                    }
                    Err(e) => error!("Failed to fetch details for {}: {}", id, e),
                }

                // Piccolo sleep per essere gentili con il server (opzionale ma consigliato)
                // tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
        Command::Search { query, limit } => {
            info!("Searching for: '{}' (limit: {})", query, limit);

            /// Query conversion to embeddings
            let vector = openai_client.get_embeddings(&query).await?;

            /// optional but search implementation in repository
            /// let results = repo.search(vector, limit).await?;
            /// println!("Found {} results.", results.len());

            println!(
                "Vector generated successfully (len: {}). Search implementation pending in repository.",
                vector.len()
            );
        }
    }

    Ok(())
}
