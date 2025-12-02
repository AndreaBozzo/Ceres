use anyhow::Context;
use clap::Parser;
use dotenvy::dotenv;
use futures::stream::{self, StreamExt};
use pgvector::Vector;
use sqlx::postgres::PgPoolOptions;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

use ceres_cli::{Command, Config, ExportFormat};
use ceres_client::{CkanClient, OpenAIClient};
use ceres_core::Dataset;
use ceres_db::DatasetRepository;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load environment variables from .env file
    dotenv().ok();

    // Setup logging (stderr to keep stdout clean for exports)
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_writer(std::io::stderr)
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
        Command::Export {
            format,
            portal,
            limit,
        } => {
            export(&repo, format, portal.as_deref(), limit).await?;
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
        println!("\n🔍 No results found for: \"{}\"\n", query);
        println!("Try:");
        println!("  • Using different keywords");
        println!("  • Searching in a different language");
        println!("  • Harvesting more portals with: ceres harvest <url>");
    } else {
        println!("\n🔍 Search Results for: \"{}\"\n", query);
        println!("Found {} matching datasets:\n", results.len());

        for (i, result) in results.iter().enumerate() {
            // Similarity indicator
            let similarity_bar = create_similarity_bar(result.similarity_score);

            println!(
                "{}. {} [{:.0}%] {}",
                i + 1,
                similarity_bar,
                result.similarity_score * 100.0,
                result.dataset.title
            );
            println!("   📍 {}", result.dataset.source_portal);
            println!("   🔗 {}", result.dataset.url);

            if let Some(desc) = &result.dataset.description {
                let truncated = truncate_text(desc, 120);
                println!("   📝 {}", truncated);
            }
            println!();
        }
    }

    Ok(())
}

/// Create a visual similarity bar
fn create_similarity_bar(score: f32) -> String {
    let filled = (score * 10.0).round() as usize;
    let empty = 10 - filled;
    format!("[{}{}]", "█".repeat(filled), "░".repeat(empty))
}

/// Truncate text to a maximum length, adding ellipsis if needed
fn truncate_text(text: &str, max_len: usize) -> String {
    // Clean up whitespace and newlines
    let cleaned: String = text
        .chars()
        .map(|c| if c.is_whitespace() { ' ' } else { c })
        .collect();
    let cleaned = cleaned.split_whitespace().collect::<Vec<_>>().join(" ");

    if cleaned.len() <= max_len {
        cleaned
    } else {
        format!("{}...", &cleaned[..max_len])
    }
}

/// Show database statistics
async fn show_stats(repo: &DatasetRepository) -> anyhow::Result<()> {
    let stats = repo.get_stats().await?;

    println!("\n📊 Database Statistics\n");
    println!("  Total datasets:        {}", stats.total_datasets);
    println!(
        "  With embeddings:       {}",
        stats.datasets_with_embeddings
    );
    println!("  Unique portals:        {}", stats.total_portals);
    if let Some(last_update) = stats.last_update {
        println!("  Last update:           {}", last_update);
    }
    println!();

    Ok(())
}

/// Export datasets to various formats
async fn export(
    repo: &DatasetRepository,
    format: ExportFormat,
    portal_filter: Option<&str>,
    limit: Option<usize>,
) -> anyhow::Result<()> {
    info!("Exporting datasets...");

    let datasets = repo.list_all(portal_filter, limit).await?;

    if datasets.is_empty() {
        eprintln!("No datasets found to export.");
        return Ok(());
    }

    info!("Found {} datasets to export", datasets.len());

    match format {
        ExportFormat::Jsonl => {
            export_jsonl(&datasets)?;
        }
        ExportFormat::Json => {
            export_json(&datasets)?;
        }
        ExportFormat::Csv => {
            export_csv(&datasets)?;
        }
    }

    info!("Export complete: {} datasets", datasets.len());
    Ok(())
}

/// Export datasets in JSON Lines format (one JSON object per line)
fn export_jsonl(datasets: &[Dataset]) -> anyhow::Result<()> {
    for dataset in datasets {
        let export_record = create_export_record(dataset);
        let json = serde_json::to_string(&export_record)?;
        println!("{}", json);
    }
    Ok(())
}

/// Export datasets as a JSON array
fn export_json(datasets: &[Dataset]) -> anyhow::Result<()> {
    let export_records: Vec<_> = datasets.iter().map(create_export_record).collect();
    let json = serde_json::to_string_pretty(&export_records)?;
    println!("{}", json);
    Ok(())
}

/// Export datasets in CSV format
fn export_csv(datasets: &[Dataset]) -> anyhow::Result<()> {
    // Print CSV header
    println!("id,original_id,source_portal,url,title,description,first_seen_at,last_updated_at");

    for dataset in datasets {
        // Escape and quote CSV fields properly
        let description = dataset
            .description
            .as_ref()
            .map(|d| escape_csv(d))
            .unwrap_or_default();

        println!(
            "{},{},{},{},{},{},{},{}",
            dataset.id,
            escape_csv(&dataset.original_id),
            escape_csv(&dataset.source_portal),
            escape_csv(&dataset.url),
            escape_csv(&dataset.title),
            description,
            dataset.first_seen_at.format("%Y-%m-%dT%H:%M:%SZ"),
            dataset.last_updated_at.format("%Y-%m-%dT%H:%M:%SZ"),
        );
    }
    Ok(())
}

/// Create an export record without the embedding (too large for export)
fn create_export_record(dataset: &Dataset) -> serde_json::Value {
    serde_json::json!({
        "id": dataset.id,
        "original_id": dataset.original_id,
        "source_portal": dataset.source_portal,
        "url": dataset.url,
        "title": dataset.title,
        "description": dataset.description,
        "metadata": dataset.metadata,
        "first_seen_at": dataset.first_seen_at,
        "last_updated_at": dataset.last_updated_at
    })
}

/// Escape a string for CSV output
fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
