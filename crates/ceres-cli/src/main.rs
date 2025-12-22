use anyhow::Context;
use clap::Parser;
use dotenvy::dotenv;
use futures::stream::{self, StreamExt};
use pgvector::Vector;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

use ceres_cli::{Command, Config, ExportFormat};
use ceres_client::{CkanClient, GeminiClient};
use ceres_core::Dataset;
use ceres_db::DatasetRepository;

/// Statistics for delta harvesting
struct HarvestStats {
    /// Datasets skipped (content unchanged)
    skipped: AtomicUsize,
    /// Datasets with new/updated embeddings
    regenerated: AtomicUsize,
    /// New datasets (first time indexed)
    new: AtomicUsize,
    /// Failed operations
    failed: AtomicUsize,
}

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
    let gemini_client = GeminiClient::new(&config.gemini_api_key);

    // Execute command
    match config.command {
        Command::Harvest { portal_url } => {
            harvest(&repo, &gemini_client, &portal_url).await?;
        }
        Command::Search { query, limit } => {
            search(&repo, &gemini_client, &query, limit).await?;
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

/// Harvest datasets from a CKAN portal with delta detection
async fn harvest(
    repo: &DatasetRepository,
    gemini_client: &GeminiClient,
    portal_url: &str,
) -> anyhow::Result<()> {
    info!("Starting delta harvest for: {}", portal_url);

    // Initialize CKAN client
    let ckan = CkanClient::new(portal_url).context("Invalid CKAN portal URL")?;

    // PHASE 1: Fetch existing hashes from database
    info!("Loading existing content hashes from database...");
    let existing_hashes = repo.get_hashes_for_portal(portal_url).await?;
    info!(
        "Found {} existing datasets in database",
        existing_hashes.len()
    );

    // PHASE 2: Fetch package list from portal
    info!("Fetching package list from portal...");
    let ids = ckan.list_package_ids().await?;
    let total = ids.len();
    info!(
        "Found {} datasets on portal. Starting concurrent processing...",
        total
    );

    // Initialize stats
    let stats = Arc::new(HarvestStats {
        skipped: AtomicUsize::new(0),
        regenerated: AtomicUsize::new(0),
        new: AtomicUsize::new(0),
        failed: AtomicUsize::new(0),
    });

    // PHASE 3: Process datasets concurrently (10 at a time)
    let results: Vec<_> = stream::iter(ids.into_iter().enumerate())
        .map(|(i, id)| {
            let ckan = ckan.clone();
            let gemini = gemini_client.clone();
            let repo = repo.clone();
            let portal_url = portal_url.to_string();
            let existing_hashes = existing_hashes.clone();
            let stats = Arc::clone(&stats);

            async move {
                // Fetch dataset details from CKAN
                let ckan_data = match ckan.show_package(&id).await {
                    Ok(data) => data,
                    Err(e) => {
                        error!("[{}/{}] Failed to fetch {}: {}", i + 1, total, id, e);
                        stats.failed.fetch_add(1, Ordering::Relaxed);
                        return Err(e);
                    }
                };

                // Convert to internal model (computes content_hash)
                let mut new_dataset = CkanClient::into_new_dataset(ckan_data, &portal_url);
                let new_hash = &new_dataset.content_hash;

                // DELTA DETECTION: Check if embedding regeneration is needed
                let needs_embedding = match existing_hashes.get(&new_dataset.original_id) {
                    // Dataset exists in DB
                    Some(existing_hash) => {
                        match existing_hash {
                            // Hash exists and matches -> skip embedding
                            Some(hash) if hash == new_hash => {
                                info!(
                                    "[{}/{}] ~ Skipped (unchanged): {}",
                                    i + 1,
                                    total,
                                    new_dataset.title
                                );
                                stats.skipped.fetch_add(1, Ordering::Relaxed);

                                // Just update the timestamp
                                if let Err(e) = repo
                                    .update_timestamp_only(&portal_url, &new_dataset.original_id)
                                    .await
                                {
                                    error!(
                                        "[{}/{}] Failed to update timestamp: {}",
                                        i + 1,
                                        total,
                                        e
                                    );
                                }
                                return Ok(());
                            }
                            // Hash differs -> content changed, regenerate
                            Some(_) => {
                                info!(
                                    "[{}/{}] * Content changed, regenerating: {}",
                                    i + 1,
                                    total,
                                    new_dataset.title
                                );
                                true
                            }
                            // Hash is NULL (legacy record) -> regenerate
                            None => {
                                info!(
                                    "[{}/{}] + Legacy record, generating hash: {}",
                                    i + 1,
                                    total,
                                    new_dataset.title
                                );
                                true
                            }
                        }
                    }
                    // Dataset not in DB -> new dataset
                    None => {
                        stats.new.fetch_add(1, Ordering::Relaxed);
                        info!("[{}/{}] + New dataset: {}", i + 1, total, new_dataset.title);
                        true
                    }
                };

                // Generate embedding only if needed
                if needs_embedding {
                    let combined_text = format!(
                        "{} {}",
                        new_dataset.title,
                        new_dataset.description.as_deref().unwrap_or_default()
                    );

                    if !combined_text.trim().is_empty() {
                        match gemini.get_embeddings(&combined_text).await {
                            Ok(emb) => {
                                new_dataset.embedding = Some(Vector::from(emb));
                                stats.regenerated.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(e) => {
                                error!(
                                    "[{}/{}] Failed to generate embedding for {}: {}",
                                    i + 1,
                                    total,
                                    id,
                                    e
                                );
                                stats.failed.fetch_add(1, Ordering::Relaxed);
                                // Continue anyway - save dataset without embedding
                            }
                        }
                    }
                }

                // Upsert to database (with content_hash)
                match repo.upsert(&new_dataset).await {
                    Ok(uuid) => {
                        if needs_embedding {
                            info!(
                                "[{}/{}] ✓ Indexed: {} ({})",
                                i + 1,
                                total,
                                new_dataset.title,
                                uuid
                            );
                        }
                        Ok(())
                    }
                    Err(e) => {
                        error!("[{}/{}] Failed to save {}: {}", i + 1, total, id, e);
                        stats.failed.fetch_add(1, Ordering::Relaxed);
                        Err(e)
                    }
                }
            }
        })
        .buffer_unordered(10)
        .collect()
        .await;

    // PHASE 4: Print summary with delta statistics
    let successful = results.iter().filter(|r| r.is_ok()).count();
    let failed = stats.failed.load(Ordering::Relaxed);
    let skipped = stats.skipped.load(Ordering::Relaxed);
    let regenerated = stats.regenerated.load(Ordering::Relaxed);
    let new_count = stats.new.load(Ordering::Relaxed);

    info!("═══════════════════════════════════════════════════════");
    info!("Delta Harvesting Complete for: {}", portal_url);
    info!("═══════════════════════════════════════════════════════");
    info!("  Total datasets on portal:    {}", total);
    info!("  Previously in database:      {}", existing_hashes.len());
    info!("───────────────────────────────────────────────────────");
    info!(
        "  ~ Skipped (unchanged):       {} ({:.1}%)",
        skipped,
        if total > 0 {
            (skipped as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    );
    info!("  * Regenerated (changed):     {}", regenerated);
    info!("  + New datasets:              {}", new_count);
    info!("  ✗ Failed:                    {}", failed);
    info!("───────────────────────────────────────────────────────");
    info!("  API calls saved:             {} (vs full sync)", skipped);
    info!("═══════════════════════════════════════════════════════");

    if successful == total {
        info!("All datasets processed successfully!");
    }

    Ok(())
}

/// Search for datasets using semantic similarity
async fn search(
    repo: &DatasetRepository,
    gemini_client: &GeminiClient,
    query: &str,
    limit: usize,
) -> anyhow::Result<()> {
    info!("Searching for: '{}' (limit: {})", query, limit);

    // Generate query embedding
    let vector = gemini_client.get_embeddings(query).await?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_similarity_bar_full() {
        let bar = create_similarity_bar(1.0);
        assert_eq!(bar, "[██████████]");
    }

    #[test]
    fn test_create_similarity_bar_half() {
        let bar = create_similarity_bar(0.5);
        assert_eq!(bar, "[█████░░░░░]");
    }

    #[test]
    fn test_create_similarity_bar_empty() {
        let bar = create_similarity_bar(0.0);
        assert_eq!(bar, "[░░░░░░░░░░]");
    }

    #[test]
    fn test_truncate_text_short() {
        let text = "Short text";
        let result = truncate_text(text, 50);
        assert_eq!(result, "Short text");
    }

    #[test]
    fn test_truncate_text_long() {
        let text = "This is a very long text that should be truncated";
        let result = truncate_text(text, 20);
        assert_eq!(result, "This is a very long ...");
    }

    #[test]
    fn test_truncate_text_with_newlines() {
        let text = "Line 1\nLine 2\nLine 3";
        let result = truncate_text(text, 50);
        assert_eq!(result, "Line 1 Line 2 Line 3");
    }

    #[test]
    fn test_escape_csv_simple() {
        assert_eq!(escape_csv("simple"), "simple");
    }

    #[test]
    fn test_escape_csv_with_comma() {
        assert_eq!(escape_csv("hello, world"), "\"hello, world\"");
    }

    #[test]
    fn test_escape_csv_with_quotes() {
        assert_eq!(escape_csv("say \"hello\""), "\"say \"\"hello\"\"\"");
    }

    #[test]
    fn test_escape_csv_with_newline() {
        assert_eq!(escape_csv("line1\nline2"), "\"line1\nline2\"");
    }
}
