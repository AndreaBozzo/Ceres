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
use ceres_core::{needs_reprocessing, Dataset, DbConfig, SyncConfig, SyncOutcome, SyncStats};
use ceres_db::DatasetRepository;

/// Thread-safe wrapper for SyncStats using atomic counters.
struct AtomicSyncStats {
    unchanged: AtomicUsize,
    updated: AtomicUsize,
    created: AtomicUsize,
    failed: AtomicUsize,
}

impl AtomicSyncStats {
    fn new() -> Self {
        Self {
            unchanged: AtomicUsize::new(0),
            updated: AtomicUsize::new(0),
            created: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
        }
    }

    fn record(&self, outcome: SyncOutcome) {
        match outcome {
            SyncOutcome::Unchanged => self.unchanged.fetch_add(1, Ordering::Relaxed),
            SyncOutcome::Updated => self.updated.fetch_add(1, Ordering::Relaxed),
            SyncOutcome::Created => self.created.fetch_add(1, Ordering::Relaxed),
            SyncOutcome::Failed => self.failed.fetch_add(1, Ordering::Relaxed),
        };
    }

    fn to_stats(&self) -> SyncStats {
        SyncStats {
            unchanged: self.unchanged.load(Ordering::Relaxed),
            updated: self.updated.load(Ordering::Relaxed),
            created: self.created.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let config = Config::parse();

    info!("Connecting to database...");
    let db_config = DbConfig::default();
    let pool = PgPoolOptions::new()
        .max_connections(db_config.max_connections)
        .connect(&config.database_url)
        .await
        .context("Failed to connect to database")?;

    let repo = DatasetRepository::new(pool);
    let gemini_client = GeminiClient::new(&config.gemini_api_key)
        .context("Failed to initialize embedding client")?;

    match config.command {
        Command::Harvest { portal_url } => {
            sync_portal(&repo, &gemini_client, &portal_url).await?;
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

async fn sync_portal(
    repo: &DatasetRepository,
    gemini_client: &GeminiClient,
    portal_url: &str,
) -> anyhow::Result<()> {
    info!("Syncing portal: {}", portal_url);

    let ckan = CkanClient::new(portal_url).context("Invalid CKAN portal URL")?;

    let existing_hashes = repo.get_hashes_for_portal(portal_url).await?;
    info!("Found {} existing datasets", existing_hashes.len());

    let ids = ckan.list_package_ids().await?;
    let total = ids.len();
    info!("Found {} datasets on portal", total);

    let stats = Arc::new(AtomicSyncStats::new());

    let _results: Vec<_> = stream::iter(ids.into_iter().enumerate())
        .map(|(i, id)| {
            let ckan = ckan.clone();
            let gemini = gemini_client.clone();
            let repo = repo.clone();
            let portal_url = portal_url.to_string();
            let existing_hashes = existing_hashes.clone();
            let stats = Arc::clone(&stats);

            async move {
                let ckan_data = match ckan.show_package(&id).await {
                    Ok(data) => data,
                    Err(e) => {
                        error!("[{}/{}] Failed to fetch {}: {}", i + 1, total, id, e);
                        stats.record(SyncOutcome::Failed);
                        return Err(e);
                    }
                };

                let mut new_dataset = CkanClient::into_new_dataset(ckan_data, &portal_url);
                let decision = needs_reprocessing(
                    existing_hashes.get(&new_dataset.original_id),
                    &new_dataset.content_hash,
                );

                match decision.outcome {
                    SyncOutcome::Unchanged => {
                        info!("[{}/{}] = Unchanged: {}", i + 1, total, new_dataset.title);
                        stats.record(SyncOutcome::Unchanged);

                        if let Err(e) = repo
                            .update_timestamp_only(&portal_url, &new_dataset.original_id)
                            .await
                        {
                            error!("[{}/{}] Failed to update timestamp: {}", i + 1, total, e);
                        }
                        return Ok(());
                    }
                    SyncOutcome::Updated => {
                        let label = if decision.is_legacy() {
                            "↑ Updated (legacy)"
                        } else {
                            "↑ Updated"
                        };
                        info!("[{}/{}] {}: {}", i + 1, total, label, new_dataset.title);
                    }
                    SyncOutcome::Created => {
                        info!("[{}/{}] + Created: {}", i + 1, total, new_dataset.title);
                    }
                    // TODO: Consider replacing unreachable! with explicit error handling
                    // if needs_reprocessing() is ever modified to return Failed
                    SyncOutcome::Failed => unreachable!("needs_reprocessing never returns Failed"),
                }

                if decision.needs_embedding {
                    let combined_text = format!(
                        "{} {}",
                        new_dataset.title,
                        new_dataset.description.as_deref().unwrap_or_default()
                    );

                    if !combined_text.trim().is_empty() {
                        match gemini.get_embeddings(&combined_text).await {
                            Ok(emb) => {
                                new_dataset.embedding = Some(Vector::from(emb));
                                stats.record(decision.outcome);
                            }
                            Err(e) => {
                                error!(
                                    "[{}/{}] Failed to generate embedding for {}: {}",
                                    i + 1,
                                    total,
                                    id,
                                    e
                                );
                                stats.record(SyncOutcome::Failed);
                            }
                        }
                    }
                }

                match repo.upsert(&new_dataset).await {
                    Ok(uuid) => {
                        if decision.needs_embedding {
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
                        stats.record(SyncOutcome::Failed);
                        Err(e)
                    }
                }
            }
        })
        .buffer_unordered(SyncConfig::default().concurrency)
        .collect()
        .await;

    let final_stats = stats.to_stats();

    info!("═══════════════════════════════════════════════════════");
    info!("Sync complete: {}", portal_url);
    info!("═══════════════════════════════════════════════════════");
    info!("  Total on portal:     {}", total);
    info!("  Previously indexed:  {}", existing_hashes.len());
    info!("───────────────────────────────────────────────────────");
    info!(
        "  = Unchanged:         {} ({:.1}%)",
        final_stats.unchanged,
        if total > 0 {
            (final_stats.unchanged as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    );
    info!("  ↑ Updated:           {}", final_stats.updated);
    info!("  + Created:           {}", final_stats.created);
    info!("  ✗ Failed:            {}", final_stats.failed);
    info!("═══════════════════════════════════════════════════════");

    if final_stats.successful() == total {
        info!("All datasets processed successfully!");
    }

    Ok(())
}

async fn search(
    repo: &DatasetRepository,
    gemini_client: &GeminiClient,
    query: &str,
    limit: usize,
) -> anyhow::Result<()> {
    info!("Searching for: '{}' (limit: {})", query, limit);

    let vector = gemini_client.get_embeddings(query).await?;
    let query_vector = Vector::from(vector);
    let results = repo.search(query_vector, limit).await?;

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

fn create_similarity_bar(score: f32) -> String {
    let filled = (score * 10.0).round() as usize;
    let empty = 10 - filled;
    format!("[{}{}]", "█".repeat(filled), "░".repeat(empty))
}

fn truncate_text(text: &str, max_len: usize) -> String {
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

fn export_jsonl(datasets: &[Dataset]) -> anyhow::Result<()> {
    for dataset in datasets {
        let export_record = create_export_record(dataset);
        let json = serde_json::to_string(&export_record)?;
        println!("{}", json);
    }
    Ok(())
}

fn export_json(datasets: &[Dataset]) -> anyhow::Result<()> {
    let export_records: Vec<_> = datasets.iter().map(create_export_record).collect();
    let json = serde_json::to_string_pretty(&export_records)?;
    println!("{}", json);
    Ok(())
}

fn export_csv(datasets: &[Dataset]) -> anyhow::Result<()> {
    println!("id,original_id,source_portal,url,title,description,first_seen_at,last_updated_at");

    for dataset in datasets {
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

    #[test]
    fn test_atomic_sync_stats_new() {
        let stats = AtomicSyncStats::new();
        let result = stats.to_stats();
        assert_eq!(result.unchanged, 0);
        assert_eq!(result.updated, 0);
        assert_eq!(result.created, 0);
        assert_eq!(result.failed, 0);
    }

    #[test]
    fn test_atomic_sync_stats_record() {
        let stats = AtomicSyncStats::new();
        stats.record(SyncOutcome::Unchanged);
        stats.record(SyncOutcome::Updated);
        stats.record(SyncOutcome::Created);
        stats.record(SyncOutcome::Failed);

        let result = stats.to_stats();
        assert_eq!(result.unchanged, 1);
        assert_eq!(result.updated, 1);
        assert_eq!(result.created, 1);
        assert_eq!(result.failed, 1);
    }

    #[test]
    fn test_atomic_sync_stats_multiple_records() {
        let stats = AtomicSyncStats::new();
        for _ in 0..10 {
            stats.record(SyncOutcome::Unchanged);
        }
        for _ in 0..5 {
            stats.record(SyncOutcome::Updated);
        }

        let result = stats.to_stats();
        assert_eq!(result.unchanged, 10);
        assert_eq!(result.updated, 5);
        assert_eq!(result.total(), 15);
        assert_eq!(result.successful(), 15);
    }
}
