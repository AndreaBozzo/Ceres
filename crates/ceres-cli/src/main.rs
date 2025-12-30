use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use dotenvy::dotenv;
use sqlx::postgres::PgPoolOptions;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use ceres_client::{CkanClientFactory, GeminiClient};
use ceres_core::{
    BatchHarvestSummary, Dataset, DbConfig, HarvestService, PortalEntry, SearchService, SyncStats,
    TracingReporter, load_portals_config,
};
use ceres_db::DatasetRepository;
use ceres_search::{Command, Config, ExportFormat};

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

    // Create services with concrete implementations (dependency injection)
    let ckan_factory = CkanClientFactory::new();
    let harvest_service = HarvestService::new(repo.clone(), gemini_client.clone(), ckan_factory);
    let search_service = SearchService::new(repo.clone(), gemini_client);

    match config.command {
        Command::Harvest {
            portal_url,
            portal,
            config: config_path,
        } => {
            handle_harvest(&harvest_service, portal_url, portal, config_path).await?;
        }
        Command::Search { query, limit } => {
            search(&search_service, &query, limit).await?;
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

/// Handle the harvest command with its three modes:
/// 1. Direct URL (backward compatible)
/// 2. Named portal from config
/// 3. Batch mode (all enabled portals)
async fn handle_harvest(
    harvest_service: &HarvestService<DatasetRepository, GeminiClient, CkanClientFactory>,
    portal_url: Option<String>,
    portal_name: Option<String>,
    config_path: Option<PathBuf>,
) -> anyhow::Result<()> {
    let reporter = TracingReporter;

    match (portal_url, portal_name) {
        // Mode 1: Direct URL (backward compatible)
        (Some(url), None) => {
            info!("Syncing portal: {}", url);
            let stats = harvest_service
                .sync_portal_with_progress(&url, &reporter)
                .await?;
            print_single_portal_summary(&url, &stats);
        }

        // Mode 2: Named portal from config
        (None, Some(name)) => {
            let portals_config = load_portals_config(config_path)?
                .ok_or_else(|| anyhow::anyhow!(
                    "No configuration file found. Create ~/.config/ceres/portals.toml or use --config"
                ))?;

            let portal = portals_config
                .find_by_name(&name)
                .ok_or_else(|| anyhow::anyhow!("Portal '{}' not found in configuration", name))?;

            if !portal.enabled {
                info!(
                    "Note: Portal '{}' is marked as disabled in configuration",
                    name
                );
            }

            info!("Syncing portal: {}", portal.url);
            let stats = harvest_service
                .sync_portal_with_progress(&portal.url, &reporter)
                .await?;
            print_single_portal_summary(&portal.url, &stats);
        }

        // Mode 3: Batch mode (all enabled portals)
        (None, None) => {
            let portals_config = load_portals_config(config_path)?
                .ok_or_else(|| anyhow::anyhow!(
                    "No configuration file found. Create ~/.config/ceres/portals.toml or use --config"
                ))?;

            let enabled: Vec<&PortalEntry> = portals_config.enabled_portals();

            if enabled.is_empty() {
                info!("No enabled portals found in configuration.");
                info!("Add portals to ~/.config/ceres/portals.toml or use: ceres harvest <url>");
                return Ok(());
            }

            info!("═══════════════════════════════════════════════════════");
            info!("Starting batch harvest of {} portals", enabled.len());
            info!("═══════════════════════════════════════════════════════");

            let summary = harvest_service
                .batch_harvest_with_progress(&enabled, &reporter)
                .await;

            print_batch_summary(&summary);
        }

        // This case is prevented by clap's conflicts_with
        (Some(_), Some(_)) => unreachable!("portal_url and portal are mutually exclusive"),
    }

    Ok(())
}

/// Print a summary of batch harvesting results.
fn print_batch_summary(summary: &BatchHarvestSummary) {
    info!("");
    info!("═══════════════════════════════════════════════════════");
    info!("BATCH HARVEST COMPLETE");
    info!("═══════════════════════════════════════════════════════");
    info!("  Portals processed:   {}", summary.total_portals());
    info!("  Successful:          {}", summary.successful_count());
    info!("  Failed:              {}", summary.failed_count());
    info!("  Total datasets:      {}", summary.total_datasets());

    if summary.failed_count() > 0 {
        info!("───────────────────────────────────────────────────────");
        info!("Failed portals:");
        for result in summary.results.iter().filter(|r| !r.is_success()) {
            if let Some(err) = &result.error {
                error!("  - {}: {}", result.portal_name, err);
            }
        }
    }
    info!("═══════════════════════════════════════════════════════");
}

/// Print a summary for single portal harvest (modes 1 and 2).
fn print_single_portal_summary(portal_url: &str, stats: &SyncStats) {
    info!("");
    info!("═══════════════════════════════════════════════════════");
    info!("Sync complete: {}", portal_url);
    info!("═══════════════════════════════════════════════════════");
    info!("  = Unchanged:         {}", stats.unchanged);
    info!("  ↑ Updated:           {}", stats.updated);
    info!("  + Created:           {}", stats.created);
    info!("  ✗ Failed:            {}", stats.failed);
    info!("───────────────────────────────────────────────────────");
    info!("  Total processed:     {}", stats.total());
    info!("  Successful:          {}", stats.successful());
    info!("═══════════════════════════════════════════════════════");

    if stats.failed == 0 {
        info!("All datasets processed successfully!");
    }
}

async fn search(
    search_service: &SearchService<DatasetRepository, GeminiClient>,
    query: &str,
    limit: usize,
) -> anyhow::Result<()> {
    info!("Searching for: '{}' (limit: {})", query, limit);

    let results = search_service.search(query, limit).await?;

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

// Use floor() instead of round() so very low similarity scores (e.g. 5%)
// do not display a filled bar, making the UI less misleading.
fn create_similarity_bar(score: f32) -> String {
    let filled = ((score * 10.0).floor() as isize).clamp(0, 10) as usize;
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
        // Safely truncate text by characters to handle multi-byte UTF-8
        let truncated: String = cleaned.chars().take(max_len).collect();
        format!("{}...", truncated)
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

// TODO(performance): Implement streaming export for large datasets
// Currently loads all datasets into memory before writing.
// For databases with millions of records, this causes OOM.
// Consider: (1) Cursor-based pagination, (2) Streaming writes as records arrive
async fn export(
    repo: &DatasetRepository,
    format: ExportFormat,
    portal_filter: Option<&str>,
    limit: Option<usize>,
) -> anyhow::Result<()> {
    info!("Exporting datasets...");

    // TODO(performance): Stream results instead of loading all into Vec
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
}
