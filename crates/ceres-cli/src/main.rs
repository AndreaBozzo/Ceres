use std::io;
use std::path::PathBuf;

use anyhow::Context;
use clap::Parser;
use dotenvy::dotenv;
use sqlx::postgres::PgPoolOptions;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

use ceres_client::{EmbeddingConfig, EmbeddingProviderEnum, PortalClientFactoryEnum};
use ceres_core::traits::EmbeddingProvider;
use ceres_core::{
    BatchHarvestSummary, DbConfig, EmbeddingService, EmbeddingStats,
    ExportFormat as CoreExportFormat, ExportService, HarvestConfig, HarvestPipeline,
    HarvestService, ParquetExportConfig, ParquetExportResult, ParquetExportService, PortalEntry,
    PortalType, SearchService, SyncConfig, SyncStats, TracingReporter, load_portals_config,
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

    // Helper: create embedding provider lazily (only when needed)
    let create_embedding = || -> anyhow::Result<EmbeddingProviderEnum> {
        EmbeddingProviderEnum::from_config(&EmbeddingConfig {
            provider: config.embedding_provider.clone(),
            gemini_api_key: config.gemini_api_key.clone(),
            openai_api_key: config.openai_api_key.clone(),
            embedding_model: config.embedding_model.clone(),
        })
    };

    // Helper: create + validate embedding provider
    let create_and_validate_embedding = || async {
        let client = create_embedding()?;
        repo.validate_embedding_dimension(client.dimension())
            .await
            .context("Embedding provider validation failed")?;
        info!(
            "Using {} embedding provider ({} dimensions)",
            client.name(),
            client.dimension()
        );
        Ok::<_, anyhow::Error>(client)
    };

    let portal_factory = PortalClientFactoryEnum::new();

    match config.command {
        Command::Harvest {
            portal_url,
            portal,
            config: config_path,
            full_sync,
            dry_run,
            metadata_only,
        } => {
            let mut harvest_config = HarvestConfig::default();
            if full_sync {
                harvest_config.force_full_sync = true;
            }
            if dry_run {
                harvest_config.dry_run = true;
            }

            if metadata_only {
                // Metadata-only: no embedding provider needed
                let harvest_service =
                    HarvestService::with_config(repo.clone(), portal_factory, harvest_config);
                handle_harvest_metadata_only(&harvest_service, portal_url, portal, config_path)
                    .await?;
            } else {
                // Default: harvest + embed via pipeline
                let embedding_client = create_and_validate_embedding().await?;
                let sync_config = {
                    let mut sc = SyncConfig::default();
                    if full_sync {
                        sc = sc.with_full_sync();
                    }
                    if dry_run {
                        sc = sc.with_dry_run();
                    }
                    sc
                };
                let pipeline = HarvestPipeline::from_sync_config(
                    repo.clone(),
                    embedding_client,
                    portal_factory,
                    sync_config,
                );
                handle_harvest(&pipeline, portal_url, portal, config_path).await?;
            }
        }
        Command::Embed { portal } => {
            let embedding_client = create_and_validate_embedding().await?;
            let embedding_service = EmbeddingService::new(repo.clone(), embedding_client);
            let reporter = TracingReporter;
            let cancel_token = tokio_util::sync::CancellationToken::new();
            let stats = embedding_service
                .embed_pending(portal.as_deref(), &reporter, cancel_token)
                .await?;
            print_embedding_summary(&stats);
        }
        Command::Search { query, limit } => {
            let embedding_client = create_and_validate_embedding().await?;
            let search_service = SearchService::new(repo.clone(), embedding_client);
            search(&search_service, &query, limit).await?;
        }
        Command::Export {
            format,
            portal,
            limit,
            output,
            config: config_path,
        } => match format {
            ExportFormat::Parquet => {
                let output_dir = output.ok_or_else(|| {
                    anyhow::anyhow!("--output <DIR> is required for parquet format")
                })?;
                if portal.is_some() {
                    eprintln!(
                        "Warning: --portal is ignored for parquet export (all portals are exported as separate files)"
                    );
                }
                if limit.is_some() {
                    eprintln!("Warning: --limit is ignored for parquet export");
                }
                let portals_config = load_portals_config(config_path)?;
                let parquet_service = ParquetExportService::new(
                    repo.clone(),
                    portals_config,
                    ParquetExportConfig::default(),
                );
                let result = parquet_service.export_to_directory(&output_dir).await?;
                print_parquet_export_summary(&result);
            }
            _ => {
                let export_service = ExportService::new(repo.clone());
                export(&export_service, format, portal.as_deref(), limit).await?;
            }
        },
        Command::Stats => {
            show_stats(&repo).await?;
        }
    }

    Ok(())
}

/// Handle the harvest command (harvest + embed via pipeline).
async fn handle_harvest(
    pipeline: &HarvestPipeline<DatasetRepository, EmbeddingProviderEnum, PortalClientFactoryEnum>,
    portal_url: Option<String>,
    portal_name: Option<String>,
    config_path: Option<PathBuf>,
) -> anyhow::Result<()> {
    let reporter = TracingReporter;

    match (portal_url, portal_name) {
        (Some(url), None) => {
            info!("Syncing portal: {}", url);
            let (result, embed_stats) = pipeline
                .sync_portal_with_progress(&url, None, "en", &reporter, PortalType::Ckan)
                .await?;
            print_single_portal_summary(&url, &result.stats);
            print_embedding_summary(&embed_stats);
        }

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
            let (result, embed_stats) = pipeline
                .sync_portal_with_progress(
                    &portal.url,
                    portal.url_template.as_deref(),
                    portal.language(),
                    &reporter,
                    portal.portal_type,
                )
                .await?;
            print_single_portal_summary(&portal.url, &result.stats);
            print_embedding_summary(&embed_stats);
        }

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

            let summary = pipeline
                .batch_harvest_with_progress(&enabled, &reporter)
                .await;

            print_batch_summary(&summary);
        }

        (Some(_), Some(_)) => unreachable!("portal_url and portal are mutually exclusive"),
    }

    Ok(())
}

/// Handle the harvest command in metadata-only mode (no embedding).
async fn handle_harvest_metadata_only(
    harvest_service: &HarvestService<DatasetRepository, PortalClientFactoryEnum>,
    portal_url: Option<String>,
    portal_name: Option<String>,
    config_path: Option<PathBuf>,
) -> anyhow::Result<()> {
    let reporter = TracingReporter;

    match (portal_url, portal_name) {
        (Some(url), None) => {
            info!("Syncing portal (metadata only): {}", url);
            let stats = harvest_service
                .sync_portal_with_progress(&url, None, "en", &reporter, PortalType::Ckan)
                .await?;
            print_single_portal_summary(&url, &stats);
        }

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

            info!("Syncing portal (metadata only): {}", portal.url);
            let stats = harvest_service
                .sync_portal_with_progress(
                    &portal.url,
                    portal.url_template.as_deref(),
                    portal.language(),
                    &reporter,
                    portal.portal_type,
                )
                .await?;
            print_single_portal_summary(&portal.url, &stats);
        }

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
            info!(
                "Starting batch harvest (metadata only) of {} portals",
                enabled.len()
            );
            info!("═══════════════════════════════════════════════════════");

            let summary = harvest_service
                .batch_harvest_with_progress(&enabled, &reporter)
                .await;

            print_batch_summary(&summary);
        }

        (Some(_), Some(_)) => unreachable!("portal_url and portal are mutually exclusive"),
    }

    Ok(())
}

/// Print a summary of embedding results.
fn print_embedding_summary(stats: &EmbeddingStats) {
    if stats.total == 0 {
        return;
    }
    info!("───────────────────────────────────────────────────────");
    info!("Embedding results:");
    info!("  Embedded:            {}", stats.embedded);
    info!("  Failed:              {}", stats.failed);
    info!("  Skipped:             {}", stats.skipped);
    info!("  Total:               {}", stats.total);
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
    search_service: &SearchService<DatasetRepository, EmbeddingProviderEnum>,
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
    if stats.stale_datasets > 0 {
        println!("  Stale datasets:        {}", stats.stale_datasets);
    }
    if let Some(last_update) = stats.last_update {
        println!("  Last update:           {}", last_update);
    }
    println!();

    Ok(())
}

fn print_parquet_export_summary(result: &ParquetExportResult) {
    eprintln!();
    eprintln!("═══════════════════════════════════════════════════════");
    eprintln!("  PARQUET EXPORT COMPLETE");
    eprintln!("═══════════════════════════════════════════════════════");
    eprintln!("  Output:              {}", result.output_dir.display());
    eprintln!("  Snapshot date:       {}", result.snapshot_date);
    eprintln!("  Total exported:      {}", result.total_exported);
    eprintln!("  Filtered (noise):    {}", result.total_filtered);
    eprintln!("  Marked duplicate:    {}", result.total_duplicates);
    eprintln!("───────────────────────────────────────────────────────");
    eprintln!("  Portal breakdown:");
    for portal in &result.portals {
        eprintln!("    {:<25} {:>6} datasets", portal.name, portal.count);
    }
    eprintln!("═══════════════════════════════════════════════════════");
    eprintln!();
}

async fn export(
    export_service: &ExportService<DatasetRepository>,
    format: ExportFormat,
    portal_filter: Option<&str>,
    limit: Option<usize>,
) -> anyhow::Result<()> {
    info!("Exporting datasets (streaming mode)...");

    let core_format = match format {
        ExportFormat::Jsonl => CoreExportFormat::Jsonl,
        ExportFormat::Json => CoreExportFormat::Json,
        ExportFormat::Csv => CoreExportFormat::Csv,
        ExportFormat::Parquet => unreachable!("parquet is handled separately"),
    };

    let stdout = io::stdout();
    let mut writer = stdout.lock();

    let count = export_service
        .export_to_writer(&mut writer, core_format, portal_filter, limit)
        .await?;

    if count == 0 {
        eprintln!("No datasets found to export.");
    } else {
        info!("Export complete: {} datasets", count);
    }

    Ok(())
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
}
