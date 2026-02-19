//! Parquet export service for publishing a curated open data index.
//!
//! Produces flattened, curated Parquet files suitable for HuggingFace
//! from the Ceres dataset index. Includes noise filtering, metadata
//! flattening, portal name resolution, and cross-portal duplicate detection.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{BooleanBuilder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use tracing::info;

use crate::config::PortalsConfig;
use crate::error::AppError;
use crate::models::Dataset;
use crate::traits::DatasetStore;

/// Configuration for Parquet export curation.
pub struct ParquetExportConfig {
    /// Minimum title length — titles shorter are filtered as noise.
    pub min_title_length: usize,
    /// Noise title patterns to filter (case-insensitive substring match).
    pub noise_patterns: Vec<String>,
    /// Number of rows per Arrow RecordBatch / row group.
    pub batch_size: usize,
}

impl Default for ParquetExportConfig {
    fn default() -> Self {
        Self {
            min_title_length: 5,
            noise_patterns: vec!["test".into(), "prova".into(), "esempio".into()],
            batch_size: 10_000,
        }
    }
}

/// Result of a Parquet export operation.
#[derive(Debug, Serialize)]
pub struct ParquetExportResult {
    pub total_exported: u64,
    pub total_filtered: u64,
    pub total_duplicates: u64,
    pub portals: Vec<PortalExportStats>,
    pub snapshot_date: String,
    pub output_dir: PathBuf,
}

/// Per-portal export statistics.
#[derive(Debug, Serialize)]
pub struct PortalExportStats {
    pub name: String,
    pub url: String,
    pub count: u64,
}

/// Intermediate flattened record between Database `Dataset` and Arrow `RecordBatch`.
struct FlatRecord {
    original_id: String,
    source_portal: String,
    portal_name: String,
    url: String,
    title: String,
    description: String,
    tags: String,
    organization: String,
    license: String,
    metadata_created: String,
    metadata_modified: String,
    first_seen_at: String,
    language: String,
    is_duplicate: bool,
}

/// Service for exporting curated datasets as Parquet.
pub struct ParquetExportService<S: DatasetStore> {
    store: S,
    config: ParquetExportConfig,
    portal_names: HashMap<String, String>,
    portal_languages: HashMap<String, String>,
}

impl<S: DatasetStore> ParquetExportService<S> {
    /// Creates a new Parquet export service.
    ///
    /// Portal names and languages are resolved from `portals_config` when provided.
    /// Portals not in the config get names derived from their URL hostname.
    pub fn new(
        store: S,
        portals_config: Option<PortalsConfig>,
        config: ParquetExportConfig,
    ) -> Self {
        let mut portal_names = HashMap::new();
        let mut portal_languages = HashMap::new();

        if let Some(pc) = &portals_config {
            for entry in &pc.portals {
                let url = normalize_portal_url(&entry.url);
                portal_names.insert(url.clone(), entry.name.clone());
                portal_languages.insert(url, entry.language().to_string());
            }
        }

        Self {
            store,
            config,
            portal_names,
            portal_languages,
        }
    }

    /// Exports curated datasets as Parquet files to the given directory.
    ///
    /// Creates:
    /// - `all.parquet` — complete curated dataset
    /// - `data/<portal-name>.parquet` — per-portal subsets
    /// - `metadata.json` — snapshot metadata with counts
    pub async fn export_to_directory(
        &self,
        output_dir: &Path,
    ) -> Result<ParquetExportResult, AppError> {
        // Create directory structure
        let data_dir = output_dir.join("data");
        fs::create_dir_all(&data_dir).map_err(|e| {
            AppError::Generic(format!(
                "Failed to create output directory {}: {}",
                data_dir.display(),
                e
            ))
        })?;

        info!("Loading cross-portal duplicate titles...");
        let duplicate_titles = self.store.get_duplicate_titles().await?;
        info!(
            "Found {} duplicate title groups across portals",
            duplicate_titles.len()
        );

        info!("Streaming datasets for Parquet export...");
        let (exported, filtered, duplicates, portal_stats) =
            self.stream_and_write(output_dir, &duplicate_titles).await?;

        let snapshot_date = Utc::now().format("%Y-%m-%d").to_string();

        let result = ParquetExportResult {
            total_exported: exported,
            total_filtered: filtered,
            total_duplicates: duplicates,
            portals: portal_stats,
            snapshot_date: snapshot_date.clone(),
            output_dir: output_dir.to_path_buf(),
        };

        // Write metadata.json
        let metadata_path = output_dir.join("metadata.json");
        let metadata_json = serde_json::to_string_pretty(&result)
            .map_err(|e| AppError::Generic(format!("Failed to serialize metadata: {}", e)))?;
        fs::write(&metadata_path, metadata_json).map_err(|e| {
            AppError::Generic(format!(
                "Failed to write {}: {}",
                metadata_path.display(),
                e
            ))
        })?;

        Ok(result)
    }

    /// Streams all datasets, applies curation, and writes Parquet files.
    ///
    /// Returns (exported_count, filtered_count, duplicate_count, per_portal_stats).
    async fn stream_and_write(
        &self,
        output_dir: &Path,
        duplicate_titles: &HashSet<String>,
    ) -> Result<(u64, u64, u64, Vec<PortalExportStats>), AppError> {
        let schema = arrow_schema();
        let writer_props = writer_properties();

        let data_dir = output_dir.join("data");

        // Open the "all" writer
        let all_path = output_dir.join("all.parquet");
        let all_file = fs::File::create(&all_path).map_err(|e| {
            AppError::Generic(format!("Failed to create {}: {}", all_path.display(), e))
        })?;
        let mut all_writer =
            ArrowWriter::try_new(all_file, schema.clone(), Some(writer_props.clone()))
                .map_err(|e| AppError::Generic(format!("Failed to create ArrowWriter: {}", e)))?;

        // Per-portal state keyed by normalized source_portal URL (stable, unique)
        let mut portal_writers: HashMap<String, ArrowWriter<fs::File>> = HashMap::new();
        let mut portal_buffers: HashMap<String, Vec<FlatRecord>> = HashMap::new();
        let mut portal_counts: HashMap<String, u64> = HashMap::new();
        // Track portal_key -> (display_name, file_name) for stats and file creation
        let mut portal_info: HashMap<String, (String, String)> = HashMap::new();

        // Main "all" buffer
        let mut all_buffer: Vec<FlatRecord> = Vec::with_capacity(self.config.batch_size);

        let mut total_exported = 0u64;
        let mut total_filtered = 0u64;
        let mut total_duplicates = 0u64;

        let mut stream = self.store.list_stream(None, None);

        while let Some(result) = stream.next().await {
            let dataset = result?;

            // Apply noise filter
            if self.is_noise(&dataset) {
                total_filtered += 1;
                continue;
            }

            let is_duplicate = duplicate_titles.contains(&dataset.title.to_lowercase());
            if is_duplicate {
                total_duplicates += 1;
            }

            let record = self.flatten_dataset(&dataset, is_duplicate);

            // Use normalized source_portal URL as the stable partition key
            let portal_key = normalize_portal_url(&record.source_portal);
            let file_name = portal_file_name(&record.portal_name);
            portal_info
                .entry(portal_key.clone())
                .or_insert_with(|| (record.portal_name.clone(), file_name));

            // Add to portal buffer
            portal_buffers
                .entry(portal_key.clone())
                .or_default()
                .push(FlatRecord {
                    original_id: record.original_id.clone(),
                    source_portal: record.source_portal.clone(),
                    portal_name: record.portal_name.clone(),
                    url: record.url.clone(),
                    title: record.title.clone(),
                    description: record.description.clone(),
                    tags: record.tags.clone(),
                    organization: record.organization.clone(),
                    license: record.license.clone(),
                    metadata_created: record.metadata_created.clone(),
                    metadata_modified: record.metadata_modified.clone(),
                    first_seen_at: record.first_seen_at.clone(),
                    language: record.language.clone(),
                    is_duplicate: record.is_duplicate,
                });

            // Add to all buffer
            all_buffer.push(record);
            total_exported += 1;
            *portal_counts.entry(portal_key.clone()).or_default() += 1;

            // Flush "all" buffer when full
            if all_buffer.len() >= self.config.batch_size {
                let batch = build_record_batch(&all_buffer, &schema)?;
                all_writer
                    .write(&batch)
                    .map_err(|e| AppError::Generic(format!("Parquet write error: {}", e)))?;
                all_buffer.clear();
            }

            // Flush portal buffer when full
            if let Some(buf) = portal_buffers.get(&portal_key) {
                if buf.len() >= self.config.batch_size {
                    let buf = portal_buffers
                        .remove(&portal_key)
                        .expect("buffer must exist: checked by get() above");
                    let batch = build_record_batch(&buf, &schema)?;
                    let (_, ref fname) = portal_info[&portal_key];
                    let writer = get_or_create_portal_writer(
                        &mut portal_writers,
                        &portal_key,
                        fname,
                        &data_dir,
                        &schema,
                        &writer_props,
                    )?;
                    writer
                        .write(&batch)
                        .map_err(|e| AppError::Generic(format!("Parquet write error: {}", e)))?;
                }
            }
        }

        // Flush remaining "all" buffer
        if !all_buffer.is_empty() {
            let batch = build_record_batch(&all_buffer, &schema)?;
            all_writer
                .write(&batch)
                .map_err(|e| AppError::Generic(format!("Parquet write error: {}", e)))?;
        }

        // Flush remaining portal buffers
        for (portal_key, buf) in portal_buffers.drain() {
            if !buf.is_empty() {
                let batch = build_record_batch(&buf, &schema)?;
                let (_, ref fname) = portal_info[&portal_key];
                let writer = get_or_create_portal_writer(
                    &mut portal_writers,
                    &portal_key,
                    fname,
                    &data_dir,
                    &schema,
                    &writer_props,
                )?;
                writer
                    .write(&batch)
                    .map_err(|e| AppError::Generic(format!("Parquet write error: {}", e)))?;
            }
        }

        // Close all writers
        all_writer
            .close()
            .map_err(|e| AppError::Generic(format!("Failed to close all.parquet: {}", e)))?;

        for (portal_key, writer) in portal_writers {
            let (_, ref fname) = portal_info[&portal_key];
            writer.close().map_err(|e| {
                AppError::Generic(format!("Failed to close {}.parquet: {}", fname, e))
            })?;
        }

        // Build per-portal stats with accurate names and URLs
        let mut portal_stats: Vec<PortalExportStats> = portal_counts
            .into_iter()
            .map(|(portal_key, count)| {
                let (ref name, _) = portal_info[&portal_key];
                PortalExportStats {
                    name: name.clone(),
                    url: portal_key,
                    count,
                }
            })
            .collect();
        portal_stats.sort_by(|a, b| b.count.cmp(&a.count));

        Ok((
            total_exported,
            total_filtered,
            total_duplicates,
            portal_stats,
        ))
    }

    /// Returns true if the dataset should be filtered out as noise.
    fn is_noise(&self, dataset: &Dataset) -> bool {
        // Filter tiny titles
        if dataset.title.len() < self.config.min_title_length {
            return true;
        }

        // Filter empty descriptions
        if dataset
            .description
            .as_ref()
            .is_none_or(|d| d.trim().is_empty())
        {
            return true;
        }

        // Filter noise patterns in title
        let title_lower = dataset.title.to_lowercase();
        for pattern in &self.config.noise_patterns {
            if title_lower.contains(pattern.as_str()) {
                return true;
            }
        }

        false
    }

    /// Flattens a Dataset into an export record with extracted metadata.
    fn flatten_dataset(&self, dataset: &Dataset, is_duplicate: bool) -> FlatRecord {
        let metadata = &dataset.metadata.0;
        let normalized_url = normalize_portal_url(&dataset.source_portal);

        let portal_name = self
            .portal_names
            .get(&normalized_url)
            .cloned()
            .unwrap_or_else(|| portal_name_from_url(&dataset.source_portal));

        let language = self
            .portal_languages
            .get(&normalized_url)
            .cloned()
            .or_else(|| {
                metadata
                    .get("language")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_lowercase())
            })
            .unwrap_or_else(|| "unknown".to_string());

        FlatRecord {
            original_id: dataset.original_id.clone(),
            source_portal: dataset.source_portal.clone(),
            portal_name,
            url: dataset.url.clone(),
            title: dataset.title.clone(),
            description: dataset.description.clone().unwrap_or_default(),
            tags: extract_tags(metadata),
            organization: extract_organization(metadata),
            license: extract_license(metadata),
            metadata_created: extract_string(metadata, "metadata_created"),
            metadata_modified: extract_string(metadata, "metadata_modified"),
            first_seen_at: dataset.first_seen_at.to_rfc3339(),
            language,
            is_duplicate,
        }
    }
}

// =============================================================================
// Arrow Schema & RecordBatch Construction
// =============================================================================

/// Returns the Arrow schema for the Parquet export.
fn arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("original_id", DataType::Utf8, false),
        Field::new("source_portal", DataType::Utf8, false),
        Field::new("portal_name", DataType::Utf8, false),
        Field::new("url", DataType::Utf8, false),
        Field::new("title", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("tags", DataType::Utf8, true),
        Field::new("organization", DataType::Utf8, true),
        Field::new("license", DataType::Utf8, true),
        Field::new("metadata_created", DataType::Utf8, true),
        Field::new("metadata_modified", DataType::Utf8, true),
        Field::new("first_seen_at", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, true),
        Field::new("is_duplicate", DataType::Boolean, false),
    ]))
}

/// Returns Parquet writer properties with Zstd compression.
fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("zstd level 3 should be valid"),
        ))
        .build()
}

/// Builds an Arrow RecordBatch from a slice of FlatRecords.
fn build_record_batch(
    records: &[FlatRecord],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, AppError> {
    let len = records.len();

    let mut original_id = StringBuilder::with_capacity(len, len * 32);
    let mut source_portal = StringBuilder::with_capacity(len, len * 64);
    let mut portal_name = StringBuilder::with_capacity(len, len * 24);
    let mut url = StringBuilder::with_capacity(len, len * 128);
    let mut title = StringBuilder::with_capacity(len, len * 64);
    let mut description = StringBuilder::with_capacity(len, len * 256);
    let mut tags = StringBuilder::with_capacity(len, len * 64);
    let mut organization = StringBuilder::with_capacity(len, len * 48);
    let mut license = StringBuilder::with_capacity(len, len * 32);
    let mut metadata_created = StringBuilder::with_capacity(len, len * 24);
    let mut metadata_modified = StringBuilder::with_capacity(len, len * 24);
    let mut first_seen_at = StringBuilder::with_capacity(len, len * 32);
    let mut language = StringBuilder::with_capacity(len, len * 8);
    let mut is_duplicate = BooleanBuilder::with_capacity(len);

    for r in records {
        original_id.append_value(&r.original_id);
        source_portal.append_value(&r.source_portal);
        portal_name.append_value(&r.portal_name);
        url.append_value(&r.url);
        title.append_value(&r.title);
        description.append_value(&r.description);
        tags.append_value(&r.tags);
        organization.append_value(&r.organization);
        license.append_value(&r.license);
        metadata_created.append_value(&r.metadata_created);
        metadata_modified.append_value(&r.metadata_modified);
        first_seen_at.append_value(&r.first_seen_at);
        language.append_value(&r.language);
        is_duplicate.append_value(r.is_duplicate);
    }

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(original_id.finish()),
            Arc::new(source_portal.finish()),
            Arc::new(portal_name.finish()),
            Arc::new(url.finish()),
            Arc::new(title.finish()),
            Arc::new(description.finish()),
            Arc::new(tags.finish()),
            Arc::new(organization.finish()),
            Arc::new(license.finish()),
            Arc::new(metadata_created.finish()),
            Arc::new(metadata_modified.finish()),
            Arc::new(first_seen_at.finish()),
            Arc::new(language.finish()),
            Arc::new(is_duplicate.finish()),
        ],
    )
    .map_err(|e| AppError::Generic(format!("Failed to build RecordBatch: {}", e)))
}

// =============================================================================
// Per-Portal Writer Management
// =============================================================================

/// Gets or creates an ArrowWriter for a specific portal.
///
/// `portal_key` is the stable map key (normalized URL), `file_name` is the
/// human-readable name used for the `.parquet` file on disk.
fn get_or_create_portal_writer<'a>(
    writers: &'a mut HashMap<String, ArrowWriter<fs::File>>,
    portal_key: &str,
    file_name: &str,
    data_dir: &Path,
    schema: &Arc<Schema>,
    writer_props: &WriterProperties,
) -> Result<&'a mut ArrowWriter<fs::File>, AppError> {
    if !writers.contains_key(portal_key) {
        let path = data_dir.join(format!("{}.parquet", file_name));
        let file = fs::File::create(&path).map_err(|e| {
            AppError::Generic(format!("Failed to create {}: {}", path.display(), e))
        })?;
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_props.clone()))
            .map_err(|e| AppError::Generic(format!("Failed to create ArrowWriter: {}", e)))?;
        writers.insert(portal_key.to_string(), writer);
    }
    Ok(writers
        .get_mut(portal_key)
        .expect("writer just inserted above"))
}

// =============================================================================
// Metadata Extraction Helpers
// =============================================================================

/// Extracts tag names from CKAN metadata as a comma-separated string.
fn extract_tags(metadata: &serde_json::Value) -> String {
    metadata
        .get("tags")
        .and_then(|t| t.as_array())
        .map(|tags| {
            tags.iter()
                .filter_map(|t| {
                    t.get("name")
                        .or(t.get("display_name"))
                        .and_then(|n| n.as_str())
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
        .unwrap_or_default()
}

/// Extracts organization name from CKAN metadata.
fn extract_organization(metadata: &serde_json::Value) -> String {
    metadata
        .get("organization")
        .and_then(|org| {
            org.get("title")
                .or(org.get("name"))
                .and_then(|n| n.as_str())
        })
        .unwrap_or_default()
        .to_string()
}

/// Extracts license from CKAN metadata.
fn extract_license(metadata: &serde_json::Value) -> String {
    metadata
        .get("license_title")
        .or(metadata.get("license_id"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

/// Extracts a string field from metadata by key.
fn extract_string(metadata: &serde_json::Value, key: &str) -> String {
    metadata
        .get(key)
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

// =============================================================================
// URL / Name Utilities
// =============================================================================

/// Normalizes a portal URL by trimming trailing slashes for consistent map lookup.
fn normalize_portal_url(url: &str) -> String {
    url.trim_end_matches('/').to_string()
}

/// Derives a human-readable portal name from its URL hostname.
///
/// e.g. `https://dati.comune.milano.it` -> `dati-comune-milano-it`
fn portal_name_from_url(url: &str) -> String {
    url.trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or("unknown")
        .replace('.', "-")
}

/// Converts a portal name to a safe file name.
fn portal_file_name(name: &str) -> String {
    name.to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_tags() {
        let metadata = serde_json::json!({
            "tags": [
                {"name": "environment", "display_name": "Environment"},
                {"name": "water", "display_name": "Water"}
            ]
        });
        assert_eq!(extract_tags(&metadata), "environment, water");
    }

    #[test]
    fn test_extract_tags_empty() {
        let metadata = serde_json::json!({});
        assert_eq!(extract_tags(&metadata), "");
    }

    #[test]
    fn test_extract_organization() {
        let metadata = serde_json::json!({
            "organization": {"title": "City of Milan", "name": "milano"}
        });
        assert_eq!(extract_organization(&metadata), "City of Milan");
    }

    #[test]
    fn test_extract_organization_fallback_to_name() {
        let metadata = serde_json::json!({
            "organization": {"name": "milano"}
        });
        assert_eq!(extract_organization(&metadata), "milano");
    }

    #[test]
    fn test_extract_license() {
        let metadata = serde_json::json!({"license_title": "CC-BY 4.0"});
        assert_eq!(extract_license(&metadata), "CC-BY 4.0");
    }

    #[test]
    fn test_extract_license_fallback() {
        let metadata = serde_json::json!({"license_id": "cc-by"});
        assert_eq!(extract_license(&metadata), "cc-by");
    }

    #[test]
    fn test_portal_name_from_url() {
        assert_eq!(
            portal_name_from_url("https://dati.comune.milano.it"),
            "dati-comune-milano-it"
        );
        assert_eq!(portal_name_from_url("https://data.gov.ie"), "data-gov-ie");
        assert_eq!(
            portal_name_from_url("https://dati.gov.it/opendata/"),
            "dati-gov-it"
        );
    }

    #[test]
    fn test_portal_file_name() {
        assert_eq!(portal_file_name("milano"), "milano");
        assert_eq!(portal_file_name("dati-gov-it"), "dati-gov-it");
        assert_eq!(portal_file_name("NRW Portal"), "nrw-portal");
    }

    #[test]
    fn test_normalize_portal_url() {
        assert_eq!(
            normalize_portal_url("https://dati.gov.it/opendata/"),
            "https://dati.gov.it/opendata"
        );
        assert_eq!(
            normalize_portal_url("https://dati.comune.milano.it"),
            "https://dati.comune.milano.it"
        );
    }

    #[test]
    fn test_build_record_batch() {
        let schema = arrow_schema();
        let records = vec![FlatRecord {
            original_id: "test-1".to_string(),
            source_portal: "https://example.com".to_string(),
            portal_name: "example".to_string(),
            url: "https://example.com/dataset/test-1".to_string(),
            title: "Test Dataset".to_string(),
            description: "A test dataset".to_string(),
            tags: "tag1, tag2".to_string(),
            organization: "Test Org".to_string(),
            license: "CC-BY".to_string(),
            metadata_created: "2025-01-01".to_string(),
            metadata_modified: "2025-06-01".to_string(),
            first_seen_at: "2025-01-01T00:00:00Z".to_string(),
            language: "en".to_string(),
            is_duplicate: false,
        }];

        let batch = build_record_batch(&records, &schema).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 14);
    }
}
