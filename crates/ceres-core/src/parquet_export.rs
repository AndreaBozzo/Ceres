//! Parquet export service for publishing a curated open data index.
//!
//! Produces flattened, curated Parquet files suitable for HuggingFace
//! from the Ceres dataset index. Includes noise filtering, metadata
//! flattening, portal name resolution, and cross-portal duplicate detection.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, BooleanBuilder, StringArray, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::Utc;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tracing::info;

use crate::config::PortalsConfig;
use crate::error::AppError;
use crate::models::Dataset;
use crate::traits::DatasetStore;

/// Schema version for the snapshot manifest written beside every Parquet export.
pub const SNAPSHOT_MANIFEST_SCHEMA_VERSION: &str = "1.0.0";

/// Schema version for the coverage and quality report written beside every export.
pub const SNAPSHOT_REPORT_SCHEMA_VERSION: &str = "1.0.0";

/// Schema version for the snapshot-to-snapshot changelog written when a previous
/// snapshot is supplied.
pub const SNAPSHOT_CHANGELOG_SCHEMA_VERSION: &str = "1.0.0";

/// Identifier of the duplicate-detection rule. The `is_duplicate` column is a
/// heuristic title-match signal, not canonical deduplication.
pub const DUPLICATE_DETECTION_METHOD: &str = "title-exact-ci-cross-portal";

/// Version of the duplicate-detection rule. Bump when the matching logic changes
/// so consumers can tell snapshots produced by different rules apart.
pub const DUPLICATE_DETECTION_VERSION: &str = "1";

/// Configuration for Parquet export curation and provenance.
pub struct ParquetExportConfig {
    /// Minimum title length — titles shorter are filtered as noise.
    pub min_title_length: usize,
    /// Noise title patterns to filter (case-insensitive substring match).
    pub noise_patterns: Vec<String>,
    /// Number of rows per Arrow RecordBatch / row group.
    pub batch_size: usize,
    /// Git commit that produced the exporting binary. `unknown` is retained for
    /// library callers that do not provide build metadata.
    pub git_commit: String,
}

impl Default for ParquetExportConfig {
    fn default() -> Self {
        Self {
            min_title_length: 5,
            noise_patterns: vec!["test".into(), "prova".into(), "esempio".into()],
            batch_size: 10_000,
            git_commit: option_env!("VERGEN_GIT_SHA")
                .unwrap_or("unknown")
                .to_string(),
        }
    }
}

impl ParquetExportConfig {
    /// Records the commit that produced the binary writing a snapshot manifest.
    pub fn with_git_commit(mut self, git_commit: impl Into<String>) -> Self {
        self.git_commit = git_commit.into();
        self
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
    pub snapshot_id: String,
    pub generated_at: String,
    pub manifest_schema_version: String,
    pub output_dir: PathBuf,
    pub report: SnapshotReport,
    pub changelog: SnapshotChangelog,
}

/// Per-portal export statistics.
#[derive(Debug, Serialize)]
pub struct PortalExportStats {
    pub name: String,
    pub url: String,
    pub count: u64,
    pub portal_type: String,
    pub profile: Option<String>,
}

/// A versioned, portable description of a published index snapshot.
///
/// This is written as `metadata.json`. It deliberately excludes the local
/// output directory so the file is portable and suitable for publication.
#[derive(Debug, Serialize)]
pub struct SnapshotManifest {
    pub schema_version: String,
    pub snapshot_id: String,
    pub generated_at: String,
    pub snapshot_date: String,
    pub ceres: CeresBuildInfo,
    pub portal_config: PortalConfigProvenance,
    pub duplicate_detection: DuplicateDetectionProvenance,
    pub row_counts: SnapshotRowCounts,
    pub canonical_file: String,
    pub files: Vec<SnapshotFile>,
    pub portals: Vec<SnapshotPortal>,
    pub warnings: Vec<String>,
}

/// Ceres build metadata embedded in a snapshot manifest.
#[derive(Debug, Serialize)]
pub struct CeresBuildInfo {
    pub version: String,
    pub git_commit: String,
}

/// Provenance for the portal configuration used while exporting a snapshot.
#[derive(Debug, Serialize)]
pub struct PortalConfigProvenance {
    /// SHA-256 of the serialized portal configuration, when one was supplied.
    pub sha256: Option<String>,
}

/// Provenance for the cross-portal duplicate-detection rule used in a snapshot.
///
/// Records that `is_duplicate` is a heuristic signal and which matching rule
/// produced it, so consumers can tell snapshots apart if the rule changes and
/// know not to treat the flag as canonical deduplication.
#[derive(Debug, Serialize)]
pub struct DuplicateDetectionProvenance {
    /// Matching rule identifier, e.g. `title-exact-ci-cross-portal`.
    pub method: String,
    /// Rule version; bumped when the matching logic changes.
    pub version: String,
    /// Number of portals that declared alias/mirror URLs folded before matching.
    pub alias_groups: u64,
    /// Human-readable caveat about the nature of the signal.
    pub note: String,
}

/// Counts showing how curation changed the database rows considered for export.
#[derive(Debug, Serialize)]
pub struct SnapshotRowCounts {
    pub raw: u64,
    pub exported: u64,
    pub filtered: u64,
    pub duplicate_flagged: u64,
}

/// Integrity metadata for a published data file.
#[derive(Debug, Serialize)]
pub struct SnapshotFile {
    pub path: String,
    pub kind: String,
    pub row_count: u64,
    pub size_bytes: u64,
    pub sha256: String,
}

/// Per-portal inclusion status for a snapshot.
#[derive(Debug, Serialize)]
pub struct SnapshotPortal {
    pub name: String,
    pub source_url: String,
    pub portal_type: String,
    pub profile: Option<String>,
    pub status: String,
    pub row_count: u64,
    pub file: String,
}

/// A coverage and quality report describing what a snapshot contains and where
/// its metadata is weak.
///
/// Written as `reports.json`. It is deterministically derived from the same
/// export inputs as the manifest, so its figures agree with `metadata.json`.
#[derive(Debug, Serialize)]
pub struct SnapshotReport {
    pub schema_version: String,
    pub snapshot_id: String,
    pub snapshot_date: String,
    pub generated_at: String,
    pub curation: CurationReport,
    pub coverage: CoverageReport,
    pub field_completeness: FieldCompletenessReport,
}

/// Curation outcomes for a snapshot: how raw rows became the exported set.
#[derive(Debug, Serialize)]
pub struct CurationReport {
    pub raw: u64,
    pub exported: u64,
    pub filtered: u64,
    pub duplicate_flagged: u64,
    /// Matching rule that produced `duplicate_flagged` (heuristic, not canonical).
    pub duplicate_detection_method: String,
    /// Version of the duplicate-detection rule.
    pub duplicate_detection_version: String,
    /// Configured portals that contributed no datasets to this snapshot
    /// (disabled, failed, or empty), by display name.
    pub excluded_portals: Vec<String>,
}

/// What the snapshot covers, broken down by independent dimensions.
#[derive(Debug, Serialize)]
pub struct CoverageReport {
    pub total_datasets: u64,
    pub portals: u64,
    pub by_portal_type: Vec<CoverageBucket>,
    pub by_profile: Vec<CoverageBucket>,
    pub by_language: Vec<CoverageBucket>,
    pub by_portal: Vec<CoverageBucket>,
}

/// One coverage bucket: a dimension value and its dataset count.
#[derive(Debug, Serialize)]
pub struct CoverageBucket {
    pub key: String,
    pub count: u64,
}

/// Field-completeness rates across the exported dataset.
#[derive(Debug, Serialize)]
pub struct FieldCompletenessReport {
    pub total: u64,
    pub description: FieldCompleteness,
    pub license: FieldCompleteness,
    pub organization: FieldCompleteness,
    pub tags: FieldCompleteness,
    pub modification_date: FieldCompleteness,
}

/// Completeness of a single metadata field: rows present and the resulting rate.
#[derive(Debug, Serialize)]
pub struct FieldCompleteness {
    pub present: u64,
    /// `present / total`, rounded to four decimals; `0.0` when `total` is zero.
    pub rate: f64,
}

/// A snapshot-to-snapshot diff, keyed by the stable identity
/// (`source_portal` + `original_id`), written as `changelog.json`.
///
/// "Changed" is defined by `content_hash` (SHA-256 of title+description); source
/// modification timestamps are deliberately not used because portal coverage of
/// them is incomplete. When no comparable previous snapshot is available, the
/// totals are all zero and `compared` is false.
#[derive(Debug, Serialize)]
pub struct SnapshotChangelog {
    pub schema_version: String,
    pub snapshot_id: String,
    pub snapshot_date: String,
    pub generated_at: String,
    /// Snapshot ID of the baseline, when one was supplied and readable.
    pub previous_snapshot_id: Option<String>,
    /// True when a previous `identity.parquet` was read and diffed.
    pub compared: bool,
    /// How "changed" is determined, for consumers reading the artifact directly.
    pub changed_definition: String,
    pub totals: ChangelogCounts,
    pub by_portal: Vec<PortalChangelog>,
}

/// Added/changed/removed/unchanged counts for a changelog or one of its portals.
#[derive(Debug, Default, Serialize)]
pub struct ChangelogCounts {
    pub added: u64,
    pub changed: u64,
    pub removed: u64,
    pub unchanged: u64,
}

/// Per-portal diff summary. `portal` is the canonical source URL.
#[derive(Debug, Serialize)]
pub struct PortalChangelog {
    pub portal: String,
    pub added: u64,
    pub changed: u64,
    pub removed: u64,
    pub unchanged: u64,
}

/// Accumulates per-record coverage and completeness counters during the export
/// stream so the report is derived from the same pass that writes Parquet.
#[derive(Default)]
struct QualityAccumulator {
    language_counts: HashMap<String, u64>,
    description_present: u64,
    license_present: u64,
    organization_present: u64,
    tags_present: u64,
    modification_date_present: u64,
}

impl QualityAccumulator {
    /// Records the metadata signals of one exported record.
    fn observe(&mut self, record: &FlatRecord) {
        *self
            .language_counts
            .entry(record.language.clone())
            .or_default() += 1;
        if !record.description.trim().is_empty() {
            self.description_present += 1;
        }
        if !record.license.trim().is_empty() {
            self.license_present += 1;
        }
        if !record.organization.trim().is_empty() {
            self.organization_present += 1;
        }
        if !record.tags.trim().is_empty() {
            self.tags_present += 1;
        }
        if !record.metadata_modified.trim().is_empty() {
            self.modification_date_present += 1;
        }
    }
}

/// Outcome of a streaming export pass: counts, per-portal stats, and the
/// quality counters used to build the snapshot report.
struct StreamOutcome {
    exported: u64,
    filtered: u64,
    duplicates: u64,
    portals: Vec<PortalExportStats>,
    quality: QualityAccumulator,
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
    portal_types: HashMap<String, String>,
    portal_profiles: HashMap<String, Option<String>>,
    portal_config_sha256: Option<String>,
    /// Normalized alias/mirror URL -> canonical portal URL. Aliased datasets are
    /// folded onto their canonical portal for duplicate detection, partitioning,
    /// and snapshot identity, so a mirror is not treated as an independent source.
    alias_map: HashMap<String, String>,
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
        let mut portal_types = HashMap::new();
        let mut portal_profiles = HashMap::new();
        let mut alias_map = HashMap::new();
        let portal_config_sha256 = portals_config.as_ref().map(hash_portals_config);

        if let Some(pc) = &portals_config {
            for entry in &pc.portals {
                let url = normalize_portal_url(&entry.url);
                portal_names.insert(url.clone(), entry.name.clone());
                portal_languages.insert(url.clone(), entry.language().to_string());
                portal_types.insert(url.clone(), entry.portal_type.to_string());
                portal_profiles.insert(url.clone(), entry.profile().map(str::to_string));
                // Fold each declared alias onto this entry's canonical URL.
                for alias in entry.aliases() {
                    alias_map.insert(normalize_portal_url(alias), url.clone());
                }
            }
        }

        Self {
            store,
            config,
            portal_names,
            portal_languages,
            portal_types,
            portal_profiles,
            portal_config_sha256,
            alias_map,
        }
    }

    /// Returns the canonical normalized URL for a portal, folding any declared
    /// alias/mirror URL onto its canonical. Non-aliased URLs are returned as-is.
    fn canonical_portal_url(&self, source_portal: &str) -> String {
        let normalized = normalize_portal_url(source_portal);
        self.alias_map
            .get(&normalized)
            .cloned()
            .unwrap_or(normalized)
    }

    /// Number of portals that declared at least one alias (provenance metric).
    fn alias_group_count(&self) -> u64 {
        self.alias_map.values().collect::<HashSet<_>>().len() as u64
    }

    /// Exports curated datasets as Parquet files to the given directory.
    ///
    /// Equivalent to [`Self::export_to_directory_with_previous`] with no baseline,
    /// so no changelog is produced.
    pub async fn export_to_directory(
        &self,
        output_dir: &Path,
    ) -> Result<ParquetExportResult, AppError> {
        self.export_to_directory_with_previous(output_dir, None)
            .await
    }

    /// Exports curated datasets as Parquet files to the given directory, optionally
    /// diffing against a previous snapshot directory to produce a changelog.
    ///
    /// Creates:
    /// - `all.parquet` — complete curated dataset
    /// - `data/<portal-name>.parquet` — per-portal subsets
    /// - `identity.parquet` — slim per-record fingerprint for snapshot diffing
    /// - `metadata.json` — versioned snapshot manifest and integrity metadata
    /// - `reports.json` — machine-readable coverage and quality report
    /// - `report.md` — human-readable coverage and quality summary
    /// - `changelog.json` / `changelog.md` — diff vs `previous` (when supplied)
    pub async fn export_to_directory_with_previous(
        &self,
        output_dir: &Path,
        previous: Option<&Path>,
    ) -> Result<ParquetExportResult, AppError> {
        // Create directory structure
        let data_dir = output_dir.join("data");
        fs::create_dir_all(&data_dir).map_err(|e| {
            AppError::IoError(format!(
                "Failed to create output directory {}: {}",
                data_dir.display(),
                e
            ))
        })?;

        info!("Loading cross-portal duplicate titles...");
        let duplicate_titles = self.store.get_duplicate_titles(&self.alias_map).await?;
        info!(
            "Found {} duplicate title groups across portals",
            duplicate_titles.len()
        );

        info!("Streaming datasets for Parquet export...");
        let outcome = self.stream_and_write(output_dir, &duplicate_titles).await?;

        let generated_at = Utc::now();
        let snapshot_date = generated_at.format("%Y-%m-%d").to_string();
        let generated_at = generated_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

        let manifest = self.build_manifest(output_dir, &outcome, &snapshot_date, &generated_at)?;

        // Write the portable snapshot manifest, never the local output path.
        let metadata_path = output_dir.join("metadata.json");
        let metadata_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| AppError::ExportError(format!("Failed to serialize metadata: {}", e)))?;
        fs::write(&metadata_path, metadata_json).map_err(|e| {
            AppError::IoError(format!(
                "Failed to write {}: {}",
                metadata_path.display(),
                e
            ))
        })?;

        // Build the coverage and quality report from the same export pass so its
        // figures agree with the manifest, then write both machine- and
        // human-readable forms beside the snapshot.
        let report = self.build_report(&manifest, &outcome, &snapshot_date, &generated_at);
        let reports_path = output_dir.join("reports.json");
        let reports_json = serde_json::to_string_pretty(&report)
            .map_err(|e| AppError::ExportError(format!("Failed to serialize report: {}", e)))?;
        fs::write(&reports_path, reports_json).map_err(|e| {
            AppError::IoError(format!("Failed to write {}: {}", reports_path.display(), e))
        })?;
        let report_md_path = output_dir.join("report.md");
        fs::write(&report_md_path, render_report_markdown(&report)).map_err(|e| {
            AppError::IoError(format!(
                "Failed to write {}: {}",
                report_md_path.display(),
                e
            ))
        })?;

        // Diff the just-written identity index against the previous snapshot (when
        // one was supplied) and publish the changelog beside the snapshot.
        let changelog = build_changelog(
            &manifest.snapshot_id,
            &snapshot_date,
            &generated_at,
            &output_dir.join("identity.parquet"),
            previous,
        )?;
        let changelog_path = output_dir.join("changelog.json");
        let changelog_json = serde_json::to_string_pretty(&changelog)
            .map_err(|e| AppError::ExportError(format!("Failed to serialize changelog: {}", e)))?;
        fs::write(&changelog_path, changelog_json).map_err(|e| {
            AppError::IoError(format!(
                "Failed to write {}: {}",
                changelog_path.display(),
                e
            ))
        })?;
        let changelog_md_path = output_dir.join("changelog.md");
        fs::write(&changelog_md_path, render_changelog_markdown(&changelog)).map_err(|e| {
            AppError::IoError(format!(
                "Failed to write {}: {}",
                changelog_md_path.display(),
                e
            ))
        })?;

        Ok(ParquetExportResult {
            total_exported: outcome.exported,
            total_filtered: outcome.filtered,
            total_duplicates: outcome.duplicates,
            snapshot_date,
            snapshot_id: manifest.snapshot_id.clone(),
            generated_at,
            manifest_schema_version: SNAPSHOT_MANIFEST_SCHEMA_VERSION.to_string(),
            output_dir: output_dir.to_path_buf(),
            report,
            changelog,
            portals: outcome.portals,
        })
    }

    fn build_manifest(
        &self,
        output_dir: &Path,
        outcome: &StreamOutcome,
        snapshot_date: &str,
        generated_at: &str,
    ) -> Result<SnapshotManifest, AppError> {
        let canonical_path = output_dir.join("all.parquet");
        let canonical_checksum = sha256_file(&canonical_path)?;
        let mut files = vec![snapshot_file(
            &canonical_path,
            "all.parquet",
            "canonical",
            outcome.exported,
            Some(&canonical_checksum),
        )?];

        // The slim identity fingerprint enables snapshot-to-snapshot diffing.
        files.push(snapshot_file(
            &output_dir.join("identity.parquet"),
            "identity.parquet",
            "identity",
            outcome.exported,
            None,
        )?);

        let mut portals = Vec::with_capacity(outcome.portals.len());
        for portal in &outcome.portals {
            let relative_path = format!("data/{}.parquet", portal_file_name(&portal.name));
            let file_path = output_dir.join(&relative_path);
            files.push(snapshot_file(
                &file_path,
                &relative_path,
                "portal_subset",
                portal.count,
                None,
            )?);
            portals.push(SnapshotPortal {
                name: portal.name.clone(),
                source_url: portal.url.clone(),
                portal_type: portal.portal_type.clone(),
                profile: portal.profile.clone(),
                status: "included".to_string(),
                row_count: portal.count,
                file: relative_path,
            });
        }

        let mut warnings = Vec::new();
        if self.portal_config_sha256.is_none() {
            warnings.push(
                "No portals configuration was supplied; portal type/profile provenance may be unknown."
                    .to_string(),
            );
        }

        Ok(SnapshotManifest {
            schema_version: SNAPSHOT_MANIFEST_SCHEMA_VERSION.to_string(),
            snapshot_id: format!(
                "ceres-{}-{}",
                snapshot_date.replace('-', ""),
                &canonical_checksum[..12]
            ),
            generated_at: generated_at.to_string(),
            snapshot_date: snapshot_date.to_string(),
            ceres: CeresBuildInfo {
                version: env!("CARGO_PKG_VERSION").to_string(),
                git_commit: self.config.git_commit.clone(),
            },
            portal_config: PortalConfigProvenance {
                sha256: self.portal_config_sha256.clone(),
            },
            duplicate_detection: DuplicateDetectionProvenance {
                method: DUPLICATE_DETECTION_METHOD.to_string(),
                version: DUPLICATE_DETECTION_VERSION.to_string(),
                alias_groups: self.alias_group_count(),
                note: "Heuristic title-match signal, not canonical deduplication.".to_string(),
            },
            row_counts: SnapshotRowCounts {
                raw: outcome.exported + outcome.filtered,
                exported: outcome.exported,
                filtered: outcome.filtered,
                duplicate_flagged: outcome.duplicates,
            },
            canonical_file: "all.parquet".to_string(),
            files,
            portals,
            warnings,
        })
    }

    /// Builds the coverage and quality report from a completed export pass.
    fn build_report(
        &self,
        manifest: &SnapshotManifest,
        outcome: &StreamOutcome,
        snapshot_date: &str,
        generated_at: &str,
    ) -> SnapshotReport {
        let total = outcome.exported;

        // Coverage aggregations from the per-portal stats.
        let mut type_counts: HashMap<String, u64> = HashMap::new();
        let mut profile_counts: HashMap<String, u64> = HashMap::new();
        let mut by_portal = Vec::with_capacity(outcome.portals.len());
        for portal in &outcome.portals {
            *type_counts.entry(portal.portal_type.clone()).or_default() += portal.count;
            let profile = portal.profile.clone().unwrap_or_else(|| "none".to_string());
            *profile_counts.entry(profile).or_default() += portal.count;
            by_portal.push(CoverageBucket {
                key: portal.name.clone(),
                count: portal.count,
            });
        }

        // Configured portals that produced no datasets in this snapshot.
        let exported_urls: HashSet<&str> = outcome.portals.iter().map(|p| p.url.as_str()).collect();
        let mut excluded_portals: Vec<String> = self
            .portal_names
            .iter()
            .filter(|(url, _)| !exported_urls.contains(url.as_str()))
            .map(|(_, name)| name.clone())
            .collect();
        excluded_portals.sort();
        excluded_portals.dedup();

        SnapshotReport {
            schema_version: SNAPSHOT_REPORT_SCHEMA_VERSION.to_string(),
            snapshot_id: manifest.snapshot_id.clone(),
            snapshot_date: snapshot_date.to_string(),
            generated_at: generated_at.to_string(),
            curation: CurationReport {
                raw: outcome.exported + outcome.filtered,
                exported: outcome.exported,
                filtered: outcome.filtered,
                duplicate_flagged: outcome.duplicates,
                duplicate_detection_method: DUPLICATE_DETECTION_METHOD.to_string(),
                duplicate_detection_version: DUPLICATE_DETECTION_VERSION.to_string(),
                excluded_portals,
            },
            coverage: CoverageReport {
                total_datasets: total,
                portals: outcome.portals.len() as u64,
                by_portal_type: sorted_buckets(type_counts),
                by_profile: sorted_buckets(profile_counts),
                by_language: sorted_buckets(outcome.quality.language_counts.clone()),
                by_portal,
            },
            field_completeness: FieldCompletenessReport {
                total,
                description: field_completeness(outcome.quality.description_present, total),
                license: field_completeness(outcome.quality.license_present, total),
                organization: field_completeness(outcome.quality.organization_present, total),
                tags: field_completeness(outcome.quality.tags_present, total),
                modification_date: field_completeness(
                    outcome.quality.modification_date_present,
                    total,
                ),
            },
        }
    }

    /// Streams all datasets, applies curation, and writes Parquet files.
    ///
    /// Returns counts, per-portal stats, and the accumulated quality counters.
    async fn stream_and_write(
        &self,
        output_dir: &Path,
        duplicate_titles: &HashSet<String>,
    ) -> Result<StreamOutcome, AppError> {
        let schema = arrow_schema();
        let writer_props = writer_properties();

        let data_dir = output_dir.join("data");

        // Open the "all" writer
        let all_path = output_dir.join("all.parquet");
        let all_file = fs::File::create(&all_path).map_err(|e| {
            AppError::IoError(format!("Failed to create {}: {}", all_path.display(), e))
        })?;
        let mut all_writer =
            ArrowWriter::try_new(all_file, schema.clone(), Some(writer_props.clone())).map_err(
                |e| AppError::ExportError(format!("Failed to create ArrowWriter: {}", e)),
            )?;

        // Open the slim identity writer (used for snapshot-to-snapshot diffing)
        let identity_schema = identity_schema();
        let identity_path = output_dir.join("identity.parquet");
        let identity_file = fs::File::create(&identity_path).map_err(|e| {
            AppError::IoError(format!(
                "Failed to create {}: {}",
                identity_path.display(),
                e
            ))
        })?;
        let mut identity_writer = ArrowWriter::try_new(
            identity_file,
            identity_schema.clone(),
            Some(writer_props.clone()),
        )
        .map_err(|e| {
            AppError::ExportError(format!("Failed to create identity ArrowWriter: {}", e))
        })?;
        let mut identity_buffer: Vec<IdentityRecord> = Vec::with_capacity(self.config.batch_size);

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
        let mut quality = QualityAccumulator::default();

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
            quality.observe(&record);

            // Record the stable identity fingerprint for snapshot diffing. The
            // record's source_portal is already canonical (aliases folded).
            identity_buffer.push(IdentityRecord {
                source_portal: record.source_portal.clone(),
                original_id: record.original_id.clone(),
                content_hash: dataset.content_hash.clone(),
            });
            if identity_buffer.len() >= self.config.batch_size {
                let batch = build_identity_batch(&identity_buffer, &identity_schema)?;
                identity_writer.write(&batch).map_err(|e| {
                    AppError::ExportError(format!("Identity parquet write error: {}", e))
                })?;
                identity_buffer.clear();
            }

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
                    .map_err(|e| AppError::ExportError(format!("Parquet write error: {}", e)))?;
                all_buffer.clear();
            }

            // Flush portal buffer when full
            if let Some(buf) = portal_buffers.get(&portal_key)
                && buf.len() >= self.config.batch_size
            {
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
                    .map_err(|e| AppError::ExportError(format!("Parquet write error: {}", e)))?;
            }
        }

        // Flush remaining "all" buffer
        if !all_buffer.is_empty() {
            let batch = build_record_batch(&all_buffer, &schema)?;
            all_writer
                .write(&batch)
                .map_err(|e| AppError::ExportError(format!("Parquet write error: {}", e)))?;
        }

        // Flush remaining identity buffer
        if !identity_buffer.is_empty() {
            let batch = build_identity_batch(&identity_buffer, &identity_schema)?;
            identity_writer.write(&batch).map_err(|e| {
                AppError::ExportError(format!("Identity parquet write error: {}", e))
            })?;
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
                    .map_err(|e| AppError::ExportError(format!("Parquet write error: {}", e)))?;
            }
        }

        // Close all writers
        all_writer
            .close()
            .map_err(|e| AppError::ExportError(format!("Failed to close all.parquet: {}", e)))?;
        identity_writer.close().map_err(|e| {
            AppError::ExportError(format!("Failed to close identity.parquet: {}", e))
        })?;

        for (portal_key, writer) in portal_writers {
            let (_, ref fname) = portal_info[&portal_key];
            writer.close().map_err(|e| {
                AppError::ExportError(format!("Failed to close {}.parquet: {}", fname, e))
            })?;
        }

        // Build per-portal stats with accurate names and URLs
        let mut portal_stats: Vec<PortalExportStats> = portal_counts
            .into_iter()
            .map(|(portal_key, count)| {
                let (ref name, _) = portal_info[&portal_key];
                let portal_type = self
                    .portal_types
                    .get(&portal_key)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                let profile = self.portal_profiles.get(&portal_key).cloned().flatten();
                PortalExportStats {
                    name: name.clone(),
                    url: portal_key,
                    count,
                    portal_type,
                    profile,
                }
            })
            .collect();
        // Stable order: count desc, then name/url asc, so identical inputs yield
        // identical manifest and report ordering (portal_stats comes from a HashMap).
        portal_stats.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| a.name.cmp(&b.name))
                .then_with(|| a.url.cmp(&b.url))
        });

        Ok(StreamOutcome {
            exported: total_exported,
            filtered: total_filtered,
            duplicates: total_duplicates,
            portals: portal_stats,
            quality,
        })
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
        let metadata = &dataset.metadata;
        // Canonicalize so alias/mirror URLs fold onto their primary portal for
        // name/language resolution, partitioning, and the exported source_portal.
        let canonical_url = self.canonical_portal_url(&dataset.source_portal);

        let portal_name = self
            .portal_names
            .get(&canonical_url)
            .cloned()
            .unwrap_or_else(|| portal_name_from_url(&canonical_url));

        let language = self
            .portal_languages
            .get(&canonical_url)
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
            source_portal: canonical_url,
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

/// Returns the Arrow schema for the slim `identity.parquet` snapshot fingerprint.
///
/// Carries only the stable identity (`source_portal` canonicalized + `original_id`)
/// and a `content_hash` fingerprint, so a later export can diff against this
/// snapshot without re-reading the full `all.parquet`.
fn identity_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("source_portal", DataType::Utf8, false),
        Field::new("original_id", DataType::Utf8, false),
        Field::new("content_hash", DataType::Utf8, true),
    ]))
}

/// One row of the identity fingerprint file.
struct IdentityRecord {
    source_portal: String,
    original_id: String,
    content_hash: Option<String>,
}

/// Builds an Arrow RecordBatch from a slice of identity records.
fn build_identity_batch(
    records: &[IdentityRecord],
    schema: &Arc<Schema>,
) -> Result<RecordBatch, AppError> {
    let len = records.len();
    let mut source_portal = StringBuilder::with_capacity(len, len * 64);
    let mut original_id = StringBuilder::with_capacity(len, len * 32);
    let mut content_hash = StringBuilder::with_capacity(len, len * 64);

    for r in records {
        source_portal.append_value(&r.source_portal);
        original_id.append_value(&r.original_id);
        content_hash.append_option(r.content_hash.as_deref());
    }

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(source_portal.finish()),
            Arc::new(original_id.finish()),
            Arc::new(content_hash.finish()),
        ],
    )
    .map_err(|e| AppError::ExportError(format!("Failed to build identity RecordBatch: {}", e)))
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
    .map_err(|e| AppError::ExportError(format!("Failed to build RecordBatch: {}", e)))
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
            AppError::IoError(format!("Failed to create {}: {}", path.display(), e))
        })?;
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_props.clone()))
            .map_err(|e| AppError::ExportError(format!("Failed to create ArrowWriter: {}", e)))?;
        writers.insert(portal_key.to_string(), writer);
    }
    Ok(writers
        .get_mut(portal_key)
        .expect("writer just inserted above"))
}

// =============================================================================
// Snapshot Manifest Helpers
// =============================================================================

/// Hashes the logical portal configuration used to resolve export provenance.
///
/// The serialized representation is deterministic for these struct fields and
/// avoids leaking a local configuration path into a published manifest.
fn hash_portals_config(config: &PortalsConfig) -> String {
    let serialized =
        serde_json::to_vec(config).expect("PortalsConfig contains only serializable values");
    sha256_bytes(&serialized)
}

/// Builds integrity metadata after a Parquet writer has been closed.
fn snapshot_file(
    path: &Path,
    relative_path: &str,
    kind: &str,
    row_count: u64,
    checksum: Option<&str>,
) -> Result<SnapshotFile, AppError> {
    let size_bytes = fs::metadata(path)
        .map_err(|e| {
            AppError::IoError(format!("Failed to read {} metadata: {}", path.display(), e))
        })?
        .len();

    Ok(SnapshotFile {
        path: relative_path.to_string(),
        kind: kind.to_string(),
        row_count,
        size_bytes,
        sha256: match checksum {
            Some(checksum) => checksum.to_string(),
            None => sha256_file(path)?,
        },
    })
}

// =============================================================================
// Coverage & Quality Report Helpers
// =============================================================================

/// Converts a count map into coverage buckets ordered by count (desc) then key
/// (asc) so the report is deterministic for identical inputs.
fn sorted_buckets(counts: HashMap<String, u64>) -> Vec<CoverageBucket> {
    let mut buckets: Vec<CoverageBucket> = counts
        .into_iter()
        .map(|(key, count)| CoverageBucket { key, count })
        .collect();
    buckets.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.key.cmp(&b.key)));
    buckets
}

/// Computes a field's completeness as a present count and a rate rounded to
/// four decimals.
fn field_completeness(present: u64, total: u64) -> FieldCompleteness {
    let rate = if total == 0 {
        0.0
    } else {
        ((present as f64 / total as f64) * 10_000.0).round() / 10_000.0
    };
    FieldCompleteness { present, rate }
}

/// Renders a concise human-readable report for the dataset card / release notes.
fn render_report_markdown(report: &SnapshotReport) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(out, "# Ceres Snapshot Report — {}", report.snapshot_date);
    let _ = writeln!(out);
    let _ = writeln!(out, "- Snapshot ID: `{}`", report.snapshot_id);
    let _ = writeln!(out, "- Generated at: {}", report.generated_at);
    let _ = writeln!(out, "- Report schema: {}", report.schema_version);
    let _ = writeln!(out);

    let c = &report.curation;
    let _ = writeln!(out, "## Curation");
    let _ = writeln!(out);
    let _ = writeln!(out, "| Metric | Count |");
    let _ = writeln!(out, "| --- | ---: |");
    let _ = writeln!(out, "| Raw rows | {} |", c.raw);
    let _ = writeln!(out, "| Exported | {} |", c.exported);
    let _ = writeln!(out, "| Filtered (noise) | {} |", c.filtered);
    let _ = writeln!(out, "| Duplicate-flagged | {} |", c.duplicate_flagged);
    let _ = writeln!(out, "| Excluded portals | {} |", c.excluded_portals.len());
    let _ = writeln!(out);

    let cov = &report.coverage;
    let _ = writeln!(
        out,
        "## Coverage — {} datasets across {} portals",
        cov.total_datasets, cov.portals
    );
    let _ = writeln!(out);
    write_bucket_table(&mut out, "By portal type", &cov.by_portal_type);
    write_bucket_table(&mut out, "By profile", &cov.by_profile);
    write_bucket_table(&mut out, "By language", &cov.by_language);

    let f = &report.field_completeness;
    let _ = writeln!(out, "## Field completeness ({} datasets)", f.total);
    let _ = writeln!(out);
    let _ = writeln!(out, "| Field | Present | Rate |");
    let _ = writeln!(out, "| --- | ---: | ---: |");
    write_completeness_row(&mut out, "description", &f.description);
    write_completeness_row(&mut out, "license", &f.license);
    write_completeness_row(&mut out, "organization", &f.organization);
    write_completeness_row(&mut out, "tags", &f.tags);
    write_completeness_row(&mut out, "modification_date", &f.modification_date);
    let _ = writeln!(out);

    out
}

/// Writes a coverage bucket table, capped to the top entries for readability.
fn write_bucket_table(out: &mut String, title: &str, buckets: &[CoverageBucket]) {
    use std::fmt::Write as _;

    let _ = writeln!(out, "### {}", title);
    let _ = writeln!(out);
    let _ = writeln!(out, "| Key | Count |");
    let _ = writeln!(out, "| --- | ---: |");
    for bucket in buckets.iter().take(15) {
        let _ = writeln!(out, "| {} | {} |", bucket.key, bucket.count);
    }
    if buckets.len() > 15 {
        let _ = writeln!(out, "| … ({} more) | |", buckets.len() - 15);
    }
    let _ = writeln!(out);
}

/// Writes one field-completeness row as a percentage.
fn write_completeness_row(out: &mut String, field: &str, completeness: &FieldCompleteness) {
    use std::fmt::Write as _;

    let _ = writeln!(
        out,
        "| {} | {} | {:.1}% |",
        field,
        completeness.present,
        completeness.rate * 100.0
    );
}

// =============================================================================
// Snapshot Changelog Helpers
// =============================================================================

/// Stable identity key for a dataset: (canonical source_portal, original_id).
type IdentityKey = (String, String);

/// How "changed" is defined, documented in the changelog artifact itself.
const CHANGELOG_CHANGED_DEFINITION: &str = "A dataset (keyed by source_portal + \
    original_id) is 'changed' when its content_hash (SHA-256 of title + description) \
    differs from the previous snapshot. Source modification timestamps are not used \
    because portal coverage of them is incomplete.";

/// Builds a snapshot-to-snapshot changelog by diffing the current identity index
/// against a previous snapshot's `identity.parquet`.
///
/// When no previous snapshot is supplied, or it lacks a readable
/// `identity.parquet`, returns a zeroed changelog with `compared == false`.
fn build_changelog(
    snapshot_id: &str,
    snapshot_date: &str,
    generated_at: &str,
    current_identity: &Path,
    previous: Option<&Path>,
) -> Result<SnapshotChangelog, AppError> {
    let base = |previous_snapshot_id, compared, totals, by_portal| SnapshotChangelog {
        schema_version: SNAPSHOT_CHANGELOG_SCHEMA_VERSION.to_string(),
        snapshot_id: snapshot_id.to_string(),
        snapshot_date: snapshot_date.to_string(),
        generated_at: generated_at.to_string(),
        previous_snapshot_id,
        compared,
        changed_definition: CHANGELOG_CHANGED_DEFINITION.to_string(),
        totals,
        by_portal,
    };

    let previous_identity = previous.map(|dir| dir.join("identity.parquet"));
    let baseline_ready = previous_identity
        .as_ref()
        .map(|p| p.exists())
        .unwrap_or(false);
    if !baseline_ready {
        return Ok(base(None, false, ChangelogCounts::default(), Vec::new()));
    }

    let previous_index = read_identity_index(&previous_identity.unwrap())?;
    let current_index = read_identity_index(current_identity)?;
    let previous_snapshot_id = previous.and_then(read_previous_snapshot_id);

    // Diff keyed by identity; bucket per canonical portal (the key's first part).
    let mut per_portal: HashMap<String, ChangelogCounts> = HashMap::new();
    let mut totals = ChangelogCounts::default();

    for (key, current_hash) in &current_index {
        let bucket = per_portal.entry(key.0.clone()).or_default();
        match previous_index.get(key) {
            None => {
                bucket.added += 1;
                totals.added += 1;
            }
            Some(previous_hash) if previous_hash == current_hash => {
                bucket.unchanged += 1;
                totals.unchanged += 1;
            }
            Some(_) => {
                bucket.changed += 1;
                totals.changed += 1;
            }
        }
    }
    for key in previous_index.keys() {
        if !current_index.contains_key(key) {
            let bucket = per_portal.entry(key.0.clone()).or_default();
            bucket.removed += 1;
            totals.removed += 1;
        }
    }

    let mut by_portal: Vec<PortalChangelog> = per_portal
        .into_iter()
        .map(|(portal, c)| PortalChangelog {
            portal,
            added: c.added,
            changed: c.changed,
            removed: c.removed,
            unchanged: c.unchanged,
        })
        .collect();
    // Most-active portals first, then by name, for a deterministic, useful order.
    by_portal.sort_by(|a, b| {
        let activity = |p: &PortalChangelog| p.added + p.changed + p.removed;
        activity(b)
            .cmp(&activity(a))
            .then_with(|| a.portal.cmp(&b.portal))
    });

    Ok(base(previous_snapshot_id, true, totals, by_portal))
}

/// Reads an `identity.parquet` into an identity -> content_hash map.
fn read_identity_index(path: &Path) -> Result<HashMap<IdentityKey, Option<String>>, AppError> {
    let file = fs::File::open(path)
        .map_err(|e| AppError::IoError(format!("Failed to open {}: {}", path.display(), e)))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| {
            AppError::ExportError(format!("Failed to open {} for diff: {}", path.display(), e))
        })?
        .build()
        .map_err(|e| {
            AppError::ExportError(format!("Failed to read {} for diff: {}", path.display(), e))
        })?;

    let mut index = HashMap::new();
    for batch in reader {
        let batch = batch
            .map_err(|e| AppError::ExportError(format!("Identity parquet read error: {}", e)))?;
        let source_portal = identity_column(&batch, 0, "source_portal")?;
        let original_id = identity_column(&batch, 1, "original_id")?;
        let content_hash = identity_column(&batch, 2, "content_hash")?;
        for row in 0..batch.num_rows() {
            let key = (
                source_portal.value(row).to_string(),
                original_id.value(row).to_string(),
            );
            let hash = if content_hash.is_null(row) {
                None
            } else {
                Some(content_hash.value(row).to_string())
            };
            index.insert(key, hash);
        }
    }
    Ok(index)
}

/// Downcasts an identity-file column to a `StringArray`, with a clear error.
fn identity_column<'a>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a StringArray, AppError> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            AppError::ExportError(format!(
                "identity.parquet column '{}' is not a string",
                name
            ))
        })
}

/// Reads the `snapshot_id` from a previous snapshot's `metadata.json`, if present.
fn read_previous_snapshot_id(dir: &Path) -> Option<String> {
    let bytes = fs::read(dir.join("metadata.json")).ok()?;
    let value: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    value
        .get("snapshot_id")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

/// Renders a concise human-readable changelog for the dataset card / release notes.
fn render_changelog_markdown(changelog: &SnapshotChangelog) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let _ = writeln!(
        out,
        "# Ceres Snapshot Changelog — {}",
        changelog.snapshot_date
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "- Snapshot ID: `{}`", changelog.snapshot_id);

    if !changelog.compared {
        let _ = writeln!(out);
        let _ = writeln!(
            out,
            "No comparable previous snapshot was supplied; all rows are treated as \
             the baseline for future changelogs."
        );
        return out;
    }

    let previous = changelog
        .previous_snapshot_id
        .as_deref()
        .unwrap_or("unknown");
    let t = &changelog.totals;
    let _ = writeln!(out, "- Previous snapshot ID: `{}`", previous);
    let _ = writeln!(out);
    let _ = writeln!(out, "## Totals");
    let _ = writeln!(out);
    let _ = writeln!(out, "| Change | Count |");
    let _ = writeln!(out, "| --- | ---: |");
    let _ = writeln!(out, "| Added | {} |", t.added);
    let _ = writeln!(out, "| Changed | {} |", t.changed);
    let _ = writeln!(out, "| Removed | {} |", t.removed);
    let _ = writeln!(out, "| Unchanged | {} |", t.unchanged);
    let _ = writeln!(out);
    let _ = writeln!(out, "_{}_", changelog.changed_definition);
    let _ = writeln!(out);

    let _ = writeln!(out, "## Most active portals");
    let _ = writeln!(out);
    let _ = writeln!(out, "| Portal | Added | Changed | Removed | Unchanged |");
    let _ = writeln!(out, "| --- | ---: | ---: | ---: | ---: |");
    for portal in changelog.by_portal.iter().take(15) {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} |",
            portal.portal, portal.added, portal.changed, portal.removed, portal.unchanged
        );
    }
    if changelog.by_portal.len() > 15 {
        let _ = writeln!(
            out,
            "| … ({} more) | | | | |",
            changelog.by_portal.len() - 15
        );
    }
    let _ = writeln!(out);

    out
}

/// Returns the SHA-256 digest of a file without loading it into memory.
fn sha256_file(path: &Path) -> Result<String, AppError> {
    let mut file = fs::File::open(path)
        .map_err(|e| AppError::IoError(format!("Failed to open {}: {}", path.display(), e)))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];

    loop {
        let read = file.read(&mut buffer).map_err(|e| {
            AppError::IoError(format!(
                "Failed to read {} for checksum: {}",
                path.display(),
                e
            ))
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
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
