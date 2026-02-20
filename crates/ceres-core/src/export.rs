//! Export service for streaming dataset exports.
//!
//! This module provides the [`ExportService`] for memory-efficient export
//! of datasets to various formats (JSONL, JSON, CSV).
//!
//! # Example
//!
//! ```ignore
//! use ceres_core::export::{ExportService, ExportFormat};
//! use std::io::stdout;
//!
//! let export_service = ExportService::new(store);
//! let mut writer = stdout().lock();
//! let count = export_service
//!     .export_to_writer(&mut writer, ExportFormat::Jsonl, None, None)
//!     .await?;
//! println!("Exported {} datasets", count);
//! ```
//!
//! Note: This example uses `ignore` because it requires a concrete
//! [`DatasetStore`] implementation which cannot be easily provided in a doctest.

use std::io::Write;

use futures::StreamExt;
use serde::Serialize;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::error::AppError;
use crate::models::Dataset;
use crate::traits::DatasetStore;

/// Supported export formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    /// JSON Lines format (one JSON object per line).
    /// Most efficient for streaming and large datasets.
    Jsonl,
    /// Standard JSON array format.
    /// Streams with manual bracket handling.
    Json,
    /// CSV format (comma-separated values).
    Csv,
}

/// Service for exporting datasets in streaming mode.
///
/// Uses [`DatasetStore::list_stream`] to fetch datasets incrementally,
/// avoiding loading all data into memory at once.
pub struct ExportService<S>
where
    S: DatasetStore,
{
    store: S,
}

impl<S> Clone for ExportService<S>
where
    S: DatasetStore + Clone,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
        }
    }
}

impl<S> ExportService<S>
where
    S: DatasetStore,
{
    /// Creates a new export service with the given store.
    pub fn new(store: S) -> Self {
        Self { store }
    }

    /// Exports datasets to a writer in streaming mode.
    ///
    /// # Arguments
    ///
    /// * `writer` - The output writer (e.g., stdout, file)
    /// * `format` - The export format (JSONL, JSON, or CSV)
    /// * `portal_filter` - Optional portal URL to filter by
    /// * `limit` - Optional maximum number of records
    ///
    /// # Returns
    ///
    /// The number of datasets exported.
    pub async fn export_to_writer<W: Write>(
        &self,
        writer: &mut W,
        format: ExportFormat,
        portal_filter: Option<&str>,
        limit: Option<usize>,
    ) -> Result<u64, AppError> {
        let mut stream = self.store.list_stream(portal_filter, limit);
        let mut count = 0u64;

        match format {
            ExportFormat::Jsonl => {
                while let Some(result) = stream.next().await {
                    let dataset = result?;
                    let record = create_export_record(&dataset);
                    let json = serde_json::to_string(&record)
                        .map_err(|e| AppError::Generic(e.to_string()))?;
                    writeln!(writer, "{}", json).map_err(|e| AppError::Generic(e.to_string()))?;
                    count += 1;
                }
            }
            ExportFormat::Json => {
                writeln!(writer, "[").map_err(|e| AppError::Generic(e.to_string()))?;
                let mut first = true;

                while let Some(result) = stream.next().await {
                    let dataset = result?;
                    let record = create_export_record(&dataset);

                    if !first {
                        writeln!(writer, ",").map_err(|e| AppError::Generic(e.to_string()))?;
                    }
                    first = false;

                    let json = serde_json::to_string_pretty(&record)
                        .map_err(|e| AppError::Generic(e.to_string()))?;
                    // Indent each line for proper formatting
                    for line in json.lines() {
                        writeln!(writer, "  {}", line)
                            .map_err(|e| AppError::Generic(e.to_string()))?;
                    }
                    count += 1;
                }

                writeln!(writer, "]").map_err(|e| AppError::Generic(e.to_string()))?;
            }
            ExportFormat::Csv => {
                writeln!(
                    writer,
                    "id,original_id,source_portal,url,title,description,first_seen_at,last_updated_at"
                )
                .map_err(|e| AppError::Generic(e.to_string()))?;

                while let Some(result) = stream.next().await {
                    let dataset = result?;
                    write_csv_row(writer, &dataset)?;
                    count += 1;
                }
            }
        }

        writer
            .flush()
            .map_err(|e| AppError::Generic(e.to_string()))?;
        Ok(count)
    }

    /// Exports datasets to an async writer in streaming mode.
    ///
    /// This method writes directly to the async writer without buffering the entire
    /// dataset in memory, making it suitable for HTTP streaming responses.
    ///
    /// # Arguments
    ///
    /// * `writer` - The async output writer
    /// * `format` - The export format (JSONL, JSON, or CSV)
    /// * `portal_filter` - Optional portal URL to filter by
    /// * `limit` - Optional maximum number of records
    ///
    /// # Returns
    ///
    /// The number of datasets exported.
    pub async fn export_to_async_writer<W: AsyncWrite + Unpin>(
        &self,
        writer: &mut W,
        format: ExportFormat,
        portal_filter: Option<&str>,
        limit: Option<usize>,
    ) -> Result<u64, AppError> {
        let mut stream = self.store.list_stream(portal_filter, limit);
        let mut count = 0u64;

        match format {
            ExportFormat::Jsonl => {
                while let Some(result) = stream.next().await {
                    let dataset = result?;
                    let record = create_export_record(&dataset);
                    let mut json = serde_json::to_string(&record)
                        .map_err(|e| AppError::Generic(e.to_string()))?;
                    json.push('\n');
                    writer
                        .write_all(json.as_bytes())
                        .await
                        .map_err(|e| AppError::Generic(e.to_string()))?;
                    count += 1;
                }
            }
            ExportFormat::Json => {
                writer
                    .write_all(b"[\n")
                    .await
                    .map_err(|e| AppError::Generic(e.to_string()))?;
                let mut first = true;

                while let Some(result) = stream.next().await {
                    let dataset = result?;
                    let record = create_export_record(&dataset);

                    if !first {
                        writer
                            .write_all(b",\n")
                            .await
                            .map_err(|e| AppError::Generic(e.to_string()))?;
                    }
                    first = false;

                    let json = serde_json::to_string_pretty(&record)
                        .map_err(|e| AppError::Generic(e.to_string()))?;
                    // Indent each line for proper formatting
                    for line in json.lines() {
                        writer
                            .write_all(format!("  {}\n", line).as_bytes())
                            .await
                            .map_err(|e| AppError::Generic(e.to_string()))?;
                    }
                    count += 1;
                }

                writer
                    .write_all(b"]\n")
                    .await
                    .map_err(|e| AppError::Generic(e.to_string()))?;
            }
            ExportFormat::Csv => {
                writer
                    .write_all(
                        b"id,original_id,source_portal,url,title,description,first_seen_at,last_updated_at\n",
                    )
                    .await
                    .map_err(|e| AppError::Generic(e.to_string()))?;

                while let Some(result) = stream.next().await {
                    let dataset = result?;
                    let row = format_csv_row(&dataset);
                    writer
                        .write_all(row.as_bytes())
                        .await
                        .map_err(|e| AppError::Generic(e.to_string()))?;
                    count += 1;
                }
            }
        }

        writer
            .flush()
            .await
            .map_err(|e| AppError::Generic(e.to_string()))?;
        Ok(count)
    }
}

/// Record structure for JSON/JSONL export.
#[derive(Serialize)]
struct ExportRecord {
    id: uuid::Uuid,
    original_id: String,
    source_portal: String,
    url: String,
    title: String,
    description: Option<String>,
    metadata: serde_json::Value,
    first_seen_at: chrono::DateTime<chrono::Utc>,
    last_updated_at: chrono::DateTime<chrono::Utc>,
}

fn create_export_record(dataset: &Dataset) -> ExportRecord {
    ExportRecord {
        id: dataset.id,
        original_id: dataset.original_id.clone(),
        source_portal: dataset.source_portal.clone(),
        url: dataset.url.clone(),
        title: dataset.title.clone(),
        description: dataset.description.clone(),
        metadata: dataset.metadata.clone(),
        first_seen_at: dataset.first_seen_at,
        last_updated_at: dataset.last_updated_at,
    }
}

fn write_csv_row<W: Write>(writer: &mut W, dataset: &Dataset) -> Result<(), AppError> {
    let row = format_csv_row(dataset);
    writer
        .write_all(row.as_bytes())
        .map_err(|e| AppError::Generic(e.to_string()))?;
    Ok(())
}

fn format_csv_row(dataset: &Dataset) -> String {
    let description = dataset
        .description
        .as_ref()
        .map(|d| escape_csv(d))
        .unwrap_or_default();

    format!(
        "{},{},{},{},{},{},{},{}\n",
        dataset.id,
        escape_csv(&dataset.original_id),
        escape_csv(&dataset.source_portal),
        escape_csv(&dataset.url),
        escape_csv(&dataset.title),
        description,
        dataset.first_seen_at.format("%Y-%m-%dT%H:%M:%SZ"),
        dataset.last_updated_at.format("%Y-%m-%dT%H:%M:%SZ"),
    )
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
    fn test_escape_csv_no_special_chars() {
        assert_eq!(escape_csv("hello"), "hello");
    }

    #[test]
    fn test_escape_csv_with_comma() {
        assert_eq!(escape_csv("hello, world"), "\"hello, world\"");
    }

    #[test]
    fn test_escape_csv_with_quote() {
        assert_eq!(escape_csv("hello \"world\""), "\"hello \"\"world\"\"\"");
    }

    #[test]
    fn test_escape_csv_with_newline() {
        assert_eq!(escape_csv("hello\nworld"), "\"hello\nworld\"");
    }
}
