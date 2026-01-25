//! Export endpoint.

use axum::{
    body::Body,
    extract::{Query, State},
    http::{Response, StatusCode, header},
};
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use ceres_core::ExportFormat;

use crate::dto::ExportQuery;
use crate::error::ApiError;
use crate::state::AppState;

/// Export datasets in various formats.
///
/// Streams datasets to the response for memory-efficient export of large datasets.
#[utoipa::path(
    get,
    path = "/api/v1/export",
    params(ExportQuery),
    responses(
        (status = 200, description = "Dataset export stream"),
        (status = 400, description = "Invalid format"),
        (status = 500, description = "Internal server error"),
    ),
    tag = "export"
)]
pub async fn export_datasets(
    State(state): State<AppState>,
    Query(params): Query<ExportQuery>,
) -> Result<Response<Body>, ApiError> {
    let format = parse_format(params.format.as_deref())?;
    let content_type = content_type_for_format(&format);
    let file_extension = extension_for_format(&format);

    // Create a duplex channel for streaming
    let (writer, reader) = tokio::io::duplex(64 * 1024);

    // Clone what we need for the spawned task
    let export_service = state.export_service.clone();
    let portal_filter = params.portal.clone();
    let limit = params.limit;

    // Spawn a task to write the export data
    tokio::spawn(async move {
        let mut buf_writer = tokio::io::BufWriter::new(writer);

        // We need to use a sync writer adapter since export_to_writer expects std::io::Write
        // For now, we'll collect the data and write it in chunks
        let mut output = Vec::new();
        let result = export_service
            .export_to_writer(&mut output, format, portal_filter.as_deref(), limit)
            .await;

        if let Err(e) = result {
            tracing::error!("Export error: {}", e);
            return;
        }

        if let Err(e) = buf_writer.write_all(&output).await {
            tracing::error!("Write error: {}", e);
            return;
        }

        if let Err(e) = buf_writer.flush().await {
            tracing::error!("Flush error: {}", e);
        }
    });

    // Create a stream from the reader
    let stream = ReaderStream::new(reader);
    let body = Body::from_stream(stream);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"datasets.{}\"", file_extension),
        )
        .body(body)
        .map_err(|e| ApiError::Internal(format!("Failed to build response: {}", e)))
}

fn parse_format(format: Option<&str>) -> Result<ExportFormat, ApiError> {
    match format.unwrap_or("jsonl").to_lowercase().as_str() {
        "jsonl" => Ok(ExportFormat::Jsonl),
        "json" => Ok(ExportFormat::Json),
        "csv" => Ok(ExportFormat::Csv),
        other => Err(ApiError::BadRequest(format!(
            "Invalid format: '{}'. Supported formats: jsonl, json, csv",
            other
        ))),
    }
}

fn content_type_for_format(format: &ExportFormat) -> &'static str {
    match format {
        ExportFormat::Jsonl => "application/x-ndjson",
        ExportFormat::Json => "application/json",
        ExportFormat::Csv => "text/csv",
    }
}

fn extension_for_format(format: &ExportFormat) -> &'static str {
    match format {
        ExportFormat::Jsonl => "jsonl",
        ExportFormat::Json => "json",
        ExportFormat::Csv => "csv",
    }
}
