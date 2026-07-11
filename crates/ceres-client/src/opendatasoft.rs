//! OpenDataSoft Explore API v2.1 client.
//!
//! OpenDataSoft portals expose their catalog at
//! `/api/explore/v2.1/catalog/datasets` with `limit`/`offset` pagination.
//! The API enforces two hard limits: `limit` is capped at 100 per page, and
//! `limit + offset` must stay below 10,000. Small catalogs are walked with
//! plain offset pagination; deeper catalogs (for example the
//! `data.opendatasoft.com` federation hub) are walked with a keyset cursor on
//! the `modified` timestamp (`order_by=modified desc` plus
//! `where=modified <= date'...'`), since ODSQL rejects range comparisons on
//! text fields such as `dataset_id`. Datasets without a `modified` timestamp
//! are collected in a final `where=modified is null` sweep.
//!
//! The complete catalog entry (including the `fields` schema hints) is
//! preserved in [`NewDataset::metadata`].

use std::collections::HashSet;
use std::time::Duration;

use ceres_core::HttpConfig;
use ceres_core::error::AppError;
use ceres_core::models::NewDataset;
use ceres_core::traits::PortalClient;
use chrono::{DateTime, SecondsFormat, Utc};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::sleep;

/// Maximum page size accepted by the Explore API (`-1 <= limit <= 100`).
const PAGE_SIZE: usize = 100;
/// Highest offset we request. The API rejects `limit + offset >= 10_000`
/// with an opaque HTTP 500, so stay well clear of the boundary.
const OFFSET_WINDOW: usize = 9_000;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// A normalized OpenDataSoft dataset plus its complete catalog entry.
#[derive(Debug, Clone)]
pub struct OpenDataSoftDataset {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub landing_page: String,
    pub modified: Option<DateTime<Utc>>,
    pub themes: Vec<String>,
    pub keywords: Vec<String>,
    pub license: Option<String>,
    pub publisher: Option<String>,
    pub raw: Value,
}

#[derive(Debug, Deserialize)]
struct CatalogResponse {
    #[serde(default)]
    results: Vec<Value>,
    #[serde(default)]
    total_count: usize,
}

struct CatalogPage {
    datasets: Vec<OpenDataSoftDataset>,
    raw_count: usize,
    total_count: usize,
}

/// Which slice of the catalog a full-sync walk is currently reading.
#[derive(Clone, Copy, PartialEq)]
enum WalkPhase {
    /// Datasets with a `modified` timestamp, newest first.
    Dated,
    /// Datasets without a `modified` timestamp (rare; final sweep).
    Undated,
    Finished,
}

struct WalkState {
    client: OpenDataSoftClient,
    phase: WalkPhase,
    offset: usize,
    /// Upper bound for the next dated query (`modified <= cursor`).
    cursor: Option<DateTime<Utc>>,
    seen_ids: HashSet<String>,
    /// Unique datasets collected when the current cursor was set; used to
    /// detect a cursor that cannot advance.
    seen_at_cursor: usize,
}

/// Client for one OpenDataSoft portal's Explore API v2.1 catalog.
#[derive(Clone)]
pub struct OpenDataSoftClient {
    client: Client,
    base_url: Url,
    catalog_url: Url,
    request_timeout: Duration,
    max_retries: u32,
    retry_base_delay: Duration,
    page_size: usize,
    offset_window: usize,
}

impl OpenDataSoftClient {
    /// Creates a client and reads an optional API key from `ODS_API_KEY`.
    pub fn new(base_url: &str) -> Result<Self, AppError> {
        let api_key = std::env::var("ODS_API_KEY")
            .ok()
            .filter(|key| !key.trim().is_empty());
        Self::new_with_api_key(base_url, api_key)
    }

    /// Creates a client with an explicitly supplied optional API key.
    pub fn new_with_api_key(base_url: &str, api_key: Option<String>) -> Result<Self, AppError> {
        Self::new_with_api_key_and_http_config(base_url, api_key, HttpConfig::from_env())
    }

    fn new_with_api_key_and_http_config(
        base_url: &str,
        api_key: Option<String>,
        http_config: HttpConfig,
    ) -> Result<Self, AppError> {
        let mut base_url =
            Url::parse(base_url).map_err(|_| AppError::InvalidPortalUrl(base_url.to_string()))?;
        if base_url.host_str().is_none_or(str::is_empty) {
            return Err(AppError::InvalidPortalUrl(base_url.to_string()));
        }
        base_url.set_query(None);
        base_url.set_fragment(None);
        let catalog_url = base_url
            .join("/api/explore/v2.1/catalog/datasets")
            .map_err(|error| AppError::InvalidPortalUrl(error.to_string()))?;

        let mut headers = HeaderMap::new();
        if let Some(key) = api_key.filter(|key| !key.trim().is_empty()) {
            let value = HeaderValue::from_str(&format!("Apikey {}", key.trim())).map_err(|_| {
                AppError::ConfigError(
                    "ODS_API_KEY contains characters that are invalid in an HTTP header"
                        .to_string(),
                )
            })?;
            headers.insert(AUTHORIZATION, value);
        }

        let client = Client::builder()
            .user_agent("Ceres/0.6 (open-data-harvester)")
            .default_headers(headers)
            .build()
            .map_err(|error| AppError::ClientError(error.to_string()))?;

        Ok(Self {
            client,
            base_url,
            catalog_url,
            request_timeout: http_config.timeout,
            max_retries: http_config.max_retries,
            retry_base_delay: http_config.retry_base_delay,
            page_size: PAGE_SIZE,
            offset_window: OFFSET_WINDOW,
        })
    }

    pub fn portal_type(&self) -> &'static str {
        "opendatasoft"
    }

    pub fn base_url(&self) -> &str {
        self.base_url.as_str()
    }

    pub async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        Ok(self
            .search_all_datasets()
            .await?
            .into_iter()
            .map(|dataset| dataset.id)
            .collect())
    }

    pub async fn get_dataset(&self, id: &str) -> Result<OpenDataSoftDataset, AppError> {
        let url = Url::parse(&format!("{}/{id}", self.catalog_url))
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        let response = match self.request_with_retry(&url).await {
            Ok(response) => response,
            Err(AppError::ClientError(message)) if message.starts_with("HTTP 404") => {
                return Err(AppError::DatasetNotFound(id.to_string()));
            }
            Err(error) => return Err(error),
        };
        let body = response
            .bytes()
            .await
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        let raw: Value = serde_json::from_slice(&body)?;
        extract_dataset(raw, &self.base_url)
            .ok_or_else(|| AppError::DatasetNotFound(id.to_string()))
    }

    pub async fn search_modified_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<OpenDataSoftDataset>, AppError> {
        let where_clause = format!("modified >= date'{}'", format_odsql_date(since));
        let mut offset = 0;
        let mut datasets = Vec::new();
        let mut seen_ids = HashSet::new();

        loop {
            let page = self
                .fetch_page(offset, Some("modified desc"), Some(&where_clause))
                .await?;
            if page.total_count > self.offset_window {
                // More modified datasets than the offset window can reach;
                // let the harvest service fall back to a full sync.
                return Err(AppError::Generic(format!(
                    "{} datasets modified since {since} exceed the OpenDataSoft pagination window",
                    page.total_count
                )));
            }
            let done = page.raw_count == 0 || offset + page.raw_count >= page.total_count;
            offset += page.raw_count;
            datasets.extend(
                page.datasets
                    .into_iter()
                    .filter(|dataset| seen_ids.insert(dataset.id.clone())),
            );
            if done {
                break;
            }
        }

        Ok(datasets)
    }

    pub async fn search_all_datasets(&self) -> Result<Vec<OpenDataSoftDataset>, AppError> {
        let mut stream = self.search_all_datasets_stream();
        let mut datasets = Vec::new();
        while let Some(page) = stream.next().await {
            datasets.extend(page?);
        }
        Ok(datasets)
    }

    /// Streams the full catalog page-by-page.
    ///
    /// Dated datasets are walked newest-first; whenever the next request
    /// would cross the offset window the walk restarts at offset 0 with
    /// `where=modified <= date'<oldest seen>'` (inclusive, deduplicated by
    /// dataset id). A final sweep collects datasets without a `modified`
    /// timestamp.
    pub fn search_all_datasets_stream(
        &self,
    ) -> BoxStream<'_, Result<Vec<OpenDataSoftDataset>, AppError>> {
        let state = WalkState {
            client: self.clone(),
            phase: WalkPhase::Dated,
            offset: 0,
            cursor: None,
            seen_ids: HashSet::new(),
            seen_at_cursor: 0,
        };
        Box::pin(stream::unfold(state, |mut state| async move {
            if state.phase == WalkPhase::Finished {
                return None;
            }

            let (order_by, where_clause) = match state.phase {
                WalkPhase::Dated => {
                    let clause = match state.cursor {
                        Some(cursor) => format!(
                            "modified is not null and modified <= date'{}'",
                            format_odsql_date(cursor)
                        ),
                        None => "modified is not null".to_string(),
                    };
                    (Some("modified desc"), clause)
                }
                WalkPhase::Undated => (None, "modified is null".to_string()),
                WalkPhase::Finished => unreachable!(),
            };

            let page = match state
                .client
                .fetch_page(state.offset, order_by, Some(&where_clause))
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    state.phase = WalkPhase::Finished;
                    return Some((Err(error), state));
                }
            };

            let query_exhausted =
                page.raw_count == 0 || state.offset + page.raw_count >= page.total_count;
            let next_offset = state.offset + page.raw_count;
            let oldest_modified = page
                .datasets
                .iter()
                .filter_map(|dataset| dataset.modified)
                .min();
            let datasets: Vec<_> = page
                .datasets
                .into_iter()
                .filter(|dataset| state.seen_ids.insert(dataset.id.clone()))
                .collect();

            match state.phase {
                WalkPhase::Dated => {
                    if query_exhausted {
                        state.phase = WalkPhase::Undated;
                        state.offset = 0;
                    } else if next_offset + state.client.page_size > state.client.offset_window {
                        // Restart within the window from the oldest timestamp
                        // seen so far. If no new datasets arrived since the
                        // last restart the cursor cannot advance (more equal
                        // timestamps than the window holds); stop instead of
                        // looping forever.
                        let stalled =
                            state.cursor.is_some() && state.seen_ids.len() == state.seen_at_cursor;
                        match (oldest_modified.or(state.cursor), stalled) {
                            (Some(cursor), false) => {
                                state.cursor = Some(cursor);
                                state.seen_at_cursor = state.seen_ids.len();
                                state.offset = 0;
                            }
                            _ => {
                                tracing::warn!(
                                    portal = %state.client.base_url,
                                    unique_ids = state.seen_ids.len(),
                                    total_count = page.total_count,
                                    "OpenDataSoft modified-cursor cannot advance; ending dated walk early"
                                );
                                state.phase = WalkPhase::Undated;
                                state.offset = 0;
                            }
                        }
                    } else {
                        state.offset = next_offset;
                    }
                }
                WalkPhase::Undated => {
                    if query_exhausted {
                        state.phase = WalkPhase::Finished;
                    } else if next_offset + state.client.page_size > state.client.offset_window {
                        tracing::warn!(
                            portal = %state.client.base_url,
                            total_count = page.total_count,
                            "OpenDataSoft catalog has more undated datasets than the pagination window; truncating"
                        );
                        state.phase = WalkPhase::Finished;
                    } else {
                        state.offset = next_offset;
                    }
                }
                WalkPhase::Finished => unreachable!(),
            }

            Some((Ok(datasets), state))
        }))
    }

    pub async fn dataset_count(&self) -> Result<usize, AppError> {
        let mut url = self.catalog_url.clone();
        url.query_pairs_mut()
            .append_pair("limit", "1")
            .append_pair("offset", "0");
        let response = self.request_with_retry(&url).await?;
        let body = response
            .bytes()
            .await
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        let response: CatalogResponse = serde_json::from_slice(&body)?;
        Ok(response.total_count)
    }

    pub fn into_new_dataset(
        data: OpenDataSoftDataset,
        portal_url: &str,
        _url_template: Option<&str>,
        language: &str,
    ) -> NewDataset {
        let content_hash = NewDataset::compute_content_hash_with_language(
            &data.title,
            data.description.as_deref(),
            language,
        );
        NewDataset {
            original_id: data.id,
            source_portal: portal_url.to_string(),
            url: data.landing_page,
            title: data.title,
            description: data.description,
            embedding: None,
            metadata: data.raw,
            content_hash,
        }
    }

    async fn fetch_page(
        &self,
        offset: usize,
        order_by: Option<&str>,
        where_clause: Option<&str>,
    ) -> Result<CatalogPage, AppError> {
        let mut url = self.catalog_url.clone();
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("limit", &self.page_size.to_string());
            query.append_pair("offset", &offset.to_string());
            if let Some(order_by) = order_by {
                query.append_pair("order_by", order_by);
            }
            if let Some(where_clause) = where_clause {
                query.append_pair("where", where_clause);
            }
        }

        let response = self.request_with_retry(&url).await?;
        let body = response
            .bytes()
            .await
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        let response: CatalogResponse = serde_json::from_slice(&body)?;
        let raw_count = response.results.len();
        let datasets = response
            .results
            .into_iter()
            .filter_map(|result| match extract_dataset(result, &self.base_url) {
                Some(dataset) => Some(dataset),
                None => {
                    tracing::warn!(portal = %self.base_url, "Skipping malformed OpenDataSoft catalog result");
                    None
                }
            })
            .collect();

        Ok(CatalogPage {
            datasets,
            raw_count,
            total_count: response.total_count,
        })
    }

    async fn request_with_retry(&self, url: &Url) -> Result<reqwest::Response, AppError> {
        let mut last_error = AppError::Generic("No OpenDataSoft request attempted".to_string());

        for attempt in 1..=self.max_retries {
            match self
                .client
                .get(url.clone())
                .timeout(self.request_timeout)
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => return Ok(response),
                Ok(response) => {
                    let status = response.status();
                    if status == StatusCode::TOO_MANY_REQUESTS {
                        last_error = AppError::RateLimitExceeded;
                        if attempt < self.max_retries {
                            let delay = response
                                .headers()
                                .get("retry-after")
                                .and_then(|value| value.to_str().ok())
                                .and_then(|value| value.parse::<u64>().ok())
                                .map(Duration::from_secs)
                                .unwrap_or_else(|| retry_delay(self.retry_base_delay, attempt));
                            tracing::warn!(
                                attempt,
                                delay_ms = delay.as_millis(),
                                "OpenDataSoft rate limit; retrying request"
                            );
                            sleep(delay).await;
                            continue;
                        }
                        return Err(last_error);
                    }

                    if status.is_server_error() {
                        last_error = AppError::ClientError(format!(
                            "OpenDataSoft server error: HTTP {}",
                            status.as_u16()
                        ));
                        if attempt < self.max_retries {
                            sleep(self.retry_base_delay.saturating_mul(attempt)).await;
                            continue;
                        }
                        return Err(last_error);
                    }

                    return Err(AppError::ClientError(format!(
                        "HTTP {} from {}",
                        status.as_u16(),
                        url
                    )));
                }
                Err(error) => {
                    last_error = if error.is_timeout() {
                        AppError::Timeout(self.request_timeout.as_secs())
                    } else if error.is_connect() {
                        AppError::NetworkError(format!("Connection failed: {error}"))
                    } else {
                        AppError::ClientError(error.to_string())
                    };
                    if attempt < self.max_retries && (error.is_timeout() || error.is_connect()) {
                        sleep(self.retry_base_delay.saturating_mul(attempt)).await;
                        continue;
                    }
                }
            }
        }

        Err(last_error)
    }
}

impl PortalClient for OpenDataSoftClient {
    type PortalData = OpenDataSoftDataset;

    fn portal_type(&self) -> &'static str {
        OpenDataSoftClient::portal_type(self)
    }

    fn base_url(&self) -> &str {
        OpenDataSoftClient::base_url(self)
    }

    async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        OpenDataSoftClient::list_dataset_ids(self).await
    }

    async fn get_dataset(&self, id: &str) -> Result<Self::PortalData, AppError> {
        OpenDataSoftClient::get_dataset(self, id).await
    }

    fn into_new_dataset(
        data: Self::PortalData,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
    ) -> NewDataset {
        Self::into_new_dataset(data, portal_url, url_template, language)
    }

    async fn search_modified_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<Self::PortalData>, AppError> {
        OpenDataSoftClient::search_modified_since(self, since).await
    }

    async fn search_all_datasets(&self) -> Result<Vec<Self::PortalData>, AppError> {
        OpenDataSoftClient::search_all_datasets(self).await
    }

    fn search_all_datasets_stream(&self) -> BoxStream<'_, Result<Vec<Self::PortalData>, AppError>> {
        OpenDataSoftClient::search_all_datasets_stream(self)
    }

    async fn dataset_count(&self) -> Result<usize, AppError> {
        OpenDataSoftClient::dataset_count(self).await
    }
}

fn retry_delay(base_delay: Duration, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(u32::BITS - 1);
    let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
    base_delay.saturating_mul(multiplier).min(MAX_RETRY_DELAY)
}

/// Formats a timestamp as an ODSQL `date'...'` literal payload (second
/// precision, UTC). Sub-second truncation is safe because the cursor
/// comparison is inclusive and pages are deduplicated by dataset id.
fn format_odsql_date(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn extract_dataset(raw: Value, base_url: &Url) -> Option<OpenDataSoftDataset> {
    let id = non_empty_string(raw.get("dataset_id")?)?;
    let default_metas = raw.pointer("/metas/default");
    let title = default_metas
        .and_then(|metas| metas.get("title"))
        .and_then(non_empty_string)
        .unwrap_or_else(|| id.clone());
    let description = default_metas
        .and_then(|metas| metas.get("description"))
        .and_then(Value::as_str)
        .map(html_to_text)
        .filter(|value| !value.is_empty());
    let modified = default_metas
        .and_then(|metas| metas.get("modified"))
        .and_then(Value::as_str)
        .and_then(parse_datetime);
    let mut themes = default_metas
        .and_then(|metas| metas.get("theme"))
        .and_then(string_list)
        .unwrap_or_default();
    deduplicate(&mut themes);
    let mut keywords = default_metas
        .and_then(|metas| metas.get("keyword"))
        .and_then(string_list)
        .unwrap_or_default();
    deduplicate(&mut keywords);
    let license = default_metas
        .and_then(|metas| metas.get("license"))
        .and_then(text_value);
    let publisher = default_metas
        .and_then(|metas| metas.get("publisher"))
        .and_then(text_value)
        .or_else(|| raw.pointer("/metas/dcat/creator").and_then(text_value))
        .or_else(|| raw.pointer("/metas/dcat/publisher").and_then(text_value));
    let landing_page = base_url
        .join(&format!("/explore/dataset/{id}/"))
        .map(|url| url.to_string())
        .unwrap_or_else(|_| format!("{}explore/dataset/{id}/", base_url));

    Some(OpenDataSoftDataset {
        id,
        title,
        description,
        landing_page,
        modified,
        themes,
        keywords,
        license,
        publisher,
        raw,
    })
}

fn non_empty_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn html_to_text(value: &str) -> String {
    let rendered = html2text::config::plain_no_decorate()
        .string_from_read(value.as_bytes(), usize::MAX)
        .unwrap_or_else(|_| value.to_string());
    rendered.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn text_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => {
            let value = value.trim();
            (!value.is_empty()).then(|| value.to_string())
        }
        Value::Object(value) => value.get("name").and_then(text_value),
        _ => None,
    }
}

fn string_list(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::Array(values) => Some(values.iter().filter_map(text_value).collect()),
        Value::String(_) => text_value(value).map(|value| vec![value]),
        _ => None,
    }
}

fn deduplicate(values: &mut Vec<String>) {
    let mut seen = HashSet::new();
    values.retain(|value| seen.insert(value.to_lowercase()));
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::TimeZone;
    use serde_json::json;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    use super::*;

    const FIXTURE: &[u8] = include_bytes!("../tests/fixtures/opendatasoft_catalog.json");

    fn test_http_config(max_retries: u32) -> HttpConfig {
        HttpConfig {
            timeout: Duration::from_secs(5),
            max_retries,
            retry_base_delay: Duration::ZERO,
        }
    }

    fn test_client(uri: &str) -> OpenDataSoftClient {
        OpenDataSoftClient::new_with_api_key_and_http_config(uri, None, test_http_config(1))
            .unwrap()
    }

    #[test]
    fn parses_and_normalizes_fixture_without_changing_raw_metadata() {
        let response: CatalogResponse = serde_json::from_slice(FIXTURE).unwrap();
        assert_eq!(response.total_count, 2);
        let raw = response.results[0].clone();
        let base_url = Url::parse("https://opendata.example.fr").unwrap();
        let dataset = extract_dataset(raw.clone(), &base_url).unwrap();

        assert_eq!(dataset.id, "arbres-remarquables");
        assert_eq!(dataset.title, "Arbres remarquables");
        assert_eq!(
            dataset.description.as_deref(),
            Some("Inventaire des arbres remarquables avec localisation & essences.")
        );
        assert_eq!(dataset.themes, ["Environnement"]);
        assert_eq!(dataset.keywords, ["arbres", "nature"]);
        assert_eq!(
            dataset.license.as_deref(),
            Some("Open Database License (ODbL)")
        );
        assert_eq!(dataset.publisher.as_deref(), Some("Ville Exemple"));
        assert_eq!(
            dataset.modified.unwrap(),
            Utc.with_ymd_and_hms(2026, 3, 14, 9, 26, 53).unwrap()
        );
        assert_eq!(
            dataset.landing_page,
            "https://opendata.example.fr/explore/dataset/arbres-remarquables/"
        );
        assert_eq!(dataset.raw, raw);

        let normalized = OpenDataSoftClient::into_new_dataset(
            dataset,
            "https://opendata.example.fr",
            None,
            "fr",
        );
        assert_eq!(normalized.metadata, raw);
        assert_eq!(normalized.original_id, "arbres-remarquables");
        assert_eq!(normalized.content_hash.len(), 64);
    }

    #[test]
    fn applies_documented_fallbacks_for_sparse_entries() {
        let response: CatalogResponse = serde_json::from_slice(FIXTURE).unwrap();
        let base_url = Url::parse("https://opendata.example.fr").unwrap();
        let dataset = extract_dataset(response.results[1].clone(), &base_url).unwrap();

        // Title falls back to the dataset id; publisher falls back to the
        // DCAT creator; missing modified/description/license stay None.
        assert_eq!(dataset.id, "budget-2026");
        assert_eq!(dataset.title, "budget-2026");
        assert!(dataset.description.is_none());
        assert!(dataset.modified.is_none());
        assert!(dataset.license.is_none());
        assert_eq!(dataset.publisher.as_deref(), Some("Direction des Finances"));
        assert!(dataset.themes.is_empty());
        assert!(dataset.keywords.is_empty());
    }

    #[test]
    fn skips_entries_without_a_dataset_id() {
        let base_url = Url::parse("https://opendata.example.fr").unwrap();
        assert!(extract_dataset(json!({"metas": {}}), &base_url).is_none());
        assert!(extract_dataset(json!({"dataset_id": "  "}), &base_url).is_none());
    }

    #[tokio::test]
    async fn sends_paginated_catalog_request_with_api_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/explore/v2.1/catalog/datasets"))
            .and(query_param("limit", "1"))
            .and(query_param("offset", "0"))
            .and(header("authorization", "Apikey secret-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "total_count": 42,
                "results": []
            })))
            .mount(&server)
            .await;

        let client =
            OpenDataSoftClient::new_with_api_key(&server.uri(), Some("secret-key".into())).unwrap();
        assert_eq!(client.dataset_count().await.unwrap(), 42);
    }

    #[tokio::test]
    async fn streams_dated_pages_then_sweeps_undated_datasets() {
        #[derive(Clone)]
        struct PhasedCatalog;

        impl Respond for PhasedCatalog {
            fn respond(&self, request: &Request) -> ResponseTemplate {
                let query = |key: &str| {
                    request
                        .url
                        .query_pairs()
                        .find(|(name, _)| name == key)
                        .map(|(_, value)| value.into_owned())
                        .unwrap_or_default()
                };
                let where_clause = query("where");
                let offset = query("offset");
                if where_clause == "modified is null" {
                    ResponseTemplate::new(200).set_body_json(json!({
                        "total_count": 1,
                        "results": [{"dataset_id": "undated", "metas": {"default": {"title": "Undated"}}}]
                    }))
                } else if offset == "0" {
                    ResponseTemplate::new(200).set_body_json(json!({
                        "total_count": 2,
                        "results": [{
                            "dataset_id": "newest",
                            "metas": {"default": {"title": "Newest", "modified": "2026-07-11T10:00:00+00:00"}}
                        }]
                    }))
                } else {
                    ResponseTemplate::new(200).set_body_json(json!({
                        "total_count": 2,
                        "results": [{
                            "dataset_id": "older",
                            "metas": {"default": {"title": "Older", "modified": "2026-07-10T10:00:00+00:00"}}
                        }]
                    }))
                }
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/explore/v2.1/catalog/datasets"))
            .and(query_param("where", "modified is not null"))
            .and(query_param("order_by", "modified desc"))
            .respond_with(PhasedCatalog)
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/explore/v2.1/catalog/datasets"))
            .and(query_param("where", "modified is null"))
            .respond_with(PhasedCatalog)
            .expect(1)
            .mount(&server)
            .await;

        let mut client = test_client(&server.uri());
        client.page_size = 1;
        let datasets = client.search_all_datasets().await.unwrap();
        assert_eq!(
            datasets
                .iter()
                .map(|dataset| dataset.id.as_str())
                .collect::<Vec<_>>(),
            ["newest", "older", "undated"]
        );
    }

    #[tokio::test]
    async fn resets_to_modified_cursor_at_the_offset_window() {
        #[derive(Clone)]
        struct DeepCatalog;

        impl Respond for DeepCatalog {
            fn respond(&self, request: &Request) -> ResponseTemplate {
                let query = |key: &str| {
                    request
                        .url
                        .query_pairs()
                        .find(|(name, _)| name == key)
                        .map(|(_, value)| value.into_owned())
                        .unwrap_or_default()
                };
                let where_clause = query("where");
                if where_clause == "modified is null" {
                    return ResponseTemplate::new(200)
                        .set_body_json(json!({"total_count": 0, "results": []}));
                }
                let offset: usize = query("offset").parse().unwrap();
                let cursor_set = where_clause.contains("<=");
                // Window of 2 with page size 1: the walk reads offsets 0 and 1,
                // then must restart from the cursor.
                let index = if cursor_set { 2 + offset } else { offset };
                let day = 10 - index; // strictly decreasing timestamps
                ResponseTemplate::new(200).set_body_json(json!({
                    "total_count": if cursor_set { 2 } else { 4 },
                    "results": [{
                        "dataset_id": format!("ds-{index}"),
                        "metas": {"default": {
                            "title": format!("Dataset {index}"),
                            "modified": format!("2026-06-{day:02}T00:00:00+00:00")
                        }}
                    }]
                }))
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/explore/v2.1/catalog/datasets"))
            .respond_with(DeepCatalog)
            .mount(&server)
            .await;

        let mut client = test_client(&server.uri());
        client.page_size = 1;
        client.offset_window = 2;
        let datasets = client.search_all_datasets().await.unwrap();
        assert_eq!(
            datasets
                .iter()
                .map(|dataset| dataset.id.as_str())
                .collect::<Vec<_>>(),
            ["ds-0", "ds-1", "ds-2", "ds-3"]
        );

        // The cursor query must carry the oldest timestamp seen (ds-1's).
        let requests = server.received_requests().await.unwrap();
        assert!(requests.iter().any(|request| {
            request.url.query_pairs().any(|(key, value)| {
                key == "where" && value.contains("modified <= date'2026-06-09T00:00:00Z'")
            })
        }));
    }

    #[tokio::test]
    async fn stalled_cursor_ends_the_walk_instead_of_looping() {
        #[derive(Clone)]
        struct SameTimestampCatalog;

        impl Respond for SameTimestampCatalog {
            fn respond(&self, request: &Request) -> ResponseTemplate {
                let query = |key: &str| {
                    request
                        .url
                        .query_pairs()
                        .find(|(name, _)| name == key)
                        .map(|(_, value)| value.into_owned())
                        .unwrap_or_default()
                };
                if query("where") == "modified is null" {
                    return ResponseTemplate::new(200)
                        .set_body_json(json!({"total_count": 0, "results": []}));
                }
                let offset: usize = query("offset").parse().unwrap();
                // Every dataset shares one timestamp, so the cursor can never
                // advance past it and re-reads the same two ids forever.
                ResponseTemplate::new(200).set_body_json(json!({
                    "total_count": 10,
                    "results": [{
                        "dataset_id": format!("ds-{offset}"),
                        "metas": {"default": {
                            "title": "Same instant",
                            "modified": "2026-06-01T00:00:00+00:00"
                        }}
                    }]
                }))
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/explore/v2.1/catalog/datasets"))
            .respond_with(SameTimestampCatalog)
            .mount(&server)
            .await;

        let mut client = test_client(&server.uri());
        client.page_size = 1;
        client.offset_window = 2;
        let datasets = client.search_all_datasets().await.unwrap();
        // Two window passes (ids ds-0, ds-1 twice, deduplicated), then stop.
        assert_eq!(datasets.len(), 2);
    }

    #[tokio::test]
    async fn retries_rate_limit_on_the_same_page() {
        #[derive(Clone)]
        struct RateLimitOnce(Arc<AtomicUsize>);

        impl Respond for RateLimitOnce {
            fn respond(&self, _request: &Request) -> ResponseTemplate {
                if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(429).insert_header("retry-after", "0")
                } else {
                    ResponseTemplate::new(200).set_body_json(json!({
                        "total_count": 0,
                        "results": []
                    }))
                }
            }
        }

        let server = MockServer::start().await;
        let calls = Arc::new(AtomicUsize::new(0));
        Mock::given(method("GET"))
            .and(path("/api/explore/v2.1/catalog/datasets"))
            .respond_with(RateLimitOnce(calls.clone()))
            .mount(&server)
            .await;

        let client = OpenDataSoftClient::new_with_api_key_and_http_config(
            &server.uri(),
            None,
            test_http_config(2),
        )
        .unwrap();
        assert!(client.search_all_datasets().await.unwrap().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 3); // dated + retry + undated sweep
    }

    #[tokio::test]
    async fn incremental_search_filters_server_side_and_paginates() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/explore/v2.1/catalog/datasets"))
            .and(query_param(
                "where",
                "modified >= date'2026-07-10T00:00:00Z'",
            ))
            .and(query_param("order_by", "modified desc"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "total_count": 1,
                "results": [{
                    "dataset_id": "recent",
                    "metas": {"default": {"title": "Recent", "modified": "2026-07-11T10:00:00+00:00"}}
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = test_client(&server.uri());
        let since = Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, 0).unwrap();
        let datasets = client.search_modified_since(since).await.unwrap();
        assert_eq!(datasets.len(), 1);
        assert_eq!(datasets[0].id, "recent");
    }

    #[tokio::test]
    async fn incremental_search_errors_when_results_exceed_the_window() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/explore/v2.1/catalog/datasets"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "total_count": 50_000,
                "results": [{
                    "dataset_id": "recent",
                    "metas": {"default": {"title": "Recent", "modified": "2026-07-11T10:00:00+00:00"}}
                }]
            })))
            .mount(&server)
            .await;

        let client = test_client(&server.uri());
        let since = Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, 0).unwrap();
        let error = client.search_modified_since(since).await.unwrap_err();
        assert!(error.to_string().contains("pagination window"));
    }

    #[tokio::test]
    async fn get_dataset_maps_missing_ids_to_dataset_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/explore/v2.1/catalog/datasets/missing"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "error_code": "ODSQLError",
                "message": "Unknown dataset"
            })))
            .mount(&server)
            .await;

        let client = test_client(&server.uri());
        let error = client.get_dataset("missing").await.unwrap_err();
        assert!(matches!(error, AppError::DatasetNotFound(_)));
    }

    #[tokio::test]
    async fn get_dataset_parses_a_single_catalog_entry() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/explore/v2.1/catalog/datasets/velib"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "dataset_id": "velib",
                "metas": {"default": {"title": "Vélib en temps réel"}}
            })))
            .mount(&server)
            .await;

        let client = test_client(&server.uri());
        let dataset = client.get_dataset("velib").await.unwrap();
        assert_eq!(dataset.id, "velib");
        assert_eq!(dataset.title, "Vélib en temps réel");
    }

    #[test]
    fn retry_backoff_starts_at_base_and_saturates_before_overflow() {
        let base = Duration::from_millis(500);
        assert_eq!(retry_delay(base, 1), base);
        assert_eq!(retry_delay(base, 2), Duration::from_secs(1));
        assert_eq!(retry_delay(base, u32::MAX), MAX_RETRY_DELAY);
    }

    #[tokio::test]
    #[ignore = "requires network access to a public OpenDataSoft portal"]
    async fn opendatasoft_smoke_catalog() {
        // Opt-in smoke test: cargo test -p ceres-client opendatasoft_smoke -- --ignored
        // Override the portal with CERES_ODS_SMOKE_URL.
        let url = std::env::var("CERES_ODS_SMOKE_URL")
            .unwrap_or_else(|_| "https://opendata.paris.fr".to_string());
        let client = OpenDataSoftClient::new_with_api_key(&url, None).unwrap();
        let count = client.dataset_count().await.unwrap();
        assert!(count > 0, "{url} returned no datasets");
        let mut stream = client.search_all_datasets_stream();
        let first_page = stream.next().await.unwrap().unwrap();
        assert!(!first_page.is_empty());
        eprintln!("{url}: {count} datasets; first={}", first_page[0].id);
    }
}
