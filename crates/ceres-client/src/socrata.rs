//! Socrata Discovery API client.
//!
//! The client scopes the Discovery catalog to one portal domain, harvests only
//! dataset assets, and streams results page-by-page so large catalogs do not
//! need to be held in memory. The complete Discovery result is preserved in
//! [`NewDataset::metadata`].

use std::collections::HashSet;
use std::time::Duration;

use ceres_core::HttpConfig;
use ceres_core::error::AppError;
use ceres_core::models::NewDataset;
use ceres_core::traits::PortalClient;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::sleep;

const PAGE_SIZE: usize = 1_000;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// A normalized Socrata dataset plus its complete Discovery API result.
#[derive(Debug, Clone)]
pub struct SocrataDataset {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub landing_page: String,
    pub modified: Option<DateTime<Utc>>,
    pub tags: Vec<String>,
    pub category: Option<String>,
    pub license: Option<String>,
    pub publisher: Option<String>,
    pub resource_url: String,
    pub raw: Value,
}

#[derive(Debug, Deserialize)]
struct CatalogResponse {
    #[serde(default)]
    results: Vec<Value>,
    #[serde(rename = "resultSetSize", default)]
    result_set_size: usize,
}

struct CatalogPage {
    datasets: Vec<SocrataDataset>,
    raw_count: usize,
    result_set_size: usize,
}

/// Client for one Socrata portal's Discovery API catalog.
#[derive(Clone)]
pub struct SocrataClient {
    client: Client,
    base_url: Url,
    catalog_url: Url,
    domain: String,
    request_timeout: Duration,
    max_retries: u32,
    retry_base_delay: Duration,
}

impl SocrataClient {
    /// Creates a client and reads an optional token from `SOCRATA_APP_TOKEN`.
    pub fn new(base_url: &str) -> Result<Self, AppError> {
        let app_token = std::env::var("SOCRATA_APP_TOKEN")
            .ok()
            .filter(|token| !token.trim().is_empty());
        Self::new_with_app_token(base_url, app_token)
    }

    /// Creates a client with an explicitly supplied optional Socrata app token.
    pub fn new_with_app_token(base_url: &str, app_token: Option<String>) -> Result<Self, AppError> {
        Self::new_with_app_token_and_http_config(base_url, app_token, HttpConfig::from_env())
    }

    fn new_with_app_token_and_http_config(
        base_url: &str,
        app_token: Option<String>,
        http_config: HttpConfig,
    ) -> Result<Self, AppError> {
        let mut base_url =
            Url::parse(base_url).map_err(|_| AppError::InvalidPortalUrl(base_url.to_string()))?;
        let domain = base_url
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| AppError::InvalidPortalUrl(base_url.to_string()))?
            .to_string();
        base_url.set_query(None);
        base_url.set_fragment(None);
        let catalog_url = base_url
            .join("/api/catalog/v1")
            .map_err(|error| AppError::InvalidPortalUrl(error.to_string()))?;

        let mut headers = HeaderMap::new();
        if let Some(token) = app_token.filter(|token| !token.trim().is_empty()) {
            let value = HeaderValue::from_str(token.trim()).map_err(|_| {
                AppError::ConfigError(
                    "SOCRATA_APP_TOKEN contains characters that are invalid in an HTTP header"
                        .to_string(),
                )
            })?;
            headers.insert("x-app-token", value);
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
            domain,
            request_timeout: http_config.timeout,
            max_retries: http_config.max_retries,
            retry_base_delay: http_config.retry_base_delay,
        })
    }

    pub fn portal_type(&self) -> &'static str {
        "socrata"
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

    pub async fn get_dataset(&self, id: &str) -> Result<SocrataDataset, AppError> {
        let page = self.fetch_page(0, 1, "name", Some(id)).await?;
        page.datasets
            .into_iter()
            .find(|dataset| dataset.id == id)
            .ok_or_else(|| AppError::DatasetNotFound(id.to_string()))
    }

    pub async fn search_modified_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<SocrataDataset>, AppError> {
        let mut offset = 0;
        let mut datasets = Vec::new();
        let mut seen_ids = HashSet::new();

        loop {
            let page = self
                .fetch_page(offset, PAGE_SIZE, "updatedAt", None)
                .await?;
            let undated_count = page
                .datasets
                .iter()
                .filter(|dataset| dataset.modified.is_none())
                .count();
            if undated_count > 0 {
                tracing::warn!(
                    portal = %self.base_url,
                    count = undated_count,
                    "Skipping undated Socrata datasets during incremental sync; they remain covered by full syncs"
                );
            }
            // Discovery normally supplies `updatedAt`. Missing timestamps are
            // full-sync-only and count as older here so they cannot make every
            // incremental run scan the entire catalog.
            let reached_cutoff = !page.datasets.is_empty()
                && page
                    .datasets
                    .iter()
                    .all(|dataset| dataset.modified.is_none_or(|modified| modified < since));

            datasets.extend(
                page.datasets
                    .into_iter()
                    .filter(|dataset| dataset.modified.is_some_and(|modified| modified >= since))
                    .filter(|dataset| seen_ids.insert(dataset.id.clone())),
            );

            let done = reached_cutoff
                || page.raw_count == 0
                || offset + page.raw_count >= page.result_set_size;
            if done {
                break;
            }
            offset += page.raw_count;
        }

        Ok(datasets)
    }

    pub async fn search_all_datasets(&self) -> Result<Vec<SocrataDataset>, AppError> {
        let mut stream = self.search_all_datasets_stream();
        let mut datasets = Vec::new();
        while let Some(page) = stream.next().await {
            datasets.extend(page?);
        }
        Ok(datasets)
    }

    pub fn search_all_datasets_stream(
        &self,
    ) -> BoxStream<'_, Result<Vec<SocrataDataset>, AppError>> {
        let client = self.clone();
        Box::pin(stream::unfold(
            (client, 0_usize, false, HashSet::new(), 0_u8),
            |(client, offset, finished, mut seen_ids, pass)| async move {
                if finished {
                    return None;
                }

                match client.fetch_page(offset, PAGE_SIZE, "name", None).await {
                    Ok(page) => {
                        let done =
                            page.raw_count == 0 || offset + page.raw_count >= page.result_set_size;
                        let next_offset = offset + page.raw_count;
                        let raw_dataset_count = page.datasets.len();
                        let datasets: Vec<_> = page
                            .datasets
                            .into_iter()
                            .filter(|dataset| seen_ids.insert(dataset.id.clone()))
                            .collect();
                        let duplicate_count = raw_dataset_count - datasets.len();
                        if duplicate_count > 0 {
                            tracing::debug!(
                                portal = %client.base_url,
                                duplicate_count,
                                "Skipping duplicate Socrata dataset IDs across catalog pages"
                            );
                        }
                        let reconcile = done && pass == 0 && seen_ids.len() < page.result_set_size;
                        if reconcile {
                            tracing::warn!(
                                portal = %client.base_url,
                                unique_ids = seen_ids.len(),
                                result_set_size = page.result_set_size,
                                "Socrata catalog returned duplicate IDs across pages; reconciling with a second pass"
                            );
                        } else if done && seen_ids.len() < page.result_set_size {
                            tracing::warn!(
                                portal = %client.base_url,
                                unique_ids = seen_ids.len(),
                                result_set_size = page.result_set_size,
                                "Socrata catalog ended with fewer unique IDs than its reported result set size"
                            );
                        }
                        let state = if reconcile {
                            (client, 0, false, seen_ids, pass + 1)
                        } else {
                            (client, next_offset, done, seen_ids, pass)
                        };
                        Some((Ok(datasets), state))
                    }
                    Err(error) => Some((Err(error), (client, offset, true, seen_ids, pass))),
                }
            },
        ))
    }

    pub async fn dataset_count(&self) -> Result<usize, AppError> {
        Ok(self.fetch_page(0, 1, "name", None).await?.result_set_size)
    }

    pub fn into_new_dataset(
        data: SocrataDataset,
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
            record_kind: ceres_core::CatalogRecordKind::Dataset,
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
        limit: usize,
        order: &str,
        id: Option<&str>,
    ) -> Result<CatalogPage, AppError> {
        let mut url = self.catalog_url.clone();
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("domains", &self.domain);
            query.append_pair("only", "datasets");
            query.append_pair("limit", &limit.to_string());
            query.append_pair("offset", &offset.to_string());
            query.append_pair("order", order);
            if let Some(id) = id {
                query.append_pair("ids", id);
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
            .filter_map(|result| match extract_dataset(result, &self.base_url, &self.domain) {
                Some(dataset) => Some(dataset),
                None => {
                    tracing::warn!(portal = %self.base_url, "Skipping malformed Socrata catalog result");
                    None
                }
            })
            .collect();

        Ok(CatalogPage {
            datasets,
            raw_count,
            result_set_size: response.result_set_size,
        })
    }

    async fn request_with_retry(&self, url: &Url) -> Result<reqwest::Response, AppError> {
        let mut last_error = AppError::Generic("No Socrata request attempted".to_string());

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
                                "Socrata rate limit; retrying request"
                            );
                            sleep(delay).await;
                            continue;
                        }
                        return Err(last_error);
                    }

                    if status.is_server_error() {
                        last_error = AppError::ClientError(format!(
                            "Socrata server error: HTTP {}",
                            status.as_u16()
                        ));
                        if attempt < self.max_retries {
                            sleep(self.retry_base_delay.saturating_mul(attempt)).await;
                            continue;
                        }
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

impl PortalClient for SocrataClient {
    type PortalData = SocrataDataset;

    fn portal_type(&self) -> &'static str {
        SocrataClient::portal_type(self)
    }

    fn base_url(&self) -> &str {
        SocrataClient::base_url(self)
    }

    async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        SocrataClient::list_dataset_ids(self).await
    }

    async fn get_dataset(&self, id: &str) -> Result<Self::PortalData, AppError> {
        SocrataClient::get_dataset(self, id).await
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
        SocrataClient::search_modified_since(self, since).await
    }

    async fn search_all_datasets(&self) -> Result<Vec<Self::PortalData>, AppError> {
        SocrataClient::search_all_datasets(self).await
    }

    fn search_all_datasets_stream(&self) -> BoxStream<'_, Result<Vec<Self::PortalData>, AppError>> {
        SocrataClient::search_all_datasets_stream(self)
    }

    async fn dataset_count(&self) -> Result<usize, AppError> {
        SocrataClient::dataset_count(self).await
    }
}

fn retry_delay(base_delay: Duration, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(u32::BITS - 1);
    let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
    base_delay.saturating_mul(multiplier).min(MAX_RETRY_DELAY)
}

fn extract_dataset(raw: Value, base_url: &Url, configured_domain: &str) -> Option<SocrataDataset> {
    let resource = raw.get("resource")?.as_object()?;
    let id = non_empty_string(resource.get("id")?)?;
    let title = non_empty_string(resource.get("name")?)?;
    let description = resource
        .get("description")
        .and_then(Value::as_str)
        .map(html_to_text)
        .filter(|value| !value.is_empty());
    let domain = raw
        .pointer("/metadata/domain")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(configured_domain);
    let landing_page = valid_url(raw.get("link"))
        .or_else(|| valid_url(raw.get("permalink")))
        .unwrap_or_else(|| format!("{}://{domain}/d/{id}", base_url.scheme()));
    let modified = ["updatedAt", "metadata_updated_at", "data_updated_at"]
        .into_iter()
        .find_map(|key| {
            resource
                .get(key)
                .and_then(Value::as_str)
                .and_then(parse_datetime)
        });

    let classification = raw.get("classification").and_then(Value::as_object);
    let category = classification
        .and_then(|value| value.get("domain_category"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
        .or_else(|| {
            classification
                .and_then(|value| value.get("categories"))
                .and_then(string_list)
                .and_then(|values| values.into_iter().next())
        });
    let mut tags = Vec::new();
    if let Some(classification) = classification {
        for key in ["domain_tags", "tags"] {
            if let Some(values) = classification.get(key).and_then(string_list) {
                tags.extend(values);
            }
        }
    }
    deduplicate(&mut tags);

    let license = resource.get("license").and_then(text_value);
    let publisher = resource
        .get("attribution")
        .and_then(text_value)
        .or_else(|| raw.pointer("/owner/display_name").and_then(text_value))
        .or_else(|| raw.pointer("/creator/display_name").and_then(text_value));
    let resource_url = format!("{}://{domain}/resource/{id}.json", base_url.scheme());

    Some(SocrataDataset {
        id,
        title,
        description,
        landing_page,
        modified,
        tags,
        category,
        license,
        publisher,
        resource_url,
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

fn valid_url(value: Option<&Value>) -> Option<String> {
    let value = value?.as_str()?;
    Url::parse(value).ok().map(|url| url.to_string())
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

    const FIXTURE: &[u8] = include_bytes!("../tests/fixtures/socrata_catalog.json");

    fn test_http_config(max_retries: u32) -> HttpConfig {
        HttpConfig {
            timeout: Duration::from_secs(5),
            max_retries,
            retry_base_delay: Duration::ZERO,
        }
    }

    #[test]
    fn parses_and_normalizes_fixture_without_changing_raw_metadata() {
        let response: CatalogResponse = serde_json::from_slice(FIXTURE).unwrap();
        let raw = response.results[0].clone();
        let base_url = Url::parse("https://data.example.gov").unwrap();
        let dataset = extract_dataset(raw.clone(), &base_url, "data.example.gov").unwrap();

        assert_eq!(dataset.id, "abcd-1234");
        assert_eq!(dataset.title, "Traffic counts");
        assert_eq!(
            dataset.description.as_deref(),
            Some("Daily traffic counts with details & context.")
        );
        assert_eq!(dataset.category.as_deref(), Some("Transportation"));
        assert_eq!(dataset.tags, ["traffic", "daily", "roads"]);
        assert_eq!(dataset.license.as_deref(), Some("Public Domain"));
        assert_eq!(dataset.publisher.as_deref(), Some("Transport Agency"));
        assert_eq!(
            dataset.resource_url,
            "https://data.example.gov/resource/abcd-1234.json"
        );
        assert_eq!(dataset.raw, raw);

        let normalized =
            SocrataClient::into_new_dataset(dataset, "https://data.example.gov", None, "en");
        assert_eq!(normalized.metadata, raw);
        assert_eq!(normalized.content_hash.len(), 64);
    }

    #[test]
    fn applies_documented_fallbacks() {
        let response: CatalogResponse = serde_json::from_slice(FIXTURE).unwrap();
        let base_url = Url::parse("https://data.example.gov").unwrap();
        let dataset =
            extract_dataset(response.results[1].clone(), &base_url, "data.example.gov").unwrap();

        assert_eq!(dataset.landing_page, "https://data.example.gov/d/wxyz-9876");
        assert_eq!(dataset.publisher.as_deref(), Some("Portal Owner"));
        assert_eq!(dataset.category.as_deref(), Some("Public Safety"));
        assert!(dataset.description.is_none());
        assert!(dataset.modified.is_some());
    }

    #[tokio::test]
    async fn sends_scoped_paginated_catalog_request_and_token() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/catalog/v1"))
            .and(query_param("domains", "127.0.0.1"))
            .and(query_param("only", "datasets"))
            .and(query_param("limit", "1"))
            .and(query_param("offset", "0"))
            .and(query_param("order", "name"))
            .and(header("x-app-token", "secret-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "results": [],
                "resultSetSize": 42
            })))
            .mount(&server)
            .await;

        let client =
            SocrataClient::new_with_app_token(&server.uri(), Some("secret-token".into())).unwrap();
        assert_eq!(client.dataset_count().await.unwrap(), 42);
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
                        "results": [],
                        "resultSetSize": 0
                    }))
                }
            }
        }

        let server = MockServer::start().await;
        let calls = Arc::new(AtomicUsize::new(0));
        Mock::given(method("GET"))
            .and(path("/api/catalog/v1"))
            .respond_with(RateLimitOnce(calls.clone()))
            .mount(&server)
            .await;

        let client = SocrataClient::new_with_app_token_and_http_config(
            &server.uri(),
            None,
            test_http_config(2),
        )
        .unwrap();
        assert!(client.search_all_datasets().await.unwrap().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn streams_multiple_pages_without_sending_an_empty_token() {
        #[derive(Clone)]
        struct PageByOffset;

        impl Respond for PageByOffset {
            fn respond(&self, request: &Request) -> ResponseTemplate {
                let offset = request
                    .url
                    .query_pairs()
                    .find(|(key, _)| key == "offset")
                    .map(|(_, value)| value.into_owned())
                    .unwrap_or_default();
                let (id, name) = if offset == "0" {
                    ("aaaa-1111", "First")
                } else {
                    ("bbbb-2222", "Second")
                };
                ResponseTemplate::new(200).set_body_json(json!({
                    "results": [{
                        "resource": {"id": id, "name": name, "updatedAt": "2026-07-11T10:00:00Z"},
                        "metadata": {"domain": "127.0.0.1"}
                    }],
                    "resultSetSize": 2
                }))
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/catalog/v1"))
            .respond_with(PageByOffset)
            .expect(2)
            .mount(&server)
            .await;

        let client = SocrataClient::new_with_app_token(&server.uri(), Some("  ".into())).unwrap();
        let datasets = client.search_all_datasets().await.unwrap();
        assert_eq!(
            datasets
                .iter()
                .map(|dataset| dataset.id.as_str())
                .collect::<Vec<_>>(),
            ["aaaa-1111", "bbbb-2222"]
        );
        let requests = server.received_requests().await.unwrap();
        assert!(
            requests
                .iter()
                .all(|request| !request.headers.contains_key("x-app-token"))
        );
    }

    #[tokio::test]
    async fn deduplicates_dataset_ids_across_catalog_pages() {
        #[derive(Clone)]
        struct DuplicateAcrossPages(Arc<AtomicUsize>);

        impl Respond for DuplicateAcrossPages {
            fn respond(&self, _request: &Request) -> ResponseTemplate {
                let call = self.0.fetch_add(1, Ordering::SeqCst);
                let id = if call < 3 { "aaaa-1111" } else { "bbbb-2222" };
                ResponseTemplate::new(200).set_body_json(json!({
                    "results": [{
                        "resource": {"id": id, "name": "Repeated"},
                        "metadata": {"domain": "127.0.0.1"}
                    }],
                    "resultSetSize": 2
                }))
            }
        }

        let server = MockServer::start().await;
        let calls = Arc::new(AtomicUsize::new(0));
        Mock::given(method("GET"))
            .and(path("/api/catalog/v1"))
            .respond_with(DuplicateAcrossPages(calls.clone()))
            .expect(4)
            .mount(&server)
            .await;

        let client = SocrataClient::new_with_app_token(&server.uri(), None).unwrap();
        let datasets = client.search_all_datasets().await.unwrap();
        assert_eq!(datasets.len(), 2);
        assert_eq!(datasets[0].id, "aaaa-1111");
        assert_eq!(datasets[1].id, "bbbb-2222");
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn server_error_after_a_completed_page_returns_no_partial_catalog() {
        #[derive(Clone)]
        struct FailSecondPage {
            first_page_calls: Arc<AtomicUsize>,
            second_page_calls: Arc<AtomicUsize>,
        }

        impl Respond for FailSecondPage {
            fn respond(&self, request: &Request) -> ResponseTemplate {
                let offset = request
                    .url
                    .query_pairs()
                    .find(|(key, _)| key == "offset")
                    .map(|(_, value)| value.into_owned())
                    .unwrap_or_default();
                if offset == "0" {
                    self.first_page_calls.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(200).set_body_json(json!({
                        "results": [{
                            "resource": {"id": "aaaa-1111", "name": "First"},
                            "metadata": {"domain": "127.0.0.1"}
                        }],
                        "resultSetSize": 2
                    }))
                } else {
                    self.second_page_calls.fetch_add(1, Ordering::SeqCst);
                    ResponseTemplate::new(503)
                }
            }
        }

        let server = MockServer::start().await;
        let first_page_calls = Arc::new(AtomicUsize::new(0));
        let second_page_calls = Arc::new(AtomicUsize::new(0));
        Mock::given(method("GET"))
            .and(path("/api/catalog/v1"))
            .respond_with(FailSecondPage {
                first_page_calls: first_page_calls.clone(),
                second_page_calls: second_page_calls.clone(),
            })
            .mount(&server)
            .await;

        let retry_config = test_http_config(3);
        let expected_retries = retry_config.max_retries;
        let client =
            SocrataClient::new_with_app_token_and_http_config(&server.uri(), None, retry_config)
                .unwrap();
        let error = client.search_all_datasets().await.unwrap_err();
        assert!(error.to_string().contains("503"));
        assert_eq!(first_page_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            second_page_calls.load(Ordering::SeqCst),
            expected_retries as usize
        );
    }

    #[tokio::test]
    async fn incremental_search_stops_after_older_page() {
        let server = MockServer::start().await;
        let recent = json!({
            "resource": {"id": "aaaa-1111", "name": "Recent", "updatedAt": "2026-07-11T10:00:00Z"},
            "metadata": {"domain": "127.0.0.1"}
        });
        let old = json!({
            "resource": {"id": "bbbb-2222", "name": "Old", "updatedAt": "2026-07-09T10:00:00Z"},
            "metadata": {"domain": "127.0.0.1"}
        });
        Mock::given(method("GET"))
            .and(query_param("order", "updatedAt"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "results": [recent, old],
                "resultSetSize": 2
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = SocrataClient::new_with_app_token(&server.uri(), None).unwrap();
        let since = Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, 0).unwrap();
        let datasets = client.search_modified_since(since).await.unwrap();
        assert_eq!(datasets.len(), 1);
        assert_eq!(datasets[0].id, "aaaa-1111");
    }

    #[tokio::test]
    async fn incremental_search_excludes_undated_datasets_and_stops_at_cutoff() {
        let server = MockServer::start().await;
        let old = json!({
            "resource": {"id": "bbbb-2222", "name": "Old", "updatedAt": "2026-07-09T10:00:00Z"},
            "metadata": {"domain": "127.0.0.1"}
        });
        let undated = json!({
            "resource": {"id": "cccc-3333", "name": "Undated"},
            "metadata": {"domain": "127.0.0.1"}
        });
        Mock::given(method("GET"))
            .and(query_param("order", "updatedAt"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "results": [old, undated],
                "resultSetSize": 10_000
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = SocrataClient::new_with_app_token(&server.uri(), None).unwrap();
        let since = Utc.with_ymd_and_hms(2026, 7, 10, 0, 0, 0).unwrap();
        let datasets = client.search_modified_since(since).await.unwrap();
        assert!(datasets.is_empty());
    }

    #[test]
    fn retry_backoff_starts_at_base_and_saturates_before_overflow() {
        let base = Duration::from_millis(500);
        assert_eq!(retry_delay(base, 1), base);
        assert_eq!(retry_delay(base, 2), Duration::from_secs(1));
        assert_eq!(retry_delay(base, u32::MAX), MAX_RETRY_DELAY);
    }

    #[tokio::test]
    #[ignore = "requires network access to a public Socrata portal"]
    async fn socrata_smoke_catalog() {
        let url = std::env::var("CERES_SOCRATA_SMOKE_URL")
            .unwrap_or_else(|_| "https://data.cityofnewyork.us".to_string());
        let client = SocrataClient::new_with_app_token(&url, None).unwrap();
        let count = client.dataset_count().await.unwrap();
        assert!(count > 0, "{url} returned no dataset assets");
        let dataset = client
            .fetch_page(0, 1, "name", None)
            .await
            .unwrap()
            .datasets
            .pop()
            .unwrap();
        eprintln!("{url}: {count} datasets; first={}", dataset.id);
    }
}
