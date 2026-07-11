//! ArcGIS Hub Search API client.
//!
//! ArcGIS Hub portals (opendata.dc.gov, opendata.gis.utah.gov, and other
//! `*.hub.arcgis.com` sites) expose their catalog through the Hub Search
//! API, an OGC API Records-style surface at
//! `/api/search/v1/collections/dataset/items` with `limit`/`startindex`
//! pagination. The backing Elasticsearch index enforces a hard result
//! window (`startindex + limit <= 10,000`) and clamps `numberMatched` at
//! 10,000, so deep catalogs are walked with a keyset cursor on the
//! `modified` timestamp (`sortBy=-properties.modified` plus
//! `filter=modified <= <epoch millis>`), mirroring the OpenDataSoft
//! client. ArcGIS items always carry a platform-managed `modified`
//! timestamp, so no separate undated sweep is needed.
//!
//! # Service URLs versus downloadable resources
//!
//! Hub catalog entries mix hosted services (Feature Services, Image
//! Services, Map Services) with file items (CSV, shapefile, PDF). The
//! `properties.url` of a service item points at an ArcGIS REST service
//! endpoint — a queryable API, **not** a file download — while file items
//! usually have no `url` at all. The normalized dataset URL is therefore
//! always the Hub landing page (`/datasets/<id>`), where every access
//! mode is presented; the raw feature (including `properties.url`,
//! `properties.type`, and `typeKeywords`) is preserved in
//! [`NewDataset::metadata`] so resource-level semantics can be derived
//! later without re-harvesting.
//!
//! Hub sites whose injected `catalogV2` has an empty item scope are rejected:
//! their otherwise valid-looking search endpoint serves the global ArcGIS
//! index, not content belonging to that portal.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ceres_core::HttpConfig;
use ceres_core::error::AppError;
use ceres_core::models::NewDataset;
use ceres_core::traits::PortalClient;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::OnceCell;
use tokio::time::sleep;

/// Maximum page size accepted by the Hub Search API (`0 <= limit <= 100`).
const PAGE_SIZE: usize = 100;
/// Highest 0-based offset we request. Elasticsearch rejects
/// `startindex + limit > 10_000` with an HTTP 500, so stay well clear.
const OFFSET_WINDOW: usize = 9_000;
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// A normalized ArcGIS Hub dataset plus its complete catalog feature.
#[derive(Debug, Clone)]
pub struct ArcGisDataset {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    /// Hub landing page for the dataset (never the raw service endpoint).
    pub landing_page: String,
    pub modified: Option<DateTime<Utc>>,
    /// ArcGIS item type ("Feature Service", "CSV", "Image Service", ...).
    pub item_type: Option<String>,
    pub categories: Vec<String>,
    pub tags: Vec<String>,
    pub license: Option<String>,
    pub publisher: Option<String>,
    pub raw: Value,
}

#[derive(Debug, Deserialize)]
struct ItemsResponse {
    #[serde(default)]
    features: Vec<Value>,
    #[serde(default, rename = "numberMatched")]
    number_matched: usize,
    #[serde(default)]
    links: Vec<Value>,
}

struct CatalogPage {
    datasets: Vec<ArcGisDataset>,
    raw_count: usize,
    /// Clamped at 10,000 by the backing index; only trust small values.
    number_matched: usize,
    has_next: bool,
}

struct WalkState {
    client: ArcGisClient,
    finished: bool,
    /// 0-based offset within the current query (`startindex` minus one).
    offset: usize,
    /// Upper bound for the next query (`modified <= cursor`, epoch millis).
    cursor: Option<i64>,
    seen_ids: HashSet<String>,
    /// Unique datasets collected when the current cursor was set; used to
    /// detect a cursor that cannot advance.
    seen_at_cursor: usize,
}

/// Client for one ArcGIS Hub portal's Search API catalog.
#[derive(Clone)]
pub struct ArcGisClient {
    client: Client,
    base_url: Url,
    items_url: Url,
    request_timeout: Duration,
    max_retries: u32,
    retry_base_delay: Duration,
    page_size: usize,
    offset_window: usize,
    site_scope_validation: Arc<OnceCell<Option<String>>>,
}

impl ArcGisClient {
    pub fn new(base_url: &str) -> Result<Self, AppError> {
        Self::new_with_http_config(base_url, HttpConfig::from_env())
    }

    fn new_with_http_config(base_url: &str, http_config: HttpConfig) -> Result<Self, AppError> {
        let mut base_url =
            Url::parse(base_url).map_err(|_| AppError::InvalidPortalUrl(base_url.to_string()))?;
        if base_url.host_str().is_none_or(str::is_empty) {
            return Err(AppError::InvalidPortalUrl(base_url.to_string()));
        }
        base_url.set_query(None);
        base_url.set_fragment(None);
        let items_url = base_url
            .join("/api/search/v1/collections/dataset/items")
            .map_err(|error| AppError::InvalidPortalUrl(error.to_string()))?;

        let client = Client::builder()
            .user_agent("Ceres/0.6 (open-data-harvester)")
            .build()
            .map_err(|error| AppError::ClientError(error.to_string()))?;

        Ok(Self {
            client,
            base_url,
            items_url,
            request_timeout: http_config.timeout,
            max_retries: http_config.max_retries,
            retry_base_delay: http_config.retry_base_delay,
            page_size: PAGE_SIZE,
            offset_window: OFFSET_WINDOW,
            site_scope_validation: Arc::new(OnceCell::new()),
        })
    }

    pub fn portal_type(&self) -> &'static str {
        "arcgis"
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

    pub async fn get_dataset(&self, id: &str) -> Result<ArcGisDataset, AppError> {
        self.validate_site_scope().await?;
        let url = Url::parse(&format!("{}/{id}", self.items_url))
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
    ) -> Result<Vec<ArcGisDataset>, AppError> {
        self.validate_site_scope().await?;
        let filter = format!("modified >= {}", since.timestamp_millis());
        let mut offset = 0;
        let mut datasets = Vec::new();
        let mut seen_ids = HashSet::new();

        loop {
            let page = self.fetch_page(offset, true, Some(&filter)).await?;
            if page.number_matched > self.offset_window {
                // More modified datasets than the result window can reach
                // (numberMatched is clamped at 10,000, still above the
                // window); let the harvest service fall back to a full sync.
                return Err(AppError::Generic(format!(
                    "{} datasets modified since {since} exceed the ArcGIS Hub pagination window",
                    page.number_matched
                )));
            }
            let done = page.raw_count == 0 || !page.has_next;
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

    pub async fn search_all_datasets(&self) -> Result<Vec<ArcGisDataset>, AppError> {
        let mut stream = self.search_all_datasets_stream();
        let mut datasets = Vec::new();
        while let Some(page) = stream.next().await {
            datasets.extend(page?);
        }
        Ok(datasets)
    }

    /// Streams the full catalog page-by-page, newest first.
    ///
    /// Whenever the next request would cross the result window the walk
    /// restarts at `startindex=1` with `filter=modified <= <oldest seen>`
    /// (inclusive, deduplicated by item id). If a restart cannot advance
    /// the cursor the stream fails rather than truncate: a partial full
    /// sync would otherwise mark every unfetched dataset stale.
    pub fn search_all_datasets_stream(
        &self,
    ) -> BoxStream<'_, Result<Vec<ArcGisDataset>, AppError>> {
        let state = WalkState {
            client: self.clone(),
            finished: false,
            offset: 0,
            cursor: None,
            seen_ids: HashSet::new(),
            seen_at_cursor: 0,
        };
        Box::pin(stream::unfold(state, |mut state| async move {
            if state.finished {
                return None;
            }

            if let Err(error) = state.client.validate_site_scope().await {
                state.finished = true;
                return Some((Err(error), state));
            }

            let filter = state.cursor.map(|cursor| format!("modified <= {cursor}"));
            let page = match state
                .client
                .fetch_page(state.offset, true, filter.as_deref())
                .await
            {
                Ok(page) => page,
                Err(error) => {
                    state.finished = true;
                    return Some((Err(error), state));
                }
            };

            // numberMatched is clamped, so exhaustion is detected from the
            // pagination links instead of the total.
            let query_exhausted = page.raw_count == 0 || !page.has_next;
            let next_offset = state.offset + page.raw_count;
            let oldest_modified = page
                .datasets
                .iter()
                .filter_map(|dataset| dataset.modified)
                .map(|modified| modified.timestamp_millis())
                .min();
            let datasets: Vec<_> = page
                .datasets
                .into_iter()
                .filter(|dataset| state.seen_ids.insert(dataset.id.clone()))
                .collect();

            if query_exhausted {
                state.finished = true;
            } else if next_offset + state.client.page_size > state.client.offset_window {
                let stalled =
                    state.cursor.is_some() && state.seen_ids.len() == state.seen_at_cursor;
                match (oldest_modified.or(state.cursor), stalled) {
                    (Some(cursor), false) => {
                        state.cursor = Some(cursor);
                        state.seen_at_cursor = state.seen_ids.len();
                        state.offset = 0;
                    }
                    _ => {
                        state.finished = true;
                        let error = AppError::Generic(format!(
                            "ArcGIS Hub modified-cursor cannot advance past {} of {} datasets on {}; aborting so the partial harvest is not treated as complete",
                            state.seen_ids.len(),
                            page.number_matched,
                            state.client.base_url
                        ));
                        return Some((Err(error), state));
                    }
                }
            } else {
                state.offset = next_offset;
            }

            Some((Ok(datasets), state))
        }))
    }

    /// Returns `numberMatched` for the dataset collection. The backing
    /// index clamps this at 10,000, so treat large values as "at least".
    pub async fn dataset_count(&self) -> Result<usize, AppError> {
        self.validate_site_scope().await?;
        let page = self.fetch_page(0, false, None).await?;
        Ok(page.number_matched)
    }

    /// Reject Hub sites whose injected catalog has no item scope. ArcGIS
    /// serves a global search index for those sites, which otherwise looks
    /// like a valid 10,000-item catalog and would corrupt portal provenance.
    async fn validate_site_scope(&self) -> Result<(), AppError> {
        let error = self
            .site_scope_validation
            .get_or_init(|| async {
                let response = match self.request_with_retry(&self.base_url).await {
                    Ok(response) => response,
                    Err(error) => {
                        tracing::warn!(
                            portal = %self.base_url,
                            %error,
                            "Could not inspect ArcGIS Hub site catalog scope; continuing with the catalog API"
                        );
                        return None;
                    }
                };
                let html = match response.text().await {
                    Ok(html) => html,
                    Err(error) => {
                        tracing::warn!(
                            portal = %self.base_url,
                            %error,
                            "Could not read ArcGIS Hub site catalog scope; continuing with the catalog API"
                        );
                        return None;
                    }
                };

                catalog_item_scope_is_empty(&html).then(|| {
                    format!(
                        "ArcGIS Hub site {} has an empty catalog item scope; its search endpoint returns global ArcGIS content instead of portal datasets",
                        self.base_url
                    )
                })
            })
            .await;

        match error {
            Some(message) => Err(AppError::ClientError(message.clone())),
            None => Ok(()),
        }
    }

    pub fn into_new_dataset(
        data: ArcGisDataset,
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
        sort_by_modified: bool,
        filter: Option<&str>,
    ) -> Result<CatalogPage, AppError> {
        let mut url = self.items_url.clone();
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("limit", &self.page_size.to_string());
            // startindex is 1-based.
            query.append_pair("startindex", &(offset + 1).to_string());
            if sort_by_modified {
                query.append_pair("sortBy", "-properties.modified");
            }
            if let Some(filter) = filter {
                query.append_pair("filter", filter);
            }
        }

        let response = self.request_with_retry(&url).await?;
        let body = response
            .bytes()
            .await
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        let response: ItemsResponse = serde_json::from_slice(&body)?;
        let raw_count = response.features.len();
        let has_next = response
            .links
            .iter()
            .any(|link| link.get("rel").and_then(Value::as_str) == Some("next"));
        let datasets = response
            .features
            .into_iter()
            .filter_map(|feature| match extract_dataset(feature, &self.base_url) {
                Some(dataset) => Some(dataset),
                None => {
                    tracing::warn!(portal = %self.base_url, "Skipping malformed ArcGIS Hub catalog feature");
                    None
                }
            })
            .collect();

        Ok(CatalogPage {
            datasets,
            raw_count,
            number_matched: response.number_matched,
            has_next,
        })
    }

    async fn request_with_retry(&self, url: &Url) -> Result<reqwest::Response, AppError> {
        let mut last_error = AppError::Generic("No ArcGIS Hub request attempted".to_string());

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
                                .map(|seconds| Duration::from_secs(seconds).min(MAX_RETRY_DELAY))
                                .unwrap_or_else(|| retry_delay(self.retry_base_delay, attempt));
                            tracing::warn!(
                                attempt,
                                delay_ms = delay.as_millis(),
                                "ArcGIS Hub rate limit; retrying request"
                            );
                            sleep(delay).await;
                            continue;
                        }
                        return Err(last_error);
                    }

                    if status.is_server_error() {
                        last_error = AppError::ClientError(format!(
                            "ArcGIS Hub server error: HTTP {}",
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

impl PortalClient for ArcGisClient {
    type PortalData = ArcGisDataset;

    fn portal_type(&self) -> &'static str {
        ArcGisClient::portal_type(self)
    }

    fn base_url(&self) -> &str {
        ArcGisClient::base_url(self)
    }

    async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        ArcGisClient::list_dataset_ids(self).await
    }

    async fn get_dataset(&self, id: &str) -> Result<Self::PortalData, AppError> {
        ArcGisClient::get_dataset(self, id).await
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
        ArcGisClient::search_modified_since(self, since).await
    }

    async fn search_all_datasets(&self) -> Result<Vec<Self::PortalData>, AppError> {
        ArcGisClient::search_all_datasets(self).await
    }

    fn search_all_datasets_stream(&self) -> BoxStream<'_, Result<Vec<Self::PortalData>, AppError>> {
        ArcGisClient::search_all_datasets_stream(self)
    }

    async fn dataset_count(&self) -> Result<usize, AppError> {
        ArcGisClient::dataset_count(self).await
    }
}

fn retry_delay(base_delay: Duration, attempt: u32) -> Duration {
    let exponent = attempt.saturating_sub(1).min(u32::BITS - 1);
    let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
    base_delay.saturating_mul(multiplier).min(MAX_RETRY_DELAY)
}

fn catalog_item_scope_is_empty(html: &str) -> bool {
    const PREFIX: &str = "window.__SITE=\"";
    let Some(encoded) = html
        .split_once(PREFIX)
        .and_then(|(_, remainder)| remainder.split_once('"').map(|(value, _)| value))
    else {
        return false;
    };
    let Some((decoded, _)) = url::form_urlencoded::parse(encoded.as_bytes()).next() else {
        return false;
    };
    let Ok(site): Result<Value, _> = serde_json::from_str(&decoded) else {
        return false;
    };

    site.pointer("/site/data/catalogV2/scopes/item/filters")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
}

fn extract_dataset(raw: Value, base_url: &Url) -> Option<ArcGisDataset> {
    let id = raw
        .get("id")
        .and_then(non_empty_string)
        .or_else(|| raw.pointer("/properties/id").and_then(non_empty_string))?;
    let properties = raw.get("properties");
    let field = |name: &str| properties.and_then(|properties| properties.get(name));

    let title = field("title")
        .and_then(non_empty_string)
        .unwrap_or_else(|| id.clone());
    let description = field("description")
        .and_then(Value::as_str)
        .map(html_to_text)
        .filter(|value| !value.is_empty())
        .or_else(|| field("snippet").and_then(non_empty_string));
    let modified = field("modified")
        .and_then(Value::as_i64)
        .and_then(DateTime::from_timestamp_millis);
    let item_type = field("type").and_then(non_empty_string);
    let mut categories = field("categories")
        .and_then(string_list)
        .unwrap_or_default();
    for category in &mut categories {
        if let Some(stripped) = category.strip_prefix("/Categories/") {
            *category = stripped.to_string();
        }
    }
    deduplicate(&mut categories);
    let mut tags = field("tags").and_then(string_list).unwrap_or_default();
    deduplicate(&mut tags);
    let license = field("license").and_then(non_empty_string).or_else(|| {
        field("licenseInfo")
            .and_then(Value::as_str)
            .map(html_to_text)
            .filter(|value| !value.is_empty())
    });
    let publisher = field("source")
        .and_then(non_empty_string)
        .or_else(|| field("accessInformation").and_then(non_empty_string))
        .or_else(|| field("owner").and_then(non_empty_string));
    // Always the Hub landing page: properties.url (when present) is an
    // ArcGIS REST service endpoint, not a downloadable resource.
    let landing_page = base_url
        .join(&format!("/datasets/{id}"))
        .map(|url| url.to_string())
        .unwrap_or_else(|_| format!("{}datasets/{id}", base_url));

    Some(ArcGisDataset {
        id,
        title,
        description,
        landing_page,
        modified,
        item_type,
        categories,
        tags,
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

fn html_to_text(value: &str) -> String {
    let rendered = html2text::config::plain_no_decorate()
        .string_from_read(value.as_bytes(), usize::MAX)
        .unwrap_or_else(|_| value.to_string());
    rendered.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn string_list(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::Array(values) => Some(values.iter().filter_map(non_empty_string).collect()),
        Value::String(_) => non_empty_string(value).map(|value| vec![value]),
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
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    use super::*;

    const FIXTURE: &[u8] = include_bytes!("../tests/fixtures/arcgis_items.json");

    fn test_http_config(max_retries: u32) -> HttpConfig {
        HttpConfig {
            timeout: Duration::from_secs(5),
            max_retries,
            retry_base_delay: Duration::ZERO,
        }
    }

    fn test_client(uri: &str) -> ArcGisClient {
        ArcGisClient::new_with_http_config(uri, test_http_config(1)).unwrap()
    }

    /// Builds an items page whose features carry only the fields the
    /// walk logic needs; `total` drives `numberMatched` and `has_next`
    /// comes from the presence of a `next` link.
    fn items_page(features: Vec<Value>, total: usize, has_next: bool) -> Value {
        let mut links = vec![json!({"rel": "self", "href": "https://example.gov/self"})];
        if has_next {
            links.push(json!({"rel": "next", "href": "https://example.gov/next"}));
        }
        let returned = features.len();
        json!({
            "type": "FeatureCollection",
            "features": features,
            "numberMatched": total,
            "numberReturned": returned,
            "links": links,
        })
    }

    fn feature(id: &str, title: &str, modified_ms: i64) -> Value {
        json!({
            "id": id,
            "type": "Feature",
            "properties": {"id": id, "title": title, "modified": modified_ms}
        })
    }

    #[test]
    fn detects_empty_injected_catalog_scope() {
        let empty = r#"<script id="site-injection">window.__SITE="%7B%22site%22%3A%7B%22data%22%3A%7B%22catalogV2%22%3A%7B%22scopes%22%3A%7B%22item%22%3A%7B%22filters%22%3A%5B%5D%7D%7D%7D%7D%7D%7D";</script>"#;
        let scoped = r#"<script id="site-injection">window.__SITE="%7B%22site%22%3A%7B%22data%22%3A%7B%22catalogV2%22%3A%7B%22scopes%22%3A%7B%22item%22%3A%7B%22filters%22%3A%5B%7B%22predicates%22%3A%5B%5D%7D%5D%7D%7D%7D%7D%7D%7D";</script>"#;

        assert!(catalog_item_scope_is_empty(empty));
        assert!(!catalog_item_scope_is_empty(scoped));
        assert!(!catalog_item_scope_is_empty("<html></html>"));
    }

    #[tokio::test]
    async fn rejects_site_whose_catalog_scope_is_global() {
        let server = MockServer::start().await;
        let html = r#"<script id="site-injection">window.__SITE="%7B%22site%22%3A%7B%22data%22%3A%7B%22catalogV2%22%3A%7B%22scopes%22%3A%7B%22item%22%3A%7B%22filters%22%3A%5B%5D%7D%7D%7D%7D%7D%7D";</script>"#;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_string(html))
            .expect(1)
            .mount(&server)
            .await;

        let client = test_client(&server.uri());
        let error = client.dataset_count().await.unwrap_err();
        assert!(error.to_string().contains("empty catalog item scope"));
    }

    #[test]
    fn parses_and_normalizes_service_item_without_changing_raw_metadata() {
        let response: ItemsResponse = serde_json::from_slice(FIXTURE).unwrap();
        assert_eq!(response.number_matched, 2);
        let raw = response.features[0].clone();
        let base_url = Url::parse("https://hub.example.gov").unwrap();
        let dataset = extract_dataset(raw.clone(), &base_url).unwrap();

        assert_eq!(dataset.id, "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6");
        assert_eq!(dataset.title, "Remarkable Trees");
        assert_eq!(
            dataset.description.as_deref(),
            Some("Inventory of remarkable trees with location & species.")
        );
        assert_eq!(dataset.item_type.as_deref(), Some("Feature Service"));
        assert_eq!(dataset.categories, ["Environment"]);
        assert_eq!(dataset.tags, ["trees", "nature"]);
        assert_eq!(dataset.license.as_deref(), Some("CC-BY-4.0"));
        assert_eq!(dataset.publisher.as_deref(), Some("City of Example"));
        assert_eq!(
            dataset.modified.unwrap(),
            Utc.timestamp_millis_opt(1_773_480_413_000).unwrap()
        );

        // Service semantics: the normalized URL is the Hub landing page,
        // never the FeatureServer endpoint, which stays in the raw metadata.
        assert_eq!(
            dataset.landing_page,
            "https://hub.example.gov/datasets/a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6"
        );
        let service_url = raw.pointer("/properties/url").and_then(Value::as_str);
        assert!(service_url.unwrap().ends_with("/FeatureServer"));
        assert_ne!(dataset.landing_page, service_url.unwrap());
        assert_eq!(dataset.raw, raw);

        let normalized =
            ArcGisClient::into_new_dataset(dataset, "https://hub.example.gov", None, "en");
        assert_eq!(normalized.metadata, raw);
        assert_eq!(normalized.original_id, "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6");
        assert_eq!(normalized.content_hash.len(), 64);
    }

    #[test]
    fn applies_documented_fallbacks_for_sparse_file_items() {
        let response: ItemsResponse = serde_json::from_slice(FIXTURE).unwrap();
        let base_url = Url::parse("https://hub.example.gov").unwrap();
        let dataset = extract_dataset(response.features[1].clone(), &base_url).unwrap();

        // Title falls back to the item id; description falls back to the
        // snippet; publisher falls back to the owner; missing modified and
        // license stay None. A file item has no service URL at all.
        assert_eq!(dataset.id, "f0e1d2c3b4a5f6e7d8c9b0a1f2e3d4c5");
        assert_eq!(dataset.title, "f0e1d2c3b4a5f6e7d8c9b0a1f2e3d4c5");
        assert_eq!(
            dataset.description.as_deref(),
            Some("Budget lookup table for 2026 survey results.")
        );
        assert_eq!(dataset.item_type.as_deref(), Some("CSV"));
        assert!(dataset.modified.is_none());
        assert!(dataset.license.is_none());
        assert_eq!(dataset.publisher.as_deref(), Some("finance_dept"));
        assert!(dataset.categories.is_empty());
        assert!(dataset.tags.is_empty());
        assert!(dataset.raw.pointer("/properties/url").unwrap().is_null());
    }

    #[test]
    fn skips_features_without_an_id() {
        let base_url = Url::parse("https://hub.example.gov").unwrap();
        assert!(extract_dataset(json!({"properties": {}}), &base_url).is_none());
        assert!(extract_dataset(json!({"id": "  "}), &base_url).is_none());
    }

    #[tokio::test]
    async fn sends_paginated_items_request() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/search/v1/collections/dataset/items"))
            .and(query_param("limit", "100"))
            .and(query_param("startindex", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(items_page(vec![], 1468, false)))
            .mount(&server)
            .await;

        let client = test_client(&server.uri());
        assert_eq!(client.dataset_count().await.unwrap(), 1468);
    }

    #[tokio::test]
    async fn streams_pages_until_the_next_link_disappears() {
        #[derive(Clone)]
        struct TwoPages;

        impl Respond for TwoPages {
            fn respond(&self, request: &Request) -> ResponseTemplate {
                let startindex = request
                    .url
                    .query_pairs()
                    .find(|(name, _)| name == "startindex")
                    .map(|(_, value)| value.into_owned())
                    .unwrap_or_default();
                if startindex == "1" {
                    ResponseTemplate::new(200).set_body_json(items_page(
                        vec![feature("newest", "Newest", 2_000)],
                        2,
                        true,
                    ))
                } else {
                    ResponseTemplate::new(200).set_body_json(items_page(
                        vec![feature("older", "Older", 1_000)],
                        2,
                        false,
                    ))
                }
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/search/v1/collections/dataset/items"))
            .and(query_param("sortBy", "-properties.modified"))
            .respond_with(TwoPages)
            .expect(2)
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
            ["newest", "older"]
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
                let offset: usize = query("startindex").parse::<usize>().unwrap() - 1;
                let cursor_set = query("filter").contains("<=");
                // Window of 2 with page size 1: the walk reads offsets 0 and 1,
                // then must restart from the cursor.
                let index = if cursor_set { 2 + offset } else { offset };
                let modified = 10_000 - (index as i64) * 1_000; // strictly decreasing
                let has_next = if cursor_set { offset < 1 } else { true };
                ResponseTemplate::new(200).set_body_json(items_page(
                    vec![feature(
                        &format!("ds-{index}"),
                        &format!("Dataset {index}"),
                        modified,
                    )],
                    if cursor_set { 2 } else { 4 },
                    has_next,
                ))
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/search/v1/collections/dataset/items"))
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
            request
                .url
                .query_pairs()
                .any(|(key, value)| key == "filter" && value == "modified <= 9000")
        }));
    }

    #[tokio::test]
    async fn stalled_cursor_fails_the_walk_instead_of_looping() {
        #[derive(Clone)]
        struct SameTimestampCatalog;

        impl Respond for SameTimestampCatalog {
            fn respond(&self, request: &Request) -> ResponseTemplate {
                let offset: usize = request
                    .url
                    .query_pairs()
                    .find(|(name, _)| name == "startindex")
                    .map(|(_, value)| value.parse::<usize>().unwrap() - 1)
                    .unwrap_or_default();
                // Every dataset shares one timestamp, so the cursor can never
                // advance past it and re-reads the same two ids forever.
                ResponseTemplate::new(200).set_body_json(items_page(
                    vec![feature(&format!("ds-{offset}"), "Same instant", 5_000)],
                    10,
                    true,
                ))
            }
        }

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/search/v1/collections/dataset/items"))
            .respond_with(SameTimestampCatalog)
            .mount(&server)
            .await;

        let mut client = test_client(&server.uri());
        client.page_size = 1;
        client.offset_window = 2;
        let error = client.search_all_datasets().await.unwrap_err();
        assert!(error.to_string().contains("cannot advance"));
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
                    ResponseTemplate::new(200).set_body_json(items_page(vec![], 0, false))
                }
            }
        }

        let server = MockServer::start().await;
        let calls = Arc::new(AtomicUsize::new(0));
        Mock::given(method("GET"))
            .and(path("/api/search/v1/collections/dataset/items"))
            .respond_with(RateLimitOnce(calls.clone()))
            .mount(&server)
            .await;

        let client =
            ArcGisClient::new_with_http_config(&server.uri(), test_http_config(2)).unwrap();
        assert!(client.search_all_datasets().await.unwrap().is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 2); // first page + retry
    }

    #[tokio::test]
    async fn incremental_search_filters_server_side_and_paginates() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/search/v1/collections/dataset/items"))
            .and(query_param("filter", "modified >= 1752105600000"))
            .and(query_param("sortBy", "-properties.modified"))
            .respond_with(ResponseTemplate::new(200).set_body_json(items_page(
                vec![feature("recent", "Recent", 1_752_192_000_000)],
                1,
                false,
            )))
            .expect(1)
            .mount(&server)
            .await;

        let client = test_client(&server.uri());
        let since = Utc.with_ymd_and_hms(2025, 7, 10, 0, 0, 0).unwrap();
        let datasets = client.search_modified_since(since).await.unwrap();
        assert_eq!(datasets.len(), 1);
        assert_eq!(datasets[0].id, "recent");
    }

    #[tokio::test]
    async fn incremental_search_errors_when_results_exceed_the_window() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/search/v1/collections/dataset/items"))
            .respond_with(ResponseTemplate::new(200).set_body_json(items_page(
                vec![feature("recent", "Recent", 1_752_192_000_000)],
                10_000, // clamped numberMatched still exceeds the window
                true,
            )))
            .mount(&server)
            .await;

        let client = test_client(&server.uri());
        let since = Utc.with_ymd_and_hms(2025, 7, 10, 0, 0, 0).unwrap();
        let error = client.search_modified_since(since).await.unwrap_err();
        assert!(error.to_string().contains("pagination window"));
    }

    #[tokio::test]
    async fn get_dataset_maps_missing_ids_to_dataset_not_found() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/search/v1/collections/dataset/items/missing"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "message": "Item not found",
                "statusCode": 404
            })))
            .mount(&server)
            .await;

        let client = test_client(&server.uri());
        let error = client.get_dataset("missing").await.unwrap_err();
        assert!(matches!(error, AppError::DatasetNotFound(_)));
    }

    #[tokio::test]
    async fn get_dataset_parses_a_single_feature() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/search/v1/collections/dataset/items/abc123"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "id": "abc123",
                "type": "Feature",
                "properties": {"id": "abc123", "title": "Street Trees"}
            })))
            .mount(&server)
            .await;

        let client = test_client(&server.uri());
        let dataset = client.get_dataset("abc123").await.unwrap();
        assert_eq!(dataset.id, "abc123");
        assert_eq!(dataset.title, "Street Trees");
    }

    #[test]
    fn retry_backoff_starts_at_base_and_saturates_before_overflow() {
        let base = Duration::from_millis(500);
        assert_eq!(retry_delay(base, 1), base);
        assert_eq!(retry_delay(base, 2), Duration::from_secs(1));
        assert_eq!(retry_delay(base, u32::MAX), MAX_RETRY_DELAY);
    }

    #[tokio::test]
    #[ignore = "requires network access to a public ArcGIS Hub portal"]
    async fn arcgis_smoke_catalog() {
        // Opt-in smoke test: cargo test -p ceres-client arcgis_smoke -- --ignored
        // Override the portal with CERES_ARCGIS_SMOKE_URL.
        let url = std::env::var("CERES_ARCGIS_SMOKE_URL")
            .unwrap_or_else(|_| "https://opendata.dc.gov".to_string());
        let client = ArcGisClient::new(&url).unwrap();
        let count = client.dataset_count().await.unwrap();
        assert!(count > 0, "{url} returned no datasets");
        let mut stream = client.search_all_datasets_stream();
        let first_page = stream.next().await.unwrap().unwrap();
        assert!(!first_page.is_empty());
        eprintln!("{url}: {count} datasets; first={}", first_page[0].id);
    }

    #[tokio::test]
    #[ignore = "requires network access to a public ArcGIS Hub portal"]
    async fn arcgis_rejects_global_scope_smoke() {
        let url = "https://maps-greenwood.hub.arcgis.com";
        let html = reqwest::get(url).await.unwrap().text().await.unwrap();
        assert!(
            catalog_item_scope_is_empty(&html),
            "the known empty-scope Hub page was not detected"
        );
        let client = ArcGisClient::new(url).unwrap();
        let error = client.dataset_count().await.unwrap_err();
        assert!(error.to_string().contains("empty catalog item scope"));
    }
}
