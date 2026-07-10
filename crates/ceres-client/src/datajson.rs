//! Project Open Data / DCAT-US static `data.json` catalog client.
//!
//! A portal URL may point directly at a JSON document or at a site root, in
//! which case `data.json` is appended. Static catalogs do not expose server-side
//! pagination or incremental queries, so Ceres downloads one size-bounded
//! document and applies `modified` filtering locally.

use std::time::Duration;

use ceres_core::error::AppError;
use ceres_core::models::NewDataset;
use chrono::{DateTime, NaiveDate, Utc};
use futures::StreamExt;
use reqwest::{Client, StatusCode, Url};
use serde_json::Value;
use tokio::time::sleep;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
const DEFAULT_MAX_CATALOG_BYTES: u64 = 256 * 1024 * 1024;
const MAX_RETRIES: u32 = 3;

/// A normalized dataset plus its complete source object.
#[derive(Debug, Clone)]
pub struct DataJsonDataset {
    pub identifier: String,
    pub title: String,
    pub description: Option<String>,
    pub landing_page: String,
    pub modified: Option<DateTime<Utc>>,
    pub raw: Value,
}

/// Client for a static Project Open Data / DCAT-US catalog.
#[derive(Clone)]
pub struct DataJsonClient {
    client: Client,
    base_url: Url,
    catalog_url: Url,
    language: String,
    max_catalog_bytes: u64,
}

impl DataJsonClient {
    pub fn new(base_url_str: &str, language: &str) -> Result<Self, AppError> {
        let base_url = Url::parse(base_url_str)
            .map_err(|_| AppError::InvalidPortalUrl(base_url_str.to_string()))?;
        let catalog_url = catalog_url(&base_url)?;
        let max_catalog_bytes = std::env::var("CERES_STATIC_JSON_MAX_BYTES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(DEFAULT_MAX_CATALOG_BYTES);
        let client = Client::builder()
            .user_agent("Ceres/0.6 (open-data-harvester)")
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|error| AppError::ClientError(error.to_string()))?;

        Ok(Self {
            client,
            base_url,
            catalog_url,
            language: language.to_string(),
            max_catalog_bytes,
        })
    }

    pub fn portal_type(&self) -> &'static str {
        "dcat"
    }

    pub fn base_url(&self) -> &str {
        self.base_url.as_str()
    }

    pub fn catalog_url(&self) -> &str {
        self.catalog_url.as_str()
    }

    pub async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        Ok(self
            .search_all_datasets()
            .await?
            .into_iter()
            .map(|dataset| dataset.identifier)
            .collect())
    }

    pub async fn get_dataset(&self, id: &str) -> Result<DataJsonDataset, AppError> {
        self.search_all_datasets()
            .await?
            .into_iter()
            .find(|dataset| dataset.identifier == id)
            .ok_or_else(|| AppError::ClientError(format!("Dataset '{id}' was not found")))
    }

    pub async fn search_modified_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<DataJsonDataset>, AppError> {
        Ok(self
            .search_all_datasets()
            .await?
            .into_iter()
            .filter(|dataset| dataset.modified.is_none_or(|modified| modified >= since))
            .collect())
    }

    pub async fn search_all_datasets(&self) -> Result<Vec<DataJsonDataset>, AppError> {
        let body = self.fetch_catalog().await?;
        parse_catalog(&body, self.base_url.as_str(), &self.language)
    }

    pub fn into_new_dataset(
        data: DataJsonDataset,
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
            original_id: data.identifier,
            source_portal: portal_url.to_string(),
            url: data.landing_page,
            title: data.title,
            description: data.description,
            embedding: None,
            metadata: data.raw,
            content_hash,
        }
    }

    async fn fetch_catalog(&self) -> Result<Vec<u8>, AppError> {
        let mut last_error = AppError::Generic("No catalog request attempted".to_string());
        for attempt in 1..=MAX_RETRIES {
            match self.client.get(self.catalog_url.clone()).send().await {
                Ok(response) if response.status().is_success() => {
                    if let Some(length) = response.content_length()
                        && length > self.max_catalog_bytes
                    {
                        return Err(catalog_too_large(length, self.max_catalog_bytes));
                    }
                    let mut body = Vec::new();
                    let mut stream = response.bytes_stream();
                    while let Some(chunk) = stream.next().await {
                        let chunk =
                            chunk.map_err(|error| AppError::ClientError(error.to_string()))?;
                        let next_len = body.len() as u64 + chunk.len() as u64;
                        if next_len > self.max_catalog_bytes {
                            return Err(catalog_too_large(next_len, self.max_catalog_bytes));
                        }
                        body.extend_from_slice(&chunk);
                    }
                    return Ok(body);
                }
                Ok(response) => {
                    let status = response.status();
                    last_error = if status == StatusCode::TOO_MANY_REQUESTS {
                        AppError::RateLimitExceeded
                    } else {
                        AppError::ClientError(format!(
                            "HTTP {} from {}",
                            status.as_u16(),
                            self.catalog_url
                        ))
                    };
                    if !(status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()) {
                        return Err(last_error);
                    }
                }
                Err(error) => {
                    last_error = if error.is_timeout() {
                        AppError::Timeout(REQUEST_TIMEOUT.as_secs())
                    } else if error.is_connect() {
                        AppError::NetworkError(format!("Connection failed: {error}"))
                    } else {
                        AppError::ClientError(error.to_string())
                    };
                }
            }
            if attempt < MAX_RETRIES {
                sleep(Duration::from_millis(500 * u64::from(attempt))).await;
            }
        }
        Err(last_error)
    }
}

fn catalog_url(base_url: &Url) -> Result<Url, AppError> {
    let path = base_url.path().trim_end_matches('/');
    if path.ends_with(".json") {
        Ok(base_url.clone())
    } else {
        base_url
            .join("data.json")
            .map_err(|error| AppError::InvalidPortalUrl(error.to_string()))
    }
}

fn catalog_too_large(actual: u64, maximum: u64) -> AppError {
    AppError::ClientError(format!(
        "Static data.json catalog is too large ({actual} bytes; limit {maximum}). Set CERES_STATIC_JSON_MAX_BYTES to raise the bounded response limit."
    ))
}

/// Parses a DCAT-US catalog. The standard shape is `{ "dataset": [...] }`;
/// a top-level array is accepted for compatibility with local-government feeds.
pub fn parse_catalog(
    body: &[u8],
    portal_url: &str,
    language: &str,
) -> Result<Vec<DataJsonDataset>, AppError> {
    let root: Value = serde_json::from_slice(body).map_err(|error| {
        AppError::ClientError(format!("Portal returned invalid data.json: {error}"))
    })?;
    let entries = match &root {
        Value::Object(object) => object.get("dataset").and_then(Value::as_array),
        Value::Array(array) => Some(array),
        _ => None,
    }
    .ok_or_else(|| {
        AppError::ClientError("data.json response is missing a dataset array".to_string())
    })?;

    Ok(entries
        .iter()
        .filter_map(|entry| extract_dataset(entry, portal_url, language))
        .collect())
}

pub fn extract_dataset(value: &Value, portal_url: &str, language: &str) -> Option<DataJsonDataset> {
    let object = value.as_object()?;
    let identifier = text(object.get("identifier")?, language)?;
    let title = text(object.get("title")?, language)?;
    if identifier.trim().is_empty() || title.trim().is_empty() {
        return None;
    }
    let description = object
        .get("description")
        .and_then(|value| text(value, language))
        .filter(|value| !value.trim().is_empty());
    let landing_page = object
        .get("landingPage")
        .or_else(|| object.get("landing_page"))
        .and_then(|value| text(value, language))
        .filter(|value| Url::parse(value).is_ok())
        .or_else(|| Url::parse(&identifier).ok().map(|url| url.to_string()))
        .unwrap_or_else(|| portal_url.to_string());
    let modified = object
        .get("modified")
        .and_then(|value| text(value, language))
        .and_then(|value| parse_datetime(&value));

    Some(DataJsonDataset {
        identifier,
        title,
        description,
        landing_page,
        modified,
        raw: value.clone(),
    })
}

fn text(value: &Value, language: &str) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Object(values) => values
            .get(language)
            .or_else(|| values.get("en"))
            .or_else(|| values.values().find(|value| value.is_string()))
            .and_then(Value::as_str)
            .map(str::to_string),
        Value::Array(values) => values.iter().find_map(|value| text(value, language)),
        _ => None,
    }
}

fn parse_datetime(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|date| date.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .ok()?
                .and_hms_opt(0, 0, 0)
                .map(|date| date.and_utc())
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &[u8] = include_bytes!("../tests/fixtures/project_open_data.json");

    #[test]
    fn parses_fixture_and_preserves_source_metadata() {
        let datasets = parse_catalog(FIXTURE, "https://agency.gov/", "en").unwrap();
        assert_eq!(datasets.len(), 3);
        assert_eq!(datasets[0].identifier, "dataset-1");
        assert_eq!(datasets[0].title, "Air quality observations");
        assert_eq!(
            datasets[0].landing_page,
            "https://agency.gov/datasets/air-quality"
        );
        assert_eq!(datasets[0].raw["distribution"][0]["format"], "CSV");
        assert!(datasets[1].description.is_none());
        assert_eq!(datasets[2].title, "English title");
        let french = parse_catalog(FIXTURE, "https://agency.gov/", "fr").unwrap();
        assert_eq!(french[2].title, "Titre français");
    }

    #[test]
    fn direct_json_url_is_not_modified() {
        let client = DataJsonClient::new("https://agency.gov/catalog/data.json", "en").unwrap();
        assert_eq!(client.catalog_url(), "https://agency.gov/catalog/data.json");
    }

    #[test]
    fn site_root_gets_data_json_path() {
        let client = DataJsonClient::new("https://agency.gov/open-data/", "en").unwrap();
        assert_eq!(
            client.catalog_url(),
            "https://agency.gov/open-data/data.json"
        );
    }

    #[test]
    fn normalizes_dataset_and_hashes_selected_language() {
        let raw = serde_json::json!({
            "identifier": "one",
            "title": "Title",
            "description": "Description",
            "keyword": ["one"],
            "publisher": {"name": "Agency"}
        });
        let data = extract_dataset(&raw, "https://agency.gov/", "en").unwrap();
        let normalized = DataJsonClient::into_new_dataset(data, "https://agency.gov/", None, "en");
        assert_eq!(normalized.original_id, "one");
        assert_eq!(normalized.metadata["publisher"]["name"], "Agency");
        assert_eq!(normalized.content_hash.len(), 64);
    }

    #[test]
    fn rejects_non_catalog_json() {
        let error =
            parse_catalog(br#"{"title":"not a catalog"}"#, "https://agency.gov", "en").unwrap_err();
        assert!(error.to_string().contains("dataset array"));
    }

    #[tokio::test]
    #[ignore = "requires network access to a Project Open Data catalog"]
    async fn data_json_smoke_catalog() {
        let url = std::env::var("CERES_DATAJSON_SMOKE_URL")
            .unwrap_or_else(|_| "https://www.data.va.gov/data.json".to_string());
        let client = DataJsonClient::new(&url, "en").unwrap();
        let datasets = client.search_all_datasets().await.unwrap();
        assert!(!datasets.is_empty(), "{url} returned no usable datasets");
        eprintln!("{url}: {} datasets", datasets.len());
    }
}
