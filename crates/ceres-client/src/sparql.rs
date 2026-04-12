//! SPARQL DCAT client for harvesting datasets from SPARQL-backed open data portals.
//!
//! Supports portals that expose DCAT-AP metadata via a SPARQL endpoint, such as:
//! - `data.europa.eu` (EU Open Data Portal)
//! - `data.norge.no` (Norwegian Data Portal)
//!
//! # API
//!
//! Endpoint: `POST {base}/sparql` with `Accept: application/sparql-results+json`
//!
//! Dataset discovery uses standard DCAT vocabulary:
//! - `dcat:Dataset` for dataset type
//! - `dct:title`, `dct:description`, `dct:identifier`, `dct:modified` for fields
//!
//! Pagination uses `LIMIT`/`OFFSET` in the SPARQL query.

use std::collections::HashMap;
use std::time::Duration;

use ceres_core::HttpConfig;
use ceres_core::error::AppError;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::dcat::DcatDataset;

/// Delay between paginated SPARQL requests.
const PAGE_DELAY: Duration = Duration::from_millis(300);

/// Per-request timeout for SPARQL queries.
///
/// SPARQL queries on large catalogs can be slow, so we use a generous timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

/// Maximum backoff delay for rate-limited retries.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Default number of results per SPARQL page.
const DEFAULT_PAGE_SIZE: usize = 1000;

// =============================================================================
// SPARQL JSON Results Format (W3C standard)
// =============================================================================

/// Top-level SPARQL JSON results response.
#[derive(Debug, Deserialize)]
struct SparqlResponse {
    results: SparqlResults,
}

/// The `results` object containing bindings.
#[derive(Debug, Deserialize)]
struct SparqlResults {
    bindings: Vec<HashMap<String, SparqlValue>>,
}

/// A single SPARQL value in a binding row.
#[derive(Debug, Deserialize)]
struct SparqlValue {
    /// The RDF term value.
    value: String,
    /// Language tag for literals (e.g., `"en"`, `"nb"`).
    #[serde(rename = "xml:lang")]
    lang: Option<String>,
}

// =============================================================================
// SparqlDcatClient
// =============================================================================

/// HTTP client for harvesting DCAT-AP catalogs via SPARQL endpoints.
#[derive(Clone)]
pub struct SparqlDcatClient {
    client: Client,
    endpoint_url: Url,
    base_url: Url,
    language: String,
    page_size: usize,
}

impl SparqlDcatClient {
    /// Creates a new SPARQL DCAT client for the specified portal.
    ///
    /// The SPARQL endpoint is derived by convention as `{base_url}/sparql`.
    ///
    /// # Arguments
    ///
    /// * `base_url_str` - The base URL of the portal (e.g., `https://data.europa.eu`)
    /// * `language` - Preferred language for resolving multilingual fields (e.g., `"en"`, `"nb"`)
    pub fn new(base_url_str: &str, language: &str) -> Result<Self, AppError> {
        // Validate language as a BCP47-like tag to prevent SPARQL injection
        if language.is_empty()
            || !language
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(AppError::ConfigError(format!(
                "Invalid language tag '{language}'. Expected a BCP47 tag (e.g., 'en', 'nb', 'fr')."
            )));
        }

        let base_url = Url::parse(base_url_str)
            .map_err(|_| AppError::InvalidPortalUrl(base_url_str.to_string()))?;

        let endpoint_url = base_url
            .join("sparql")
            .map_err(|e| AppError::Generic(format!("Failed to build SPARQL endpoint URL: {e}")))?;

        let client = Client::builder()
            .user_agent("Ceres/0.1 (semantic-search-bot)")
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| AppError::ClientError(e.to_string()))?;

        Ok(Self {
            client,
            endpoint_url,
            base_url,
            language: language.to_string(),
            page_size: DEFAULT_PAGE_SIZE,
        })
    }

    /// Returns the portal type identifier.
    pub fn portal_type(&self) -> &'static str {
        "dcat"
    }

    /// Returns the base URL of the portal.
    pub fn base_url(&self) -> &str {
        self.base_url.as_str()
    }

    /// Returns all dataset identifiers via a lightweight SPARQL query.
    pub async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        let mut ids = Vec::new();
        let mut offset = 0usize;

        loop {
            let query = format!(
                "SELECT DISTINCT ?dataset \
                 WHERE {{ ?dataset a <http://www.w3.org/ns/dcat#Dataset> }} \
                 LIMIT {} OFFSET {}",
                self.page_size, offset
            );

            let bindings = self.execute_query(&query).await?;
            if bindings.is_empty() {
                break;
            }

            let count = bindings.len();
            for binding in bindings {
                if let Some(v) = binding.get("dataset") {
                    ids.push(extract_last_segment(&v.value));
                }
            }

            if count < self.page_size {
                break;
            }
            offset += self.page_size;
            sleep(PAGE_DELAY).await;
        }

        Ok(ids)
    }

    /// Not implemented: SPARQL single-dataset lookup is not used by the harvest pipeline.
    pub async fn get_dataset(&self, _id: &str) -> Result<DcatDataset, AppError> {
        Err(AppError::Generic(
            "get_dataset is not implemented for SPARQL DCAT portals. \
             Use search_all_datasets() instead."
                .to_string(),
        ))
    }

    /// Searches for datasets modified since the given timestamp.
    pub async fn search_modified_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<DcatDataset>, AppError> {
        let filter = format!(
            "FILTER (?modified > \"{}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)",
            since.to_rfc3339()
        );
        self.paginate_sparql(Some(&filter)).await
    }

    /// Fetches all datasets from the portal using paginated SPARQL queries.
    pub async fn search_all_datasets(&self) -> Result<Vec<DcatDataset>, AppError> {
        self.paginate_sparql(None).await
    }

    /// Returns the total number of datasets in the catalog.
    pub async fn dataset_count(&self) -> Result<usize, AppError> {
        let query = "SELECT (COUNT(DISTINCT ?dataset) AS ?count) \
             WHERE { ?dataset a <http://www.w3.org/ns/dcat#Dataset> }";

        let bindings = self.execute_query(query).await?;
        bindings
            .first()
            .and_then(|b| b.get("count"))
            .and_then(|v| v.value.parse::<usize>().ok())
            .ok_or_else(|| AppError::Generic("Failed to parse dataset count".to_string()))
    }

    /// Streams catalog datasets page-by-page using LIMIT/OFFSET pagination.
    ///
    /// Each yielded item contains the datasets extracted from one SPARQL page.
    /// Memory is bounded to a single page at a time.
    pub fn paginate_sparql_stream(
        &self,
        modified_since: Option<String>,
    ) -> BoxStream<'_, Result<Vec<DcatDataset>, AppError>> {
        let filter = modified_since.map(|since| {
            format!(
                "FILTER (?modified > \"{}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)",
                since
            )
        });

        struct PaginationState {
            offset: usize,
            done: bool,
        }

        let initial = PaginationState {
            offset: 0,
            done: false,
        };

        Box::pin(futures::stream::unfold(initial, move |mut state| {
            let filter = filter.clone();
            async move {
                if state.done {
                    return None;
                }

                let query = self.build_dataset_query(state.offset, filter.as_deref());

                match self.execute_query(&query).await {
                    Ok(bindings) => {
                        if bindings.is_empty() {
                            return None;
                        }

                        let count = bindings.len();
                        let datasets = self.bindings_to_datasets(bindings);

                        if count < self.page_size {
                            state.done = true;
                        } else {
                            state.offset += self.page_size;
                            sleep(PAGE_DELAY).await;
                        }

                        Some((Ok(datasets), state))
                    }
                    Err(e) => {
                        if state.offset == 0 {
                            // First page failed — propagate error
                            Some((
                                Err(e),
                                PaginationState {
                                    offset: 0,
                                    done: true,
                                },
                            ))
                        } else {
                            tracing::warn!(
                                offset = state.offset,
                                error = %e,
                                "SPARQL page fetch failed; stopping stream"
                            );
                            None
                        }
                    }
                }
            }
        }))
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    /// Core paginated SPARQL fetcher.
    async fn paginate_sparql(
        &self,
        extra_filter: Option<&str>,
    ) -> Result<Vec<DcatDataset>, AppError> {
        let mut all_datasets = Vec::new();
        let mut offset = 0usize;

        loop {
            let query = self.build_dataset_query(offset, extra_filter);
            let bindings = match self.execute_query(&query).await {
                Ok(b) => b,
                Err(e) => {
                    if all_datasets.is_empty() {
                        return Err(e);
                    }
                    tracing::warn!(
                        offset,
                        collected = all_datasets.len(),
                        error = %e,
                        "SPARQL page fetch failed; returning partial results"
                    );
                    break;
                }
            };

            if bindings.is_empty() {
                break;
            }

            let count = bindings.len();
            let datasets = self.bindings_to_datasets(bindings);
            all_datasets.extend(datasets);

            if count < self.page_size {
                break;
            }
            offset += self.page_size;
            sleep(PAGE_DELAY).await;
        }

        Ok(all_datasets)
    }

    /// Builds the SPARQL SELECT query for dataset discovery.
    fn build_dataset_query(&self, offset: usize, extra_filter: Option<&str>) -> String {
        let lang = &self.language;
        let extra = extra_filter.unwrap_or("");

        format!(
            "PREFIX dcat: <http://www.w3.org/ns/dcat#>\n\
             PREFIX dct:  <http://purl.org/dc/terms/>\n\
             \n\
             SELECT ?dataset ?title ?description ?identifier ?modified\n\
             WHERE {{\n\
               ?dataset a dcat:Dataset .\n\
               ?dataset dct:title ?title .\n\
               FILTER (lang(?title) = \"{lang}\" || lang(?title) = \"en\" || lang(?title) = \"\")\n\
               OPTIONAL {{ ?dataset dct:description ?description .\n\
                 FILTER (lang(?description) = \"{lang}\" || lang(?description) = \"en\" || lang(?description) = \"\")\n\
               }}\n\
               OPTIONAL {{ ?dataset dct:identifier ?identifier }}\n\
               OPTIONAL {{ ?dataset dct:modified ?modified }}\n\
               {extra}\n\
             }}\n\
             LIMIT {} OFFSET {}",
            self.page_size, offset
        )
    }

    /// Executes a SPARQL query and returns the result bindings.
    async fn execute_query(
        &self,
        query: &str,
    ) -> Result<Vec<HashMap<String, SparqlValue>>, AppError> {
        let resp = self.request_with_retry(query).await?;

        let body: SparqlResponse = resp.json().await.map_err(|e| {
            AppError::ClientError(format!("SPARQL endpoint returned non-JSON response: {e}"))
        })?;

        Ok(body.results.bindings)
    }

    /// HTTP POST with exponential backoff retry on transient errors and rate limits.
    async fn request_with_retry(&self, query: &str) -> Result<reqwest::Response, AppError> {
        let http_config = HttpConfig::default();
        let max_retries = http_config.max_retries;
        let base_delay = http_config.retry_base_delay;
        let mut last_error = AppError::Generic("No attempts made".to_string());

        for attempt in 1..=max_retries {
            let body = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("query", query)
                .finish();
            match self
                .client
                .post(self.endpoint_url.clone())
                .header("Accept", "application/sparql-results+json")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(body)
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();

                    if status.is_success() {
                        return Ok(resp);
                    }

                    if status == StatusCode::TOO_MANY_REQUESTS {
                        last_error = AppError::RateLimitExceeded;
                        if attempt < max_retries {
                            let delay = resp
                                .headers()
                                .get("retry-after")
                                .and_then(|v| v.to_str().ok())
                                .and_then(|v| v.parse::<u64>().ok())
                                .map(Duration::from_secs)
                                .unwrap_or_else(|| {
                                    (base_delay * 2_u32.pow(attempt)).min(MAX_RETRY_DELAY)
                                });
                            sleep(delay).await;
                            continue;
                        }
                        return Err(AppError::RateLimitExceeded);
                    }

                    if status.is_server_error() {
                        last_error = AppError::ClientError(format!(
                            "Server error: HTTP {}",
                            status.as_u16()
                        ));
                        if attempt < max_retries {
                            sleep(base_delay * attempt).await;
                            continue;
                        }
                    }

                    return Err(AppError::ClientError(format!(
                        "HTTP {} from {}",
                        status.as_u16(),
                        self.endpoint_url
                    )));
                }
                Err(e) => {
                    if e.is_timeout() {
                        last_error = AppError::Timeout(REQUEST_TIMEOUT.as_secs());
                    } else if e.is_connect() {
                        last_error = AppError::NetworkError(format!("Connection failed: {e}"));
                    } else {
                        last_error = AppError::ClientError(e.to_string());
                    }
                    if attempt < max_retries && (e.is_timeout() || e.is_connect()) {
                        sleep(base_delay * attempt).await;
                        continue;
                    }
                }
            }
        }

        Err(last_error)
    }

    /// Returns a language-preference rank for selecting the best value.
    /// Higher rank = better match: preferred language > "en" > untagged/other.
    fn lang_rank(&self, lang: Option<&str>) -> u8 {
        match lang {
            Some(l) if l == self.language => 2,
            Some("en") => 1,
            _ => 0,
        }
    }

    /// Selects the best value for a field from grouped bindings based on language preference.
    fn best_value<'a>(
        &self,
        bindings: &'a [HashMap<String, SparqlValue>],
        key: &str,
    ) -> Option<&'a SparqlValue> {
        bindings
            .iter()
            .filter_map(|b| b.get(key))
            .filter(|v| !v.value.is_empty())
            .max_by(|a, b| {
                self.lang_rank(a.lang.as_deref())
                    .cmp(&self.lang_rank(b.lang.as_deref()))
            })
    }

    /// Converts SPARQL bindings into `DcatDataset` objects, deduplicating by dataset URI.
    ///
    /// SPARQL may return multiple rows per dataset (e.g., different language tags).
    /// For each dataset, the title and description with the best language match are selected
    /// (preferred language > "en" > untagged).
    fn bindings_to_datasets(
        &self,
        bindings: Vec<HashMap<String, SparqlValue>>,
    ) -> Vec<DcatDataset> {
        let mut grouped: HashMap<String, Vec<HashMap<String, SparqlValue>>> = HashMap::new();

        for binding in bindings {
            let Some(dataset_uri) = binding.get("dataset").map(|v| v.value.clone()) else {
                continue;
            };
            grouped.entry(dataset_uri).or_default().push(binding);
        }

        let mut datasets = Vec::new();

        for (dataset_uri, rows) in &grouped {
            let Some(title_val) = self.best_value(rows, "title") else {
                continue; // Skip datasets without a title
            };
            let title = title_val.value.clone();

            let description = self
                .best_value(rows, "description")
                .map(|v| v.value.clone());

            let identifier = rows
                .iter()
                .filter_map(|b| b.get("identifier"))
                .map(|v| v.value.clone())
                .find(|s| !s.is_empty())
                .unwrap_or_else(|| extract_last_segment(dataset_uri));

            // Build raw metadata from the row that provided the selected title
            let raw_binding = rows
                .iter()
                .find(|b| b.get("title").map(|v| v.value.as_str()) == Some(title.as_str()))
                .unwrap_or(&rows[0]);
            let raw = binding_to_json(raw_binding);

            datasets.push(DcatDataset {
                id_uri: dataset_uri.clone(),
                identifier,
                title,
                description,
                raw,
            });
        }

        datasets
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Extracts the last non-empty path segment from a URI.
fn extract_last_segment(uri: &str) -> String {
    uri.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(uri)
        .to_string()
}

/// Converts a SPARQL binding row into a `serde_json::Value` for raw metadata storage.
fn binding_to_json(binding: &HashMap<String, SparqlValue>) -> Value {
    let mut map = serde_json::Map::new();
    for (key, val) in binding {
        let mut entry = serde_json::Map::new();
        entry.insert("value".to_string(), json!(val.value));
        if let Some(ref lang) = val.lang {
            entry.insert("xml:lang".to_string(), json!(lang));
        }
        map.insert(key.clone(), Value::Object(entry));
    }
    Value::Object(map)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- SPARQL response parsing -------------------------------------------

    fn sample_sparql_json() -> &'static str {
        r#"{
            "head": { "vars": ["dataset", "title", "description", "identifier", "modified"] },
            "results": {
                "bindings": [
                    {
                        "dataset": { "type": "uri", "value": "https://data.europa.eu/dataset/1234" },
                        "title": { "type": "literal", "value": "Air Quality Data", "xml:lang": "en" },
                        "description": { "type": "literal", "value": "Air quality measurements", "xml:lang": "en" },
                        "identifier": { "type": "literal", "value": "air-quality-1234" }
                    },
                    {
                        "dataset": { "type": "uri", "value": "https://data.europa.eu/dataset/5678" },
                        "title": { "type": "literal", "value": "Population Census", "xml:lang": "en" }
                    }
                ]
            }
        }"#
    }

    #[test]
    fn parse_sparql_response() {
        let resp: SparqlResponse = serde_json::from_str(sample_sparql_json()).unwrap();
        assert_eq!(resp.results.bindings.len(), 2);

        let first = &resp.results.bindings[0];
        assert_eq!(
            first["dataset"].value,
            "https://data.europa.eu/dataset/1234"
        );
        assert_eq!(first["title"].value, "Air Quality Data");
        assert_eq!(first["title"].lang.as_deref(), Some("en"));
        assert_eq!(first["identifier"].value, "air-quality-1234");

        let second = &resp.results.bindings[1];
        assert_eq!(
            second["dataset"].value,
            "https://data.europa.eu/dataset/5678"
        );
        assert!(second.get("description").is_none());
    }

    // ---- bindings_to_datasets ----------------------------------------------

    #[test]
    fn bindings_to_datasets_basic() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en").unwrap();
        let resp: SparqlResponse = serde_json::from_str(sample_sparql_json()).unwrap();
        let datasets = client.bindings_to_datasets(resp.results.bindings);

        assert_eq!(datasets.len(), 2);

        // Find by URI (order is not guaranteed from HashMap)
        let air = datasets
            .iter()
            .find(|d| d.id_uri == "https://data.europa.eu/dataset/1234")
            .unwrap();
        assert_eq!(air.title, "Air Quality Data");
        assert_eq!(air.description.as_deref(), Some("Air quality measurements"));
        assert_eq!(air.identifier, "air-quality-1234");

        let pop = datasets
            .iter()
            .find(|d| d.id_uri == "https://data.europa.eu/dataset/5678")
            .unwrap();
        assert_eq!(pop.title, "Population Census");
        assert!(pop.description.is_none());
        assert_eq!(pop.identifier, "5678"); // fallback to last URI segment
    }

    #[test]
    fn bindings_deduplicates_by_uri() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en").unwrap();
        let json = r#"{
            "results": {
                "bindings": [
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/d/1" },
                        "title": { "type": "literal", "value": "First Title", "xml:lang": "en" }
                    },
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/d/1" },
                        "title": { "type": "literal", "value": "Duplicate Title", "xml:lang": "en" }
                    }
                ]
            }
        }"#;
        let resp: SparqlResponse = serde_json::from_str(json).unwrap();
        let datasets = client.bindings_to_datasets(resp.results.bindings);
        assert_eq!(datasets.len(), 1);
    }

    #[test]
    fn bindings_prefers_requested_language() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "nb").unwrap();
        let json = r#"{
            "results": {
                "bindings": [
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/d/1" },
                        "title": { "type": "literal", "value": "English Title", "xml:lang": "en" },
                        "description": { "type": "literal", "value": "English desc", "xml:lang": "en" }
                    },
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/d/1" },
                        "title": { "type": "literal", "value": "Norsk Tittel", "xml:lang": "nb" },
                        "description": { "type": "literal", "value": "Norsk beskrivelse", "xml:lang": "nb" }
                    }
                ]
            }
        }"#;
        let resp: SparqlResponse = serde_json::from_str(json).unwrap();
        let datasets = client.bindings_to_datasets(resp.results.bindings);
        assert_eq!(datasets.len(), 1);
        assert_eq!(datasets[0].title, "Norsk Tittel");
        assert_eq!(
            datasets[0].description.as_deref(),
            Some("Norsk beskrivelse")
        );
    }

    #[test]
    fn bindings_skip_empty_title() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en").unwrap();
        let json = r#"{
            "results": {
                "bindings": [
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/d/1" },
                        "title": { "type": "literal", "value": "" }
                    },
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/d/2" },
                        "title": { "type": "literal", "value": "Valid Title" }
                    }
                ]
            }
        }"#;
        let resp: SparqlResponse = serde_json::from_str(json).unwrap();
        let datasets = client.bindings_to_datasets(resp.results.bindings);
        assert_eq!(datasets.len(), 1);
        assert_eq!(datasets[0].title, "Valid Title");
    }

    // ---- extract_last_segment -----------------------------------------------

    #[test]
    fn extract_last_segment_basic() {
        assert_eq!(
            extract_last_segment("https://data.europa.eu/dataset/1234"),
            "1234"
        );
    }

    #[test]
    fn extract_last_segment_trailing_slash() {
        assert_eq!(
            extract_last_segment("https://data.europa.eu/dataset/1234/"),
            "1234"
        );
    }

    // ---- query building ----------------------------------------------------

    #[test]
    fn build_dataset_query_basic() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en").unwrap();
        let query = client.build_dataset_query(0, None);
        assert!(query.contains("dcat:Dataset"));
        assert!(query.contains("dct:title"));
        assert!(query.contains("LIMIT 1000 OFFSET 0"));
        assert!(query.contains("lang(?title) = \"en\""));
    }

    #[test]
    fn build_dataset_query_with_offset() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "nb").unwrap();
        let query = client.build_dataset_query(2000, None);
        assert!(query.contains("LIMIT 1000 OFFSET 2000"));
        assert!(query.contains("lang(?title) = \"nb\""));
    }

    #[test]
    fn build_dataset_query_with_filter() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en").unwrap();
        let filter = "FILTER (?modified > \"2026-01-01T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)";
        let query = client.build_dataset_query(0, Some(filter));
        assert!(query.contains(filter));
    }

    // ---- SparqlDcatClient::new ---------------------------------------------

    #[test]
    fn sparql_client_new_valid() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en").unwrap();
        assert_eq!(client.portal_type(), "dcat");
        assert_eq!(client.base_url(), "https://data.europa.eu/");
        assert_eq!(
            client.endpoint_url.as_str(),
            "https://data.europa.eu/sparql"
        );
    }

    #[test]
    fn sparql_client_new_invalid_url() {
        let result = SparqlDcatClient::new("not-a-url", "en");
        assert!(matches!(result, Err(AppError::InvalidPortalUrl(_))));
    }

    #[test]
    fn sparql_client_rejects_invalid_language() {
        let result = SparqlDcatClient::new("https://data.europa.eu", "en\"}");
        assert!(matches!(result, Err(AppError::ConfigError(_))));

        let result = SparqlDcatClient::new("https://data.europa.eu", "");
        assert!(matches!(result, Err(AppError::ConfigError(_))));
    }

    // ---- binding_to_json ---------------------------------------------------

    #[test]
    fn binding_to_json_preserves_fields() {
        let mut binding = HashMap::new();
        binding.insert(
            "dataset".to_string(),
            SparqlValue {
                value: "https://example.org/d/1".to_string(),
                lang: None,
            },
        );
        binding.insert(
            "title".to_string(),
            SparqlValue {
                value: "Test Title".to_string(),
                lang: Some("en".to_string()),
            },
        );

        let json = binding_to_json(&binding);
        let obj = json.as_object().unwrap();
        assert_eq!(obj["dataset"]["value"], "https://example.org/d/1");
        assert_eq!(obj["title"]["value"], "Test Title");
        assert_eq!(obj["title"]["xml:lang"], "en");
    }

    // ---- Integration smoke test (requires network) -------------------------

    #[tokio::test]
    #[ignore = "requires network access to data.europa.eu"]
    async fn test_sparql_smoke_europa() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en").unwrap();
        let count = client.dataset_count().await.unwrap();
        assert!(count > 100, "Expected >100 datasets, got {}", count);

        let datasets = client.search_all_datasets().await.unwrap();
        assert!(
            !datasets.is_empty(),
            "Expected at least some datasets from data.europa.eu"
        );
        let first = &datasets[0];
        assert!(!first.title.is_empty(), "First dataset title is empty");
        assert!(!first.id_uri.is_empty(), "First dataset id_uri is empty");
    }
}
