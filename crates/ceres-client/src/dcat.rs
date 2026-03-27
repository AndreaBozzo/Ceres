//! DCAT-AP REST client for harvesting datasets from udata-based open data portals.
//!
//! Supports the **udata REST flavor** of DCAT-AP used by portals such as:
//! - `data.public.lu` (Luxembourg, ~2,490 datasets)
//! - `data.gouv.fr` (France)
//!
//! # API
//!
//! Catalog endpoint: `GET {base}/api/1/site/catalog.jsonld?page=1&page_size=100`
//!
//! Responses are JSON-LD with an `@graph` array mixing `Dataset`, `Distribution`,
//! `Catalog`, and `hydra:PartialCollectionView` nodes. Only `Dataset` nodes are
//! harvested. Pagination follows the `next` link in the `hydra:PartialCollectionView`
//! node until absent.
//!
//! # Note on `get_dataset`
//!
//! The udata single-dataset endpoint (`GET {base}/api/1/datasets/{id}/`) returns
//! plain JSON, not JSON-LD — a different structure than the catalog response. Since
//! the harvest hot path uses `search_all_datasets`, `get_dataset` is not implemented
//! and returns an error. Implement it if single-dataset lookup becomes necessary.

use std::time::Duration;

use ceres_core::HttpConfig;
use ceres_core::error::AppError;
use ceres_core::models::NewDataset;
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode, Url};
use serde_json::Value;
use tokio::time::sleep;

/// Delay between paginated catalog requests to be polite to udata portals.
const PAGE_DELAY: Duration = Duration::from_millis(200);

/// Per-request timeout for DCAT catalog fetches.
///
/// JSON-LD pages carry large payloads (100 datasets with full metadata each),
/// so we use a longer timeout than the default `HttpConfig` value.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(90);

/// Maximum backoff delay for rate-limited retries.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Portal-specific dataset data parsed from a DCAT-AP JSON-LD `@graph` node.
#[derive(Debug, Clone)]
pub struct DcatDataset {
    /// Canonical landing page URI from JSON-LD `@id` field.
    pub id_uri: String,
    /// Dataset identifier from `dcat:identifier` (falls back to last segment of `id_uri`).
    pub identifier: String,
    /// Dataset title, resolved to the preferred language.
    pub title: String,
    /// Optional dataset description, resolved to the preferred language.
    pub description: Option<String>,
    /// Full raw `@graph` node preserved for the `metadata` field.
    pub raw: Value,
}

/// HTTP client for harvesting DCAT-AP portals using the udata REST catalog endpoint.
#[derive(Clone)]
pub struct DcatClient {
    client: Client,
    base_url: Url,
    language: String,
}

impl DcatClient {
    /// Creates a new DCAT client for the specified portal.
    ///
    /// # Arguments
    ///
    /// * `base_url_str` - The base URL of the portal (e.g., `https://data.public.lu`)
    /// * `language` - Preferred language for resolving multilingual fields (e.g., `"fr"`, `"en"`)
    ///
    /// # Errors
    ///
    /// Returns `AppError::InvalidPortalUrl` if the URL is malformed.
    /// Returns `AppError::ClientError` if the HTTP client cannot be built.
    pub fn new(base_url_str: &str, language: &str) -> Result<Self, AppError> {
        let base_url = Url::parse(base_url_str)
            .map_err(|_| AppError::InvalidPortalUrl(base_url_str.to_string()))?;

        let client = Client::builder()
            .user_agent("Ceres/0.1 (semantic-search-bot)")
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| AppError::ClientError(e.to_string()))?;

        Ok(Self {
            client,
            base_url,
            language: language.to_string(),
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

    /// Returns all dataset identifiers by fetching all datasets and extracting their IDs.
    ///
    /// This is not optimal — udata has no lightweight ID-list endpoint — but is
    /// acceptable since the harvest pipeline typically uses `search_all_datasets`.
    pub async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        let datasets = self.search_all_datasets().await?;
        Ok(datasets.into_iter().map(|d| d.identifier).collect())
    }

    /// Not implemented: the udata single-dataset endpoint returns plain JSON,
    /// not JSON-LD, requiring a separate parser. Use `search_all_datasets` instead.
    pub async fn get_dataset(&self, _id: &str) -> Result<DcatDataset, AppError> {
        Err(AppError::Generic(
            "get_dataset is not implemented for DCAT portals. \
             Use search_all_datasets() instead — it fetches complete metadata \
             for all datasets in a single paginated catalog request."
                .to_string(),
        ))
    }

    /// Searches for datasets modified since the given timestamp.
    ///
    /// Uses the `modified_since` query parameter supported by udata portals.
    /// The response is still paginated and follows the same `hydra:next` pattern.
    pub async fn search_modified_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<DcatDataset>, AppError> {
        let since_str = since.to_rfc3339();
        self.paginate_catalog(Some(&since_str)).await
    }

    /// Fetches all datasets from the portal using paginated catalog requests.
    pub async fn search_all_datasets(&self) -> Result<Vec<DcatDataset>, AppError> {
        self.paginate_catalog(None).await
    }

    /// Converts a `DcatDataset` into the normalized `NewDataset` model.
    ///
    /// The `url_template` parameter is ignored for DCAT portals because the JSON-LD
    /// `@id` field already provides the canonical landing page URL.
    pub fn into_new_dataset(
        data: DcatDataset,
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
            url: data.id_uri,
            title: data.title,
            description: data.description,
            embedding: None,
            metadata: data.raw,
            content_hash,
        }
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    /// Core paginated catalog fetcher.
    ///
    /// Fetches the first catalog page, then follows the `hydra:next` URL from
    /// each response until no more pages remain. This correctly handles portals
    /// that use cursor-based or non-numeric pagination schemes.
    ///
    /// # Arguments
    ///
    /// * `modified_since` - If `Some`, appends a `modified_since` query param to
    ///   the first page URL to request only recently-modified datasets.
    async fn paginate_catalog(
        &self,
        modified_since: Option<&str>,
    ) -> Result<Vec<DcatDataset>, AppError> {
        let mut all_datasets = Vec::new();
        let mut next_url = Some(self.build_first_page_url(modified_since)?);
        let mut page = 0u32;

        while let Some(url) = next_url {
            page += 1;

            let graph = match self.fetch_graph(&url).await {
                Ok(g) => g,
                Err(e) => {
                    // If we already collected datasets, return partial results
                    // rather than discarding everything.
                    if all_datasets.is_empty() {
                        return Err(e);
                    }
                    tracing::warn!(
                        page,
                        collected = all_datasets.len(),
                        error = %e,
                        "Page fetch failed; returning partial results"
                    );
                    break;
                }
            };

            // Extract Dataset nodes from @graph
            for node in &graph {
                if is_dataset_node(node)
                    && let Some(dataset) = extract_dataset(node, &self.language)
                {
                    all_datasets.push(dataset);
                }
            }

            // Follow hydra:next link if present, otherwise stop
            next_url =
                match extract_hydra_next(&graph) {
                    Some(next) => {
                        sleep(PAGE_DELAY).await;
                        Some(Url::parse(&next).map_err(|e| {
                            AppError::Generic(format!("Invalid hydra:next URL: {e}"))
                        })?)
                    }
                    None => None,
                };
        }

        Ok(all_datasets)
    }

    /// Builds the catalog URL for the first page of results.
    fn build_first_page_url(&self, modified_since: Option<&str>) -> Result<Url, AppError> {
        let mut url = self
            .base_url
            .join("api/1/site/catalog.jsonld")
            .map_err(|e| AppError::Generic(e.to_string()))?;

        {
            let mut pairs = url.query_pairs_mut();
            pairs
                .append_pair("page", "1")
                .append_pair("page_size", "100");
            if let Some(since) = modified_since {
                pairs.append_pair("modified_since", since);
            }
        }

        Ok(url)
    }

    /// Fetches a single catalog page and returns the `@graph` array.
    async fn fetch_graph(&self, url: &Url) -> Result<Vec<Value>, AppError> {
        let resp = self.request_with_retry(url).await?;

        let body: Value = resp.json().await.map_err(|e| {
            AppError::ClientError(format!("Portal returned non-JSON response: {e}"))
        })?;

        match body.get("@graph") {
            Some(Value::Array(graph)) => Ok(graph.clone()),
            Some(_) => Err(AppError::ClientError(
                "DCAT response @graph is not an array".to_string(),
            )),
            None => Err(AppError::ClientError(
                "DCAT response missing @graph".to_string(),
            )),
        }
    }

    /// HTTP GET with exponential backoff retry on transient errors and rate limits.
    async fn request_with_retry(&self, url: &Url) -> Result<reqwest::Response, AppError> {
        let http_config = HttpConfig::default();
        let max_retries = http_config.max_retries;
        let base_delay = http_config.retry_base_delay;
        let mut last_error = AppError::Generic("No attempts made".to_string());

        for attempt in 1..=max_retries {
            match self.client.get(url.clone()).send().await {
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
                        url
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
}

// =============================================================================
// JSON-LD Parsing Helpers
// =============================================================================

/// Resolves a JSON-LD text value to a plain string using the given language preference.
///
/// Handles three forms encountered in DCAT-AP JSON-LD:
///
/// 1. Plain string: `"Dataset Title"` → returned as-is
/// 2. Single language object: `{"@language": "fr", "@value": "Titre"}` → `@value`
/// 3. Array of language objects: picks entry matching `language`, falls back to
///    `"en"`, then returns the `@value` of the first non-empty entry
///
/// Returns an empty string if the value is `null`, a number, or unrecognized.
pub fn resolve_jsonld_text(value: &Value, language: &str) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Object(obj) => {
            // Single language-tagged literal: {"@language": "fr", "@value": "Titre"}
            obj.get("@value")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        }
        Value::Array(arr) => {
            // Array of language-tagged literals — pick preferred language, then "en", then first
            let find_lang = |lang: &str| -> Option<&str> {
                arr.iter().find_map(|item| {
                    let obj = item.as_object()?;
                    let item_lang = obj.get("@language")?.as_str()?;
                    if item_lang.eq_ignore_ascii_case(lang) {
                        obj.get("@value")?.as_str()
                    } else {
                        None
                    }
                })
            };

            if let Some(s) = find_lang(language) {
                return s.to_string();
            }
            if language != "en"
                && let Some(s) = find_lang("en")
            {
                return s.to_string();
            }
            // Fall back to first non-empty @value
            arr.iter()
                .find_map(|item| {
                    item.as_object()
                        .and_then(|o| o.get("@value"))
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                })
                .unwrap_or("")
                .to_string()
        }
        _ => String::new(),
    }
}

/// Returns `true` if a JSON-LD `@graph` node represents a `dcat:Dataset`.
///
/// `@type` can be a string or an array. Accepts the short form `"Dataset"`,
/// the prefixed form `"dcat:Dataset"`, and the full URI
/// `"http://www.w3.org/ns/dcat#Dataset"`.
pub fn is_dataset_node(node: &Value) -> bool {
    let type_value = match node.get("@type") {
        Some(v) => v,
        None => return false,
    };

    let is_dataset_type = |s: &str| {
        matches!(
            s,
            "Dataset" | "dcat:Dataset" | "http://www.w3.org/ns/dcat#Dataset"
        )
    };

    match type_value {
        Value::String(s) => is_dataset_type(s.as_str()),
        Value::Array(arr) => arr
            .iter()
            .any(|v| v.as_str().map(is_dataset_type).unwrap_or(false)),
        _ => false,
    }
}

/// Extracts the `next` pagination link from a `hydra:PartialCollectionView` node.
///
/// The key can appear as `"next"`, `"hydra:next"`, or the full URI
/// `"http://www.w3.org/ns/hydra/core#next"` depending on the portal's `@context`.
///
/// Returns `None` if no pagination node with a `next` link is found (last page).
pub fn extract_hydra_next(graph: &[Value]) -> Option<String> {
    for node in graph {
        let Some(obj) = node.as_object() else {
            continue;
        };

        // Detect hydra:PartialCollectionView by @type
        let is_pcv = obj.get("@type").map(|t| match t {
            Value::String(s) => {
                s == "hydra:PartialCollectionView"
                    || s == "http://www.w3.org/ns/hydra/core#PartialCollectionView"
            }
            Value::Array(arr) => arr.iter().any(|v| {
                v.as_str()
                    .map(|s| {
                        s == "hydra:PartialCollectionView"
                            || s == "http://www.w3.org/ns/hydra/core#PartialCollectionView"
                    })
                    .unwrap_or(false)
            }),
            _ => false,
        });

        if is_pcv != Some(true) {
            continue;
        }

        // Try all three key forms for the next link
        for key in &["next", "hydra:next", "http://www.w3.org/ns/hydra/core#next"] {
            if let Some(next) = obj.get(*key) {
                if let Some(s) = next.as_str() {
                    return Some(s.to_string());
                }
                // Sometimes the value is {"@id": "..."} in expanded JSON-LD
                if let Some(id) = next.get("@id").and_then(|v| v.as_str()) {
                    return Some(id.to_string());
                }
            }
        }
    }

    None
}

/// Parses a single `@graph` node into a `DcatDataset`.
///
/// Returns `None` if the node is missing required fields (`@id` or a non-empty title).
///
/// The `identifier` field is taken from `dcat:identifier` when present; otherwise
/// it falls back to the last path segment of `@id`.
pub fn extract_dataset(node: &Value, language: &str) -> Option<DcatDataset> {
    let id_uri = node.get("@id")?.as_str()?.to_string();
    if id_uri.is_empty() {
        return None;
    }

    // dcat:identifier, falling back to the last non-empty segment of @id
    let identifier = node
        .get("identifier")
        .and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            Value::Object(o) => o.get("@value").and_then(|v| v.as_str()).map(String::from),
            _ => None,
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| {
            id_uri
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .unwrap_or(&id_uri)
                .to_string()
        });

    // dct:title
    let title = node
        .get("title")
        .map(|v| resolve_jsonld_text(v, language))
        .filter(|s| !s.is_empty())?;

    // dct:description (optional)
    let description = node
        .get("description")
        .map(|v| resolve_jsonld_text(v, language))
        .filter(|s| !s.is_empty());

    Some(DcatDataset {
        id_uri,
        identifier,
        title,
        description,
        raw: node.clone(),
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ---- resolve_jsonld_text ------------------------------------------------

    #[test]
    fn resolve_plain_string() {
        let v = json!("Dataset Title");
        assert_eq!(resolve_jsonld_text(&v, "en"), "Dataset Title");
    }

    #[test]
    fn resolve_single_lang_object() {
        let v = json!({"@language": "fr", "@value": "Titre du jeu de données"});
        assert_eq!(resolve_jsonld_text(&v, "fr"), "Titre du jeu de données");
    }

    #[test]
    fn resolve_array_with_lang_match() {
        let v = json!([
            {"@language": "en", "@value": "English Title"},
            {"@language": "fr", "@value": "Titre Français"}
        ]);
        assert_eq!(resolve_jsonld_text(&v, "fr"), "Titre Français");
    }

    #[test]
    fn resolve_array_fallback_to_en() {
        let v = json!([
            {"@language": "en", "@value": "English Title"},
            {"@language": "de", "@value": "Deutscher Titel"}
        ]);
        assert_eq!(resolve_jsonld_text(&v, "fr"), "English Title");
    }

    #[test]
    fn resolve_array_fallback_to_first() {
        let v = json!([
            {"@language": "de", "@value": "Deutscher Titel"},
            {"@language": "nl", "@value": "Nederlandse Titel"}
        ]);
        assert_eq!(resolve_jsonld_text(&v, "fr"), "Deutscher Titel");
    }

    #[test]
    fn resolve_null_returns_empty() {
        assert_eq!(resolve_jsonld_text(&Value::Null, "en"), "");
    }

    // ---- is_dataset_node ---------------------------------------------------

    #[test]
    fn is_dataset_node_string_form() {
        let node = json!({"@type": "Dataset", "@id": "https://example.org/d/1"});
        assert!(is_dataset_node(&node));
    }

    #[test]
    fn is_dataset_node_array_form() {
        let node = json!({"@type": ["Dataset", "dcat:Dataset"], "@id": "https://example.org/d/1"});
        assert!(is_dataset_node(&node));
    }

    #[test]
    fn is_dataset_node_full_uri() {
        let node =
            json!({"@type": "http://www.w3.org/ns/dcat#Dataset", "@id": "https://example.org/d/1"});
        assert!(is_dataset_node(&node));
    }

    #[test]
    fn is_dataset_node_distribution_false() {
        let node = json!({"@type": "Distribution"});
        assert!(!is_dataset_node(&node));
    }

    #[test]
    fn is_dataset_node_missing_type_false() {
        let node = json!({"@id": "https://example.org/d/1"});
        assert!(!is_dataset_node(&node));
    }

    // ---- extract_hydra_next ------------------------------------------------

    #[test]
    fn extract_hydra_next_with_next_key() {
        let graph = vec![
            json!({"@type": "hydra:PartialCollectionView", "next": "https://example.org/catalog.jsonld?page=2&page_size=100"}),
        ];
        assert_eq!(
            extract_hydra_next(&graph),
            Some("https://example.org/catalog.jsonld?page=2&page_size=100".to_string())
        );
    }

    #[test]
    fn extract_hydra_next_with_hydra_prefix() {
        let graph = vec![
            json!({"@type": "hydra:PartialCollectionView", "hydra:next": "https://example.org/catalog.jsonld?page=2"}),
        ];
        assert_eq!(
            extract_hydra_next(&graph),
            Some("https://example.org/catalog.jsonld?page=2".to_string())
        );
    }

    #[test]
    fn extract_hydra_next_absent_returns_none() {
        let graph = vec![
            json!({"@type": "Dataset", "title": "foo"}),
            json!({"@type": ["Catalog", "hydra:Collection"], "totalItems": 100}),
        ];
        assert_eq!(extract_hydra_next(&graph), None);
    }

    #[test]
    fn extract_hydra_next_skips_non_object_nodes() {
        // Non-object nodes in @graph must not cause early return
        let graph = vec![
            json!("https://example.org/context"),
            json!(42),
            json!({"@type": "hydra:PartialCollectionView", "next": "https://example.org/?page=2"}),
        ];
        assert_eq!(
            extract_hydra_next(&graph),
            Some("https://example.org/?page=2".to_string())
        );
    }

    // ---- extract_dataset ---------------------------------------------------

    #[test]
    fn extract_dataset_basic() {
        let node = json!({
            "@id": "https://data.public.lu/datasets/abc123/",
            "@type": "Dataset",
            "identifier": "abc123",
            "title": "Test Dataset",
            "description": "A test description"
        });
        let dataset = extract_dataset(&node, "en").unwrap();
        assert_eq!(dataset.id_uri, "https://data.public.lu/datasets/abc123/");
        assert_eq!(dataset.identifier, "abc123");
        assert_eq!(dataset.title, "Test Dataset");
        assert_eq!(dataset.description.as_deref(), Some("A test description"));
    }

    #[test]
    fn extract_dataset_identifier_fallback_to_id_segment() {
        let node = json!({
            "@id": "https://data.public.lu/datasets/my-dataset/",
            "@type": "Dataset",
            "title": "My Dataset"
        });
        let dataset = extract_dataset(&node, "en").unwrap();
        assert_eq!(dataset.identifier, "my-dataset");
    }

    #[test]
    fn extract_dataset_missing_id_returns_none() {
        let node = json!({"@type": "Dataset", "title": "No ID"});
        assert!(extract_dataset(&node, "en").is_none());
    }

    #[test]
    fn extract_dataset_missing_title_returns_none() {
        let node = json!({"@id": "https://example.org/d/1", "@type": "Dataset"});
        assert!(extract_dataset(&node, "en").is_none());
    }

    // ---- into_new_dataset --------------------------------------------------

    #[test]
    fn into_new_dataset_mapping() {
        let dataset = DcatDataset {
            id_uri: "https://data.public.lu/datasets/abc123/".to_string(),
            identifier: "abc123".to_string(),
            title: "Test Dataset".to_string(),
            description: Some("A description".to_string()),
            raw: json!({"@id": "https://data.public.lu/datasets/abc123/"}),
        };

        let new_dataset =
            DcatClient::into_new_dataset(dataset, "https://data.public.lu", None, "fr");

        assert_eq!(new_dataset.original_id, "abc123");
        assert_eq!(new_dataset.source_portal, "https://data.public.lu");
        assert_eq!(new_dataset.url, "https://data.public.lu/datasets/abc123/");
        assert_eq!(new_dataset.title, "Test Dataset");
        assert_eq!(new_dataset.description.as_deref(), Some("A description"));
        assert!(new_dataset.embedding.is_none());

        let expected_hash = NewDataset::compute_content_hash_with_language(
            "Test Dataset",
            Some("A description"),
            "fr",
        );
        assert_eq!(new_dataset.content_hash, expected_hash);
    }

    #[test]
    fn into_new_dataset_url_template_ignored() {
        let dataset = DcatDataset {
            id_uri: "https://data.public.lu/datasets/abc123/".to_string(),
            identifier: "abc123".to_string(),
            title: "Test".to_string(),
            description: None,
            raw: json!({}),
        };

        let new_dataset = DcatClient::into_new_dataset(
            dataset,
            "https://data.public.lu",
            Some("https://example.org/{id}"),
            "en",
        );

        // url_template must be ignored — url is always id_uri
        assert_eq!(new_dataset.url, "https://data.public.lu/datasets/abc123/");
    }

    // ---- DcatClient::new ---------------------------------------------------

    #[test]
    fn dcat_client_new_valid() {
        assert!(DcatClient::new("https://data.public.lu", "fr").is_ok());
    }

    #[test]
    fn dcat_client_new_invalid_url() {
        let result = DcatClient::new("not-a-url", "en");
        assert!(matches!(result, Err(AppError::InvalidPortalUrl(_))));
    }

    // ---- Integration smoke test (requires network) -------------------------

    #[tokio::test]
    #[ignore = "requires network access to data.public.lu"]
    async fn test_dcat_smoke_luxembourg() {
        let client = DcatClient::new("https://data.public.lu", "fr").unwrap();
        let datasets = client.search_all_datasets().await.unwrap();
        assert!(
            datasets.len() > 100,
            "Expected >100 datasets, got {}",
            datasets.len()
        );
        let first = &datasets[0];
        assert!(!first.title.is_empty(), "First dataset title is empty");
        assert!(!first.id_uri.is_empty(), "First dataset id_uri is empty");
        assert!(
            !first.identifier.is_empty(),
            "First dataset identifier is empty"
        );
    }
}
