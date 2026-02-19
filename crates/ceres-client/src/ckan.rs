//! CKAN client for harvesting datasets from CKAN-compatible open data portals.
//!
//! # Future Extensions
//!
//! The `PortalClient` trait (defined in `ceres_core::traits`) was introduced in PR #90,
//! abstracting over portal types. Remaining portal implementations tracked in #61:
//! - Socrata API (used by many US cities): <https://dev.socrata.com/>
//! - DCAT-AP harvester for EU portals: <https://joinup.ec.europa.eu/collection/semantic-interoperability-community-semic/solution/dcat-application-profile-data-portals-europe>

use std::time::Duration;

use ceres_core::HttpConfig;
use ceres_core::LocalizedField;
use ceres_core::error::AppError;
use ceres_core::models::NewDataset;
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::sleep;

/// Generic wrapper for CKAN API responses.
///
/// CKAN API reference: <https://docs.ckan.org/en/2.9/api/>
///
/// CKAN always returns responses with the structure:
/// ```json
/// {
///     "success": bool,
///     "result": T
/// }
/// ```
#[derive(Deserialize, Debug)]
struct CkanResponse<T> {
    success: bool,
    result: T,
}

/// Response structure for CKAN package_search API.
#[derive(Deserialize, Debug)]
struct PackageSearchResult {
    count: usize,
    results: Vec<CkanDataset>,
}

/// Data Transfer Object for CKAN dataset details.
///
/// This structure represents the core fields returned by the CKAN `package_show` API.
/// Additional fields returned by CKAN are captured in the `extras` map.
///
/// The `title` and `notes` fields use [`LocalizedField`] to support both plain
/// strings and multilingual objects (e.g., `{"en": "...", "de": "..."}`).
///
/// # Examples
///
/// ```
/// use ceres_client::ckan::CkanDataset;
///
/// // Plain string fields (most portals)
/// let json = r#"{
///     "id": "dataset-123",
///     "name": "my-dataset",
///     "title": "My Dataset",
///     "notes": "Description of the dataset",
///     "organization": {"name": "test-org"}
/// }"#;
///
/// let dataset: CkanDataset = serde_json::from_str(json).unwrap();
/// assert_eq!(dataset.id, "dataset-123");
/// assert_eq!(dataset.title.resolve("en"), "My Dataset");
/// assert!(dataset.extras.contains_key("organization"));
///
/// // Multilingual fields (e.g., Swiss portals)
/// let json = r#"{
///     "id": "dataset-456",
///     "name": "swiss-dataset",
///     "title": {"en": "English Title", "de": "Deutscher Titel"},
///     "notes": {"en": "English description", "de": "Deutsche Beschreibung"}
/// }"#;
///
/// let dataset: CkanDataset = serde_json::from_str(json).unwrap();
/// assert_eq!(dataset.title.resolve("de"), "Deutscher Titel");
/// assert_eq!(dataset.notes.as_ref().unwrap().resolve("en"), "English description");
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct CkanDataset {
    /// Unique identifier for the dataset
    pub id: String,
    /// URL-friendly name/slug of the dataset
    pub name: String,
    /// Human-readable title of the dataset (plain string or multilingual object)
    pub title: LocalizedField,
    /// Optional description/notes about the dataset (plain string or multilingual object)
    pub notes: Option<LocalizedField>,
    /// All other fields returned by CKAN (e.g., organization, tags, resources)
    #[serde(flatten)]
    pub extras: serde_json::Map<String, Value>,
}

/// HTTP client for interacting with CKAN open data portals.
///
/// CKAN (Comprehensive Knowledge Archive Network) is an open-source data management
/// system used by many government open data portals worldwide.
///
/// # Examples
///
/// ```no_run
/// use ceres_client::CkanClient;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let client = CkanClient::new("https://dati.gov.it")?;
/// let dataset_ids = client.list_package_ids().await?;
/// println!("Found {} datasets", dataset_ids.len());
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct CkanClient {
    client: Client,
    base_url: Url,
}

impl CkanClient {
    /// Delay between paginated API requests to avoid rate limiting.
    const PAGE_DELAY: Duration = Duration::from_secs(1);

    /// Maximum backoff delay for rate-limited retries within `request_with_retry`.
    const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

    /// Cooldown when a page request is rate-limited after exhausting low-level retries.
    /// The pagination loop waits this long before retrying the same page.
    const PAGE_RATE_LIMIT_COOLDOWN: Duration = Duration::from_secs(60);

    /// Maximum number of page-level retries when a page is rate-limited.
    const PAGE_RATE_LIMIT_RETRIES: u32 = 3;

    /// Creates a new CKAN client for the specified portal.
    ///
    /// # Arguments
    ///
    /// * `base_url_str` - The base URL of the CKAN portal (e.g., <https://dati.gov.it>)
    ///
    /// # Returns
    ///
    /// Returns a configured `CkanClient` instance.
    ///
    /// # Errors
    ///
    /// Returns `AppError::Generic` if the URL is invalid or malformed.
    /// Returns `AppError::ClientError` if the HTTP client cannot be built.
    // TODO(validation): Add optional portal validation on construction
    // Could probe /api/3/action/site_read to verify it's a valid CKAN portal.
    // Add: pub async fn new_validated(url: &str) -> Result<Self, AppError>
    pub fn new(base_url_str: &str) -> Result<Self, AppError> {
        let base_url = Url::parse(base_url_str)
            .map_err(|_| AppError::InvalidPortalUrl(base_url_str.to_string()))?;

        let http_config = HttpConfig::default();
        let client = Client::builder()
            // TODO(config): Make User-Agent configurable or use version from Cargo.toml
            .user_agent("Ceres/0.1 (semantic-search-bot)")
            .timeout(http_config.timeout)
            .build()
            .map_err(|e| AppError::ClientError(e.to_string()))?;

        Ok(Self { client, base_url })
    }

    /// Fetches the complete list of dataset IDs from the CKAN portal.
    ///
    /// This method calls the CKAN `package_list` API endpoint, which returns
    /// all dataset identifiers available in the portal.
    ///
    /// # Returns
    ///
    /// A vector of dataset ID strings.
    ///
    /// # Errors
    ///
    /// Returns `AppError::ClientError` if the HTTP request fails.
    /// Returns `AppError::Generic` if the CKAN API returns an error.
    ///
    /// # Performance Note
    ///
    /// TODO(performance): Add pagination for large portals
    /// Large portals can have 100k+ datasets. CKAN supports limit/offset params.
    /// Consider: `list_package_ids_paginated(limit: usize, offset: usize)`
    /// Or streaming: `list_package_ids_stream() -> impl Stream<Item = ...>`
    pub async fn list_package_ids(&self) -> Result<Vec<String>, AppError> {
        let url = self
            .base_url
            .join("api/3/action/package_list")
            .map_err(|e| AppError::Generic(e.to_string()))?;

        let resp = self.request_with_retry(&url).await?;

        let ckan_resp: CkanResponse<Vec<String>> = resp
            .json()
            .await
            .map_err(|e| AppError::ClientError(e.to_string()))?;

        if !ckan_resp.success {
            return Err(AppError::Generic(
                "CKAN API returned success: false".to_string(),
            ));
        }

        Ok(ckan_resp.result)
    }

    /// Fetches the full details of a specific dataset by ID.
    ///
    /// This method calls the CKAN `package_show` API endpoint to retrieve
    /// complete metadata for a single dataset.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier or name slug of the dataset
    ///
    /// # Returns
    ///
    /// A `CkanDataset` containing the dataset's metadata.
    pub async fn show_package(&self, id: &str) -> Result<CkanDataset, AppError> {
        let mut url = self
            .base_url
            .join("api/3/action/package_show")
            .map_err(|e| AppError::Generic(e.to_string()))?;

        url.query_pairs_mut().append_pair("id", id);

        let resp = self.request_with_retry(&url).await?;

        let ckan_resp: CkanResponse<CkanDataset> = resp
            .json()
            .await
            .map_err(|e| AppError::ClientError(e.to_string()))?;

        if !ckan_resp.success {
            return Err(AppError::Generic(format!(
                "CKAN failed to show package {}",
                id
            )));
        }

        Ok(ckan_resp.result)
    }

    /// Searches for datasets modified since a given timestamp.
    ///
    /// Uses CKAN's `package_search` API with a `metadata_modified` filter to fetch
    /// only datasets that have been updated since the last sync. This enables
    /// incremental harvesting with ~99% fewer API calls in steady state.
    ///
    /// # Arguments
    ///
    /// * `since` - Only return datasets modified after this timestamp
    ///
    /// # Returns
    ///
    /// A vector of `CkanDataset` containing all datasets modified since the given time.
    /// Unlike `list_package_ids()` + `show_package()`, this returns complete dataset
    /// objects in a single paginated query.
    ///
    /// # Errors
    ///
    /// Returns `AppError::ClientError` if the HTTP request fails.
    /// Returns `AppError::Generic` if the CKAN API returns an error or doesn't support
    /// the `package_search` endpoint (some older CKAN instances).
    pub async fn search_modified_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<CkanDataset>, AppError> {
        let since_str = since.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let fq = Some(format!("metadata_modified:[{} TO *]", since_str));
        self.paginated_search(fq.as_deref()).await
    }

    /// Fetches all datasets from the portal using paginated `package_search`.
    ///
    /// This makes ~N/1000 API calls instead of N individual `package_show` calls,
    /// which is critical for large portals like HDX (~40k datasets) that enforce
    /// strict rate limits.
    pub async fn search_all_datasets(&self) -> Result<Vec<CkanDataset>, AppError> {
        self.paginated_search(None).await
    }

    /// Core paginated search with page-level rate limit resilience.
    ///
    /// Fetches all matching datasets using `package_search`, handling pagination
    /// and rate limiting at two levels:
    /// 1. Per-request retries via `request_with_retry` (immediate backoff)
    /// 2. Per-page retries with longer cooldown when a page is persistently rate-limited
    ///
    /// # Arguments
    ///
    /// * `fq` - Optional Solr filter query (e.g., `metadata_modified:[... TO *]`)
    async fn paginated_search(&self, fq: Option<&str>) -> Result<Vec<CkanDataset>, AppError> {
        const PAGE_SIZE: usize = 1000;
        let mut all_datasets = Vec::new();
        let mut start: usize = 0;
        let mut page_delay = Self::PAGE_DELAY;

        loop {
            let mut url = self
                .base_url
                .join("api/3/action/package_search")
                .map_err(|e| AppError::Generic(e.to_string()))?;

            {
                let mut pairs = url.query_pairs_mut();
                if let Some(filter) = fq {
                    pairs.append_pair("fq", filter);
                }
                pairs
                    .append_pair("rows", &PAGE_SIZE.to_string())
                    .append_pair("start", &start.to_string())
                    .append_pair("sort", "metadata_modified asc");
            }

            // Page-level retry: if the request is rate-limited after exhausting
            // low-level retries, wait a longer cooldown and try the page again.
            let mut page_result = None;
            for page_attempt in 0..=Self::PAGE_RATE_LIMIT_RETRIES {
                match self.request_with_retry(&url).await {
                    Ok(resp) => {
                        page_result = Some(Ok(resp));
                        break;
                    }
                    Err(AppError::RateLimitExceeded)
                        if page_attempt < Self::PAGE_RATE_LIMIT_RETRIES =>
                    {
                        let cooldown = Self::PAGE_RATE_LIMIT_COOLDOWN * (page_attempt + 1);
                        sleep(cooldown).await;
                        page_delay = (page_delay * 2).min(Duration::from_secs(5));
                    }
                    Err(e) => {
                        page_result = Some(Err(e));
                        break;
                    }
                }
            }

            let resp = match page_result {
                Some(Ok(resp)) => resp,
                Some(Err(e)) => return Err(e),
                None => return Err(AppError::RateLimitExceeded),
            };

            let ckan_resp: CkanResponse<PackageSearchResult> = resp
                .json()
                .await
                .map_err(|e| AppError::ClientError(e.to_string()))?;

            if !ckan_resp.success {
                return Err(AppError::Generic(
                    "CKAN package_search returned success: false".to_string(),
                ));
            }

            let page_count = ckan_resp.result.results.len();
            all_datasets.extend(ckan_resp.result.results);

            // Check if we've fetched all results
            if start + page_count >= ckan_resp.result.count || page_count < PAGE_SIZE {
                break;
            }

            start += PAGE_SIZE;

            // Polite delay between pages to avoid triggering rate limits
            sleep(page_delay).await;
        }

        Ok(all_datasets)
    }

    /// Maximum retries for rate-limited (429) responses.
    /// Higher than normal retries because rate limits are transient.
    /// With 500ms base and 30s cap: 1s, 2s, 4s, 8s, 16s, 30s, 30s, 30s, 30s = ~151s total wait.
    const RATE_LIMIT_MAX_RETRIES: u32 = 10;

    // TODO(observability): Add detailed retry logging
    // Should log: (1) Attempt number and delay, (2) Reason for retry,
    // (3) Final error if all retries exhausted. Use tracing crate.
    async fn request_with_retry(&self, url: &Url) -> Result<reqwest::Response, AppError> {
        let http_config = HttpConfig::default();
        let max_retries = http_config.max_retries;
        let base_delay = http_config.retry_base_delay;
        let mut last_error = AppError::Generic("No attempts made".to_string());
        // Use higher retry count for 429s since they are transient
        let effective_max = Self::RATE_LIMIT_MAX_RETRIES.max(max_retries);

        for attempt in 1..=effective_max {
            match self.client.get(url.clone()).send().await {
                Ok(resp) => {
                    let status = resp.status();

                    if status.is_success() {
                        return Ok(resp);
                    }

                    if status == StatusCode::TOO_MANY_REQUESTS {
                        last_error = AppError::RateLimitExceeded;
                        if attempt < effective_max {
                            // Respect Retry-After header if present, otherwise exponential backoff (capped)
                            let delay = resp
                                .headers()
                                .get("retry-after")
                                .and_then(|v| v.to_str().ok())
                                .and_then(|v| v.parse::<u64>().ok())
                                .map(Duration::from_secs)
                                .unwrap_or_else(|| {
                                    (base_delay * 2_u32.pow(attempt)).min(Self::MAX_RETRY_DELAY)
                                });
                            sleep(delay).await;
                            continue;
                        }
                    }

                    if status.is_server_error() {
                        last_error = AppError::ClientError(format!(
                            "Server error: HTTP {}",
                            status.as_u16()
                        ));
                        if attempt < max_retries {
                            let delay = base_delay * attempt;
                            sleep(delay).await;
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
                        last_error = AppError::Timeout(http_config.timeout.as_secs());
                    } else if e.is_connect() {
                        last_error = AppError::NetworkError(format!("Connection failed: {}", e));
                    } else {
                        last_error = AppError::ClientError(e.to_string());
                    }

                    if attempt < max_retries && (e.is_timeout() || e.is_connect()) {
                        let delay = base_delay * attempt;
                        sleep(delay).await;
                        continue;
                    }
                }
            }
        }

        Err(last_error)
    }

    /// Converts a CKAN dataset into Ceres' internal `NewDataset` model.
    ///
    /// This helper method transforms CKAN-specific data structures into the format
    /// used by Ceres for database storage. Multilingual fields are resolved using
    /// the specified language preference.
    ///
    /// # Arguments
    ///
    /// * `dataset` - The CKAN dataset to convert
    /// * `portal_url` - The base URL of the CKAN portal
    /// * `url_template` - Optional URL template with `{id}` and `{name}` placeholders
    /// * `language` - Preferred language for resolving multilingual fields
    ///
    /// # Returns
    ///
    /// A `NewDataset` ready to be inserted into the database.
    ///
    /// # Examples
    ///
    /// ```
    /// use ceres_client::CkanClient;
    /// use ceres_client::ckan::CkanDataset;
    /// use ceres_core::LocalizedField;
    ///
    /// let ckan_dataset = CkanDataset {
    ///     id: "abc-123".to_string(),
    ///     name: "air-quality-data".to_string(),
    ///     title: LocalizedField::Plain("Air Quality Monitoring".to_string()),
    ///     notes: Some(LocalizedField::Plain("Data from air quality sensors".to_string())),
    ///     extras: serde_json::Map::new(),
    /// };
    ///
    /// let new_dataset = CkanClient::into_new_dataset(
    ///     ckan_dataset,
    ///     "https://dati.gov.it",
    ///     None,
    ///     "en",
    /// );
    ///
    /// assert_eq!(new_dataset.original_id, "abc-123");
    /// assert_eq!(new_dataset.url, "https://dati.gov.it/dataset/air-quality-data");
    /// assert_eq!(new_dataset.title, "Air Quality Monitoring");
    /// ```
    pub fn into_new_dataset(
        dataset: CkanDataset,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
    ) -> NewDataset {
        let landing_page = match url_template {
            Some(template) => template
                .replace("{id}", &dataset.id)
                .replace("{name}", &dataset.name),
            None => format!(
                "{}/dataset/{}",
                portal_url.trim_end_matches('/'),
                dataset.name
            ),
        };

        let metadata_json = serde_json::Value::Object(dataset.extras.clone());

        // Resolve multilingual fields using preferred language
        let title = dataset.title.resolve(language);
        // Some portals (e.g., opendata.swiss) store the description in a
        // "description" field instead of "notes". Fall back to extras if needed.
        let description = dataset
            .notes
            .map(|n| n.resolve(language))
            .or_else(|| {
                dataset
                    .extras
                    .get("description")
                    .and_then(|v| serde_json::from_value::<LocalizedField>(v.clone()).ok())
                    .map(|f| f.resolve(language))
            })
            .filter(|d| !d.is_empty());

        // Content hash includes language for correct delta detection
        let content_hash = NewDataset::compute_content_hash_with_language(
            &title,
            description.as_deref(),
            language,
        );

        NewDataset {
            original_id: dataset.id,
            source_portal: portal_url.to_string(),
            url: landing_page,
            title,
            description,
            embedding: None,
            metadata: metadata_json,
            content_hash,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_with_valid_url() {
        let result = CkanClient::new("https://dati.gov.it");
        assert!(result.is_ok());
        let client = result.unwrap();
        assert_eq!(client.base_url.as_str(), "https://dati.gov.it/");
    }

    #[test]
    fn test_new_with_invalid_url() {
        let result = CkanClient::new("not-a-valid-url");
        assert!(result.is_err());

        if let Err(AppError::InvalidPortalUrl(url)) = result {
            assert_eq!(url, "not-a-valid-url");
        } else {
            panic!("Expected AppError::InvalidPortalUrl");
        }
    }

    #[test]
    fn test_into_new_dataset_basic() {
        let ckan_dataset = CkanDataset {
            id: "dataset-123".to_string(),
            name: "my-dataset".to_string(),
            title: LocalizedField::Plain("My Dataset".to_string()),
            notes: Some(LocalizedField::Plain("This is a test dataset".to_string())),
            extras: serde_json::Map::new(),
        };

        let portal_url = "https://dati.gov.it";
        let new_dataset = CkanClient::into_new_dataset(ckan_dataset, portal_url, None, "en");

        assert_eq!(new_dataset.original_id, "dataset-123");
        assert_eq!(new_dataset.source_portal, "https://dati.gov.it");
        assert_eq!(new_dataset.url, "https://dati.gov.it/dataset/my-dataset");
        assert_eq!(new_dataset.title, "My Dataset");
        assert!(new_dataset.embedding.is_none());

        // Verify content hash is computed correctly (includes language)
        let expected_hash = NewDataset::compute_content_hash_with_language(
            "My Dataset",
            Some("This is a test dataset"),
            "en",
        );
        assert_eq!(new_dataset.content_hash, expected_hash);
        assert_eq!(new_dataset.content_hash.len(), 64);
    }

    #[test]
    fn test_into_new_dataset_with_url_template() {
        let ckan_dataset = CkanDataset {
            id: "52db43b1-4d6a-446c-a3fc-b2e470fe5a45".to_string(),
            name: "raccolta-differenziata".to_string(),
            title: LocalizedField::Plain("Raccolta Differenziata".to_string()),
            notes: Some(LocalizedField::Plain(
                "Percentuale raccolta differenziata".to_string(),
            )),
            extras: serde_json::Map::new(),
        };

        let portal_url = "https://dati.gov.it/opendata/";
        let template = "https://www.dati.gov.it/view-dataset/dataset?id={id}";
        let new_dataset =
            CkanClient::into_new_dataset(ckan_dataset, portal_url, Some(template), "en");

        assert_eq!(
            new_dataset.url,
            "https://www.dati.gov.it/view-dataset/dataset?id=52db43b1-4d6a-446c-a3fc-b2e470fe5a45"
        );
        assert_eq!(new_dataset.source_portal, "https://dati.gov.it/opendata/");
    }

    #[test]
    fn test_into_new_dataset_url_template_with_name() {
        let ckan_dataset = CkanDataset {
            id: "abc-123".to_string(),
            name: "air-quality-data".to_string(),
            title: LocalizedField::Plain("Air Quality".to_string()),
            notes: None,
            extras: serde_json::Map::new(),
        };

        let template = "https://example.com/datasets/{name}/view";
        let new_dataset =
            CkanClient::into_new_dataset(ckan_dataset, "https://example.com", Some(template), "en");

        assert_eq!(
            new_dataset.url,
            "https://example.com/datasets/air-quality-data/view"
        );
    }

    #[test]
    fn test_ckan_response_deserialization() {
        let json = r#"{
            "success": true,
            "result": ["dataset-1", "dataset-2", "dataset-3"]
        }"#;

        let response: CkanResponse<Vec<String>> = serde_json::from_str(json).unwrap();
        assert!(response.success);
        assert_eq!(response.result.len(), 3);
    }

    #[test]
    fn test_ckan_dataset_deserialization() {
        let json = r#"{
            "id": "test-id",
            "name": "test-name",
            "title": "Test Title",
            "notes": "Test notes",
            "organization": {
                "name": "test-org"
            }
        }"#;

        let dataset: CkanDataset = serde_json::from_str(json).unwrap();
        assert_eq!(dataset.id, "test-id");
        assert_eq!(dataset.name, "test-name");
        assert_eq!(dataset.title.resolve("en"), "Test Title");
        assert!(dataset.extras.contains_key("organization"));
    }

    #[test]
    fn test_ckan_dataset_multilingual_deserialization() {
        let json = r#"{
            "id": "swiss-123",
            "name": "swiss-dataset",
            "title": {"en": "English Title", "de": "Deutscher Titel", "fr": "Titre Francais"},
            "notes": {"en": "English description", "de": "Deutsche Beschreibung"}
        }"#;

        let dataset: CkanDataset = serde_json::from_str(json).unwrap();
        assert_eq!(dataset.id, "swiss-123");
        assert_eq!(dataset.title.resolve("en"), "English Title");
        assert_eq!(dataset.title.resolve("de"), "Deutscher Titel");
        assert_eq!(dataset.title.resolve("it"), "English Title"); // fallback to en
        assert_eq!(
            dataset.notes.as_ref().unwrap().resolve("de"),
            "Deutsche Beschreibung"
        );
    }

    #[test]
    fn test_into_new_dataset_multilingual() {
        let json = r#"{
            "id": "swiss-dataset",
            "name": "test-multilingual",
            "title": {"en": "English Title", "de": "Deutscher Titel"},
            "notes": {"en": "English description", "de": "Deutsche Beschreibung"}
        }"#;
        let dataset: CkanDataset = serde_json::from_str(json).unwrap();
        let new_ds =
            CkanClient::into_new_dataset(dataset, "https://ckan.opendata.swiss", None, "de");
        assert_eq!(new_ds.title, "Deutscher Titel");
        assert_eq!(
            new_ds.description,
            Some("Deutsche Beschreibung".to_string())
        );
    }

    #[test]
    fn test_into_new_dataset_description_fallback() {
        // Swiss portal stores description in "description" field, not "notes"
        let json = r#"{
            "id": "swiss-no-notes",
            "name": "dataset-without-notes",
            "title": {"en": "English Title", "de": "Deutscher Titel"},
            "description": {"en": "English desc", "de": "Deutsche Beschreibung"}
        }"#;
        let dataset: CkanDataset = serde_json::from_str(json).unwrap();
        assert!(dataset.notes.is_none());
        let new_ds =
            CkanClient::into_new_dataset(dataset, "https://ckan.opendata.swiss", None, "en");
        assert_eq!(new_ds.description, Some("English desc".to_string()));
    }

    #[test]
    fn test_into_new_dataset_description_fallback_empty() {
        // If "description" exists but all translations are empty, description should be None
        let json = r#"{
            "id": "swiss-empty-desc",
            "name": "dataset-empty-desc",
            "title": "Some Title",
            "description": {"en": "", "de": "", "fr": ""}
        }"#;
        let dataset: CkanDataset = serde_json::from_str(json).unwrap();
        let new_ds =
            CkanClient::into_new_dataset(dataset, "https://ckan.opendata.swiss", None, "en");
        assert_eq!(new_ds.description, None);
    }

    #[test]
    fn test_into_new_dataset_notes_takes_priority() {
        // When both "notes" and "description" exist, "notes" should win
        let json = r#"{
            "id": "both-fields",
            "name": "dataset-both",
            "title": "Title",
            "notes": "Notes description",
            "description": {"en": "Extras description"}
        }"#;
        let dataset: CkanDataset = serde_json::from_str(json).unwrap();
        let new_ds = CkanClient::into_new_dataset(dataset, "https://example.com", None, "en");
        assert_eq!(new_ds.description, Some("Notes description".to_string()));
    }
}

// =============================================================================
// Trait Implementations: PortalClient and PortalClientFactory
// =============================================================================

impl ceres_core::traits::PortalClient for CkanClient {
    type PortalData = CkanDataset;

    fn portal_type(&self) -> &'static str {
        "ckan"
    }

    fn base_url(&self) -> &str {
        self.base_url.as_str()
    }

    async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        self.list_package_ids().await
    }

    async fn get_dataset(&self, id: &str) -> Result<Self::PortalData, AppError> {
        self.show_package(id).await
    }

    fn into_new_dataset(
        data: Self::PortalData,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
    ) -> NewDataset {
        CkanClient::into_new_dataset(data, portal_url, url_template, language)
    }

    async fn search_modified_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<Self::PortalData>, AppError> {
        self.search_modified_since(since).await
    }

    async fn search_all_datasets(&self) -> Result<Vec<Self::PortalData>, AppError> {
        self.search_all_datasets().await
    }
}

/// Factory for creating CKAN portal clients.
#[derive(Debug, Clone, Default)]
pub struct CkanClientFactory;

impl CkanClientFactory {}

impl ceres_core::traits::PortalClientFactory for CkanClientFactory {
    type Client = CkanClient;

    fn create(
        &self,
        portal_url: &str,
        portal_type: ceres_core::config::PortalType,
    ) -> Result<Self::Client, AppError> {
        match portal_type {
            ceres_core::config::PortalType::Ckan => CkanClient::new(portal_url),
            other => Err(AppError::ConfigError(format!(
                "CkanClientFactory can only create CKAN clients, but portal type {:?} was requested for URL {}",
                other, portal_url
            ))),
        }
    }
}
