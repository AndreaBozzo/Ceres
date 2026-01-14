//! CKAN client for harvesting datasets from CKAN-compatible open data portals.
//!
//! # Future Extensions
//!
//! TODO: Add support for other portal types (roadmap v0.3+):
//! - Socrata API (used by many US cities): <https://dev.socrata.com/>
//! - DCAT-AP harvester for EU portals: <https://joinup.ec.europa.eu/collection/semantic-interoperability-community-semic/solution/dcat-application-profile-data-portals-europe>
//!
//! Consider creating a `PortalClient` trait that abstracts over different portal types:
//! ```ignore
//! pub trait PortalClient {
//!     async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError>;
//!     async fn get_dataset(&self, id: &str) -> Result<NewDataset, AppError>;
//! }
//! ```

use ceres_core::HttpConfig;
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
/// # Examples
///
/// ```
/// use ceres_client::ckan::CkanDataset;
///
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
/// assert_eq!(dataset.title, "My Dataset");
/// assert!(dataset.extras.contains_key("organization"));
/// ```
#[derive(Deserialize, Debug, Clone)]
pub struct CkanDataset {
    /// Unique identifier for the dataset
    pub id: String,
    /// URL-friendly name/slug of the dataset
    pub name: String,
    /// Human-readable title of the dataset
    pub title: String,
    /// Optional description/notes about the dataset
    pub notes: Option<String>,
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
            .map_err(|_| AppError::Generic(format!("Invalid CKAN URL: {}", base_url_str)))?;

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
        const PAGE_SIZE: usize = 1000;
        let mut all_datasets = Vec::new();
        let mut start: usize = 0;

        // Format timestamp for Solr query: YYYY-MM-DDTHH:MM:SSZ
        let since_str = since.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let fq = format!("metadata_modified:[{} TO *]", since_str);

        loop {
            let mut url = self
                .base_url
                .join("api/3/action/package_search")
                .map_err(|e| AppError::Generic(e.to_string()))?;

            url.query_pairs_mut()
                .append_pair("fq", &fq)
                .append_pair("rows", &PAGE_SIZE.to_string())
                .append_pair("start", &start.to_string())
                .append_pair("sort", "metadata_modified asc");

            let resp = self.request_with_retry(&url).await?;

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
        }

        Ok(all_datasets)
    }

    // TODO(observability): Add detailed retry logging
    // Should log: (1) Attempt number and delay, (2) Reason for retry,
    // (3) Final error if all retries exhausted. Use tracing crate.
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
                            let delay = base_delay * 2_u32.pow(attempt);
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
    /// used by Ceres for database storage.
    ///
    /// # Arguments
    ///
    /// * `dataset` - The CKAN dataset to convert
    /// * `portal_url` - The base URL of the CKAN portal
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
    ///
    /// let ckan_dataset = CkanDataset {
    ///     id: "abc-123".to_string(),
    ///     name: "air-quality-data".to_string(),
    ///     title: "Air Quality Monitoring".to_string(),
    ///     notes: Some("Data from air quality sensors".to_string()),
    ///     extras: serde_json::Map::new(),
    /// };
    ///
    /// let new_dataset = CkanClient::into_new_dataset(
    ///     ckan_dataset,
    ///     "https://dati.gov.it"
    /// );
    ///
    /// assert_eq!(new_dataset.original_id, "abc-123");
    /// assert_eq!(new_dataset.url, "https://dati.gov.it/dataset/air-quality-data");
    /// assert_eq!(new_dataset.title, "Air Quality Monitoring");
    /// ```
    pub fn into_new_dataset(dataset: CkanDataset, portal_url: &str) -> NewDataset {
        let landing_page = format!(
            "{}/dataset/{}",
            portal_url.trim_end_matches('/'),
            dataset.name
        );

        let metadata_json = serde_json::Value::Object(dataset.extras.clone());

        // Compute content hash for delta detection
        let content_hash =
            NewDataset::compute_content_hash(&dataset.title, dataset.notes.as_deref());

        NewDataset {
            original_id: dataset.id,
            source_portal: portal_url.to_string(),
            url: landing_page,
            title: dataset.title,
            description: dataset.notes,
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

        if let Err(AppError::Generic(msg)) = result {
            assert!(msg.contains("Invalid CKAN URL"));
        } else {
            panic!("Expected AppError::Generic");
        }
    }

    #[test]
    fn test_into_new_dataset_basic() {
        let ckan_dataset = CkanDataset {
            id: "dataset-123".to_string(),
            name: "my-dataset".to_string(),
            title: "My Dataset".to_string(),
            notes: Some("This is a test dataset".to_string()),
            extras: serde_json::Map::new(),
        };

        let portal_url = "https://dati.gov.it";
        let new_dataset = CkanClient::into_new_dataset(ckan_dataset.clone(), portal_url);

        assert_eq!(new_dataset.original_id, "dataset-123");
        assert_eq!(new_dataset.source_portal, "https://dati.gov.it");
        assert_eq!(new_dataset.url, "https://dati.gov.it/dataset/my-dataset");
        assert_eq!(new_dataset.title, "My Dataset");
        assert!(new_dataset.embedding.is_none());

        // Verify content hash is computed correctly
        let expected_hash =
            NewDataset::compute_content_hash(&ckan_dataset.title, ckan_dataset.notes.as_deref());
        assert_eq!(new_dataset.content_hash, expected_hash);
        assert_eq!(new_dataset.content_hash.len(), 64);
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
        assert!(dataset.extras.contains_key("organization"));
    }
}

// =============================================================================
// Trait Implementations: PortalClient and PortalClientFactory
// =============================================================================

impl ceres_core::traits::PortalClient for CkanClient {
    type PortalData = CkanDataset;

    async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        self.list_package_ids().await
    }

    async fn get_dataset(&self, id: &str) -> Result<Self::PortalData, AppError> {
        self.show_package(id).await
    }

    fn into_new_dataset(data: Self::PortalData, portal_url: &str) -> NewDataset {
        CkanClient::into_new_dataset(data, portal_url)
    }

    async fn search_modified_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<Self::PortalData>, AppError> {
        self.search_modified_since(since).await
    }
}

/// Factory for creating CKAN portal clients.
#[derive(Debug, Clone, Default)]
pub struct CkanClientFactory;

impl CkanClientFactory {
    /// Creates a new CKAN client factory.
    pub fn new() -> Self {
        Self
    }
}

impl ceres_core::traits::PortalClientFactory for CkanClientFactory {
    type Client = CkanClient;

    fn create(&self, portal_url: &str) -> Result<Self::Client, AppError> {
        CkanClient::new(portal_url)
    }
}
