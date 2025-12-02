use ceres_core::error::AppError;
use ceres_core::models::NewDataset;
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use tokio::time::sleep;

/// Maximum number of retry attempts for failed requests.
const MAX_RETRIES: u32 = 3;

/// Base delay between retries (will be multiplied by attempt number).
const RETRY_BASE_DELAY_MS: u64 = 500;

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

/// Data Transfer Object for CKAN dataset details.
///
/// This structure represents the core fields returned by the CKAN `package_show` API.
/// Additional fields returned by CKAN are captured in the `extras` map.
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
    /// * `base_url_str` - The base URL of the CKAN portal (e.g., "https://dati.gov.it")
    ///
    /// # Returns
    ///
    /// Returns a configured `CkanClient` instance.
    ///
    /// # Errors
    ///
    /// Returns `AppError::Generic` if the URL is invalid or malformed.
    /// Returns `AppError::ClientError` if the HTTP client cannot be built.
    pub fn new(base_url_str: &str) -> Result<Self, AppError> {
        let base_url = Url::parse(base_url_str)
            .map_err(|_| AppError::Generic(format!("Invalid CKAN URL: {}", base_url_str)))?;

        let client = Client::builder()
            .user_agent("Ceres/0.1 (semantic-search-bot)")
            .timeout(Duration::from_secs(30))
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

    /// Makes an HTTP GET request with automatic retry on transient failures.
    ///
    /// Implements exponential backoff for retries on:
    /// - Network errors
    /// - Timeouts
    /// - Server errors (5xx)
    /// - Rate limiting (429)
    async fn request_with_retry(&self, url: &Url) -> Result<reqwest::Response, AppError> {
        let mut last_error = AppError::Generic("No attempts made".to_string());

        for attempt in 1..=MAX_RETRIES {
            match self.client.get(url.clone()).send().await {
                Ok(resp) => {
                    let status = resp.status();

                    // Success
                    if status.is_success() {
                        return Ok(resp);
                    }

                    // Rate limited - retry with backoff
                    if status == StatusCode::TOO_MANY_REQUESTS {
                        last_error = AppError::RateLimitExceeded;
                        if attempt < MAX_RETRIES {
                            let delay =
                                Duration::from_millis(RETRY_BASE_DELAY_MS * 2_u64.pow(attempt));
                            sleep(delay).await;
                            continue;
                        }
                    }

                    // Server error - retry
                    if status.is_server_error() {
                        last_error = AppError::ClientError(format!(
                            "Server error: HTTP {}",
                            status.as_u16()
                        ));
                        if attempt < MAX_RETRIES {
                            let delay = Duration::from_millis(RETRY_BASE_DELAY_MS * attempt as u64);
                            sleep(delay).await;
                            continue;
                        }
                    }

                    // Client error (4xx except 429) - don't retry
                    return Err(AppError::ClientError(format!(
                        "HTTP {} from {}",
                        status.as_u16(),
                        url
                    )));
                }
                Err(e) => {
                    // Network/timeout errors - retry
                    if e.is_timeout() {
                        last_error = AppError::Timeout(30);
                    } else if e.is_connect() {
                        last_error = AppError::NetworkError(format!("Connection failed: {}", e));
                    } else {
                        last_error = AppError::ClientError(e.to_string());
                    }

                    if attempt < MAX_RETRIES && (e.is_timeout() || e.is_connect()) {
                        let delay = Duration::from_millis(RETRY_BASE_DELAY_MS * attempt as u64);
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
    pub fn into_new_dataset(dataset: CkanDataset, portal_url: &str) -> NewDataset {
        let landing_page = format!(
            "{}/dataset/{}",
            portal_url.trim_end_matches('/'),
            dataset.name
        );

        let metadata_json = serde_json::Value::Object(dataset.extras.clone());

        NewDataset {
            original_id: dataset.id,
            source_portal: portal_url.to_string(),
            url: landing_page,
            title: dataset.title,
            description: dataset.notes,
            embedding: None,
            metadata: metadata_json,
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
        let new_dataset = CkanClient::into_new_dataset(ckan_dataset, portal_url);

        assert_eq!(new_dataset.original_id, "dataset-123");
        assert_eq!(new_dataset.source_portal, "https://dati.gov.it");
        assert_eq!(new_dataset.url, "https://dati.gov.it/dataset/my-dataset");
        assert_eq!(new_dataset.title, "My Dataset");
        assert!(new_dataset.embedding.is_none());
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
