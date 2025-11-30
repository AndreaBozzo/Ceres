use crate::error::AppError;
use crate::models::NewDataset;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;

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
    // CKAN may also return "error" field on failure
    // error: Option<Value>,
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
/// use ceres::clients::ckan::CkanClient;
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ceres::clients::ckan::CkanClient;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = CkanClient::new("https://dati.gov.it")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(base_url_str: &str) -> Result<Self, AppError> {
        // Robust parsing of base URL
        let base_url = Url::parse(base_url_str)
            .map_err(|_| AppError::Generic(format!("Invalid CKAN URL: {}", base_url_str)))?;

        // It's good practice to set a specific User-Agent.
        // Many portals (e.g., dati.gov.it) block generic clients or those without a User-Agent.
        let client = Client::builder()
            .user_agent("Ceres/0.1 (semantic-search-bot)")
            .timeout(Duration::from_secs(30))
            .build()?;

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
    /// Returns `AppError::Generic` if:
    /// - The HTTP response status is not successful
    /// - The CKAN API returns `success: false`
    /// - Response parsing fails
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ceres::clients::ckan::CkanClient;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = CkanClient::new("https://dati.gov.it")?;
    /// let ids = client.list_package_ids().await?;
    /// println!("Found {} datasets", ids.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_package_ids(&self) -> Result<Vec<String>, AppError> {
        let url = self
            .base_url
            .join("api/3/action/package_list")
            .map_err(|e| AppError::Generic(e.to_string()))?;

        let resp = self.client.get(url).send().await?;

        // Check HTTP status
        if !resp.status().is_success() {
            return Err(AppError::Generic(format!(
                "CKAN API error: HTTP {}",
                resp.status()
            )));
        }

        let ckan_resp: CkanResponse<Vec<String>> = resp.json().await?;

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
    /// complete metadata for a single dataset, including title, description,
    /// resources, tags, and other attributes.
    ///
    /// # Arguments
    ///
    /// * `id` - The unique identifier or name slug of the dataset
    ///
    /// # Returns
    ///
    /// A `CkanDataset` containing the dataset's metadata.
    ///
    /// # Errors
    ///
    /// Returns `AppError::ClientError` if the HTTP request fails.
    /// Returns `AppError::Generic` if:
    /// - The HTTP response status is not successful
    /// - The CKAN API returns `success: false`
    /// - Response parsing fails
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ceres::clients::ckan::CkanClient;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = CkanClient::new("https://dati.gov.it")?;
    /// let dataset = client.show_package("my-dataset-id").await?;
    /// println!("Title: {}", dataset.title);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn show_package(&self, id: &str) -> Result<CkanDataset, AppError> {
        let mut url = self
            .base_url
            .join("api/3/action/package_show")
            .map_err(|e| AppError::Generic(e.to_string()))?;

        // Add the id parameter to the query string
        url.query_pairs_mut().append_pair("id", id);

        let resp = self.client.get(url).send().await?;

        if !resp.status().is_success() {
            return Err(AppError::Generic(format!(
                "CKAN API error fetching {}: HTTP {}",
                id,
                resp.status()
            )));
        }

        let ckan_resp: CkanResponse<CkanDataset> = resp.json().await?;

        if !ckan_resp.success {
            return Err(AppError::Generic(format!(
                "CKAN failed to show package {}",
                id
            )));
        }

        Ok(ckan_resp.result)
    }

    /// Converts a CKAN dataset into Ceres' internal `NewDataset` model.
    ///
    /// This helper method transforms CKAN-specific data structures into the format
    /// used by Ceres for database storage. It constructs the dataset's landing page URL
    /// and preserves all metadata in JSON format.
    ///
    /// # Arguments
    ///
    /// * `dataset` - The CKAN dataset to convert
    /// * `portal_url` - The base URL of the CKAN portal (used to construct landing page URL)
    ///
    /// # Returns
    ///
    /// A `NewDataset` ready to be inserted into the database. The `embedding` field
    /// will be `None` and should be populated separately.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use ceres::clients::ckan::CkanClient;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = CkanClient::new("https://dati.gov.it")?;
    /// let ckan_dataset = client.show_package("my-dataset").await?;
    /// let new_dataset = CkanClient::into_new_dataset(ckan_dataset, "https://dati.gov.it");
    /// # Ok(())
    /// # }
    /// ```
    pub fn into_new_dataset(dataset: CkanDataset, portal_url: &str) -> NewDataset {
        // Build the public URL of the dataset (not the API URL)
        // Usually it's: BASE_URL/dataset/NAME
        let landing_page = format!(
            "{}/dataset/{}",
            portal_url.trim_end_matches('/'),
            dataset.name
        );

        // Prepare the raw metadata
        let metadata_json = serde_json::Value::Object(dataset.extras.clone());

        NewDataset {
            original_id: dataset.id,
            source_portal: portal_url.to_string(),
            url: landing_page,
            title: dataset.title,
            description: dataset.notes,
            embedding: None, // to be filled later
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
    fn test_new_with_empty_url() {
        let result = CkanClient::new("");
        assert!(result.is_err());
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
        assert_eq!(
            new_dataset.description,
            Some("This is a test dataset".to_string())
        );
        assert!(new_dataset.embedding.is_none());
    }

    #[test]
    fn test_into_new_dataset_with_trailing_slash() {
        let ckan_dataset = CkanDataset {
            id: "dataset-456".to_string(),
            name: "another-dataset".to_string(),
            title: "Another Dataset".to_string(),
            notes: None,
            extras: serde_json::Map::new(),
        };

        let portal_url = "https://dati.gov.it/";
        let new_dataset = CkanClient::into_new_dataset(ckan_dataset, portal_url);

        // Should handle trailing slash correctly
        assert_eq!(
            new_dataset.url,
            "https://dati.gov.it/dataset/another-dataset"
        );
        assert_eq!(new_dataset.description, None);
    }

    #[test]
    fn test_into_new_dataset_preserves_extras() {
        let mut extras = serde_json::Map::new();
        extras.insert(
            "organization".to_string(),
            serde_json::json!({"name": "test-org"}),
        );
        extras.insert("tags".to_string(), serde_json::json!(["tag1", "tag2"]));

        let ckan_dataset = CkanDataset {
            id: "dataset-789".to_string(),
            name: "dataset-with-extras".to_string(),
            title: "Dataset With Extras".to_string(),
            notes: Some("Has extra fields".to_string()),
            extras: extras.clone(),
        };

        let portal_url = "https://example.com";
        let new_dataset = CkanClient::into_new_dataset(ckan_dataset, portal_url);

        // Check that extras are preserved in metadata
        assert!(new_dataset.metadata.is_object());
        let metadata_obj = new_dataset.metadata.as_object().unwrap();
        assert_eq!(metadata_obj.len(), extras.len());
        assert!(metadata_obj.contains_key("organization"));
        assert!(metadata_obj.contains_key("tags"));
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
        assert_eq!(response.result[0], "dataset-1");
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
            },
            "tags": ["tag1", "tag2"]
        }"#;

        let dataset: CkanDataset = serde_json::from_str(json).unwrap();
        assert_eq!(dataset.id, "test-id");
        assert_eq!(dataset.name, "test-name");
        assert_eq!(dataset.title, "Test Title");
        assert_eq!(dataset.notes, Some("Test notes".to_string()));

        // Check flattened extras
        assert!(dataset.extras.contains_key("organization"));
        assert!(dataset.extras.contains_key("tags"));
    }

    #[test]
    fn test_ckan_dataset_deserialization_minimal() {
        let json = r#"{
            "id": "minimal-id",
            "name": "minimal-name",
            "title": "Minimal Dataset"
        }"#;

        let dataset: CkanDataset = serde_json::from_str(json).unwrap();
        assert_eq!(dataset.id, "minimal-id");
        assert_eq!(dataset.name, "minimal-name");
        assert_eq!(dataset.title, "Minimal Dataset");
        assert_eq!(dataset.notes, None);
        assert!(dataset.extras.is_empty());
    }
}
