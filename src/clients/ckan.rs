use crate::error::AppError;
use crate::models::NewDataset;
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::Value;

/// My generic wrapper around CKAN API (sorry)
/// CKAN API reference: https://docs.ckan.org/en/2.9/api/
/// CKAN always returns with {"success": bool, "result": T}
#[derive(Deserialize, Debug)]
struct CkanResponse<T> {
    success: bool,
    result: T,
    /// CKAN may also return "error" field on failure
    /// error: Option<Value>,
}

/// DTO for CKAN dataset creation response
#[derive(Deserialize, Debug, Clone)]
pub struct CkanDataset {
    pub id: String,
    pub name: String,
    pub title: String,
    pub notes: Option<String>,
    /// might contain other fields, but I only care about these for now
    /// Generic JSON for other fields
    #[serde(flatten)]
    pub extras: serde_json::Map<String, Value>,
}

#[derive(Clone)]
pub struct CkanClient {
    client: Client,
    base_url: Url,
}

impl CkanClient {
    pub fn new(base_url_str: &str) -> Result<Self, AppError> {
        /// Robust parsing of base URL
        let base_url = Url::parse(base_url_str)
            .map_err(|_| AppError::Generic(format!("Invalid CKAN URL: {}", base_url_str)))?;

        /// It's good practice to set a specific User-Agent.
        /// Many portals (e.g., dati.gov.it) block generic clients or those without a User-Agent.
        let client = Client::builder()
            .user_agent("Ceres/0.1 (semantic-search-bot)")
            .build()?;

        Ok(Self { client, base_url })
    }

    /// Gets the list of all dataset IDs (package_list)
    pub async fn list_package_ids(&self) -> Result<Vec<String>, AppError> {
        let url = self.base_url.join("api/3/action/package_list")
            .map_err(|e| AppError::Generic(e.to_string()))?;

        let resp = self.client.get(url)
            .send()
            .await?;

        // Check HTTP status
        if !resp.status().is_success() {
            return Err(AppError::Generic(format!("CKAN API error: HTTP {}", resp.status())));
        }

        let ckan_resp: CkanResponse<Vec<String>> = resp.json().await?;

        if !ckan_resp.success {
            return Err(AppError::Generic("CKAN API returned success: false".to_string()));
        }

        Ok(ckan_resp.result)
    }

    /// Gets the full details of a single dataset (package_show)
    pub async fn show_package(&self, id: &str) -> Result<CkanDataset, AppError> {
        let mut url = self.base_url.join("api/3/action/package_show")
            .map_err(|e| AppError::Generic(e.to_string()))?;

        // Add the id parameter to the query string
        url.query_pairs_mut().append_pair("id", id);

        let resp = self.client.get(url).send().await?;

        if !resp.status().is_success() {
            return Err(AppError::Generic(format!("CKAN API error fetching {}: HTTP {}", id, resp.status())));
        }

        let ckan_resp: CkanResponse<CkanDataset> = resp.json().await?;

        if !ckan_resp.success {
            return Err(AppError::Generic(format!("CKAN failed to show package {}", id)));
        }

        Ok(ckan_resp.result)
    }

    /// Helper method to convert the CKAN DTO into Ceres' internal model
    /// This prepares the data to be saved in the DB.
    pub fn into_new_dataset(dataset: CkanDataset, portal_url: &str) -> NewDataset {
        /// Build the public URL of the dataset (not the API URL)
        /// Usually it's: BASE_URL/dataset/NAME
        let landing_page = format!("{}/dataset/{}", portal_url.trim_end_matches('/'), dataset.name);

        /// Prepare the raw metadata
        let metadata_json = serde_json::Value::Object(dataset.extras.clone());

        NewDataset {
            original_id: dataset.id,
            source_portal: portal_url.to_string(),
            url: landing_page,
            title: dataset.title,
            description: dataset.notes,
            embedding: None, /// to be filled later
            metadata: metadata_json,
        }
    }
}