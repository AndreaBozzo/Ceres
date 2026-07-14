//! Collection-level SpatioTemporal Asset Catalog (STAC) API client.
//!
//! Ceres deliberately harvests one row per STAC Collection. It never follows
//! Collection `items` links, which keeps imagery catalogs from expanding into
//! millions of scene-level records by accident.

use std::{collections::HashSet, sync::Arc, time::Duration};

use ceres_core::{AppError, CatalogRecordKind, NewDataset, traits::PortalClient};
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::BoxStream};
use reqwest::{Client, Url};
use serde_json::Value;
use tokio::sync::OnceCell;

const MAX_PAGES: usize = 100_000;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

/// A normalized STAC Collection plus its complete source JSON.
#[derive(Debug, Clone)]
pub struct StacCollection {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub landing_page: String,
    pub modified: Option<DateTime<Utc>>,
    pub metadata: Value,
}

#[derive(Debug)]
struct Page {
    collections: Vec<StacCollection>,
    next: Option<Url>,
}

/// Client for the Collections conformance class of a STAC API.
#[derive(Clone)]
pub struct StacClient {
    client: Client,
    base_url: Url,
    collections_url: Arc<OnceCell<Url>>,
}

impl StacClient {
    pub fn new(base_url: &str) -> Result<Self, AppError> {
        let base_url =
            Url::parse(base_url).map_err(|_| AppError::InvalidPortalUrl(base_url.to_string()))?;
        if !matches!(base_url.scheme(), "http" | "https") {
            return Err(AppError::InvalidPortalUrl(base_url.to_string()));
        }
        let client = Client::builder()
            // A project URL keeps the agent identifiable without the generic
            // "harvester" token that some public STAC WAF rules reject.
            .user_agent("Ceres/0.6 (+https://github.com/AndreaBozzo/Ceres)")
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        Ok(Self {
            client,
            base_url,
            collections_url: Arc::new(OnceCell::new()),
        })
    }

    async fn bounded_get_json(&self, url: Url) -> Result<(Value, Url), AppError> {
        let response = self
            .client
            .get(url.clone())
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        if !response.status().is_success() {
            return Err(AppError::ClientError(format!(
                "HTTP {} from {url}",
                response.status()
            )));
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
        {
            return Err(AppError::ClientError(format!(
                "STAC response exceeds {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        let response_url = response.url().clone();
        let bytes = response
            .bytes()
            .await
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        if bytes.len() > MAX_RESPONSE_BYTES {
            return Err(AppError::ClientError(format!(
                "STAC response exceeds {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        let json = serde_json::from_slice(&bytes)
            .map_err(|error| AppError::ClientError(format!("Invalid STAC JSON: {error}")))?;
        Ok((json, response_url))
    }

    async fn collections_url(&self) -> Result<&Url, AppError> {
        self.collections_url
            .get_or_try_init(|| async {
                let (landing, response_url) = self.bounded_get_json(self.base_url.clone()).await?;
                discover_collections_url(&landing, &response_url)
            })
            .await
    }

    async fn page(&self, url: Url) -> Result<Page, AppError> {
        let (json, response_url) = self.bounded_get_json(url).await?;
        parse_collections_page(json, &response_url)
    }

    /// Streams Collection pages by following `rel=next` links. Only GET links
    /// are accepted; an API advertising a stateful POST cursor fails explicitly
    /// instead of silently treating a partial result as complete.
    pub fn paginate_stream(&self) -> BoxStream<'_, Result<Vec<StacCollection>, AppError>> {
        struct State {
            next: Option<Url>,
            pages: usize,
            seen: HashSet<String>,
            done: bool,
        }

        Box::pin(futures::stream::unfold(
            (
                self.clone(),
                State {
                    next: None,
                    pages: 0,
                    seen: HashSet::new(),
                    done: false,
                },
            ),
            |(client, mut state)| async move {
                if state.done {
                    return None;
                }
                let url = match state.next.take() {
                    Some(url) => url,
                    None => match client.collections_url().await {
                        Ok(url) => url.clone(),
                        Err(error) => {
                            state.done = true;
                            return Some((Err(error), (client, state)));
                        }
                    },
                };
                if state.pages >= MAX_PAGES || !state.seen.insert(url.as_str().to_string()) {
                    state.done = true;
                    return Some((
                        Err(AppError::ClientError(
                            "STAC pagination did not terminate deterministically".into(),
                        )),
                        (client, state),
                    ));
                }
                state.pages += 1;
                match client.page(url).await {
                    Ok(page) => {
                        state.next = page.next;
                        state.done = state.next.is_none();
                        Some((Ok(page.collections), (client, state)))
                    }
                    Err(error) => {
                        state.done = true;
                        Some((Err(error), (client, state)))
                    }
                }
            },
        ))
    }
}

impl PortalClient for StacClient {
    type PortalData = StacCollection;

    fn portal_type(&self) -> &'static str {
        "stac"
    }

    fn base_url(&self) -> &str {
        self.base_url.as_str()
    }

    async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        Ok(self
            .search_all_datasets()
            .await?
            .into_iter()
            .map(|collection| collection.id)
            .collect())
    }

    async fn get_dataset(&self, id: &str) -> Result<StacCollection, AppError> {
        self.search_all_datasets()
            .await?
            .into_iter()
            .find(|collection| collection.id == id)
            .ok_or_else(|| AppError::ClientError(format!("STAC Collection '{id}' not found")))
    }

    fn into_new_dataset(
        data: StacCollection,
        portal_url: &str,
        _url_template: Option<&str>,
        language: &str,
    ) -> NewDataset {
        NewDataset {
            content_hash: NewDataset::compute_content_hash_with_language(
                &data.title,
                data.description.as_deref(),
                language,
            ),
            original_id: data.id,
            source_portal: portal_url.to_string(),
            url: data.landing_page,
            title: data.title,
            description: data.description,
            record_kind: CatalogRecordKind::Series,
            embedding: None,
            metadata: data.metadata,
        }
    }

    async fn search_modified_since(
        &self,
        _since: DateTime<Utc>,
    ) -> Result<Vec<StacCollection>, AppError> {
        Err(AppError::ClientError(
            "STAC Collection incremental sync is not supported".into(),
        ))
    }

    async fn search_all_datasets(&self) -> Result<Vec<StacCollection>, AppError> {
        let mut collections = Vec::new();
        let mut stream = self.paginate_stream();
        while let Some(page) = stream.next().await {
            collections.extend(page?);
        }
        Ok(collections)
    }

    fn search_all_datasets_stream(&self) -> BoxStream<'_, Result<Vec<StacCollection>, AppError>> {
        self.paginate_stream()
    }
}

fn discover_collections_url(landing: &Value, base: &Url) -> Result<Url, AppError> {
    let stac_version = landing
        .get("stac_version")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::ClientError("STAC landing page is missing stac_version".into()))?;
    if !(stac_version.starts_with("1.0") || stac_version.starts_with("1.1")) {
        return Err(AppError::ClientError(format!(
            "Unsupported STAC version '{stac_version}' (expected 1.0 or 1.1)"
        )));
    }
    let conforms = landing
        .get("conformsTo")
        .or_else(|| landing.get("conforms_to"))
        .and_then(Value::as_array)
        .ok_or_else(|| AppError::ClientError("STAC landing page is missing conformsTo".into()))?;
    if !conforms.iter().filter_map(Value::as_str).any(|uri| {
        let uri = uri.to_ascii_lowercase();
        uri.contains("stac") && (uri.contains("collection") || uri.ends_with("/core"))
    }) {
        return Err(AppError::ClientError(
            "STAC API does not declare Core or Collections conformance".into(),
        ));
    }
    find_link(landing, base, "data")?.ok_or_else(|| {
        AppError::ClientError("STAC landing page has no rel=data Collections link".into())
    })
}

fn parse_collections_page(json: Value, base: &Url) -> Result<Page, AppError> {
    let raw_collections = json
        .get("collections")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            AppError::ClientError("STAC response is missing collections array".into())
        })?;
    let collections = raw_collections
        .iter()
        .cloned()
        .map(|collection| parse_collection(collection, base))
        .collect::<Result<Vec<_>, _>>()?;
    let next = find_link(&json, base, "next")?;
    Ok(Page { collections, next })
}

fn parse_collection(metadata: Value, base: &Url) -> Result<StacCollection, AppError> {
    let id = metadata
        .get("id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| AppError::ClientError("STAC Collection is missing id".into()))?
        .to_string();
    let title = metadata
        .get("title")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(&id)
        .to_string();
    let description = metadata
        .get("description")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string);
    let landing_page = ["about", "canonical", "self"]
        .into_iter()
        .find_map(|rel| find_link(&metadata, base, rel).transpose())
        .transpose()?
        .unwrap_or_else(|| base.clone())
        .to_string();
    let modified = ["updated", "created"]
        .into_iter()
        .find_map(|key| metadata.get(key).and_then(Value::as_str))
        .and_then(parse_date);
    Ok(StacCollection {
        id,
        title,
        description,
        landing_page,
        modified,
        metadata,
    })
}

fn find_link(document: &Value, base: &Url, rel: &str) -> Result<Option<Url>, AppError> {
    let Some(link) = document
        .get("links")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .find(|link| link.get("rel").and_then(Value::as_str) == Some(rel))
    else {
        return Ok(None);
    };
    if link
        .get("method")
        .and_then(Value::as_str)
        .is_some_and(|method| !method.eq_ignore_ascii_case("GET"))
    {
        return Err(AppError::ClientError(format!(
            "STAC rel={rel} link requires an unsupported non-GET request"
        )));
    }
    let href = link
        .get("href")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::ClientError(format!("STAC rel={rel} link is missing href")))?;
    let url = base
        .join(href)
        .map_err(|error| AppError::ClientError(format!("Invalid STAC rel={rel} URL: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(AppError::ClientError(format!(
            "Unsupported STAC link scheme '{}'",
            url.scheme()
        )));
    }
    Ok(Some(url))
}

fn parse_date(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|date| date.with_timezone(&Utc))
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    const COLLECTIONS_1_0: &[u8] = include_bytes!("../tests/fixtures/stac_collections_1_0.json");
    const COLLECTIONS_1_1: &[u8] = include_bytes!("../tests/fixtures/stac_collections_1_1.json");

    #[test]
    fn discovers_collections_from_landing_links() {
        let landing = serde_json::json!({
            "stac_version": "1.1.0",
            "conformsTo": ["https://api.stacspec.org/v1.0.0/collections"],
            "links": [{"rel": "data", "href": "./collections", "type": "application/json"}]
        });
        let url =
            discover_collections_url(&landing, &Url::parse("https://catalog.test/stac/").unwrap())
                .unwrap();
        assert_eq!(url.as_str(), "https://catalog.test/stac/collections");
    }

    #[test]
    fn parses_stac_1_0_collection_and_preserves_source() {
        let json: Value = serde_json::from_slice(COLLECTIONS_1_0).unwrap();
        let page = parse_collections_page(
            json,
            &Url::parse("https://catalog.test/collections").unwrap(),
        )
        .unwrap();
        assert_eq!(page.collections.len(), 1);
        let collection = &page.collections[0];
        assert_eq!(collection.id, "sentinel-2-l2a");
        assert_eq!(collection.title, "Sentinel-2 Level 2A");
        assert_eq!(
            collection.landing_page,
            "https://catalog.test/collections/sentinel-2-l2a"
        );
        assert_eq!(collection.metadata["license"], "proprietary");
        assert!(collection.metadata["extent"]["spatial"]["bbox"].is_array());
        assert!(collection.metadata["providers"].is_array());
        assert!(collection.metadata["summaries"].is_object());
        assert!(collection.metadata["assets"].is_object());
    }

    #[test]
    fn parses_stac_1_1_page_and_next_link() {
        let json: Value = serde_json::from_slice(COLLECTIONS_1_1).unwrap();
        let page = parse_collections_page(
            json,
            &Url::parse("https://catalog.test/collections?limit=1").unwrap(),
        )
        .unwrap();
        assert_eq!(page.collections[0].id, "landsat-c2-l2");
        assert_eq!(
            page.collections[0]
                .modified
                .unwrap()
                .date_naive()
                .to_string(),
            "2026-06-15"
        );
        assert_eq!(
            page.next.unwrap().as_str(),
            "https://catalog.test/collections?limit=1&token=next"
        );
    }

    #[test]
    fn normalizes_collection_as_series() {
        let json: Value = serde_json::from_slice(COLLECTIONS_1_0).unwrap();
        let collection = parse_collections_page(
            json,
            &Url::parse("https://catalog.test/collections").unwrap(),
        )
        .unwrap()
        .collections
        .remove(0);
        let dataset = StacClient::into_new_dataset(collection, "https://catalog.test", None, "en");
        assert_eq!(dataset.record_kind, CatalogRecordKind::Series);
        assert_eq!(dataset.original_id, "sentinel-2-l2a");
        assert_eq!(dataset.metadata["stac_version"], "1.0.0");
    }

    #[tokio::test]
    async fn streams_linked_collection_pages_without_following_items() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "stac_version": "1.1.0",
                "conformsTo": ["https://api.stacspec.org/v1.0.0/collections"],
                "links": [{"rel": "data", "href": format!("{}/collections/page-1", server.uri())}]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/collections/page-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "collections": [minimal_collection("first", &server.uri())],
                "links": [{"rel": "next", "href": format!("{}/collections/page-2", server.uri())}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/collections/page-2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "collections": [minimal_collection("second", &server.uri())],
                "links": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = StacClient::new(&format!("{}/", server.uri())).unwrap();
        let ids: Vec<String> = client
            .search_all_datasets()
            .await
            .unwrap()
            .into_iter()
            .map(|collection| collection.id)
            .collect();
        assert_eq!(ids, ["first", "second"]);
    }

    fn minimal_collection(id: &str, base: &str) -> Value {
        serde_json::json!({
            "stac_version": "1.1.0",
            "type": "Collection",
            "id": id,
            "description": format!("Collection {id}"),
            "license": "proprietary",
            "extent": {
                "spatial": {"bbox": [[-180, -90, 180, 90]]},
                "temporal": {"interval": [[null, null]]}
            },
            "links": [
                {"rel": "self", "href": format!("{base}/collections/{id}")},
                {"rel": "items", "href": format!("{base}/collections/{id}/items")}
            ]
        })
    }

    #[tokio::test]
    #[ignore = "requires network access to the Copernicus Data Space STAC API"]
    async fn copernicus_stac_smoke() {
        let url = std::env::var("CERES_STAC_COPERNICUS_URL")
            .unwrap_or_else(|_| "https://stac.dataspace.copernicus.eu/v1/".into());
        let client = StacClient::new(&url).unwrap();
        let first_page = client.paginate_stream().next().await.unwrap().unwrap();
        assert!(!first_page.is_empty());
    }

    #[tokio::test]
    #[ignore = "requires network access to the Canada DataCube STAC API"]
    async fn canada_datacube_stac_smoke() {
        let url = std::env::var("CERES_STAC_CANADA_URL")
            .unwrap_or_else(|_| "https://datacube.services.geo.ca/stac/api/".into());
        let client = StacClient::new(&url).unwrap();
        let first_page = client.paginate_stream().next().await.unwrap().unwrap();
        assert!(!first_page.is_empty());
    }
}
