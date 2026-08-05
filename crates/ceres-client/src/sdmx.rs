//! SDMX statistical dataflow client (SDMX-ML 2.1 / 3.0 structure messages).
//!
//! National statistics offices and international institutions publish their
//! catalogs as SDMX rather than as an open-data-portal API. Ceres harvests one
//! row per **dataflow** — the unit an SDMX service names, documents, and serves
//! data for — and never queries the observation cubes behind them. That mirrors
//! the STAC decision to stop at Collections: a single Eurostat dataflow holds
//! millions of observations, and expanding one into rows would drown the index
//! in cells rather than describe the catalog.
//!
//! # Why XML rather than SDMX-JSON
//!
//! SDMX-ML is the format every service in the family actually serves. Of the
//! public endpoints probed while building this client — Eurostat, ECB, OECD,
//! ILO, ISTAT, INSEE, IMF, BIS, UNICEF, SPC, Norges Bank — every one answered a
//! structure query with `application/vnd.sdmx.structure+xml;version=2.1`, and
//! Eurostat and the ECB reject an SDMX-JSON `Accept` outright with a 406.
//! Parsing one format keeps the mapping single-sourced; a service that serves
//! only SDMX-JSON would fail loudly here rather than be silently half-read.
//!
//! Element matching is by local name, so an SDMX 3.0 service — same document
//! shape under a `v3_0` namespace — parses through the same path.

use std::{sync::Arc, time::Duration};

use ceres_core::{AppError, CatalogRecordKind, NewDataset, traits::PortalClient};
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use reqwest::{Client, Url};
use roxmltree::{Document, Node};
use serde_json::{Map, Value, json};
use tokio::sync::OnceCell;

/// Eurostat's full dataflow structure message is ~37 MB — the largest in the
/// family by an order of magnitude. SDMX structure queries are not paginated by
/// the standard, so the whole catalog arrives in one response and the cap has to
/// clear that outlier with room to spare.
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Dataflows handed downstream per chunk. The fetch is one request, but the
/// harvest pipeline batches per stream item, so emitting in chunks keeps upsert
/// batches the same size they are for a paginated portal.
const CHUNK_SIZE: usize = 500;

/// A structure message of tens of megabytes takes longer to transfer than a
/// catalog page, so the per-request budget is wider than the paginated clients'.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// Annotation types under which services record when a dataflow's data last
/// changed, most specific first.
const UPDATE_ANNOTATIONS: &[&str] = &[
    "UPDATE_DATA",
    "DISSEMINATION_TIMESTAMP_DATA",
    "LAST_UPDATE",
    "LASTUPDATE",
    "UPDATE_STRUCTURE",
];

/// A normalized SDMX dataflow plus everything its structure message declared.
#[derive(Debug, Clone)]
pub struct SdmxDataflow {
    /// Stable identity, `AGENCY:ID` — see [`SdmxClient::into_new_dataset`].
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub landing_page: String,
    pub modified: Option<DateTime<Utc>>,
    pub metadata: Value,
}

/// Client for the `dataflow` resource of an SDMX REST service.
#[derive(Clone)]
pub struct SdmxClient {
    client: Client,
    /// Service root, e.g. `https://sdmx.oecd.org/public/rest/`.
    base_url: Url,
    language: String,
    dataflows: Arc<OnceCell<Vec<SdmxDataflow>>>,
}

impl SdmxClient {
    pub fn new(base_url: &str, language: &str) -> Result<Self, AppError> {
        let base_url =
            Url::parse(base_url).map_err(|_| AppError::InvalidPortalUrl(base_url.to_string()))?;
        if !matches!(base_url.scheme(), "http" | "https") {
            return Err(AppError::InvalidPortalUrl(base_url.to_string()));
        }
        let client = Client::builder()
            .user_agent("Ceres/0.7 (+https://github.com/AndreaBozzo/Ceres)")
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        Ok(Self {
            client,
            base_url,
            language: language.to_string(),
            dataflows: Arc::new(OnceCell::new()),
        })
    }

    /// Joins `path` onto the service root, keeping any path prefix the root
    /// carries (`/public/rest`, `/SDMXWS/rest`) rather than replacing it.
    fn service_url(&self, path: &str) -> Result<Url, AppError> {
        Url::parse(&format!(
            "{}/{path}",
            self.base_url.as_str().trim_end_matches('/')
        ))
        .map_err(|error| AppError::ClientError(format!("Invalid SDMX URL: {error}")))
    }

    async fn bounded_get(&self, url: Url) -> Result<String, AppError> {
        let response = self
            .client
            .get(url.clone())
            .header(
                reqwest::header::ACCEPT,
                "application/vnd.sdmx.structure+xml;version=2.1, \
                 application/vnd.sdmx.structure+xml;version=3.0;q=0.9, \
                 application/xml;q=0.8",
            )
            .send()
            .await
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        // Every non-success status is an error, 404 included: a service with
        // nothing to publish answers with an empty `Dataflows` container, so a
        // 404 means the endpoint is wrong, not that the catalog is empty.
        let status = response.status();
        if !status.is_success() {
            return Err(AppError::ClientError(format!("HTTP {status} from {url}")));
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAX_RESPONSE_BYTES as u64)
        {
            return Err(AppError::ClientError(format!(
                "SDMX response exceeds {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        if bytes.len() > MAX_RESPONSE_BYTES {
            return Err(AppError::ClientError(format!(
                "SDMX response exceeds {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        String::from_utf8(bytes.to_vec())
            .map_err(|error| AppError::ClientError(format!("SDMX returned non-UTF-8 XML: {error}")))
    }

    /// Fetches every dataflow the service publishes, once per client instance.
    ///
    /// `detail=full` keeps the names, descriptions, and annotations that carry
    /// the update timestamps and provenance links; `references=none` keeps the
    /// referenced data structure definitions out, which for Eurostat alone would
    /// otherwise pull 8,000 codelist-bearing DSDs into one response.
    async fn dataflows(&self) -> Result<&Vec<SdmxDataflow>, AppError> {
        self.dataflows
            .get_or_try_init(|| async {
                let mut url = self.service_url("dataflow/all/all/latest")?;
                url.query_pairs_mut()
                    .append_pair("detail", "full")
                    .append_pair("references", "none");
                let xml = self.bounded_get(url).await?;
                parse_dataflows(&xml, &self.base_url, &self.language)
            })
            .await
    }

    /// Streams the catalog in fixed-size chunks.
    pub fn paginate_stream(&self) -> BoxStream<'_, Result<Vec<SdmxDataflow>, AppError>> {
        Box::pin(futures::stream::unfold(
            (self.clone(), 0usize, false),
            |(client, offset, done)| async move {
                if done {
                    return None;
                }
                let all = match client.dataflows().await {
                    Ok(all) => all,
                    Err(error) => return Some((Err(error), (client.clone(), offset, true))),
                };
                if offset >= all.len() {
                    return None;
                }
                let end = (offset + CHUNK_SIZE).min(all.len());
                let chunk = all[offset..end].to_vec();
                Some((Ok(chunk), (client.clone(), end, false)))
            },
        ))
    }
}

impl PortalClient for SdmxClient {
    type PortalData = SdmxDataflow;

    fn portal_type(&self) -> &'static str {
        "sdmx"
    }

    fn base_url(&self) -> &str {
        self.base_url.as_str()
    }

    async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        Ok(self
            .dataflows()
            .await?
            .iter()
            .map(|flow| flow.id.clone())
            .collect())
    }

    /// Fetches one dataflow by its `AGENCY:ID` identity.
    async fn get_dataset(&self, id: &str) -> Result<SdmxDataflow, AppError> {
        let (agency, flow_id) = id
            .split_once(':')
            .ok_or_else(|| AppError::ClientError(format!("SDMX id '{id}' is not 'AGENCY:ID'")))?;
        let mut url = self.service_url(&format!("dataflow/{agency}/{flow_id}/latest"))?;
        url.query_pairs_mut()
            .append_pair("detail", "full")
            .append_pair("references", "none");
        let xml = self.bounded_get(url).await?;
        parse_dataflows(&xml, &self.base_url, &self.language)?
            .into_iter()
            .next()
            .ok_or_else(|| AppError::ClientError(format!("SDMX dataflow '{id}' not found")))
    }

    /// Identity is `AGENCY:ID`, deliberately without the version.
    ///
    /// The catalog is always read through `all/all/latest`, so exactly one
    /// version of each dataflow is ever seen; folding the version into the
    /// identity would instead retire the row and create a new one every time a
    /// service bumps a dataflow from `1.0` to `1.1`, losing its history for a
    /// change that is not a new dataset. The version stays in `metadata.sdmx`.
    fn into_new_dataset(
        data: SdmxDataflow,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
    ) -> NewDataset {
        let landing_page = match url_template {
            Some(template) => {
                let sdmx = data.metadata.get("sdmx");
                let field = |key: &str| {
                    sdmx.and_then(|s| s.get(key))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string()
                };
                template
                    .replace("{id}", &field("dataflow_id"))
                    .replace("{agency}", &field("agency_id"))
                    .replace("{version}", &field("version"))
                    .replace("{flow_ref}", &field("flow_ref"))
            }
            None => data.landing_page,
        };
        NewDataset {
            content_hash: NewDataset::compute_content_hash_with_language(
                &data.title,
                data.description.as_deref(),
                language,
            ),
            original_id: data.id,
            source_portal: portal_url.to_string(),
            url: landing_page,
            title: data.title,
            description: data.description,
            // A dataflow is a time series family, not a single file.
            record_kind: CatalogRecordKind::Series,
            embedding: None,
            metadata: data.metadata,
        }
    }

    /// SDMX 2.1 defines `updatedAfter` for *data* queries only; structure
    /// queries have no standard modified-since filter, so the harvest falls back
    /// to a full sync.
    async fn search_modified_since(
        &self,
        _since: DateTime<Utc>,
    ) -> Result<Vec<SdmxDataflow>, AppError> {
        Err(AppError::ClientError(
            "SDMX structure queries do not support incremental sync".into(),
        ))
    }

    async fn search_all_datasets(&self) -> Result<Vec<SdmxDataflow>, AppError> {
        Ok(self.dataflows().await?.clone())
    }

    fn search_all_datasets_stream(&self) -> BoxStream<'_, Result<Vec<SdmxDataflow>, AppError>> {
        self.paginate_stream()
    }

    async fn dataset_count(&self) -> Result<usize, AppError> {
        Ok(self.dataflows().await?.len())
    }
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

fn parse_dataflows(
    xml: &str,
    base_url: &Url,
    language: &str,
) -> Result<Vec<SdmxDataflow>, AppError> {
    let doc = Document::parse(xml).map_err(|error| {
        AppError::ClientError(format!("Invalid SDMX structure message: {error}"))
    })?;
    if let Some(message) = sdmx_error(&doc) {
        return Err(AppError::ClientError(message));
    }

    let root = doc.root_element();
    if local(root) != "Structure" {
        return Err(AppError::ClientError(format!(
            "Expected an SDMX Structure message, got <{}>",
            root.tag_name().name()
        )));
    }
    let namespace = root.tag_name().namespace().unwrap_or_default();
    if !(namespace.is_empty() || namespace.contains("/v2_1/") || namespace.contains("/v3_0/")) {
        return Err(AppError::ClientError(format!(
            "Unsupported SDMX-ML namespace '{namespace}' (expected 2.1 or 3.0)"
        )));
    }

    // Scoped to the `Dataflows` container so that a message which also carries
    // referenced artifacts cannot contribute anything but dataflows.
    Ok(doc
        .descendants()
        .filter(|node| local(*node) == "Dataflows")
        .flat_map(|container| container.children())
        .filter(|node| node.is_element() && local(*node) == "Dataflow")
        .filter_map(|node| parse_dataflow(node, base_url, language))
        .collect())
}

fn parse_dataflow(node: Node<'_, '_>, base_url: &Url, language: &str) -> Option<SdmxDataflow> {
    let urn = node.attribute("urn").map(str::to_string);
    // A stub message can omit the attributes and carry only the URN, so the URN
    // is the fallback source for all three parts of the identity.
    let from_urn = urn.as_deref().and_then(parse_urn);
    let attr = |name: &str| {
        node.attribute(name)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };
    let flow_id = attr("id").or_else(|| from_urn.as_ref().map(|u| u.1.clone()))?;
    let agency = attr("agencyID")
        .or_else(|| from_urn.as_ref().map(|u| u.0.clone()))
        .unwrap_or_else(|| "all".to_string());
    let version = attr("version")
        .or_else(|| from_urn.as_ref().map(|u| u.2.clone()))
        .unwrap_or_else(|| "latest".to_string());

    let names = localized(node, "Name");
    let descriptions = localized(node, "Description");
    // A dataflow with no name at all is still a real catalog entry; its
    // identifier is what the service itself displays for it.
    let title = pick_language(&names, language).unwrap_or_else(|| flow_id.clone());
    let description = pick_language(&descriptions, language);

    let annotations = parse_annotations(node);
    let modified = annotations
        .iter()
        .filter_map(|annotation| {
            let kind = annotation.get("type").and_then(Value::as_str)?;
            let rank = UPDATE_ANNOTATIONS
                .iter()
                .position(|candidate| candidate.eq_ignore_ascii_case(kind))?;
            let value = annotation.get("title").and_then(Value::as_str)?;
            parse_date(value).map(|date| (rank, date))
        })
        .min_by_key(|(rank, _)| *rank)
        .map(|(_, date)| date);

    let service_root = base_url.as_str().trim_end_matches('/').to_string();
    let flow_ref = format!("{agency},{flow_id},{version}");
    let structure_url = node
        .attribute("structureURL")
        .map(str::to_string)
        .unwrap_or_else(|| format!("{service_root}/dataflow/{agency}/{flow_id}/{version}"));
    let data_url = format!("{service_root}/data/{flow_ref}");

    let structure_ref = node
        .children()
        .find(|child| child.is_element() && local(*child) == "Structure")
        .map(|structure| {
            let reference = structure
                .children()
                .find(|child| child.is_element() && local(*child) == "Ref");
            let ref_attr = |name: &str| {
                reference
                    .and_then(|node| node.attribute(name))
                    .map(str::to_string)
            };
            json!({
                "id": ref_attr("id"),
                "agency_id": ref_attr("agencyID"),
                "version": ref_attr("version"),
                "class": ref_attr("class"),
                "urn": node_value(structure),
            })
        });

    Some(SdmxDataflow {
        id: format!("{agency}:{flow_id}"),
        title,
        description,
        landing_page: structure_url.clone(),
        modified,
        metadata: json!({
            "catalog_record_kind": CatalogRecordKind::Series,
            "source_format": "application/vnd.sdmx.structure+xml",
            "sdmx": {
                "agency_id": agency,
                "dataflow_id": flow_id,
                "version": version,
                "flow_ref": flow_ref,
                "urn": urn,
                "is_final": node.attribute("isFinal").map(|v| v == "true"),
                "structure_url": structure_url,
                "data_url": data_url,
                "data_structure": structure_ref,
            },
            "names": names,
            "descriptions": descriptions,
            "annotations": annotations,
            "modified": modified.map(|date| date.to_rfc3339()),
        }),
    })
}

/// Collects an element's localized children into a `{lang: text}` map.
///
/// A value with no `xml:lang` is filed under `""`, so a service that publishes
/// unlocalized names still round-trips through [`pick_language`].
fn localized(node: Node<'_, '_>, name: &str) -> Map<String, Value> {
    let mut out = Map::new();
    for child in node
        .children()
        .filter(|child| child.is_element() && local(*child) == name)
    {
        let Some(text) = node_value(child) else {
            continue;
        };
        let lang = child
            .attribute(("http://www.w3.org/XML/1998/namespace", "lang"))
            .or_else(|| child.attribute("lang"))
            .unwrap_or_default()
            .to_ascii_lowercase();
        out.entry(lang).or_insert_with(|| Value::String(text));
    }
    out
}

/// Resolves a localized map against the configured language.
///
/// Falls back to English and then to any value present, because a portal's
/// language is a display preference and never a reason to drop a dataset.
fn pick_language(values: &Map<String, Value>, language: &str) -> Option<String> {
    let requested = language
        .split(['-', '_'])
        .next()
        .unwrap_or(language)
        .to_ascii_lowercase();
    let by_prefix = |prefix: &str| {
        values
            .iter()
            .find(|(lang, _)| lang.split(['-', '_']).next().unwrap_or(lang) == prefix)
            .and_then(|(_, value)| value.as_str())
    };
    by_prefix(&requested)
        .or_else(|| by_prefix("en"))
        .or_else(|| values.values().find_map(Value::as_str))
        .map(str::to_string)
}

fn parse_annotations(node: Node<'_, '_>) -> Vec<Value> {
    node.children()
        .filter(|child| child.is_element() && local(*child) == "Annotations")
        .flat_map(|container| container.children())
        .filter(|child| child.is_element() && local(*child) == "Annotation")
        .map(|annotation| {
            let child_value = |name: &str| {
                annotation
                    .children()
                    .find(|child| child.is_element() && local(*child) == name)
                    .and_then(node_value)
            };
            json!({
                "id": annotation.attribute("id"),
                "type": child_value("AnnotationType"),
                "title": child_value("AnnotationTitle"),
                "url": child_value("AnnotationURL"),
                "texts": Value::Object(localized(annotation, "AnnotationText")),
            })
        })
        .collect()
}

/// Splits `urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=ESTAT:LFSQ(1.0)`
/// into its agency, id, and version.
fn parse_urn(urn: &str) -> Option<(String, String, String)> {
    let tail = urn.rsplit_once('=')?.1;
    let (agency, rest) = tail.split_once(':')?;
    let (id, version) = rest.split_once('(')?;
    let version = version.strip_suffix(')')?;
    (!agency.is_empty() && !id.is_empty() && !version.is_empty())
        .then(|| (agency.to_string(), id.to_string(), version.to_string()))
}

fn sdmx_error(doc: &Document<'_>) -> Option<String> {
    doc.descendants()
        .find(|node| local(*node) == "ErrorMessage")
        .map(|node| {
            let code = node.attribute("code").unwrap_or("unknown");
            let text = node_value(node).unwrap_or_else(|| "no detail".into());
            format!("SDMX service error {code}: {text}")
        })
}

fn local<'a, 'input>(node: Node<'a, 'input>) -> &'input str {
    node.tag_name().name()
}

fn node_value(node: Node<'_, '_>) -> Option<String> {
    node.descendants().find_map(|child| {
        if !child.is_text() {
            return None;
        }
        let value = child.text().unwrap_or_default().trim();
        (!value.is_empty()).then(|| value.to_string())
    })
}

/// Parses the timestamp shapes SDMX annotations carry in the wild.
///
/// Eurostat writes `2026-04-16T23:00:00+0200` — RFC 3339 except for the missing
/// colon in the offset, which `DateTime::parse_from_rfc3339` rejects.
fn parse_date(value: &str) -> Option<DateTime<Utc>> {
    let value = value.trim();
    DateTime::parse_from_rfc3339(value)
        .or_else(|_| DateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%z"))
        .map(|date| date.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .ok()
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|date| date.and_utc())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ceres_core::schema::DatasetSchema;
    use futures::StreamExt;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    const DATAFLOWS: &str = include_str!("../tests/fixtures/sdmx_dataflows.xml");

    fn base() -> Url {
        Url::parse("https://sdmx.test/rest/").unwrap()
    }

    fn parse(language: &str) -> Vec<SdmxDataflow> {
        parse_dataflows(DATAFLOWS, &base(), language).unwrap()
    }

    #[test]
    fn parses_every_dataflow_in_the_message() {
        let flows = parse("en");
        assert_eq!(
            flows.iter().map(|f| f.id.as_str()).collect::<Vec<_>>(),
            ["ESTAT:NAMA_10_GDP", "SPC:DF_ADBKI", "NB:ANN_FX_SPU"]
        );
    }

    #[test]
    fn resolves_the_configured_language_and_falls_back_to_english() {
        let english = parse("en");
        assert_eq!(english[1].title, "Asian Development Bank Key Indicators");

        let french = parse("fr");
        assert_eq!(
            french[1].title,
            "Indicateurs clés de la Banque asiatique de développement"
        );
        // The Eurostat flow publishes no French name, so English carries it
        // rather than the dataset disappearing from a French harvest.
        assert_eq!(french[0].title, "GDP and main aggregates");
    }

    #[test]
    fn a_dataflow_without_a_name_falls_back_to_its_identifier() {
        let flows = parse("en");
        assert_eq!(flows[2].title, "ANN_FX_SPU");
        assert_eq!(flows[2].description, None);
    }

    #[test]
    fn identity_omits_the_version_but_metadata_keeps_it() {
        let flow = &parse("en")[1];
        assert_eq!(flow.id, "SPC:DF_ADBKI");
        assert_eq!(flow.metadata["sdmx"]["version"], "1.1");
        assert_eq!(flow.metadata["sdmx"]["flow_ref"], "SPC,DF_ADBKI,1.1");
    }

    #[test]
    fn identity_falls_back_to_the_urn_when_attributes_are_absent() {
        assert_eq!(
            parse_urn("urn:sdmx:org.sdmx.infomodel.datastructure.Dataflow=NB:ANN_FX_SPU(1.0)"),
            Some(("NB".into(), "ANN_FX_SPU".into(), "1.0".into()))
        );
        assert_eq!(parse_urn("not-a-urn"), None);
    }

    #[test]
    fn annotations_are_preserved_and_yield_the_update_timestamp() {
        let flow = &parse("en")[0];
        let annotations = flow.metadata["annotations"].as_array().unwrap();
        assert_eq!(annotations.len(), 3);
        assert_eq!(
            annotations[2]["url"],
            "https://ec.europa.eu/eurostat/cache/metadata/en/nama10_esms.htm"
        );
        // UPDATE_DATA outranks UPDATE_STRUCTURE, and Eurostat's colon-less UTC
        // offset still parses.
        assert_eq!(
            flow.modified.unwrap().to_rfc3339(),
            "2026-04-16T21:00:00+00:00"
        );
    }

    #[test]
    fn the_structure_reference_is_kept_without_being_followed() {
        let flow = &parse("en")[0];
        assert_eq!(flow.metadata["sdmx"]["data_structure"]["id"], "NAMA_10_GDP");
        assert_eq!(
            flow.metadata["sdmx"]["data_structure"]["class"],
            "DataStructure"
        );
    }

    #[test]
    fn landing_page_prefers_a_published_structure_url() {
        let flows = parse("en");
        assert_eq!(
            flows[0].landing_page,
            "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/NAMA_10_GDP/1.0"
        );
        // Without one, it is derived from the service root.
        assert_eq!(
            flows[1].landing_page,
            "https://sdmx.test/rest/dataflow/SPC/DF_ADBKI/1.1"
        );
    }

    #[test]
    fn normalizes_a_dataflow_as_a_series_with_a_data_endpoint() {
        let flow = parse("en").remove(1);
        let dataset = SdmxClient::into_new_dataset(flow, "https://sdmx.test", None, "en");
        assert_eq!(dataset.record_kind, CatalogRecordKind::Series);
        assert_eq!(dataset.original_id, "SPC:DF_ADBKI");

        let resources = DatasetSchema::from_metadata(&dataset.metadata).resources;
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].name.as_deref(), Some("SPC,DF_ADBKI,1.1"));
        assert_eq!(resources[0].format.as_deref(), Some("SDMX-CSV"));
        assert_eq!(
            resources[0].url.as_deref(),
            Some("https://sdmx.test/rest/data/SPC,DF_ADBKI,1.1")
        );
    }

    #[test]
    fn a_url_template_addresses_the_service_browser() {
        let flow = parse("en").remove(0);
        let dataset = SdmxClient::into_new_dataset(
            flow,
            "https://ec.europa.eu/eurostat",
            Some("https://ec.europa.eu/eurostat/databrowser/view/{id}/default/table"),
            "en",
        );
        assert_eq!(
            dataset.url,
            "https://ec.europa.eu/eurostat/databrowser/view/NAMA_10_GDP/default/table"
        );
    }

    #[test]
    fn a_service_error_response_is_reported_rather_than_read_as_empty() {
        let xml = r#"<mes:Error xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
                                xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
            <mes:ErrorMessage code="140"><com:Text>Unauthorized</com:Text></mes:ErrorMessage>
        </mes:Error>"#;
        let error = parse_dataflows(xml, &base(), "en").unwrap_err();
        assert!(error.to_string().contains("140"));
        assert!(error.to_string().contains("Unauthorized"));
    }

    #[test]
    fn a_non_structure_message_is_rejected() {
        let xml = r#"<mes:GenericData xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"/>"#;
        let error = parse_dataflows(xml, &base(), "en").unwrap_err();
        assert!(error.to_string().contains("GenericData"));
    }

    #[test]
    fn referenced_artifacts_outside_the_dataflows_container_are_not_harvested() {
        let xml = r#"<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
                                    xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
                                    xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
          <mes:Structures>
            <str:Dataflows>
              <str:Dataflow id="REAL" agencyID="X" version="1.0">
                <com:Name xml:lang="en">Real</com:Name>
              </str:Dataflow>
            </str:Dataflows>
            <str:DataStructures>
              <str:Dataflow id="DECOY" agencyID="X" version="1.0"/>
            </str:DataStructures>
          </mes:Structures>
        </mes:Structure>"#;
        let flows = parse_dataflows(xml, &base(), "en").unwrap();
        assert_eq!(
            flows.iter().map(|f| f.id.as_str()).collect::<Vec<_>>(),
            ["X:REAL"]
        );
    }

    #[tokio::test]
    async fn streams_the_catalog_in_chunks_from_a_single_request() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rest/dataflow/all/all/latest"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/vnd.sdmx.structure+xml")
                    .set_body_string(DATAFLOWS),
            )
            // One request backs the whole stream, however many chunks it yields.
            .expect(1)
            .mount(&server)
            .await;

        let client = SdmxClient::new(&format!("{}/rest", server.uri()), "en").unwrap();
        let mut stream = client.paginate_stream();
        let mut ids = Vec::new();
        while let Some(chunk) = stream.next().await {
            ids.extend(chunk.unwrap().into_iter().map(|flow| flow.id));
        }
        assert_eq!(ids, ["ESTAT:NAMA_10_GDP", "SPC:DF_ADBKI", "NB:ANN_FX_SPU"]);
        assert_eq!(client.dataset_count().await.unwrap(), 3);
    }

    #[test]
    fn a_service_with_nothing_to_publish_parses_as_empty() {
        // The empty answer is an empty container, not a 404 — which is why a
        // 404 is treated as the wrong endpoint rather than an empty catalog.
        let xml = r#"<mes:Structure xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
                                    xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure">
          <mes:Structures><str:Dataflows /></mes:Structures>
        </mes:Structure>"#;
        assert!(parse_dataflows(xml, &base(), "en").unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_404_is_reported_rather_than_read_as_an_empty_catalog() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rest/dataflow/all/all/latest"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = SdmxClient::new(&format!("{}/rest", server.uri()), "en").unwrap();
        let error = client.search_all_datasets().await.unwrap_err();
        assert!(error.to_string().contains("404"), "{error}");
    }

    #[tokio::test]
    async fn a_missing_dataflow_is_named_in_the_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/rest/dataflow/NB/NOPE/latest"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let client = SdmxClient::new(&format!("{}/rest", server.uri()), "en").unwrap();
        let error = client.get_dataset("NB:NOPE").await.unwrap_err();
        assert!(error.to_string().contains("404"), "{error}");
    }

    #[tokio::test]
    #[ignore = "requires network access to a public SDMX REST service"]
    async fn sdmx_smoke_catalog() {
        // Norges Bank publishes ~25 dataflows: small and fast enough for CI.
        let url = std::env::var("CERES_SDMX_SMOKE_URL")
            .unwrap_or_else(|_| "https://data.norges-bank.no/api".into());
        let client = SdmxClient::new(&url, "en").unwrap();
        let flows = client.search_all_datasets().await.unwrap();
        assert!(!flows.is_empty(), "{url} published no dataflows");
        assert!(flows.iter().all(|flow| !flow.title.is_empty()));
        assert!(
            flows
                .iter()
                .all(|flow| flow.metadata["sdmx"]["data_url"].is_string()),
            "every dataflow needs the data endpoint its resource is derived from"
        );
    }

    /// The largest catalog in the family — proves the unpaginated 37 MB
    /// structure message is read whole rather than truncated at a size cap.
    #[tokio::test]
    #[ignore = "requires network access to the Eurostat SDMX API"]
    async fn eurostat_sdmx_smoke() {
        let url = std::env::var("CERES_SDMX_EUROSTAT_URL")
            .unwrap_or_else(|_| "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1".into());
        let client = SdmxClient::new(&url, "en").unwrap();
        let first = client.paginate_stream().next().await.unwrap().unwrap();
        assert_eq!(first.len(), CHUNK_SIZE);
        assert!(client.dataset_count().await.unwrap() > 5_000);
    }
}
