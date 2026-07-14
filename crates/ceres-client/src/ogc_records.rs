//! OGC catalogue records client using the CSW 2.0.2 discovery protocol.

use std::{collections::HashSet, sync::Arc, time::Duration};

use ceres_core::{AppError, CatalogRecordKind, NewDataset, traits::PortalClient};
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::BoxStream};
use reqwest::{Client, Url};
use roxmltree::{Document, Node};
use serde_json::{Value, json};
use tokio::sync::OnceCell;

const PAGE_SIZE: usize = 100;
const MAX_PAGES: usize = 100_000;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone)]
struct CswBindings {
    get_records: Url,
    get_record_by_id: Url,
}

#[derive(Debug, Clone)]
pub struct OgcRecord {
    pub identifier: String,
    pub title: String,
    pub description: Option<String>,
    pub landing_page: String,
    pub modified: Option<DateTime<Utc>>,
    pub record_kind: CatalogRecordKind,
    pub metadata: Value,
}

#[derive(Debug)]
struct Page {
    records: Vec<OgcRecord>,
    next_record: usize,
    matched: usize,
}

#[derive(Clone)]
pub struct OgcRecordsClient {
    client: Client,
    base_url: Url,
    endpoint: Url,
    language: String,
    bindings: Arc<OnceCell<CswBindings>>,
}

impl OgcRecordsClient {
    pub fn new(base_url: &str, language: &str, endpoint: Option<&str>) -> Result<Self, AppError> {
        let base_url =
            Url::parse(base_url).map_err(|_| AppError::InvalidPortalUrl(base_url.to_string()))?;
        let endpoint = Url::parse(endpoint.unwrap_or(base_url.as_str())).map_err(|_| {
            AppError::InvalidPortalUrl(endpoint.unwrap_or(base_url.as_str()).to_string())
        })?;
        let client = Client::builder()
            .user_agent("Ceres/0.6 (open-data-harvester)")
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        Ok(Self {
            client,
            base_url,
            endpoint,
            language: language.to_string(),
            bindings: Arc::new(OnceCell::new()),
        })
    }

    async fn bounded_get(
        &self,
        mut url: Url,
        params: &[(&str, String)],
    ) -> Result<String, AppError> {
        url.query_pairs_mut()
            .extend_pairs(params.iter().map(|(k, v)| (*k, v.as_str())));
        let response = self
            .client
            .get(url.clone())
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
                "CSW response exceeds {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        if bytes.len() > MAX_RESPONSE_BYTES {
            return Err(AppError::ClientError(format!(
                "CSW response exceeds {MAX_RESPONSE_BYTES} bytes"
            )));
        }
        String::from_utf8(bytes.to_vec())
            .map_err(|error| AppError::ClientError(format!("CSW returned non-UTF-8 XML: {error}")))
    }

    async fn bindings(&self) -> Result<&CswBindings, AppError> {
        self.bindings
            .get_or_try_init(|| async {
                let xml = self
                    .bounded_get(
                        self.endpoint.clone(),
                        &[
                            ("service", "CSW".into()),
                            ("version", "2.0.2".into()),
                            ("request", "GetCapabilities".into()),
                        ],
                    )
                    .await?;
                parse_capabilities(&xml, &self.endpoint)
            })
            .await
    }

    async fn page(&self, start: usize) -> Result<Page, AppError> {
        let endpoint = self.bindings().await?.get_records.clone();
        let xml = self
            .bounded_get(
                endpoint,
                &[
                    ("service", "CSW".into()),
                    ("version", "2.0.2".into()),
                    ("request", "GetRecords".into()),
                    ("resultType", "results".into()),
                    ("typeNames", "gmd:MD_Metadata".into()),
                    (
                        "namespace",
                        "xmlns(gmd=http://www.isotc211.org/2005/gmd)".into(),
                    ),
                    ("elementSetName", "full".into()),
                    ("outputSchema", "http://www.isotc211.org/2005/gmd".into()),
                    ("startPosition", start.to_string()),
                    ("maxRecords", PAGE_SIZE.to_string()),
                ],
            )
            .await?;
        parse_get_records(&xml, self.base_url.as_str(), &self.language)
    }

    pub fn paginate_stream(&self) -> BoxStream<'_, Result<Vec<OgcRecord>, AppError>> {
        struct State {
            start: usize,
            pages: usize,
            seen: HashSet<usize>,
            done: bool,
        }
        Box::pin(futures::stream::unfold(
            (
                self.clone(),
                State {
                    start: 1,
                    pages: 0,
                    seen: HashSet::new(),
                    done: false,
                },
            ),
            |(client, mut state)| async move {
                if state.done {
                    return None;
                }
                if state.pages >= MAX_PAGES || !state.seen.insert(state.start) {
                    state.done = true;
                    return Some((
                        Err(AppError::ClientError(
                            "CSW pagination did not terminate deterministically".into(),
                        )),
                        (client, state),
                    ));
                }
                state.pages += 1;
                match client.page(state.start).await {
                    Ok(page) => {
                        if page.next_record == 0 || page.next_record > page.matched {
                            state.done = true;
                        } else if page.next_record <= state.start {
                            state.done = true;
                            return Some((
                                Err(AppError::ClientError(format!(
                                    "CSW nextRecord {} did not advance from {}",
                                    page.next_record, state.start
                                ))),
                                (client, state),
                            ));
                        } else {
                            state.start = page.next_record;
                        }
                        Some((Ok(page.records), (client, state)))
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

impl PortalClient for OgcRecordsClient {
    type PortalData = OgcRecord;
    fn portal_type(&self) -> &'static str {
        "ogc_records"
    }
    fn base_url(&self) -> &str {
        self.base_url.as_str()
    }
    async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        Ok(self
            .search_all_datasets()
            .await?
            .into_iter()
            .map(|r| r.identifier)
            .collect())
    }
    async fn get_dataset(&self, id: &str) -> Result<OgcRecord, AppError> {
        let endpoint = self.bindings().await?.get_record_by_id.clone();
        let xml = self
            .bounded_get(
                endpoint,
                &[
                    ("service", "CSW".into()),
                    ("version", "2.0.2".into()),
                    ("request", "GetRecordById".into()),
                    ("elementSetName", "full".into()),
                    ("outputSchema", "http://www.isotc211.org/2005/gmd".into()),
                    ("id", id.into()),
                ],
            )
            .await?;
        parse_single_record(&xml, self.base_url.as_str(), &self.language)
    }
    fn into_new_dataset(
        data: OgcRecord,
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
            original_id: data.identifier,
            source_portal: portal_url.into(),
            url: data.landing_page,
            title: data.title,
            description: data.description,
            record_kind: data.record_kind,
            embedding: None,
            metadata: data.metadata,
        }
    }
    async fn search_modified_since(
        &self,
        _since: DateTime<Utc>,
    ) -> Result<Vec<OgcRecord>, AppError> {
        Err(AppError::ClientError(
            "CSW incremental sync is not supported".into(),
        ))
    }
    async fn search_all_datasets(&self) -> Result<Vec<OgcRecord>, AppError> {
        let mut all = Vec::new();
        let mut stream = self.paginate_stream();
        while let Some(page) = stream.next().await {
            all.extend(page?);
        }
        Ok(all)
    }
    fn search_all_datasets_stream(&self) -> BoxStream<'_, Result<Vec<OgcRecord>, AppError>> {
        self.paginate_stream()
    }
}

fn parse_capabilities(xml: &str, fallback: &Url) -> Result<CswBindings, AppError> {
    let doc = Document::parse(xml).map_err(xml_error)?;
    if doc.descendants().any(|n| local(n) == "ExceptionReport") {
        return Err(csw_exception(&doc));
    }
    let operation_url = |name: &str| -> Option<Url> {
        doc.descendants()
            .find(|n| local(*n) == "Operation" && n.attribute("name") == Some(name))
            .and_then(|operation| {
                operation.descendants().find_map(|n| {
                    if local(n) != "Get" {
                        return None;
                    }
                    n.attributes()
                        .find(|a| a.name().ends_with("href"))
                        .and_then(|a| fallback.join(a.value()).ok())
                })
            })
    };
    Ok(CswBindings {
        get_records: operation_url("GetRecords").unwrap_or_else(|| fallback.clone()),
        get_record_by_id: operation_url("GetRecordById").unwrap_or_else(|| fallback.clone()),
    })
}

fn parse_get_records(xml: &str, portal_url: &str, language: &str) -> Result<Page, AppError> {
    let doc = Document::parse(xml).map_err(xml_error)?;
    if doc.descendants().any(|n| local(n) == "ExceptionReport") {
        return Err(csw_exception(&doc));
    }
    let results = doc
        .descendants()
        .find(|n| local(*n) == "SearchResults")
        .ok_or_else(|| AppError::ClientError("CSW response is missing SearchResults".into()))?;
    let number = |name| {
        results
            .attribute(name)
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0)
    };
    let records: Vec<OgcRecord> = results
        .children()
        .filter(|n| n.is_element())
        .filter_map(|n| parse_record(n, xml, portal_url, language))
        .collect();
    let returned = number("numberOfRecordsReturned");
    if returned != records.len() {
        return Err(AppError::ClientError(format!(
            "CSW declared {returned} returned records but contained {}",
            records.len()
        )));
    }
    Ok(Page {
        records,
        next_record: number("nextRecord"),
        matched: number("numberOfRecordsMatched"),
    })
}

fn parse_single_record(xml: &str, portal_url: &str, language: &str) -> Result<OgcRecord, AppError> {
    let doc = Document::parse(xml).map_err(xml_error)?;
    doc.root_element()
        .children()
        .find(|n| n.is_element())
        .and_then(|n| parse_record(n, xml, portal_url, language))
        .or_else(|| parse_record(doc.root_element(), xml, portal_url, language))
        .ok_or_else(|| AppError::ClientError("CSW response contained no parseable record".into()))
}

fn parse_record(
    node: Node<'_, '_>,
    xml: &str,
    portal_url: &str,
    language: &str,
) -> Option<OgcRecord> {
    let values = |names: &[&str]| -> Vec<String> {
        node.descendants()
            .filter(|n| names.contains(&local(*n)))
            .filter_map(node_value)
            .collect()
    };
    let first = |names: &[&str]| values(names).into_iter().find(|v| !v.trim().is_empty());
    let identifier = first(&["fileIdentifier", "identifier"])?;
    // Some legacy ISO records are structurally present but omit the citation
    // title. Preserve them using their stable identifier as the display
    // fallback instead of silently dropping the complete source record.
    let title = localized_field(node, &["title"], language).unwrap_or_else(|| identifier.clone());
    let description = localized_field(node, &["abstract", "description"], language);
    let modified_text = first(&["dateStamp", "modified"]);
    let modified = modified_text.as_deref().and_then(parse_date);
    let scope = node
        .descendants()
        .filter(|n| matches!(local(*n), "MD_ScopeCode" | "hierarchyLevel" | "type"))
        .find_map(|n| {
            n.attribute("codeListValue")
                .map(str::to_owned)
                .or_else(|| node_value(n))
        })
        .unwrap_or_default();
    let record_kind = classify_kind(&scope);
    let online_resources: Vec<Value> = node
        .descendants()
        .filter(|n| local(*n) == "CI_OnlineResource")
        .map(|n| {
            let url = n
                .descendants()
                .find(|x| local(*x) == "URL")
                .and_then(node_value);
            let protocol = n
                .descendants()
                .find(|x| local(*x) == "protocol")
                .and_then(node_value);
            let function = n
                .descendants()
                .find(|x| local(*x) == "CI_OnLineFunctionCode")
                .and_then(|x| {
                    x.attribute("codeListValue")
                        .map(str::to_owned)
                        .or_else(|| node_value(x))
                });
            let downloadable = protocol.as_deref().is_some_and(|p| {
                let p = p.to_ascii_lowercase();
                p.contains("download") || p.contains("wfs") || p.contains("file")
            }) || function
                .as_deref()
                .is_some_and(|f| f.eq_ignore_ascii_case("download"));
            json!({
                "url": url,
                "protocol": protocol,
                "function": function,
                "downloadable": downloadable,
            })
        })
        .collect();
    let landing_page = online_resources
        .iter()
        .filter(|resource| resource.get("downloadable") != Some(&Value::Bool(true)))
        .find_map(|resource| resource.get("url").and_then(Value::as_str))
        .or_else(|| {
            online_resources
                .iter()
                .find_map(|resource| resource.get("url").and_then(Value::as_str))
        })
        .unwrap_or(portal_url)
        .to_string();
    let number = |name: &str| first(&[name]).and_then(|value| value.parse::<f64>().ok());
    let spatial_bbox = match (
        number("westBoundLongitude"),
        number("southBoundLatitude"),
        number("eastBoundLongitude"),
        number("northBoundLatitude"),
    ) {
        (Some(west), Some(south), Some(east), Some(north)) => {
            Some(json!([west, south, east, north]))
        }
        _ => None,
    };
    let contacts: Vec<Value> = node
        .descendants()
        .filter(|candidate| local(*candidate) == "CI_ResponsibleParty")
        .map(|contact| {
            let contact_value = |name: &str| {
                contact
                    .descendants()
                    .find(|candidate| local(*candidate) == name)
                    .and_then(node_value)
            };
            let role = contact
                .descendants()
                .find(|candidate| local(*candidate) == "CI_RoleCode")
                .and_then(|candidate| {
                    candidate
                        .attribute("codeListValue")
                        .map(str::to_owned)
                        .or_else(|| node_value(candidate))
                });
            json!({
                "individual": contact_value("individualName"),
                "organization": contact_value("organisationName"),
                "email": contact_value("electronicMailAddress"),
                "role": role,
            })
        })
        .collect();
    let raw_xml = xml.get(node.range()).unwrap_or_default();
    Some(OgcRecord {
        identifier,
        title,
        description,
        landing_page,
        modified,
        record_kind,
        metadata: json!({
            "catalog_record_kind": record_kind,
            "source_format": "application/xml",
            "source_xml": raw_xml,
            "scope": scope,
            "keywords": values(&["keyword", "subject"]),
            "publisher": first(&["organisationName", "publisher"]),
            "license": first(&["useLimitation", "license"]),
            "access_constraints": values(&["accessConstraints", "otherConstraints"]),
            "modified": modified_text,
            "spatial": {"bbox": spatial_bbox},
            "temporal": {
                "start": first(&["beginPosition"]),
                "end": first(&["endPosition"]),
            },
            "contacts": contacts,
            "online_resources": online_resources,
        }),
    })
}

fn localized_field(node: Node<'_, '_>, names: &[&str], language: &str) -> Option<String> {
    let requested = language
        .split(['-', '_'])
        .next()
        .unwrap_or(language)
        .trim_start_matches('#')
        .to_ascii_lowercase();
    let fields: Vec<Node<'_, '_>> = node
        .descendants()
        .filter(|candidate| names.contains(&local(*candidate)))
        .collect();
    fields
        .iter()
        .flat_map(|field| field.descendants())
        .filter(|candidate| local(*candidate) == "LocalisedCharacterString")
        .find(|candidate| {
            candidate
                .attribute("locale")
                .map(|locale| {
                    locale
                        .trim_start_matches('#')
                        .to_ascii_lowercase()
                        .starts_with(&requested)
                })
                .unwrap_or(false)
        })
        .and_then(node_value)
        .or_else(|| {
            fields.iter().find_map(|field| {
                field
                    .descendants()
                    .find(|candidate| matches!(local(*candidate), "CharacterString" | "Anchor"))
                    .and_then(node_value)
            })
        })
        .or_else(|| fields.into_iter().find_map(node_value))
}

fn classify_kind(value: &str) -> CatalogRecordKind {
    match value.to_ascii_lowercase().as_str() {
        "dataset" => CatalogRecordKind::Dataset,
        "series" | "collection" => CatalogRecordKind::Series,
        "service" => CatalogRecordKind::Service,
        "map" | "model" | "tile" => CatalogRecordKind::Map,
        _ => CatalogRecordKind::Other,
    }
}
fn local<'a, 'input>(node: Node<'a, 'input>) -> &'input str {
    node.tag_name().name()
}
fn node_value(node: Node<'_, '_>) -> Option<String> {
    node.descendants().find_map(|n| {
        if !n.is_text() {
            return None;
        }
        let value = n.text().unwrap_or_default().trim();
        (!value.is_empty()).then(|| value.to_string())
    })
}
fn parse_date(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(value)
        .map(|d| d.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .map(|d| d.and_utc())
        })
}
fn xml_error(error: roxmltree::Error) -> AppError {
    AppError::ClientError(format!("Invalid CSW XML: {error}"))
}
fn csw_exception(doc: &Document<'_>) -> AppError {
    AppError::ClientError(
        doc.descendants()
            .find(|n| local(*n) == "ExceptionText")
            .and_then(node_value)
            .unwrap_or_else(|| "CSW exception response".into()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("../tests/fixtures/csw_get_records.xml");

    #[test]
    fn parses_get_records_fixture_page() {
        let page = parse_get_records(FIXTURE, "https://catalog.example.test", "en").unwrap();
        assert_eq!(page.matched, 3);
        assert_eq!(page.next_record, 0);

        let kinds: Vec<CatalogRecordKind> = page.records.iter().map(|r| r.record_kind).collect();
        assert_eq!(
            kinds,
            [
                CatalogRecordKind::Dataset,
                CatalogRecordKind::Series,
                CatalogRecordKind::Service,
            ]
        );

        let dataset = &page.records[0];
        assert_eq!(
            dataset.identifier,
            "b1a7e9c2-0d43-4f6a-9a21-demo-dataset-001"
        );
        assert_eq!(dataset.title, "Mean sea surface temperature 2000-2025");
        assert!(
            dataset
                .description
                .as_deref()
                .unwrap()
                .contains("sea surface temperature")
        );
        // Landing pages prefer a descriptive link over a download URL.
        assert_eq!(
            dataset.landing_page,
            "https://catalog.example.test/records/demo-dataset-001"
        );
        assert_eq!(
            dataset.modified.unwrap().date_naive().to_string(),
            "2026-05-14"
        );

        // Records without online resources fall back to the portal URL.
        assert_eq!(page.records[1].landing_page, "https://catalog.example.test");
    }

    #[test]
    fn fixture_dataset_metadata_preserves_source_details() {
        let page = parse_get_records(FIXTURE, "https://catalog.example.test", "en").unwrap();
        let metadata = &page.records[0].metadata;

        assert_eq!(metadata["publisher"], "European Marine Observation Network");
        assert_eq!(metadata["license"], "CC-BY 4.0");
        assert_eq!(metadata["keywords"], json!(["oceanography", "temperature"]));
        assert_eq!(metadata["scope"], "dataset");
        assert_eq!(metadata["source_format"], "application/xml");
        assert!(
            metadata["source_xml"]
                .as_str()
                .unwrap()
                .contains("MD_Metadata")
        );

        let resources = metadata["online_resources"].as_array().unwrap();
        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0]["downloadable"], true);
        assert_eq!(resources[0]["protocol"], "WWW:DOWNLOAD-1.0-http--download");
        assert_eq!(resources[1]["downloadable"], false);
    }

    #[test]
    fn selects_localized_text_and_normalizes_extent_and_contacts() {
        let xml = r##"<csw:GetRecordsResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gco="http://www.isotc211.org/2005/gco"><csw:SearchResults numberOfRecordsMatched="1" numberOfRecordsReturned="1" nextRecord="0"><gmd:MD_Metadata><gmd:fileIdentifier><gco:CharacterString>localized</gco:CharacterString></gmd:fileIdentifier><gmd:title><gco:CharacterString>Default title</gco:CharacterString><gmd:PT_FreeText><gmd:textGroup><gmd:LocalisedCharacterString locale="#FR">Titre français</gmd:LocalisedCharacterString></gmd:textGroup></gmd:PT_FreeText></gmd:title><gmd:abstract><gco:CharacterString>Default description</gco:CharacterString><gmd:PT_FreeText><gmd:textGroup><gmd:LocalisedCharacterString locale="#FR">Description française</gmd:LocalisedCharacterString></gmd:textGroup></gmd:PT_FreeText></gmd:abstract><gmd:EX_GeographicBoundingBox><gmd:westBoundLongitude><gco:Decimal>-5</gco:Decimal></gmd:westBoundLongitude><gmd:eastBoundLongitude><gco:Decimal>10</gco:Decimal></gmd:eastBoundLongitude><gmd:southBoundLatitude><gco:Decimal>40</gco:Decimal></gmd:southBoundLatitude><gmd:northBoundLatitude><gco:Decimal>52</gco:Decimal></gmd:northBoundLatitude></gmd:EX_GeographicBoundingBox><gmd:CI_ResponsibleParty><gmd:organisationName><gco:CharacterString>Marine Office</gco:CharacterString></gmd:organisationName><gmd:electronicMailAddress><gco:CharacterString>data@example.test</gco:CharacterString></gmd:electronicMailAddress><gmd:role><gmd:CI_RoleCode codeListValue="publisher"/></gmd:role></gmd:CI_ResponsibleParty></gmd:MD_Metadata></csw:SearchResults></csw:GetRecordsResponse>"##;
        let page = parse_get_records(xml, "https://example.test", "fr").unwrap();
        let record = &page.records[0];
        assert_eq!(record.title, "Titre français");
        assert_eq!(record.description.as_deref(), Some("Description française"));
        assert_eq!(
            record.metadata["spatial"]["bbox"],
            json!([-5.0, 40.0, 10.0, 52.0])
        );
        assert_eq!(
            record.metadata["contacts"][0]["organization"],
            "Marine Office"
        );
        assert_eq!(record.metadata["contacts"][0]["role"], "publisher");
    }

    #[test]
    fn parses_iso_record_and_preserves_xml() {
        let xml = r#"<csw:GetRecordsResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gco="http://www.isotc211.org/2005/gco"><csw:SearchResults numberOfRecordsMatched="1" numberOfRecordsReturned="1" nextRecord="0"><gmd:MD_Metadata><gmd:fileIdentifier><gco:CharacterString>abc</gco:CharacterString></gmd:fileIdentifier><gmd:hierarchyLevel><gmd:MD_ScopeCode codeListValue="service"/></gmd:hierarchyLevel><gmd:title><gco:CharacterString>Marine service</gco:CharacterString></gmd:title><gmd:abstract><gco:CharacterString>Description</gco:CharacterString></gmd:abstract></gmd:MD_Metadata></csw:SearchResults></csw:GetRecordsResponse>"#;
        let page = parse_get_records(xml, "https://example.test", "en").unwrap();
        assert_eq!(page.records[0].record_kind, CatalogRecordKind::Service);
        assert!(
            page.records[0].metadata["source_xml"]
                .as_str()
                .unwrap()
                .contains("MD_Metadata")
        );
    }

    #[test]
    fn discovers_get_bindings_from_capabilities() {
        let xml = r#"<ows:Capabilities xmlns:ows="http://www.opengis.net/ows" xmlns:xlink="http://www.w3.org/1999/xlink"><ows:OperationsMetadata><ows:Operation name="GetRecords"><ows:DCP><ows:HTTP><ows:Get xlink:href="https://catalog.test/query"/></ows:HTTP></ows:DCP></ows:Operation><ows:Operation name="GetRecordById"><ows:DCP><ows:HTTP><ows:Get xlink:href="https://catalog.test/id"/></ows:HTTP></ows:DCP></ows:Operation></ows:OperationsMetadata></ows:Capabilities>"#;
        let bindings =
            parse_capabilities(xml, &Url::parse("https://catalog.test/csw").unwrap()).unwrap();
        assert_eq!(bindings.get_records.as_str(), "https://catalog.test/query");
        assert_eq!(
            bindings.get_record_by_id.as_str(),
            "https://catalog.test/id"
        );
    }

    #[test]
    fn rejects_inconsistent_returned_count() {
        let xml = r#"<csw:GetRecordsResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2"><csw:SearchResults numberOfRecordsMatched="1" numberOfRecordsReturned="1" nextRecord="0"/></csw:GetRecordsResponse>"#;
        assert!(parse_get_records(xml, "https://catalog.test", "en").is_err());
    }

    #[tokio::test]
    #[ignore = "requires network access to EMODnet"]
    async fn emodnet_csw_smoke() {
        let client = OgcRecordsClient::new(
            "https://emodnet.ec.europa.eu",
            "en",
            Some("https://emodnet.ec.europa.eu/geonetwork/emodnet/eng/csw"),
        )
        .unwrap();
        let first_page = client.paginate_stream().next().await.unwrap().unwrap();
        assert!(!first_page.is_empty());
    }

    #[tokio::test]
    #[ignore = "requires network access to Copernicus Marine"]
    async fn copernicus_marine_csw_smoke() {
        let client = OgcRecordsClient::new(
            "https://marine.copernicus.eu",
            "en",
            Some("https://csw.marine.copernicus.eu/geonetwork/csw-MYOCEAN-CORE-PRODUCTS/eng/csw"),
        )
        .unwrap();
        let first_page = client.paginate_stream().next().await.unwrap().unwrap();
        assert!(!first_page.is_empty());
    }
}
