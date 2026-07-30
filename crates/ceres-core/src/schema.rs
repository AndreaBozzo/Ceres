//! Dataset resource schema, derived from harvested metadata.
//!
//! Portals expose information about the individual resources (CKAN) or
//! distributions (DCAT) that make up a dataset: their format, access URL, and
//! sometimes a column-level schema (field names and types). That information is
//! already harvested in full into the `metadata` JSONB column of each dataset,
//! so this module normalizes it **on read** rather than storing a separate copy.
//!
//! The extraction is intentionally defensive and portal-agnostic: it operates on
//! a raw [`serde_json::Value`] and pulls whatever fields are present, tolerating
//! the structural differences between CKAN `package_show` output and DCAT-AP
//! JSON-LD dataset nodes.
//!
//! # Limitations
//!
//! - Field-level schema only appears when the portal inlined it in the harvested
//!   metadata (e.g. a Frictionless `schema.fields` block). We do not call CKAN's
//!   `datastore_info`/`datastore_search`, so DataStore-only schemas are not enriched.
//! - For DCAT portals, distributions published as separate `@graph` nodes are
//!   inlined onto the dataset node at harvest time by the DCAT client. A
//!   distribution paginated onto a later catalog page cannot be resolved, so its
//!   reference is preserved verbatim in `metadata` — but an unresolved
//!   `{"@id": "..."}` yields no [`DatasetResource`], because no facet can be read
//!   from it. Every resource this module emits carries at least one of `name`,
//!   `format`, `media_type`, `url`, or a non-empty `fields`, so a non-empty
//!   `resources` does imply usable resource depth. A description alone does not
//!   qualify, and neither does a value that is blank once trimmed; see
//!   `is_informative`.
//! - An OpenDataSoft dataset is a single table whose column schema lives at the
//!   *dataset* level, so its primary resource is synthesized rather than read
//!   from an array, and carries no URL: the catalog payload holds no absolute
//!   one. The Explore export endpoint is not synthesized either — it is a
//!   portal-wide API capability rather than per-dataset metadata, and it does
//!   not resolve for the federated `dataset_id@domain` entries that make up all
//!   of `data.opendatasoft.com`. Attachments and alternative exports do carry
//!   real URLs and are read as additional resources.
//! - A Socrata dataset is likewise a single table, described by parallel
//!   `resource.columns_*` arrays rather than a resource array. Its resource is
//!   synthesized too, and unlike OpenDataSoft it does carry a URL: the SODA
//!   endpoint is rebuilt from the payload's own domain and dataset identifier.
//! - ArcGIS Hub and STAC harvest resource detail into shapes this module does
//!   not yet read; see the resource-parity suite in
//!   `ceres-client/tests/resource_parity.rs` for the current baseline.

use serde::Serialize;
use serde_json::Value;

/// A single field (column) within a resource's schema.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ResourceField {
    /// Field/column name.
    pub name: String,
    /// Declared data type, when the portal provides one (e.g. `"text"`, `"numeric"`).
    pub r#type: Option<String>,
    /// Optional human-readable description of the field.
    pub description: Option<String>,
}

/// A single resource (CKAN) or distribution (DCAT) belonging to a dataset.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DatasetResource {
    /// Resource name/title, when available.
    pub name: Option<String>,
    /// File format (e.g. `"CSV"`, `"JSON"`).
    pub format: Option<String>,
    /// MIME / media type (e.g. `"text/csv"`).
    pub media_type: Option<String>,
    /// Direct access URL for the resource.
    pub url: Option<String>,
    /// Optional resource description.
    pub description: Option<String>,
    /// Column-level schema, when the portal exposed it inline. Empty otherwise.
    pub fields: Vec<ResourceField>,
}

/// Normalized resource schema for a dataset.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DatasetSchema {
    /// The resources/distributions that make up the dataset.
    pub resources: Vec<DatasetResource>,
}

impl DatasetSchema {
    /// Derives a [`DatasetSchema`] from a dataset's raw `metadata` JSON.
    ///
    /// Looks for resource/distribution arrays under the keys used by the
    /// supported portal types and normalizes each entry. Always returns a value;
    /// an absent or unrecognized structure yields an empty `resources` vector.
    pub fn from_metadata(metadata: &Value) -> Self {
        // The dataset's own table, where the portal describes it at the dataset
        // level rather than in a resource array, comes before the artifacts
        // hanging off it.
        let mut resources: Vec<DatasetResource> = opendatasoft_table(metadata)
            .or_else(|| socrata_table(metadata))
            .into_iter()
            .collect();

        for key in [
            "resources",
            "distribution",
            "dcat:distribution",
            "distributions",
            "online_resources",
            "attachments",
            "alternative_exports",
        ] {
            if let Some(Value::Array(items)) = metadata.get(key) {
                resources.extend(items.iter().filter_map(extract_resource));
            }
        }

        Self { resources }
    }
}

/// Trims a raw metadata string, treating a blank one as absent.
///
/// Portals ship whitespace-only values in the wild — an ArcGIS Hub item with
/// `"title": "   "` is in this crate's own fixtures. Surfacing one as a
/// populated facet would let a resource satisfy [`is_informative`] while saying
/// nothing, so blanks are dropped at the point they are read and every facet
/// this module emits is non-blank by construction.
fn non_blank(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Reads the first present string value among `keys` from a JSON object.
///
/// Accepts a plain string, or a JSON-LD language object `{"@value": "..."}`,
/// returning the first match that is not blank, trimmed.
fn first_str(obj: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        match obj.get(*key) {
            Some(Value::String(s)) => {
                if let Some(value) = non_blank(s) {
                    return Some(value);
                }
            }
            Some(Value::Object(o)) => {
                if let Some(value) = o.get("@value").and_then(Value::as_str).and_then(non_blank) {
                    return Some(value);
                }
            }
            _ => {}
        }
    }
    None
}

/// Reads the first present value among `keys`, additionally accepting a JSON-LD
/// node reference `{"@id": "..."}`.
///
/// DCAT-AP producers overwhelmingly express `dcat:downloadURL`, `dcat:accessURL`,
/// and `dct:format` as references to a URI rather than as literals, so a
/// `@value`-only reader silently drops them.
fn first_ref(obj: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(value) = first_str(obj, &[key]) {
            return Some(value);
        }
        if let Some(id) = obj
            .get(*key)
            .and_then(|v| v.get("@id"))
            .and_then(Value::as_str)
            .and_then(non_blank)
        {
            return Some(id);
        }
    }
    None
}

/// Reduces a controlled-vocabulary format URI to its final segment.
///
/// DCAT-AP portals cite formats as authority URIs (for example
/// `http://publications.europa.eu/resource/authority/file-type/CSV`); consumers
/// want `CSV`. Plain format literals pass through untouched.
fn shorten_format(format: String) -> String {
    if !format.contains("://") {
        return format;
    }
    format
        .trim_end_matches('/')
        .rsplit('/')
        .find(|segment| !segment.is_empty())
        .map(str::to_string)
        .unwrap_or(format)
}

/// Splits an ISO 19139 `CI_OnlineResource/protocol` into a format and a media
/// type, returning `(format, media_type)`.
///
/// The field is overloaded in practice. Across the harvested index it holds MIME
/// types (`image/png`, `text/xml`), OGC service identifiers (`OGC:WFS`,
/// `OGC Web Map Service`), and pure access methods (`WWW:LINK-1.0-http--link`,
/// `WWW:DOWNLOAD-1.0-http--download`). Only the first two describe the resource;
/// access methods say how to fetch it, not what it is, so they are dropped rather
/// than polluting the format distribution reported for the index.
fn split_protocol(protocol: &str) -> (Option<String>, Option<String>) {
    let protocol = protocol.trim();
    let lowered = protocol.to_ascii_lowercase();

    // A URI is never a media type — its scheme separator would otherwise read as
    // a MIME slash. It may still name a service, though: the values that reach
    // here are free text lifted from XML, and in practice a URI-valued protocol
    // that mentions a service (an OGC `serviceType` URI, a GetCapabilities
    // endpoint) genuinely identifies that service, so the check below still runs.
    let is_uri = lowered.contains("://");

    // MIME type, possibly with parameters (`image/png; mode=24bit`).
    if !is_uri && protocol.contains('/') {
        let media_type = protocol
            .split(';')
            .next()
            .unwrap_or(protocol)
            .trim()
            .to_string();
        return (None, Some(media_type));
    }

    // OGC service identifier, spelled either `OGC:WFS` or `OGC Web Feature Service`.
    for service in ["wmts", "wms", "wfs", "wcs", "csw", "sos"] {
        let spelled_out = match service {
            "wms" => "web map service",
            "wfs" => "web feature service",
            "wcs" => "web coverage service",
            "wmts" => "web map tile service",
            "csw" => "catalogue service",
            _ => "sensor observation service",
        };
        if lowered.contains(service) || lowered.contains(spelled_out) {
            return (Some(service.to_ascii_uppercase()), None);
        }
    }

    (None, None)
}

/// Whether a normalized resource carries any usable detail.
///
/// A resource that names nothing, points nowhere, declares no format or media
/// type, and lists no columns tells a consumer nothing at all, and counting it
/// would overstate the index's resource depth for every consumer downstream:
/// the per-portal signals in the snapshot report, the `resources` list in the
/// Parquet export, and the `/datasets/{id}/schema` contract.
///
/// A facet that was blank in the payload does not count as present — [`non_blank`]
/// has already dropped it — so this predicate never mistakes `"   "` for a name.
///
/// A **description alone does not qualify**. Prose says neither what the
/// distribution is nor where to get it, which is the whole job of a normalized
/// resource, and the dataset already carries a description of its own. Nothing
/// is lost by the omission: the raw node stays verbatim in the dataset's
/// `metadata`, and this module only ever derives a view of it on read. Any
/// other facet alongside a description does make the node a resource, and the
/// description then rides along with it.
fn is_informative(resource: &DatasetResource) -> bool {
    resource.name.is_some()
        || resource.format.is_some()
        || resource.media_type.is_some()
        || resource.url.is_some()
        || !resource.fields.is_empty()
}

/// Normalizes a single resource/distribution node.
///
/// Returns `None` if the node is not a JSON object, or if what could be read
/// from it does not clear [`is_informative`] — no facet at all, a description
/// on its own, or values that were blank once trimmed.
fn extract_resource(node: &Value) -> Option<DatasetResource> {
    if !node.is_object() {
        return None;
    }

    let (protocol_format, protocol_media_type) = first_str(node, &["protocol"])
        .map(|protocol| split_protocol(&protocol))
        .unwrap_or((None, None));

    Some(DatasetResource {
        name: first_str(node, &["name", "title", "dct:title"]),
        format: first_ref(node, &["format", "dct:format"])
            .map(shorten_format)
            .or(protocol_format),
        media_type: first_ref(
            node,
            &[
                "mimetype",
                "mediaType",
                "mediatype",
                "media_type",
                "dcat:mediaType",
            ],
        )
        .or(protocol_media_type),
        url: first_ref(
            node,
            &[
                "url",
                "downloadURL",
                "accessURL",
                "dcat:downloadURL",
                "dcat:accessURL",
            ],
        ),
        description: first_str(node, &["description", "dct:description"]),
        fields: extract_fields(node),
    })
    .filter(is_informative)
}

/// Synthesizes the single tabular resource an OpenDataSoft dataset represents.
///
/// An ODS dataset is one table, so the Explore catalog entry describes its
/// columns at the *dataset* level in `fields[]` rather than inside a resource
/// array — there is no node for [`extract_resource`] to normalize. Recognized
/// by the catalog-entry signature (a `dataset_id` alongside a `metas` block),
/// which no other supported family emits.
///
/// Returns `None` unless the column schema is actually populated. This is
/// stricter than the shared `is_informative` rule, which a name alone would
/// satisfy: ODS ships `fields: []` for every dataset without records (all but
/// one of `webstat.banque-france.fr`, for instance), and since this resource is
/// synthesized rather than read, its name is the dataset's own title. A resource
/// that only restates the dataset's title proves no resource depth.
///
/// The resource carries no URL. The catalog payload holds no absolute one, and
/// the Explore export endpoint is deliberately not synthesized: it describes
/// what the API can do rather than what the portal published, and it 400s for
/// the federated `dataset_id@domain` entries that make up the whole of the
/// `data.opendatasoft.com` hub. `attachments` and `alternative_exports` are the
/// artifacts that do carry real URLs, and are read as resources in their own
/// right by [`DatasetSchema::from_metadata`].
fn opendatasoft_table(metadata: &Value) -> Option<DatasetResource> {
    let dataset_id = metadata.get("dataset_id").and_then(Value::as_str)?;
    if !metadata.get("metas").is_some_and(Value::is_object) {
        return None;
    }

    let fields: Vec<ResourceField> = metadata
        .get("fields")
        .and_then(Value::as_array)?
        .iter()
        .filter_map(extract_field)
        .collect();
    if fields.is_empty() {
        return None;
    }

    // Blank titles occur in the wild, and `first_str` reads one as absent; the
    // client falls back to the dataset id for the dataset's own title, so the
    // resource does the same.
    let name = metadata
        .pointer("/metas/default")
        .and_then(|metas| first_str(metas, &["title"]))
        .unwrap_or_else(|| dataset_id.to_string());

    Some(DatasetResource {
        name: Some(name),
        format: None,
        media_type: None,
        url: None,
        // The dataset-level description is already carried by the dataset
        // itself; repeating it here would add bytes to every exported row
        // without adding information.
        description: None,
        fields,
    })
}

/// Synthesizes the single tabular resource a Socrata dataset represents.
///
/// The Discovery API describes a dataset's columns as parallel `columns_*`
/// arrays hanging off the `resource` **object**, not as the `resources` array
/// the generic path looks for. Recognized by that object sitting alongside a
/// Discovery envelope (`metadata.domain`), which no other supported family
/// emits.
///
/// Unlike its OpenDataSoft counterpart this resource carries a URL. The SODA
/// endpoint `https://{domain}/resource/{4x4}.json` is the documented, per
/// dataset access path, both halves come from the payload itself, and it is
/// exactly the `resource_url` the client already computes and then discards.
/// `format` and `media_type` describe that URL rather than the dataset, so they
/// are set only when it is.
fn socrata_table(metadata: &Value) -> Option<DatasetResource> {
    let resource = metadata.get("resource").filter(|value| value.is_object())?;
    let domain = metadata
        .pointer("/metadata/domain")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|domain| !domain.is_empty())?;

    let fields = socrata_columns(resource);
    let name = first_str(resource, &["name"]);
    let url = first_str(resource, &["id"])
        .filter(|id| is_four_by_four(id))
        .map(|id| format!("{}://{domain}/resource/{id}.json", socrata_scheme(metadata)));

    Some(DatasetResource {
        name,
        format: url.is_some().then(|| "JSON".to_string()),
        media_type: url.is_some().then(|| "application/json".to_string()),
        url,
        // As for OpenDataSoft: the dataset already carries its description.
        description: None,
        fields,
    })
    // A Discovery envelope with nothing to say about its table is not a
    // resource, by the same rule the generic path applies.
    .filter(is_informative)
}

/// Zips Socrata's parallel `columns_*` arrays into column-level fields.
///
/// The arrays are positional and the API guarantees no alignment between them,
/// so each column reads its own index independently: a short `columns_datatype`
/// leaves the trailing columns untyped rather than dropping them or panicking.
/// A column with neither an API field name nor a display name is skipped.
///
/// `columns_field_name` holds the identifier a SODA query uses
/// (`before_initial_contact`) and `columns_name` its display label
/// (`Before Initial Contact`), so the former becomes the field name and the
/// latter falls back to its description. That matches how CKAN's Frictionless
/// `name`/`title` pair and OpenDataSoft's `name`/`label` pair are read.
fn socrata_columns(resource: &Value) -> Vec<ResourceField> {
    let column = |key: &str, index: usize| -> Option<String> {
        resource
            .get(key)?
            .as_array()?
            .get(index)?
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    };

    let columns = ["columns_field_name", "columns_name"]
        .into_iter()
        .filter_map(|key| resource.get(key).and_then(Value::as_array))
        .map(Vec::len)
        .max()
        .unwrap_or(0);

    (0..columns)
        .filter_map(|index| {
            let label = column("columns_name", index);
            let name = column("columns_field_name", index).or_else(|| label.clone())?;
            Some(ResourceField {
                r#type: column("columns_datatype", index),
                description: column("columns_description", index)
                    .or_else(|| label.filter(|label| *label != name)),
                name,
            })
        })
        .collect()
}

/// Whether an identifier has the Socrata four-by-four shape (`abcd-1234`),
/// which is what the SODA endpoint addresses.
fn is_four_by_four(id: &str) -> bool {
    let Some((head, tail)) = id.split_once('-') else {
        return false;
    };
    [head, tail].iter().all(|part| {
        part.len() == 4
            && part
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit())
    })
}

/// Reads the scheme Socrata itself used, rather than assuming one.
///
/// `link` and `permalink` are absolute URLs on the dataset's own domain.
fn socrata_scheme(metadata: &Value) -> &str {
    ["link", "permalink"]
        .into_iter()
        .filter_map(|key| metadata.get(key).and_then(Value::as_str))
        .find_map(|url| url.split_once("://").map(|(scheme, _)| scheme))
        .filter(|scheme| matches!(*scheme, "http" | "https"))
        .unwrap_or("https")
}

/// Extracts column-level fields from a resource node.
///
/// Supports the Frictionless table-schema shape (`schema.fields`) and a flat
/// DataStore-style `fields` array.
fn extract_fields(node: &Value) -> Vec<ResourceField> {
    // Frictionless: resource["schema"]["fields"]
    if let Some(fields) = node
        .get("schema")
        .and_then(|s| s.get("fields"))
        .and_then(|f| f.as_array())
    {
        return fields.iter().filter_map(extract_field).collect();
    }

    // DataStore-style: resource["fields"]
    if let Some(Value::Array(fields)) = node.get("fields") {
        return fields.iter().filter_map(extract_field).collect();
    }

    Vec::new()
}

/// Normalizes a single field node. Requires a non-empty `name`/`id`.
fn extract_field(node: &Value) -> Option<ResourceField> {
    let name = first_str(node, &["name", "id"])?;
    Some(ResourceField {
        name,
        r#type: first_str(node, &["type", "datastore_type"]),
        description: first_str(node, &["description", "title", "label"]),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ckan_resource_with_frictionless_schema() {
        let metadata = json!({
            "resources": [{
                "name": "Air quality 2024",
                "format": "CSV",
                "mimetype": "text/csv",
                "url": "https://example.org/aq.csv",
                "description": "Hourly readings",
                "schema": {
                    "fields": [
                        {"name": "station", "type": "string", "description": "Station id"},
                        {"name": "pm10", "type": "number"}
                    ]
                }
            }]
        });

        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources.len(), 1);
        let r = &schema.resources[0];
        assert_eq!(r.name.as_deref(), Some("Air quality 2024"));
        assert_eq!(r.format.as_deref(), Some("CSV"));
        assert_eq!(r.media_type.as_deref(), Some("text/csv"));
        assert_eq!(r.url.as_deref(), Some("https://example.org/aq.csv"));
        assert_eq!(r.fields.len(), 2);
        assert_eq!(r.fields[0].name, "station");
        assert_eq!(r.fields[0].r#type.as_deref(), Some("string"));
        assert_eq!(r.fields[0].description.as_deref(), Some("Station id"));
        assert_eq!(r.fields[1].name, "pm10");
        assert_eq!(r.fields[1].description, None);
    }

    #[test]
    fn ckan_datastore_style_fields() {
        let metadata = json!({
            "resources": [{
                "name": "Records",
                "fields": [
                    {"id": "_id", "type": "int"},
                    {"id": "value", "datastore_type": "numeric"}
                ]
            }]
        });

        let schema = DatasetSchema::from_metadata(&metadata);
        let r = &schema.resources[0];
        assert_eq!(r.fields.len(), 2);
        assert_eq!(r.fields[0].name, "_id");
        assert_eq!(r.fields[0].r#type.as_deref(), Some("int"));
        assert_eq!(r.fields[1].name, "value");
        assert_eq!(r.fields[1].r#type.as_deref(), Some("numeric"));
    }

    #[test]
    fn ckan_resource_without_fields() {
        let metadata = json!({
            "resources": [{"name": "doc.pdf", "format": "PDF", "url": "https://example.org/doc.pdf"}]
        });

        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources.len(), 1);
        assert!(schema.resources[0].fields.is_empty());
        assert_eq!(schema.resources[0].format.as_deref(), Some("PDF"));
    }

    #[test]
    fn resources_absent_yields_empty() {
        let metadata = json!({"title": "no resources here"});
        assert!(DatasetSchema::from_metadata(&metadata).resources.is_empty());
    }

    #[test]
    fn null_metadata_yields_empty() {
        assert!(
            DatasetSchema::from_metadata(&Value::Null)
                .resources
                .is_empty()
        );
    }

    #[test]
    fn dcat_inline_distribution() {
        let metadata = json!({
            "distribution": [{
                "@type": "Distribution",
                "dct:title": {"@value": "GeoJSON export"},
                "dct:format": "GeoJSON",
                "dcat:downloadURL": "https://example.org/data.geojson"
            }]
        });

        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources.len(), 1);
        let r = &schema.resources[0];
        assert_eq!(r.name.as_deref(), Some("GeoJSON export"));
        assert_eq!(r.format.as_deref(), Some("GeoJSON"));
        assert_eq!(r.url.as_deref(), Some("https://example.org/data.geojson"));
        assert!(r.fields.is_empty());
    }

    #[test]
    fn dcat_node_references_resolve_to_urls_and_formats() {
        // The shape DCAT-AP portals actually emit: URLs and formats as JSON-LD
        // node references rather than literals.
        let metadata = json!({
            "distribution": [{
                "@type": "dcat:Distribution",
                "dct:title": {"@language": "en", "@value": "Air quality CSV"},
                "dct:format": {"@id": "http://publications.europa.eu/resource/authority/file-type/CSV"},
                "dcat:mediaType": "text/csv",
                "dcat:downloadURL": {"@id": "https://example.org/files/air-quality.csv"}
            }]
        });

        let schema = DatasetSchema::from_metadata(&metadata);
        let r = &schema.resources[0];
        assert_eq!(r.name.as_deref(), Some("Air quality CSV"));
        assert_eq!(r.format.as_deref(), Some("CSV"));
        assert_eq!(r.media_type.as_deref(), Some("text/csv"));
        assert_eq!(
            r.url.as_deref(),
            Some("https://example.org/files/air-quality.csv")
        );
    }

    #[test]
    fn plain_format_literals_are_not_shortened() {
        let metadata =
            json!({"resources": [{"format": "CSV", "url": "https://example.org/a.csv"}]});
        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources[0].format.as_deref(), Some("CSV"));
        assert_eq!(
            schema.resources[0].url.as_deref(),
            Some("https://example.org/a.csv")
        );
    }

    #[test]
    fn literal_values_win_over_node_references() {
        let metadata = json!({
            "distribution": [{
                "dct:format": "GeoJSON",
                "dcat:downloadURL": {"@id": "https://example.org/data.geojson"}
            }]
        });
        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources[0].format.as_deref(), Some("GeoJSON"));
    }

    #[test]
    fn ogc_online_resources_are_read() {
        let metadata = json!({
            "online_resources": [
                {"url": "https://catalog.test/download/sst.nc",
                 "protocol": "WWW:DOWNLOAD-1.0-http--download", "downloadable": true},
                {"url": "https://catalog.test/wfs", "protocol": "OGC:WFS", "downloadable": true},
                {"url": "https://catalog.test/preview.png", "protocol": "image/png; mode=24bit",
                 "downloadable": false}
            ]
        });

        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources.len(), 3);

        // A pure access method describes how to fetch, not what the resource is.
        assert_eq!(schema.resources[0].format, None);
        assert_eq!(schema.resources[0].media_type, None);
        assert_eq!(
            schema.resources[0].url.as_deref(),
            Some("https://catalog.test/download/sst.nc")
        );

        assert_eq!(schema.resources[1].format.as_deref(), Some("WFS"));
        assert_eq!(schema.resources[2].media_type.as_deref(), Some("image/png"));
        assert_eq!(schema.resources[2].format, None);
    }

    #[test]
    fn spelled_out_ogc_service_protocols_are_recognized() {
        let metadata = json!({
            "online_resources": [
                {"url": "https://catalog.test/wms", "protocol": "OGC Web Map Service"},
                {"url": "https://catalog.test/wfs", "protocol": "OGC Web Feature Service"}
            ]
        });
        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources[0].format.as_deref(), Some("WMS"));
        assert_eq!(schema.resources[1].format.as_deref(), Some("WFS"));
    }

    #[test]
    fn a_url_valued_protocol_is_not_read_as_a_media_type() {
        // Case-insensitively: the scheme separator must not read as a MIME slash
        // whatever the casing.
        for protocol in [
            "https://example.org/spec",
            "HTTP://EXAMPLE.ORG/SPEC",
            "http://",
        ] {
            let metadata = json!({
                "online_resources": [{"url": "https://catalog.test/a", "protocol": protocol}]
            });
            let schema = DatasetSchema::from_metadata(&metadata);
            assert_eq!(
                schema.resources[0].media_type, None,
                "{protocol} should not yield a media type"
            );
            assert_eq!(
                schema.resources[0].format, None,
                "{protocol} names no service"
            );
        }
    }

    #[test]
    fn a_uri_valued_protocol_naming_a_service_still_yields_its_format() {
        // Both shapes occur verbatim in the harvested index: an OGC `serviceType`
        // URI leaked with its surrounding markup, and a GetCapabilities endpoint.
        for protocol in [
            r#"xlink:href="http://www.opengis.net/def/serviceType/ogc/wms">OGC Web Map Service"#,
            "https://example.org/svc?service=WMS&request=GetCapabilities",
        ] {
            let metadata = json!({
                "online_resources": [{"url": "https://catalog.test/a", "protocol": protocol}]
            });
            let schema = DatasetSchema::from_metadata(&metadata);
            assert_eq!(schema.resources[0].format.as_deref(), Some("WMS"));
            assert_eq!(schema.resources[0].media_type, None);
        }
    }

    #[test]
    fn explicit_format_wins_over_the_protocol_fallback() {
        let metadata = json!({
            "online_resources": [{"format": "NetCDF", "protocol": "OGC:WFS", "mimetype": "application/x-netcdf"}]
        });
        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources[0].format.as_deref(), Some("NetCDF"));
        assert_eq!(
            schema.resources[0].media_type.as_deref(),
            Some("application/x-netcdf")
        );
    }

    /// A trimmed Explore v2.1 catalog entry, in the shape the client persists.
    fn ods_entry(fields: Value, extra: Value) -> Value {
        let mut entry = json!({
            "dataset_id": "arbres-remarquables",
            "has_records": true,
            "attachments": [],
            "alternative_exports": [],
            "fields": fields,
            "metas": {"default": {"title": "Arbres remarquables", "records_count": 187}}
        });
        for (key, value) in extra.as_object().unwrap() {
            entry[key] = value.clone();
        }
        entry
    }

    #[test]
    fn opendatasoft_dataset_level_fields_become_one_table_resource() {
        let metadata = ods_entry(
            json!([
                {"name": "essence", "label": "Essence", "description": null, "type": "text"},
                {"name": "geo_point", "label": "Coordonnées géo", "description": null,
                 "type": "geo_point_2d"}
            ]),
            json!({}),
        );

        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources.len(), 1);
        let r = &schema.resources[0];
        assert_eq!(r.name.as_deref(), Some("Arbres remarquables"));
        // The catalog entry carries no absolute URL and the export endpoint is
        // not synthesized; the column schema is what this resource contributes.
        assert_eq!(r.url, None);
        assert_eq!(r.format, None);
        assert_eq!(r.fields.len(), 2);
        assert_eq!(r.fields[0].name, "essence");
        assert_eq!(r.fields[0].r#type.as_deref(), Some("text"));
        // ODS leaves `description` null and puts the human label in `label`.
        assert_eq!(r.fields[0].description.as_deref(), Some("Essence"));
        assert_eq!(r.fields[1].r#type.as_deref(), Some("geo_point_2d"));
    }

    #[test]
    fn opendatasoft_blank_title_falls_back_to_the_dataset_id() {
        for title in [json!("   "), json!(""), Value::Null] {
            let mut metadata = ods_entry(json!([{"name": "col", "type": "text"}]), json!({}));
            metadata["metas"]["default"]["title"] = title.clone();

            let schema = DatasetSchema::from_metadata(&metadata);
            assert_eq!(
                schema.resources[0].name.as_deref(),
                Some("arbres-remarquables"),
                "title {title} should fall back to the dataset id"
            );
        }
    }

    #[test]
    fn opendatasoft_without_records_yields_no_phantom_table() {
        // ODS ships `fields: []` for every dataset that has no records. A
        // resource naming the dataset and nothing else proves no depth.
        let metadata = ods_entry(json!([]), json!({"has_records": false}));
        assert!(DatasetSchema::from_metadata(&metadata).resources.is_empty());
    }

    #[test]
    fn opendatasoft_attachments_and_alternative_exports_are_resources() {
        let metadata = ods_entry(
            json!([{"name": "essence", "label": "Essence", "type": "text"}]),
            json!({
                "attachments": [{
                    "id": "notice_pdf",
                    "title": "Notice.pdf",
                    "mimetype": "application/pdf",
                    "url": "https://opendata.example.fr/api/explore/v2.1/catalog/datasets/arbres-remarquables/attachments/notice_pdf"
                }],
                "alternative_exports": [{
                    "id": "arbres_shp_zip",
                    "title": "arbres.zip",
                    "mimetype": "application/zip",
                    "description": "Shapefile en RGF93",
                    "url": "https://opendata.example.fr/api/explore/v2.1/catalog/datasets/arbres-remarquables/alternative_exports/arbres_shp_zip"
                }]
            }),
        );

        let schema = DatasetSchema::from_metadata(&metadata);
        // The dataset's own table first, then the artifacts hanging off it.
        assert_eq!(schema.resources.len(), 3);
        assert_eq!(
            schema.resources[0].name.as_deref(),
            Some("Arbres remarquables")
        );

        let attachment = &schema.resources[1];
        assert_eq!(attachment.name.as_deref(), Some("Notice.pdf"));
        assert_eq!(attachment.media_type.as_deref(), Some("application/pdf"));
        assert!(
            attachment
                .url
                .as_deref()
                .is_some_and(|url| url.ends_with("/attachments/notice_pdf"))
        );

        let alternative = &schema.resources[2];
        assert_eq!(alternative.name.as_deref(), Some("arbres.zip"));
        assert_eq!(alternative.media_type.as_deref(), Some("application/zip"));
        assert_eq!(
            alternative.description.as_deref(),
            Some("Shapefile en RGF93")
        );
    }

    #[test]
    fn a_dataset_level_fields_array_alone_is_not_an_opendatasoft_table() {
        // The ODS signature is a catalog entry: `dataset_id` plus a `metas`
        // block. A bare `fields` array on some other family's payload must not
        // be read as a table resource.
        for metadata in [
            json!({"fields": [{"name": "col", "type": "text"}]}),
            json!({"dataset_id": "x", "fields": [{"name": "col", "type": "text"}]}),
            json!({"metas": {"default": {}}, "fields": [{"name": "col", "type": "text"}]}),
        ] {
            assert!(
                DatasetSchema::from_metadata(&metadata).resources.is_empty(),
                "{metadata} is not an OpenDataSoft catalog entry"
            );
        }
    }

    /// A trimmed Discovery API result, in the shape the client persists.
    fn socrata_result(resource: Value) -> Value {
        let mut result = json!({
            "resource": {"id": "abcd-1234", "name": "Traffic counts"},
            "classification": {"domain_category": "Transportation"},
            "metadata": {"domain": "data.example.gov"},
            "link": "https://data.example.gov/Transportation/Traffic-Counts/abcd-1234",
            "permalink": "https://data.example.gov/d/abcd-1234"
        });
        for (key, value) in resource.as_object().unwrap() {
            result["resource"][key] = value.clone();
        }
        result
    }

    #[test]
    fn socrata_parallel_columns_zip_into_one_table_resource() {
        let metadata = socrata_result(json!({
            "columns_name": ["Count date", "Before Initial Contact"],
            "columns_field_name": ["count_date", "before_initial_contact"],
            "columns_datatype": ["Calendar date", "Number"],
            "columns_description": ["Date of the count", ""]
        }));

        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources.len(), 1);
        let r = &schema.resources[0];
        assert_eq!(r.name.as_deref(), Some("Traffic counts"));
        assert_eq!(
            r.url.as_deref(),
            Some("https://data.example.gov/resource/abcd-1234.json")
        );
        assert_eq!(r.format.as_deref(), Some("JSON"));
        assert_eq!(r.media_type.as_deref(), Some("application/json"));

        assert_eq!(r.fields.len(), 2);
        // The SODA field name identifies the column; the display label falls
        // back to its description, as for CKAN `title` and ODS `label`.
        assert_eq!(r.fields[0].name, "count_date");
        assert_eq!(r.fields[0].r#type.as_deref(), Some("Calendar date"));
        assert_eq!(
            r.fields[0].description.as_deref(),
            Some("Date of the count")
        );
        assert_eq!(r.fields[1].name, "before_initial_contact");
        assert_eq!(
            r.fields[1].description.as_deref(),
            Some("Before Initial Contact")
        );
    }

    #[test]
    fn socrata_ragged_column_arrays_leave_the_tail_untyped() {
        // The Discovery API guarantees no alignment between the parallel
        // arrays. A short `columns_datatype` must not drop the columns it does
        // not cover, nor panic.
        let metadata = socrata_result(json!({
            "columns_name": ["One", "Two", "Three"],
            "columns_field_name": ["one", "two", "three"],
            "columns_datatype": ["Number"],
            "columns_description": []
        }));

        let schema = DatasetSchema::from_metadata(&metadata);
        let fields = &schema.resources[0].fields;
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].r#type.as_deref(), Some("Number"));
        assert_eq!(fields[1].r#type, None);
        assert_eq!(fields[2].r#type, None);
        assert_eq!(fields[2].name, "three");
    }

    #[test]
    fn socrata_columns_survive_a_missing_field_name_array() {
        // Older Discovery payloads omit `columns_field_name` entirely; the
        // display names then have to carry the schema.
        let metadata = socrata_result(json!({
            "columns_name": ["Count date", "Vehicles"],
            "columns_datatype": ["Calendar date", "Number"]
        }));

        let fields = &DatasetSchema::from_metadata(&metadata).resources[0].fields;
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name, "Count date");
        // The label became the name, so it is not repeated as the description.
        assert_eq!(fields[0].description, None);
        assert_eq!(fields[1].name, "Vehicles");
    }

    #[test]
    fn socrata_dataset_without_columns_still_exposes_its_soda_endpoint() {
        let schema = DatasetSchema::from_metadata(&socrata_result(json!({})));
        let r = &schema.resources[0];
        assert_eq!(r.name.as_deref(), Some("Traffic counts"));
        assert_eq!(
            r.url.as_deref(),
            Some("https://data.example.gov/resource/abcd-1234.json")
        );
        assert!(r.fields.is_empty());
    }

    #[test]
    fn socrata_url_needs_a_four_by_four_identifier() {
        for id in [
            "missing-title",
            "abcd-12345",
            "ABCD-1234",
            "abcd1234",
            "ab-cd",
        ] {
            let mut metadata = socrata_result(json!({}));
            metadata["resource"]["id"] = json!(id);
            let r = &DatasetSchema::from_metadata(&metadata).resources[0];
            assert_eq!(r.url, None, "{id} is not a Socrata four-by-four");
            // format and media type describe the URL, so they go with it.
            assert_eq!(r.format, None, "{id}");
            assert_eq!(r.media_type, None, "{id}");
        }
    }

    #[test]
    fn socrata_uses_the_scheme_the_portal_published() {
        let mut metadata = socrata_result(json!({}));
        metadata["link"] = json!("http://data.example.gov/d/abcd-1234");
        metadata["permalink"] = json!("http://data.example.gov/d/abcd-1234");
        assert_eq!(
            DatasetSchema::from_metadata(&metadata).resources[0]
                .url
                .as_deref(),
            Some("http://data.example.gov/resource/abcd-1234.json")
        );

        // Absent or unusable links fall back to https rather than guessing.
        let mut metadata = socrata_result(json!({}));
        metadata["link"] = json!("/relative/path");
        metadata["permalink"] = Value::Null;
        assert_eq!(
            DatasetSchema::from_metadata(&metadata).resources[0]
                .url
                .as_deref(),
            Some("https://data.example.gov/resource/abcd-1234.json")
        );
    }

    #[test]
    fn socrata_needs_the_discovery_envelope_and_something_to_say() {
        // A bare `resource` object on some other family's payload is not a
        // Socrata result, and an empty one yields no phantom resource.
        for metadata in [
            json!({"resource": {"id": "abcd-1234", "name": "Traffic counts"}}),
            json!({"resource": "not-an-object", "metadata": {"domain": "data.example.gov"}}),
            json!({"resource": {}, "metadata": {"domain": "data.example.gov"}}),
            json!({"resource": {"id": "not-a-4x4"}, "metadata": {"domain": "data.example.gov"}}),
        ] {
            assert!(
                DatasetSchema::from_metadata(&metadata).resources.is_empty(),
                "{metadata} should yield no resource"
            );
        }
    }

    #[test]
    fn unresolvable_reference_nodes_yield_no_resource() {
        // A distribution published as a separate `@graph` node that the DCAT
        // client could not inline stays an opaque reference. It is a JSON
        // object, but no facet can be read from it, so it is not a resource:
        // emitting one would claim resource depth the portal never published.
        let metadata = json!({
            "distribution": [
                {"@id": "https://example.org/dist/1"},
                {"@id": "https://example.org/dist/2", "@type": "dcat:Distribution"}
            ]
        });
        assert!(DatasetSchema::from_metadata(&metadata).resources.is_empty());
    }

    #[test]
    fn blank_facets_are_absent_rather_than_present_and_empty() {
        // Portals ship whitespace-only strings in the wild. A node offering
        // nothing but those has as little to say as an empty one, so it is not
        // a resource — otherwise `resources` would be non-empty while proving
        // no depth, which is exactly what this rule exists to prevent.
        let metadata = json!({"resources": [{"name": "   ", "url": "\t\n", "format": ""}]});
        assert!(DatasetSchema::from_metadata(&metadata).resources.is_empty());

        // Where a real facet survives, the blank ones stay absent rather than
        // becoming empty strings, and the survivor is trimmed.
        let metadata = json!({"resources": [{"name": "  ", "format": "  CSV  "}]});
        let r = &DatasetSchema::from_metadata(&metadata).resources[0];
        assert_eq!(r.name, None);
        assert_eq!(r.format.as_deref(), Some("CSV"));

        // The same for the JSON-LD spellings, literal and node reference alike.
        let metadata = json!({
            "distribution": [{
                "dct:title": {"@value": "  "},
                "dcat:downloadURL": {"@id": "  https://example.org/a.csv  "}
            }]
        });
        let r = &DatasetSchema::from_metadata(&metadata).resources[0];
        assert_eq!(r.name, None);
        assert_eq!(r.url.as_deref(), Some("https://example.org/a.csv"));
    }

    #[test]
    fn a_description_alone_is_not_a_resource() {
        // Prose says nothing about what the distribution is or where to get it,
        // and the raw node is preserved in `metadata` either way.
        let metadata = json!({
            "distribution": [{"dct:description": {"@value": "Available on request"}}]
        });
        assert!(DatasetSchema::from_metadata(&metadata).resources.is_empty());

        // Any other facet alongside it does make the node a resource, and the
        // description then rides along.
        let metadata = json!({
            "distribution": [{
                "dct:description": {"@value": "Available on request"},
                "dct:format": "CSV"
            }]
        });
        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources.len(), 1);
        assert_eq!(
            schema.resources[0].description.as_deref(),
            Some("Available on request")
        );
    }

    #[test]
    fn an_empty_field_list_alone_is_not_a_resource() {
        // `fields: []` is the shape a portal emits for a table whose schema it
        // does not know; it is no more informative than an absent one.
        let metadata = json!({"resources": [{"fields": []}, {"schema": {"fields": []}}]});
        assert!(DatasetSchema::from_metadata(&metadata).resources.is_empty());
    }

    #[test]
    fn non_object_resource_entries_skipped() {
        let metadata = json!({"resources": ["just-a-string", 42, {"name": "ok"}]});
        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources.len(), 1);
        assert_eq!(schema.resources[0].name.as_deref(), Some("ok"));
    }
}
