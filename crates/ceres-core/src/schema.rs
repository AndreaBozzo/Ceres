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
//!   reference is preserved verbatim.
//! - An unresolved `{"@id": "..."}` reference is still a JSON object, so it
//!   currently normalizes into a [`DatasetResource`] with every facet `None`.
//!   A non-empty `resources` therefore does not by itself imply usable resource
//!   depth — check that at least one facet is populated. Tracked in #207.
//! - Socrata, OpenDataSoft, ArcGIS Hub, STAC, and OGC CSW harvest resource
//!   detail into shapes this module does not yet read; see the resource-parity
//!   suite in `ceres-client/tests/resource_parity.rs` for the current baseline.

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
        let mut resources = Vec::new();

        for key in [
            "resources",
            "distribution",
            "dcat:distribution",
            "distributions",
        ] {
            if let Some(Value::Array(items)) = metadata.get(key) {
                resources.extend(items.iter().filter_map(extract_resource));
            }
        }

        Self { resources }
    }
}

/// Reads the first present string value among `keys` from a JSON object.
///
/// Accepts a plain string, or a JSON-LD language object `{"@value": "..."}`,
/// returning the first non-empty match.
fn first_str(obj: &Value, keys: &[&str]) -> Option<String> {
    for key in keys {
        match obj.get(*key) {
            Some(Value::String(s)) if !s.is_empty() => return Some(s.clone()),
            Some(Value::Object(o)) => {
                if let Some(s) = o
                    .get("@value")
                    .and_then(|v| v.as_str())
                    .filter(|s| !s.is_empty())
                {
                    return Some(s.to_string());
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
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
        {
            return Some(id.to_string());
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

/// Normalizes a single resource/distribution node, returning `None` if it is not
/// a JSON object.
fn extract_resource(node: &Value) -> Option<DatasetResource> {
    if !node.is_object() {
        return None;
    }

    Some(DatasetResource {
        name: first_str(node, &["name", "title", "dct:title"]),
        format: first_ref(node, &["format", "dct:format"]).map(shorten_format),
        media_type: first_ref(
            node,
            &[
                "mimetype",
                "mediaType",
                "mediatype",
                "media_type",
                "dcat:mediaType",
            ],
        ),
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
    fn non_object_resource_entries_skipped() {
        let metadata = json!({"resources": ["just-a-string", 42, {"name": "ok"}]});
        let schema = DatasetSchema::from_metadata(&metadata);
        assert_eq!(schema.resources.len(), 1);
        assert_eq!(schema.resources[0].name.as_deref(), Some("ok"));
    }
}
