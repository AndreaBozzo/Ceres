//! Cross-client resource-parity suite.
//!
//! [`DatasetSchema::from_metadata`] derives resource/distribution detail *on
//! read* from the `metadata` JSON each client persists. Whether that yields
//! anything therefore depends entirely on the shape each client stores — a
//! property no single-client test can guard.
//!
//! This suite closes that loop end to end: every portal family is driven
//! through its **real** client against a mock server serving a representative
//! catalog payload, and the assertion runs on the `metadata` the client
//! actually persists. A client that stops preserving resource detail fails
//! here even if its own unit tests still pass.
//!
//! # Reading the expectation table
//!
//! [`expectations`] records, per family, what resource detail is reachable
//! **today**. Families whose source payload carries resource detail that
//! [`DatasetSchema`] cannot yet reach are marked [`Expect::Gap`] with the
//! tracking issue. Those rows assert the gap is still *exactly* as described,
//! so closing one without updating this table fails the suite rather than
//! silently drifting — the table is the milestone's ratchet, not a wishlist.

use std::collections::BTreeMap;

use ceres_core::schema::DatasetSchema;
use ceres_core::traits::PortalClient;
use ceres_core::{DatasetResource, NewDataset};
use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use ceres_client::{
    ArcGisClient, CkanClient, DataJsonClient, DcatClient, OgcRecordsClient, OpenDataSoftClient,
    SocrataClient, StacClient,
};

// ---------------------------------------------------------------------------
// Expectation table
// ---------------------------------------------------------------------------

/// What a portal family is expected to yield through `DatasetSchema`.
#[derive(Debug)]
enum Expect {
    /// The family reaches its resource detail. Asserts at least one dataset
    /// exposes a resource whose listed facets are populated.
    Resources {
        /// Facets that must be non-`None` on at least one resource.
        facets: &'static [Facet],
        /// Whether at least one resource must carry column-level fields.
        fields: bool,
    },
    /// The source payload carries resource detail that `DatasetSchema` cannot
    /// reach yet. `where_it_lives` documents the key holding it, so the row
    /// doubles as the specification for closing the gap.
    Gap {
        issue: &'static str,
        where_it_lives: &'static str,
    },
}

/// A normalized resource facet, named so failures point at a concrete field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Facet {
    Name,
    Format,
    MediaType,
    Url,
}

impl Facet {
    fn get(self, resource: &DatasetResource) -> Option<&str> {
        match self {
            Facet::Name => resource.name.as_deref(),
            Facet::Format => resource.format.as_deref(),
            Facet::MediaType => resource.media_type.as_deref(),
            Facet::Url => resource.url.as_deref(),
        }
    }
}

/// The reachability baseline, one row per portal family.
///
/// Measured against the live index at the time of writing: of 2.62M harvested
/// datasets, the `Resources` rows below account for ~1.47M with reachable
/// resources, while the `Gap` rows account for ~27k whose detail is harvested
/// but unreachable.
///
/// A `Resources` row means the family's detail is reachable *where the portal
/// published it*, not that every dataset in the family has some. OpenDataSoft
/// is the clearest case: 58,510 of its 170,144 harvested datasets yield an
/// informative resource, because ODS ships an empty column schema for every
/// dataset that carries no records.
fn expectations() -> BTreeMap<&'static str, Expect> {
    BTreeMap::from([
        (
            "ckan",
            Expect::Resources {
                facets: &[Facet::Name, Facet::Format, Facet::MediaType, Facet::Url],
                fields: true,
            },
        ),
        (
            "project_open_data",
            Expect::Resources {
                facets: &[Facet::Name, Facet::Format, Facet::Url],
                fields: false,
            },
        ),
        (
            "dcat_udata",
            Expect::Resources {
                facets: &[Facet::Name, Facet::Format, Facet::MediaType, Facet::Url],
                fields: false,
            },
        ),
        (
            // One table per dataset, zipped from the parallel `columns_*`
            // arrays, addressed by the SODA endpoint rebuilt from the payload's
            // own domain and four-by-four identifier.
            "socrata",
            Expect::Resources {
                facets: &[Facet::Name, Facet::Format, Facet::MediaType, Facet::Url],
                fields: true,
            },
        ),
        (
            // The dataset's own table is synthesized from the dataset-level
            // `fields[]` and carries no URL — the catalog entry holds no
            // absolute one. `Url` and `MediaType` come from the attachments and
            // alternative exports hanging off it, which do.
            "opendatasoft",
            Expect::Resources {
                facets: &[Facet::Name, Facet::MediaType, Facet::Url],
                fields: true,
            },
        ),
        (
            "arcgis",
            Expect::Gap {
                issue: "#206",
                where_it_lives: "`properties.url` — the service endpoint, with `properties.type` \
                                 as its format",
            },
        ),
        (
            "stac",
            Expect::Gap {
                issue: "#205",
                where_it_lives: "`assets` — a keyed object rather than an array",
            },
        ),
        (
            // The shared CSW fixture's records carry only link and download
            // access protocols, which describe how to fetch a resource rather
            // than what it is, so no format or media type is expected here. The
            // `protocol` split is unit-tested against all three real-world
            // shapes in `ceres_core::schema`.
            "ogc_csw",
            Expect::Resources {
                facets: &[Facet::Url],
                fields: false,
            },
        ),
    ])
}

// ---------------------------------------------------------------------------
// The parity test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn every_client_family_matches_its_documented_resource_reachability() {
    let harvested: BTreeMap<&'static str, Vec<NewDataset>> = BTreeMap::from([
        ("ckan", harvest_ckan().await),
        ("project_open_data", harvest_project_open_data().await),
        ("dcat_udata", harvest_dcat().await),
        ("socrata", harvest_socrata().await),
        ("opendatasoft", harvest_opendatasoft().await),
        ("arcgis", harvest_arcgis().await),
        ("stac", harvest_stac().await),
        ("ogc_csw", harvest_ogc_csw().await),
    ]);

    let expectations = expectations();
    assert_eq!(
        harvested.keys().collect::<Vec<_>>(),
        expectations.keys().collect::<Vec<_>>(),
        "every harvested family needs a row in the expectation table"
    );

    for (family, datasets) in &harvested {
        assert!(
            !datasets.is_empty(),
            "{family}: the mock catalog yielded no datasets, so this row proves nothing"
        );

        let schemas: Vec<DatasetSchema> = datasets
            .iter()
            .map(|dataset| DatasetSchema::from_metadata(&dataset.metadata))
            .collect();
        // Every resource `DatasetSchema` emits is informative by construction:
        // a node from which no facet could be read — a DCAT `{"@id": "..."}`
        // reference that never resolved, say — yields no resource at all (#207).
        // So a non-empty list here means real reachable depth, not phantoms.
        let resources: Vec<&DatasetResource> = schemas
            .iter()
            .flat_map(|schema| schema.resources.iter())
            .collect();

        match &expectations[family] {
            Expect::Resources { facets, fields } => {
                assert!(
                    !resources.is_empty(),
                    "{family}: expected reachable resources but DatasetSchema found none — \
                     the client stopped preserving resource detail in `metadata`"
                );

                for facet in *facets {
                    assert!(
                        resources.iter().any(|r| facet.get(r).is_some()),
                        "{family}: no resource populated {facet:?}, though the source payload \
                         provides it"
                    );
                }

                if *fields {
                    assert!(
                        resources.iter().any(|r| !r.fields.is_empty()),
                        "{family}: no resource carried column-level fields"
                    );
                }
            }
            Expect::Gap {
                issue,
                where_it_lives,
            } => {
                assert!(
                    resources.is_empty(),
                    "{family}: resources are now reachable ({} found) — the {issue} gap is \
                     closed, so move this row to Expect::Resources. Detail lived in: \
                     {where_it_lives}",
                    resources.len()
                );

                assert!(
                    datasets
                        .iter()
                        .any(|dataset| carries_unreachable_detail(family, &dataset.metadata)),
                    "{family}: the mock payload no longer carries the unreachable resource \
                     detail this row describes ({where_it_lives}), so the gap assertion is \
                     vacuous"
                );
            }
        }
    }
}

/// Opt-in check that `@graph` distribution resolution holds against a live
/// DCAT-AP portal, not just the fixture:
///
/// ```text
/// cargo test -p ceres-client --test resource_parity dcat_resource_smoke -- --ignored --exact
/// ```
///
/// Override the portal with `CERES_DCAT_SMOKE_URL`.
#[tokio::test]
#[ignore = "requires network access to a public DCAT-AP portal"]
async fn dcat_resource_smoke() {
    let portal_url = std::env::var("CERES_DCAT_SMOKE_URL")
        .unwrap_or_else(|_| "https://data.public.lu".to_string());

    let client = DcatClient::new(&portal_url, "en").unwrap();
    let datasets = normalize(client.search_all_datasets().await.unwrap(), |record| {
        DcatClient::into_new_dataset(record, &portal_url, None, "en")
    });
    assert!(!datasets.is_empty(), "{portal_url} returned no datasets");

    let with_resources = datasets
        .iter()
        .filter(|dataset| {
            !DatasetSchema::from_metadata(&dataset.metadata)
                .resources
                .is_empty()
        })
        .count();

    eprintln!(
        "{portal_url}: {with_resources}/{} datasets expose informative resources",
        datasets.len()
    );
    assert!(
        with_resources > 0,
        "{portal_url}: no dataset exposed an informative resource, so `@graph` \
         distribution resolution is not reaching real portal payloads"
    );
}

/// Confirms a `Gap` family really does hold resource detail in `metadata`, so
/// the "still empty" assertion above cannot pass vacuously against a payload
/// that simply has no resources to find.
fn carries_unreachable_detail(family: &str, metadata: &Value) -> bool {
    match family {
        "dcat_udata" => metadata.get("distribution").is_some_and(|distribution| {
            !distribution.is_array() || distribution.as_array().is_some_and(|d| !d.is_empty())
        }),
        "arcgis" => metadata.pointer("/properties/url").is_some(),
        "stac" => metadata
            .get("assets")
            .and_then(Value::as_object)
            .is_some_and(|assets| !assets.is_empty()),
        "ogc_csw" => metadata
            .get("online_resources")
            .and_then(Value::as_array)
            .is_some_and(|resources| !resources.is_empty()),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Per-family harvests
//
// Each helper serves the crate's existing catalog fixture over a mock server
// and returns what the real client would persist. Endpoints and pagination
// terminators mirror the per-client tests in `src/`.
// ---------------------------------------------------------------------------

async fn harvest_ckan() -> Vec<NewDataset> {
    const FIXTURE: &[u8] = include_bytes!("fixtures/ckan_package_search.json");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/3/action/package_search"))
        .respond_with(json_body(FIXTURE))
        .mount(&server)
        .await;

    let portal_url = server.uri();
    let client = CkanClient::new(&server.uri()).unwrap();
    normalize(client.search_all_datasets().await.unwrap(), |record| {
        CkanClient::into_new_dataset(record, &portal_url, None, "en")
    })
}

async fn harvest_project_open_data() -> Vec<NewDataset> {
    const FIXTURE: &[u8] = include_bytes!("fixtures/project_open_data.json");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data.json"))
        .respond_with(json_body(FIXTURE))
        .mount(&server)
        .await;

    let portal_url = server.uri();
    let client = DataJsonClient::new(&format!("{}/data.json", server.uri()), "en").unwrap();
    normalize(client.search_all_datasets().await.unwrap(), |record| {
        DataJsonClient::into_new_dataset(record, &portal_url, None, "en")
    })
}

async fn harvest_dcat() -> Vec<NewDataset> {
    const FIXTURE: &[u8] = include_bytes!("fixtures/dcat_udata_distributions.jsonld");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/1/site/catalog.jsonld"))
        .respond_with(json_body(FIXTURE))
        .mount(&server)
        .await;

    let portal_url = server.uri();
    let client = DcatClient::new(&server.uri(), "en").unwrap();
    normalize(client.search_all_datasets().await.unwrap(), |record| {
        DcatClient::into_new_dataset(record, &portal_url, None, "en")
    })
}

async fn harvest_socrata() -> Vec<NewDataset> {
    const FIXTURE: &[u8] = include_bytes!("fixtures/socrata_catalog.json");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/catalog/v1"))
        .respond_with(json_body(FIXTURE))
        .mount(&server)
        .await;

    let portal_url = server.uri();
    let client = SocrataClient::new(&server.uri()).unwrap();
    normalize(client.search_all_datasets().await.unwrap(), |record| {
        SocrataClient::into_new_dataset(record, &portal_url, None, "en")
    })
}

async fn harvest_opendatasoft() -> Vec<NewDataset> {
    const FIXTURE: &[u8] = include_bytes!("fixtures/opendatasoft_catalog.json");

    let server = MockServer::start().await;
    // The full-catalog walk sweeps dated datasets first, then the datasets
    // without a `modified` timestamp; only the dated sweep serves the fixture.
    Mock::given(method("GET"))
        .and(path("/api/explore/v2.1/catalog/datasets"))
        .and(query_param("where", "modified is null"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "total_count": 0,
            "results": [],
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/explore/v2.1/catalog/datasets"))
        .respond_with(json_body(FIXTURE))
        .mount(&server)
        .await;

    let portal_url = server.uri();
    let client = OpenDataSoftClient::new(&server.uri()).unwrap();
    normalize(client.search_all_datasets().await.unwrap(), |record| {
        OpenDataSoftClient::into_new_dataset(record, &portal_url, None, "en")
    })
}

async fn harvest_arcgis() -> Vec<NewDataset> {
    const FIXTURE: &[u8] = include_bytes!("fixtures/arcgis_items.json");

    let server = MockServer::start().await;
    // Site-scope validation reads the site root before the catalog API; a page
    // without an injected catalog scope is treated as validly scoped.
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html></html>"))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/search/v1/collections/dataset/items"))
        .respond_with(json_body(FIXTURE))
        .mount(&server)
        .await;

    let portal_url = server.uri();
    let client = ArcGisClient::new(&server.uri()).unwrap();
    normalize(client.search_all_datasets().await.unwrap(), |record| {
        ArcGisClient::into_new_dataset(record, &portal_url, None, "en")
    })
}

async fn harvest_stac() -> Vec<NewDataset> {
    const FIXTURE: &[u8] = include_bytes!("fixtures/stac_collections_1_0.json");

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "stac_version": "1.0.0",
            "type": "Catalog",
            "id": "test-catalog",
            "description": "Parity fixture catalog",
            "conformsTo": ["https://api.stacspec.org/v1.0.0/collections"],
            "links": [{
                "rel": "data",
                "href": format!("{}/collections", server.uri()),
                "type": "application/json",
            }],
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/collections"))
        .respond_with(json_body(FIXTURE))
        .mount(&server)
        .await;

    let portal_url = server.uri();
    let client = StacClient::new(&server.uri()).unwrap();
    normalize(client.search_all_datasets().await.unwrap(), |record| {
        StacClient::into_new_dataset(record, &portal_url, None, "en")
    })
}

async fn harvest_ogc_csw() -> Vec<NewDataset> {
    const RECORDS: &str = include_str!("fixtures/csw_get_records.xml");

    let server = MockServer::start().await;
    // GetCapabilities advertises the operation endpoints, so its hrefs have to
    // point back at the mock server and cannot live in a static fixture.
    let capabilities = format!(
        r#"<ows:Capabilities xmlns:ows="http://www.opengis.net/ows" xmlns:xlink="http://www.w3.org/1999/xlink">
  <ows:OperationsMetadata>
    <ows:Operation name="GetRecords">
      <ows:DCP><ows:HTTP><ows:Get xlink:href="{base}/csw"/></ows:HTTP></ows:DCP>
    </ows:Operation>
    <ows:Operation name="GetRecordById">
      <ows:DCP><ows:HTTP><ows:Get xlink:href="{base}/csw"/></ows:HTTP></ows:DCP>
    </ows:Operation>
  </ows:OperationsMetadata>
</ows:Capabilities>"#,
        base = server.uri()
    );
    Mock::given(method("GET"))
        .and(path("/csw"))
        .and(query_param("request", "GetCapabilities"))
        .respond_with(xml_body(&capabilities))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/csw"))
        .and(query_param("request", "GetRecords"))
        .respond_with(xml_body(RECORDS))
        .mount(&server)
        .await;

    let endpoint = format!("{}/csw", server.uri());
    let portal_url = server.uri();
    let client = OgcRecordsClient::new(&server.uri(), "en", Some(&endpoint)).unwrap();
    normalize(client.search_all_datasets().await.unwrap(), |record| {
        OgcRecordsClient::into_new_dataset(record, &portal_url, None, "en")
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn json_body(fixture: &[u8]) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("content-type", "application/json")
        .set_body_bytes(fixture.to_vec())
}

fn xml_body(fixture: &str) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("content-type", "application/xml")
        .set_body_string(fixture.to_string())
}

/// Runs each harvested record through the client's own `into_new_dataset`, so
/// the assertions see exactly the `metadata` a harvest would persist.
///
/// Takes a closure because the DCAT and Project Open Data clients expose
/// `into_new_dataset` as an inherent method rather than through [`PortalClient`].
fn normalize<T>(data: Vec<T>, into_new_dataset: impl Fn(T) -> NewDataset) -> Vec<NewDataset> {
    data.into_iter().map(into_new_dataset).collect()
}
