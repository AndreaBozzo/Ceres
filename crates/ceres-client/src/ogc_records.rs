//! OGC catalogue records client using the CSW 2.0.2 discovery protocol.

use std::{collections::HashSet, sync::Arc, time::Duration};

use ceres_core::{AppError, CatalogRecordKind, NewDataset, traits::PortalClient};
use chrono::{DateTime, Utc};
use futures::{StreamExt, stream::BoxStream};
use reqwest::{Client, Url};
use roxmltree::{Document, Node};
use serde_json::{Value, json};
use tokio::{sync::OnceCell, time::sleep};

/// Delay before each narrowing retry, growing with the attempt.
///
/// Narrowing multiplies requests exactly when a service is least able to serve
/// them: a failing window costs four. Without a pause, a struggling backend gets
/// four times the load and the client's own retries deepen the problem — which
/// is what happened to `geocatalogue.fr`, whose Elasticsearch backend began
/// failing under an hour of unbroken 100-record ISO windows.
const NARROW_BACKOFF: &[Duration] = &[
    Duration::from_secs(0),
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(10),
];

/// Consecutive skipped records after which the catalogue is judged unwell.
///
/// One unreadable record is a record. Several in a row, at different offsets,
/// is the service — and stepping through a failing catalogue one record at a
/// time would issue four requests each for as long as it kept failing. Stopping
/// reports a truncated harvest, which is honest and actionable, instead of
/// hours of futile pressure on someone else's server.
const MAX_CONSECUTIVE_SKIPS: usize = 3;

/// Per-request budget for the largest window, scaled down as windows narrow.
const WINDOW_TIMEOUT: Duration = Duration::from_secs(120);

/// Window sizes tried in turn, largest first; the first is the normal page size.
///
/// A catalogue can serve almost everything and still crash on one record:
/// `geocatalogue.fr` answers every 100-record window until `startPosition=3101`,
/// where GeoNetwork returns HTTP 200 carrying a `NullPointerException`, while the
/// same offset at 10 records succeeds. Narrowing isolates the poisoned record
/// instead of abandoning the 168,000 that follow it.
const PAGE_SIZE_LADDER: &[usize] = &[100, 25, 5, 1];
const MAX_PAGES: usize = 100_000;
const MAX_RESPONSE_BYTES: usize = 32 * 1024 * 1024;
/// An `ExceptionReport` is a few hundred bytes. The error path gets its own,
/// far smaller budget than a record page: it exists to read a reason, not a
/// catalog, so it should never be a route to a large allocation.
const MAX_EXCEPTION_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone)]
struct CswBindings {
    get_records: Url,
    get_record_by_id: Url,
}

/// The record profile a catalogue answers `GetRecords` in.
///
/// CSW 2.0.2 makes `csw:Record` (Dublin Core) mandatory and the ISO 19115
/// `gmd:MD_Metadata` profile an optional extension. Ceres asks for ISO first
/// because it is far richer, and falls back per catalogue when the service
/// rejects it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CswProfile {
    Iso,
    DublinCore,
}

impl CswProfile {
    /// The profile to try when this one cannot render a record.
    ///
    /// Dublin Core is mandatory in CSW 2.0.2, so it renders anything a
    /// catalogue holds — at less depth than ISO, but present rather than lost.
    fn alternate(self) -> Self {
        match self {
            Self::Iso => Self::DublinCore,
            Self::DublinCore => Self::Iso,
        }
    }

    /// The `typeNames` / `namespace` / `outputSchema` triple this profile needs.
    fn query(self) -> [(&'static str, String); 3] {
        match self {
            Self::Iso => [
                ("typeNames", "gmd:MD_Metadata".into()),
                (
                    "namespace",
                    "xmlns(gmd=http://www.isotc211.org/2005/gmd)".into(),
                ),
                ("outputSchema", "http://www.isotc211.org/2005/gmd".into()),
            ],
            Self::DublinCore => [
                ("typeNames", "csw:Record".into()),
                (
                    "namespace",
                    "xmlns(csw=http://www.opengis.net/cat/csw/2.0.2)".into(),
                ),
                (
                    "outputSchema",
                    "http://www.opengis.net/cat/csw/2.0.2".into(),
                ),
            ],
        }
    }
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
    profile: Arc<OnceCell<CswProfile>>,
    /// Delay before each narrowing retry. A field rather than a constant so the
    /// tests can exercise the narrowing logic without waiting out the real
    /// backoff; production always uses [`NARROW_BACKOFF`].
    backoff: &'static [Duration],
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
            profile: Arc::new(OnceCell::new()),
            backoff: NARROW_BACKOFF,
        })
    }

    async fn bounded_get(
        &self,
        mut url: Url,
        params: &[(&str, String)],
        timeout: Duration,
    ) -> Result<String, AppError> {
        url.query_pairs_mut()
            .extend_pairs(params.iter().map(|(k, v)| (*k, v.as_str())));
        let response = self
            .client
            .get(url.clone())
            .timeout(timeout)
            .send()
            .await
            .map_err(|error| AppError::ClientError(error.to_string()))?;
        // OWS signals a rejected parameter with HTTP 400 carrying an
        // `ExceptionReport` body, so a failing status is read rather than
        // discarded: the body is the only place the reason is stated, and the
        // profile fallback depends on seeing it. Italy's RNDT is the live case.
        let status = response.status();
        if !status.is_success() {
            let detail = read_exception_report(response).await;
            return Err(AppError::ClientError(match detail {
                Some(detail) => format!("HTTP {status} from {url}: {detail}"),
                None => format!("HTTP {status} from {url}"),
            }));
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
                        WINDOW_TIMEOUT,
                    )
                    .await?;
                parse_capabilities(&xml, &self.endpoint)
            })
            .await
    }

    /// Builds a client that does not wait between narrowing retries.
    ///
    /// The waiting is what protects a struggling service, not what makes the
    /// narrowing correct, so the tests skip it and assert on the narrowing.
    #[cfg(test)]
    fn without_backoff(mut self) -> Self {
        self.backoff = &[];
        self
    }

    /// Fetches one window of `size` records in `profile`.
    async fn page_in(
        &self,
        start: usize,
        size: usize,
        profile: CswProfile,
    ) -> Result<Page, AppError> {
        let endpoint = self.bindings().await?.get_records.clone();
        let mut params = vec![
            ("service", "CSW".to_string()),
            ("version", "2.0.2".to_string()),
            ("request", "GetRecords".to_string()),
            ("resultType", "results".to_string()),
            ("elementSetName", "full".to_string()),
            ("startPosition", start.to_string()),
            ("maxRecords", size.to_string()),
        ];
        params.extend(profile.query());
        // A one-record window has no business taking as long as a hundred, and
        // waiting the full budget on each narrowing step is most of why a
        // failing offset is slow to give up on.
        let timeout = window_timeout(size);
        let xml = self.bounded_get(endpoint, &params, timeout).await?;
        parse_get_records(&xml, self.base_url.as_str(), &self.language)
    }

    /// Fetches a page, resolving the catalogue's record profile on first use.
    ///
    /// The profile cannot be read from `GetCapabilities`: Italy's RNDT
    /// advertises the ISO profile under `outputSchema` and then rejects it, and
    /// GeoNetwork deployments may advertise no `ows:Parameter` for it at all. So
    /// the ISO request is made and the rejection is what selects Dublin Core.
    /// The choice is memoized, so the extra round trip happens at most once per
    /// client.
    async fn page(&self, start: usize, size: usize) -> Result<Page, AppError> {
        if let Some(profile) = self.profile.get() {
            return self.page_in(start, size, *profile).await;
        }
        match self.page_in(start, size, CswProfile::Iso).await {
            Ok(page) => {
                let _ = self.profile.set(CswProfile::Iso);
                Ok(page)
            }
            Err(error) if is_profile_rejection(&error) => {
                tracing::info!(
                    portal = self.base_url.as_str(),
                    %error,
                    "Catalogue rejected the ISO profile; retrying as Dublin Core"
                );
                let page = self.page_in(start, size, CswProfile::DublinCore).await?;
                let _ = self.profile.set(CswProfile::DublinCore);
                Ok(page)
            }
            Err(error) => Err(error),
        }
    }

    /// Fetches the window at `start`, narrowing on failure.
    ///
    /// Returns the page and the window size that produced it, so the caller can
    /// tell a full window from a narrowed one. `Ok(None)` means even a
    /// single-record window failed: that one record is unreadable and is skipped
    /// rather than ending the catalogue.
    async fn page_resiliently(&self, start: usize) -> Result<Option<(Page, usize)>, AppError> {
        let mut last: Option<AppError> = None;
        for (attempt, &size) in PAGE_SIZE_LADDER.iter().enumerate() {
            if let Some(delay) = self.backoff.get(attempt).copied()
                && !delay.is_zero()
            {
                sleep(delay).await;
            }
            match self.page(start, size).await {
                Ok(page) => {
                    if attempt > 0 {
                        tracing::info!(
                            portal = self.base_url.as_str(),
                            start,
                            size,
                            "Recovered a failing CSW window at a reduced page size"
                        );
                    }
                    return Ok(Some((page, size)));
                }
                // Before the profile is settled, a rejection describes the
                // catalogue: narrowing cannot help, and the error has to reach
                // `page()` so the Dublin Core fallback can act on it.
                //
                // Afterwards the same exception means something else entirely.
                // Once pages have been read in a profile, the catalogue clearly
                // supports it, so a later `OutputSchema` rejection is about one
                // record that cannot be rendered in it — nationaalgeoregister.nl
                // serves 2,600 ISO records and then one ISO 19110 feature
                // catalogue. Treating that as catalogue-wide cost the remaining
                // 7,000 records.
                Err(error) if is_profile_rejection(&error) && self.profile.get().is_none() => {
                    return Err(error);
                }
                // The profile is settled, so the catalogue supports it — this
                // window simply holds records it cannot render. A mixed
                // catalogue is ordinary: nationaalgeoregister.nl serves ISO
                // 19139 throughout with ISO 19110 feature catalogues among them.
                //
                // Those records are not unreadable, only unreadable *here*, so
                // read the window in the mandatory profile instead of narrowing
                // toward a skip. Narrowing cannot help when the offending
                // records are clustered — each smaller window at this offset
                // named a different one — and skipping would discard records the
                // catalogue is willing to serve.
                //
                // The settled profile is left alone: the rest of the catalogue
                // is still richer in it.
                Err(error) if is_profile_rejection(&error) => {
                    let alternate = self
                        .profile
                        .get()
                        .copied()
                        .unwrap_or(CswProfile::Iso)
                        .alternate();
                    match self.page_in(start, size, alternate).await {
                        Ok(page) => {
                            tracing::info!(
                                portal = self.base_url.as_str(),
                                start,
                                size,
                                profile = ?alternate,
                                "Window holds records the catalogue's profile cannot render; \
                                 read it in the mandatory profile instead"
                            );
                            return Ok(Some((page, size)));
                        }
                        Err(fallback_error) => {
                            tracing::warn!(
                                portal = self.base_url.as_str(),
                                start,
                                size,
                                %fallback_error,
                                "Neither profile could read this window; narrowing"
                            );
                            last = Some(error);
                        }
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        portal = self.base_url.as_str(),
                        start,
                        size,
                        %error,
                        "CSW window failed; narrowing"
                    );
                    last = Some(error);
                }
            }
        }
        tracing::warn!(
            portal = self.base_url.as_str(),
            start,
            error = %last.map(|e| e.to_string()).unwrap_or_default(),
            "Skipping a single unreadable CSW record"
        );
        Ok(None)
    }

    pub fn paginate_stream(&self) -> BoxStream<'_, Result<Vec<OgcRecord>, AppError>> {
        struct State {
            start: usize,
            pages: usize,
            seen: HashSet<usize>,
            skipped: usize,
            consecutive_skips: usize,
            done: bool,
        }
        Box::pin(futures::stream::unfold(
            (
                self.clone(),
                State {
                    start: 1,
                    pages: 0,
                    seen: HashSet::new(),
                    skipped: 0,
                    consecutive_skips: 0,
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
                match client.page_resiliently(state.start).await {
                    Ok(Some((page, size))) => {
                        state.consecutive_skips = 0;
                        // The walk is the only place that knows the catalogue is
                        // finished *and* how much of it was unreadable, so the
                        // tally is reported here rather than lost in the state.
                        if page.records.is_empty() && state.skipped > 0 {
                            tracing::warn!(
                                portal = client.base_url.as_str(),
                                skipped = state.skipped,
                                "CSW catalogue finished with unreadable records skipped"
                            );
                        }
                        // `nextRecord` is advisory and several services return 0
                        // or a stale value once a window has been narrowed, so
                        // the walk trusts the window it actually asked for and
                        // stops on the record count instead.
                        let next = state.start + size.min(page.records.len().max(1));
                        if page.records.is_empty()
                            || (page.matched > 0 && next > page.matched)
                            || (page.next_record == 0 && page.records.len() < size)
                        {
                            state.done = true;
                        } else {
                            state.start = next;
                        }
                        Some((Ok(page.records), (client, state)))
                    }
                    // One unreadable record: step over it and keep going, rather
                    // than losing every record after it. Several in a row means
                    // the service is failing, not the records, and stepping
                    // through it one at a time would cost four requests each for
                    // as long as it stayed down.
                    Ok(None) => {
                        state.skipped += 1;
                        state.consecutive_skips += 1;
                        state.start += 1;
                        if state.consecutive_skips >= MAX_CONSECUTIVE_SKIPS {
                            state.done = true;
                            return Some((
                                Err(AppError::ClientError(format!(
                                    concat!(
                                        "CSW catalogue failed {} windows in a row ",
                                        "around record {} in both profiles; stopping ",
                                        "rather than stepping through it one record at a time",
                                    ),
                                    state.consecutive_skips, state.start
                                ))),
                                (client, state),
                            ));
                        }
                        Some((Ok(Vec::new()), (client, state)))
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
        // Reuses whatever profile pagination settled on; an unread client has
        // not been told otherwise, so it asks for the richer ISO profile.
        let profile = self.profile.get().copied().unwrap_or(CswProfile::Iso);
        let [_, _, output_schema] = profile.query();
        let xml = self
            .bounded_get(
                endpoint,
                &[
                    ("service", "CSW".into()),
                    ("version", "2.0.2".into()),
                    ("request", "GetRecordById".into()),
                    ("elementSetName", "full".into()),
                    output_schema,
                    ("id", id.into()),
                ],
                WINDOW_TIMEOUT,
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
    // Dublin Core has no `CI_OnlineResource`. It carries its links in one of
    // two shapes, and both occur among the catalogues Ceres harvests, so both
    // are read:
    //
    // * `dct:references scheme="WMS"` — Italy's RNDT;
    // * `dc:URI protocol="OGC:WMS" name="…" description="…"` — GeoNetwork's own
    //   output, which is richer and is what Eurac publishes.
    //
    // Either way the service identifier lands in the slot ISO calls `protocol`,
    // the same slot `ceres_core::schema::split_protocol` reads a format or a
    // media type out of.
    let dublin_core_resources: Vec<Value> = node
        .descendants()
        .filter(|n| matches!(local(*n), "references" | "URI"))
        .filter_map(|n| {
            let url = node_value(n)?;
            let protocol = n
                .attribute("scheme")
                .or_else(|| n.attribute("protocol"))
                .map(str::to_owned);
            let downloadable = protocol.as_deref().is_some_and(|protocol| {
                let protocol = protocol.to_ascii_lowercase();
                protocol.contains("download") || protocol.contains("wfs")
            });
            Some(json!({
                "url": url,
                "name": n.attribute("name"),
                "description": n.attribute("description"),
                "protocol": protocol,
                "function": Value::Null,
                "downloadable": downloadable,
            }))
        })
        .collect();
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
        .chain(dublin_core_resources)
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
    let spatial_bbox = node
        .descendants()
        .filter(|candidate| local(*candidate) == "EX_GeographicBoundingBox")
        .find_map(|bbox| {
            let number = |name: &str| {
                bbox.descendants()
                    .find(|candidate| local(*candidate) == name)
                    .and_then(node_value)
                    .and_then(|value| value.parse::<f64>().ok())
            };
            match (
                number("westBoundLongitude"),
                number("southBoundLatitude"),
                number("eastBoundLongitude"),
                number("northBoundLatitude"),
            ) {
                (Some(west), Some(south), Some(east), Some(north)) => {
                    Some(json!([west, south, east, north]))
                }
                _ => None,
            }
        })
        // Dublin Core states the extent as an `ows:BoundingBox` with two
        // space-separated corners instead of four named bounds.
        .or_else(|| {
            node.descendants()
                .find(|candidate| local(*candidate) == "BoundingBox")
                .and_then(|bbox| {
                    let corner = |name: &str| -> Option<(f64, f64)> {
                        let text = bbox
                            .descendants()
                            .find(|candidate| local(*candidate) == name)
                            .and_then(node_value)?;
                        let mut parts = text
                            .split_whitespace()
                            .filter_map(|value| value.parse::<f64>().ok());
                        Some((parts.next()?, parts.next()?))
                    };
                    let (west, south) = corner("LowerCorner")?;
                    let (east, north) = corner("UpperCorner")?;
                    Some(json!([west, south, east, north]))
                })
        });
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
/// Scales the request budget to the window size.
///
/// The full budget is for a full window. Narrowed retries get proportionally
/// less — floored at 15 seconds, which is generous for a handful of records —
/// so giving up on a failing offset takes a bounded amount of time instead of
/// four full timeouts.
fn window_timeout(size: usize) -> Duration {
    let largest = PAGE_SIZE_LADDER[0].max(1);
    let scaled = WINDOW_TIMEOUT.mul_f64(size as f64 / largest as f64);
    scaled.max(Duration::from_secs(15))
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
/// Reads an `ExceptionReport` body from a failing response, within a hard cap.
///
/// OWS signals a rejected parameter with HTTP 400 carrying the reason in the
/// body, and the profile fallback depends on seeing it — a discarded body means
/// the client only knows *that* the request failed, never *why*. Italy's RNDT is
/// the live case.
///
/// The body is read in chunks and abandoned the moment it passes
/// [`MAX_EXCEPTION_BYTES`], so a hostile or misbehaving endpoint cannot make an
/// error path allocate without limit — `content_length` is consulted first but
/// is not trusted on its own, since it can be absent or wrong. An exception
/// report is a few hundred bytes; anything approaching the cap is not one, and
/// the caller still reports the status.
async fn read_exception_report(mut response: reqwest::Response) -> Option<String> {
    if response
        .content_length()
        .is_some_and(|n| n > MAX_EXCEPTION_BYTES as u64)
    {
        return None;
    }
    let mut body = Vec::new();
    while let Ok(Some(chunk)) = response.chunk().await {
        if body.len() + chunk.len() > MAX_EXCEPTION_BYTES {
            return None;
        }
        body.extend_from_slice(&chunk);
    }
    let body = String::from_utf8(body).ok()?;
    let document = Document::parse(&body).ok()?;
    document
        .descendants()
        .any(|node| local(node) == "ExceptionReport")
        .then(|| csw_exception(&document).to_string())
}

/// Whether a CSW exception says the request's record profile is unsupported.
///
/// Two shapes occur in the wild and both have to be read:
///
/// * the conformant one, where `exceptionCode="InvalidParameterValue"` and
///   `locator` names `typeNames` or `outputSchema` — Italy's RNDT;
/// * GeoNetwork's, which reports `NoApplicableCode` and buries the real code in
///   the exception text as a stringified `InvalidParameterValueEx`.
///
/// Matching the text alone would be enough for both, but the attributes are the
/// standard's own signal and are checked first.
fn is_profile_rejection(error: &AppError) -> bool {
    let message = error.to_string();
    let lower = message.to_ascii_lowercase();
    lower.contains("invalidparametervalue")
        && (lower.contains("typenames") || lower.contains("outputschema"))
}

fn csw_exception(doc: &Document<'_>) -> AppError {
    let exception = doc.descendants().find(|n| local(*n) == "Exception");
    let attribute = |name: &str| {
        exception
            .and_then(|node| node.attribute(name))
            .unwrap_or_default()
    };
    let text = doc
        .descendants()
        .find(|n| local(*n) == "ExceptionText")
        .and_then(node_value)
        .unwrap_or_else(|| "CSW exception response".into());
    // The code and locator are carried into the message, not dropped: they are
    // what `is_profile_rejection` reads on a conformant service, and they are
    // absent from the text on exactly those services.
    let code = attribute("exceptionCode");
    let locator = attribute("locator");
    if code.is_empty() && locator.is_empty() {
        return AppError::ClientError(text);
    }
    AppError::ClientError(format!("{code} (locator={locator}): {text}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = include_str!("../tests/fixtures/csw_get_records.xml");
    const DUBLIN_CORE: &str = include_str!("../tests/fixtures/csw_dublin_core.xml");
    const DUBLIN_CORE_GEONETWORK: &str =
        include_str!("../tests/fixtures/csw_dublin_core_geonetwork.xml");

    /// GeoNetwork publishes links as `dc:URI protocol="OGC:WMS"` rather than
    /// RNDT's `dct:references scheme="WMS"`. Reading only the latter left 362 of
    /// Eurac's 363 harvested records with no reachable resource at all.
    #[test]
    fn geonetwork_dublin_core_uris_become_online_resources() {
        use ceres_core::schema::DatasetSchema;

        let page = parse_get_records(DUBLIN_CORE_GEONETWORK, "https://edp-portal.eurac.edu", "en")
            .unwrap();
        let record = &page.records[0];
        assert_eq!(record.title, "World Land Cover Himalayas 2015");

        let resources = record.metadata["online_resources"].as_array().unwrap();
        assert!(!resources.is_empty());
        let wms = resources
            .iter()
            .find(|r| r["protocol"] == "OGC:WMS")
            .expect("the WMS service");
        assert_eq!(wms["url"], "https://maps.eurac.edu/geoserver/ows");
        // `name` and `description` are attributes here, unlike RNDT's shape,
        // and they are what gives the normalized resource a usable label.
        assert_eq!(wms["name"], "geonode:World_Land_Cover_Himalayas_2015_v1");

        let normalized = DatasetSchema::from_metadata(
            &OgcRecordsClient::into_new_dataset(
                page.records.into_iter().next().unwrap(),
                "https://edp-portal.eurac.edu",
                None,
                "en",
            )
            .metadata,
        )
        .resources;
        assert!(!normalized.is_empty());
        assert!(
            normalized
                .iter()
                .any(|r| r.format.as_deref() == Some("WMS")),
            "{normalized:?}"
        );
        // `protocol="image/png"` is a media type, not a service.
        assert!(
            normalized
                .iter()
                .any(|r| r.media_type.as_deref() == Some("image/png")),
            "{normalized:?}"
        );
        assert!(normalized.iter().any(|r| r.name.is_some()));
    }

    // -----------------------------------------------------------------------
    // Dublin Core profile (#242)
    // -----------------------------------------------------------------------

    #[test]
    fn parses_dublin_core_records() {
        let page = parse_get_records(DUBLIN_CORE, "https://geodati.gov.it", "it").unwrap();
        assert_eq!(page.matched, 23663);
        assert_eq!(page.records.len(), 3);

        let record = &page.records[0];
        assert_eq!(record.identifier, "age:D_E973_MARSAGLIA");
        assert_eq!(record.title, "Cartografia catastale - Comune di MARSAGLIA");
        assert_eq!(record.description.as_deref(), Some("Cartografia catastale"));
        assert_eq!(record.record_kind, CatalogRecordKind::Dataset);
        assert_eq!(
            record.modified.unwrap().date_naive().to_string(),
            "2026-04-25"
        );
        assert!(
            record.metadata["keywords"]
                .as_array()
                .unwrap()
                .iter()
                .any(|k| k == "Parcelle catastali")
        );
    }

    #[test]
    fn dublin_core_references_become_online_resources() {
        let page = parse_get_records(DUBLIN_CORE, "https://geodati.gov.it", "it").unwrap();
        let resources = page.records[0].metadata["online_resources"]
            .as_array()
            .unwrap();
        // Dublin Core has no CI_OnlineResource; without reading dct:references
        // every record in this profile would carry no resource at all.
        assert!(!resources.is_empty());
        let wms = resources
            .iter()
            .find(|r| r["protocol"] == "WMS")
            .expect("the WMS reference");
        assert!(
            wms["url"]
                .as_str()
                .unwrap()
                .starts_with("https://wms.cartografia.agenziaentrate.gov.it/")
        );
        // A WFS reference is a download; a WMS one is not.
        assert_eq!(wms["downloadable"], false);
        assert_eq!(
            resources
                .iter()
                .find(|r| r["protocol"] == "WFS")
                .map(|r| r["downloadable"].clone()),
            Some(serde_json::Value::Bool(true))
        );
    }

    #[test]
    fn dublin_core_records_reach_normalized_resources() {
        use ceres_core::schema::DatasetSchema;

        let page = parse_get_records(DUBLIN_CORE, "https://geodati.gov.it", "it").unwrap();
        let dataset = OgcRecordsClient::into_new_dataset(
            page.records.into_iter().next().unwrap(),
            "https://geodati.gov.it",
            None,
            "it",
        );
        let resources = DatasetSchema::from_metadata(&dataset.metadata).resources;
        assert_eq!(resources.len(), 5);
        // `scheme` lands in the slot `split_protocol` reads a service type from,
        // so an OGC reference is typed rather than left a bare URL.
        let formats: Vec<Option<&str>> = resources.iter().map(|r| r.format.as_deref()).collect();
        assert!(formats.contains(&Some("WMS")), "{resources:?}");
        assert!(formats.contains(&Some("WFS")), "{resources:?}");
        // The `alternate.*` references are the catalogue's own metadata views.
        // They carry no service type, so they stay untyped rather than being
        // given an invented one — a URL is still resource depth this profile
        // would otherwise not have at all.
        assert_eq!(formats.iter().filter(|f| f.is_none()).count(), 3);
        assert!(resources.iter().all(|r| r.url.is_some()));
    }

    #[test]
    fn dublin_core_bounding_box_is_read() {
        let page = parse_get_records(DUBLIN_CORE, "https://geodati.gov.it", "it").unwrap();
        let bbox = &page.records[0].metadata["spatial"]["bbox"];
        assert_eq!(bbox[0], 7.9496964);
        assert_eq!(bbox[3], 44.475662);
    }

    #[test]
    fn a_profile_rejection_is_recognized_in_both_shapes() {
        // Conformant: RNDT states the code and locator as attributes.
        let rndt = r#"<ExceptionReport xmlns="http://www.opengis.net/ows">
            <Exception exceptionCode="InvalidParameterValue" locator="typeNames">
              <ExceptionText>CSW: The typeNames parameter must be csw:Record</ExceptionText>
            </Exception>
          </ExceptionReport>"#;
        let error = parse_get_records(rndt, "https://geodati.gov.it", "it").unwrap_err();
        assert!(is_profile_rejection(&error), "{error}");

        // GeoNetwork reports NoApplicableCode and buries the real code in the text.
        let geonetwork = r#"<ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows">
            <ows:Exception exceptionCode="NoApplicableCode">
              <ows:ExceptionText>java.lang.RuntimeException: InvalidParameterValueEx: code=InvalidParameterValue, locator=OutputSchema, message=OutputSchema 'gmd' not supported</ows:ExceptionText>
            </ows:Exception>
          </ows:ExceptionReport>"#;
        let error =
            parse_get_records(geonetwork, "https://edp-portal.eurac.edu", "en").unwrap_err();
        assert!(is_profile_rejection(&error), "{error}");
    }

    /// The whole point of the fallback: a catalogue that rejects ISO still
    /// harvests, and the retry happens once rather than on every page.
    #[tokio::test]
    async fn a_catalogue_rejecting_iso_harvests_as_dublin_core() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path, query_param},
        };

        let server = MockServer::start().await;
        let capabilities = format!(
            r#"<ows:Capabilities xmlns:ows="http://www.opengis.net/ows" xmlns:xlink="http://www.w3.org/1999/xlink">
  <ows:OperationsMetadata>
    <ows:Operation name="GetRecords">
      <ows:DCP><ows:HTTP><ows:Get xlink:href="{base}/csw"/></ows:HTTP></ows:DCP>
    </ows:Operation>
  </ows:OperationsMetadata>
</ows:Capabilities>"#,
            base = server.uri()
        );
        let xml = |body: &str| {
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/xml")
                .set_body_string(body.to_string())
        };

        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("request", "GetCapabilities"))
            .respond_with(xml(&capabilities))
            .mount(&server)
            .await;
        // Mirrors RNDT: the ISO profile is advertised, then refused.
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("typeNames", "gmd:MD_Metadata"))
            .respond_with(xml(
                r#"<ExceptionReport xmlns="http://www.opengis.net/ows">
                     <Exception exceptionCode="InvalidParameterValue" locator="typeNames">
                       <ExceptionText>CSW: The typeNames parameter must be csw:Record</ExceptionText>
                     </Exception>
                   </ExceptionReport>"#,
            ))
            // Exactly once: the profile is memoized, not re-probed per page.
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("typeNames", "csw:Record"))
            .respond_with(xml(DUBLIN_CORE))
            .mount(&server)
            .await;

        let endpoint = format!("{}/csw", server.uri());
        let client = OgcRecordsClient::new(&server.uri(), "it", Some(&endpoint))
            .unwrap()
            .without_backoff();
        let page = client.page(1, PAGE_SIZE_LADDER[0]).await.unwrap();
        assert_eq!(page.records.len(), 3);
        assert_eq!(client.profile.get(), Some(&CswProfile::DublinCore));

        // A second page reuses the resolved profile, so the ISO mock's
        // `.expect(1)` holds when the server is verified on drop.
        let again = client.page(4, PAGE_SIZE_LADDER[0]).await.unwrap();
        assert_eq!(again.records.len(), 3);
    }

    /// The mock above answers 200; RNDT answers **400** with the same body.
    /// Discarding a failing status without reading it loses the only statement
    /// of why, and the fallback never fires — which is exactly what the live
    /// smoke caught.
    #[tokio::test]
    async fn a_rejection_carried_by_an_error_status_is_still_read() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path, query_param},
        };

        let server = MockServer::start().await;
        let xml = |code: u16, body: &str| {
            ResponseTemplate::new(code)
                .insert_header("content-type", "application/xml")
                .set_body_string(body.to_string())
        };
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("request", "GetCapabilities"))
            .respond_with(xml(
                200,
                &format!(
                    r#"<ows:Capabilities xmlns:ows="http://www.opengis.net/ows" xmlns:xlink="http://www.w3.org/1999/xlink">
  <ows:OperationsMetadata><ows:Operation name="GetRecords">
    <ows:DCP><ows:HTTP><ows:Get xlink:href="{base}/csw"/></ows:HTTP></ows:DCP>
  </ows:Operation></ows:OperationsMetadata>
</ows:Capabilities>"#,
                    base = server.uri()
                ),
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("typeNames", "gmd:MD_Metadata"))
            .respond_with(xml(
                400,
                r#"<ExceptionReport xmlns="http://www.opengis.net/ows">
                     <Exception exceptionCode="InvalidParameterValue" locator="typeNames">
                       <ExceptionText>CSW: The typeNames parameter must be csw:Record</ExceptionText>
                     </Exception>
                   </ExceptionReport>"#,
            ))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("typeNames", "csw:Record"))
            .respond_with(xml(200, DUBLIN_CORE))
            .mount(&server)
            .await;

        let endpoint = format!("{}/csw", server.uri());
        let client = OgcRecordsClient::new(&server.uri(), "it", Some(&endpoint))
            .unwrap()
            .without_backoff();
        let page = client.page(1, PAGE_SIZE_LADDER[0]).await.unwrap();
        assert_eq!(page.records.len(), 3);
        assert_eq!(client.profile.get(), Some(&CswProfile::DublinCore));
    }

    #[tokio::test]
    async fn an_error_status_without_an_exception_body_still_reports_the_status() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/csw"))
            .respond_with(ResponseTemplate::new(503).set_body_string("<html>down</html>"))
            .mount(&server)
            .await;

        let endpoint = format!("{}/csw", server.uri());
        let client = OgcRecordsClient::new(&server.uri(), "en", Some(&endpoint))
            .unwrap()
            .without_backoff();
        let error = client.page(1, PAGE_SIZE_LADDER[0]).await.unwrap_err();
        assert!(error.to_string().contains("503"), "{error}");
        // An outage must not be mistaken for a profile problem.
        assert!(!is_profile_rejection(&error), "{error}");
    }

    #[tokio::test]
    async fn an_oversized_error_body_is_abandoned_rather_than_buffered() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        };

        // A valid ExceptionReport padded past the cap. Reading it would mean an
        // error path is a route to an unbounded allocation, so it is dropped and
        // only the status survives — the fallback then does not fire, which is
        // the safe direction.
        let padding = "x".repeat(MAX_EXCEPTION_BYTES);
        let oversized = format!(
            r#"<ExceptionReport xmlns="http://www.opengis.net/ows">
                 <Exception exceptionCode="InvalidParameterValue" locator="typeNames">
                   <ExceptionText>must be csw:Record {padding}</ExceptionText>
                 </Exception>
               </ExceptionReport>"#
        );
        assert!(oversized.len() > MAX_EXCEPTION_BYTES);

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/csw"))
            .respond_with(
                ResponseTemplate::new(400)
                    .insert_header("content-type", "application/xml")
                    .set_body_string(oversized),
            )
            .mount(&server)
            .await;

        let endpoint = format!("{}/csw", server.uri());
        let client = OgcRecordsClient::new(&server.uri(), "en", Some(&endpoint))
            .unwrap()
            .without_backoff();
        let error = client.page(1, PAGE_SIZE_LADDER[0]).await.unwrap_err();
        assert!(error.to_string().contains("400"), "{error}");
        assert!(
            !error.to_string().contains("csw:Record"),
            "the oversized body was buffered: {error}"
        );
        assert!(!is_profile_rejection(&error), "{error}");
    }

    // -----------------------------------------------------------------------
    // Page resilience (#247)
    // -----------------------------------------------------------------------

    /// Builds a GetRecords response carrying `n` minimal ISO records.
    fn iso_page(start: usize, n: usize, matched: usize) -> String {
        let records: String = (0..n)
            .map(|i| {
                format!(
                    concat!(
                        r#"<gmd:MD_Metadata xmlns:gmd="http://www.isotc211.org/2005/gmd" "#,
                        r#"xmlns:gco="http://www.isotc211.org/2005/gco">"#,
                        "<gmd:fileIdentifier><gco:CharacterString>record-{id}",
                        "</gco:CharacterString></gmd:fileIdentifier>",
                        "<gmd:identificationInfo><gmd:MD_DataIdentification><gmd:citation>",
                        "<gmd:CI_Citation><gmd:title><gco:CharacterString>Record {id}",
                        "</gco:CharacterString></gmd:title></gmd:CI_Citation></gmd:citation>",
                        "</gmd:MD_DataIdentification></gmd:identificationInfo></gmd:MD_Metadata>",
                    ),
                    id = start + i
                )
            })
            .collect();
        format!(
            concat!(
                r#"<csw:GetRecordsResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2">"#,
                r#"<csw:SearchResults numberOfRecordsMatched="{matched}" "#,
                r#"numberOfRecordsReturned="{n}" nextRecord="{next}">{records}"#,
                "</csw:SearchResults></csw:GetRecordsResponse>",
            ),
            matched = matched,
            n = n,
            records = records,
            next = start + n
        )
    }

    /// Builds a GetRecords response carrying `n` minimal Dublin Core records.
    fn dublin_core_page(start: usize, n: usize, matched: usize) -> String {
        let records: String = (0..n)
            .map(|i| {
                format!(
                    concat!(
                        r#"<csw:Record xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" "#,
                        r#"xmlns:dc="http://purl.org/dc/elements/1.1/">"#,
                        "<dc:identifier>dc-{id}</dc:identifier>",
                        "<dc:title>Feature catalogue {id}</dc:title>",
                        "<dc:type>dataset</dc:type></csw:Record>",
                    ),
                    id = start + i
                )
            })
            .collect();
        format!(
            concat!(
                r#"<csw:GetRecordsResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2">"#,
                r#"<csw:SearchResults numberOfRecordsMatched="{matched}" "#,
                r#"numberOfRecordsReturned="{n}" nextRecord="{next}">{records}"#,
                "</csw:SearchResults></csw:GetRecordsResponse>",
            ),
            matched = matched,
            n = n,
            records = records,
            next = start + n
        )
    }

    fn capabilities_for(base: &str) -> String {
        format!(
            concat!(
                r#"<ows:Capabilities xmlns:ows="http://www.opengis.net/ows" "#,
                r#"xmlns:xlink="http://www.w3.org/1999/xlink"><ows:OperationsMetadata>"#,
                r#"<ows:Operation name="GetRecords"><ows:DCP><ows:HTTP>"#,
                r#"<ows:Get xlink:href="{base}/csw"/></ows:HTTP></ows:DCP></ows:Operation>"#,
                "</ows:OperationsMetadata></ows:Capabilities>",
            ),
            base = base
        )
    }

    const NPE: &str = concat!(
        r#"<ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows">"#,
        r#"<ows:Exception exceptionCode="NoApplicableCode"><ows:ExceptionText>"#,
        "java.lang.RuntimeException: java.lang.NullPointerException",
        "</ows:ExceptionText></ows:Exception></ows:ExceptionReport>",
    );

    /// A catalogue that crashes on one record must not cost every record after
    /// it. `geocatalogue.fr` lost 167,997 of its 171,098 records to exactly this.
    #[tokio::test]
    async fn a_poisoned_window_narrows_instead_of_ending_the_catalogue() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path, query_param},
        };

        let server = MockServer::start().await;
        let xml = |body: String| {
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/xml")
                .set_body_string(body)
        };
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("request", "GetCapabilities"))
            .respond_with(xml(capabilities_for(&server.uri())))
            .mount(&server)
            .await;

        // Records 1-100 and 106-110 serve fine. The window at 101 fails at
        // widths 100 and 25 and succeeds at 5 — geocatalogue.fr's shape.
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "1"))
            .respond_with(xml(iso_page(1, 100, 110)))
            .mount(&server)
            .await;
        for size in ["100", "25"] {
            Mock::given(method("GET"))
                .and(path("/csw"))
                .and(query_param("startPosition", "101"))
                .and(query_param("maxRecords", size))
                .respond_with(xml(NPE.to_string()))
                .mount(&server)
                .await;
        }
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "101"))
            .and(query_param("maxRecords", "5"))
            .respond_with(xml(iso_page(101, 5, 110)))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "106"))
            .respond_with(xml(iso_page(106, 5, 110)))
            .mount(&server)
            .await;

        let endpoint = format!("{}/csw", server.uri());
        let client = OgcRecordsClient::new(&server.uri(), "en", Some(&endpoint))
            .unwrap()
            .without_backoff();
        let records = client.search_all_datasets().await.unwrap();
        assert_eq!(
            records.len(),
            110,
            "narrowing should recover the whole catalogue"
        );
        assert_eq!(records[0].identifier, "record-1");
        assert_eq!(records[109].identifier, "record-110");
    }

    /// When even a one-record window fails, that record is unreadable. Skipping
    /// it costs one record; ending the walk costs every record after it.
    #[tokio::test]
    async fn a_single_unreadable_record_is_skipped_not_fatal() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path, query_param},
        };

        let server = MockServer::start().await;
        let xml = |body: String| {
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/xml")
                .set_body_string(body)
        };
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("request", "GetCapabilities"))
            .respond_with(xml(capabilities_for(&server.uri())))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "1"))
            .respond_with(xml(iso_page(1, 1, 3)))
            .mount(&server)
            .await;
        // Record 2 is unreadable at every width.
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "2"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "3"))
            .respond_with(xml(iso_page(3, 1, 3)))
            .mount(&server)
            .await;

        let endpoint = format!("{}/csw", server.uri());
        let client = OgcRecordsClient::new(&server.uri(), "en", Some(&endpoint))
            .unwrap()
            .without_backoff();
        let records = client.search_all_datasets().await.unwrap();
        let ids: Vec<&str> = records.iter().map(|r| r.identifier.as_str()).collect();
        assert_eq!(ids, ["record-1", "record-3"]);
    }

    /// A catalogue may hold records its own preferred profile cannot render.
    /// `nationaalgeoregister.nl` serves ISO 19139 throughout with ISO 19110
    /// feature catalogues among them; narrowing named a different unrenderable
    /// record at every window size, because they are clustered rather than
    /// isolated. Dublin Core reads them, so the window is re-read rather than
    /// skipped — the records exist and the catalogue will serve them.
    #[tokio::test]
    async fn a_window_the_profile_cannot_render_is_read_in_the_other_profile() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path, query_param},
        };

        let server = MockServer::start().await;
        let xml = |body: String| {
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/xml")
                .set_body_string(body)
        };
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("request", "GetCapabilities"))
            .respond_with(xml(capabilities_for(&server.uri())))
            .mount(&server)
            .await;

        // The first window settles the profile as ISO.
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "1"))
            .and(query_param("typeNames", "gmd:MD_Metadata"))
            .respond_with(xml(iso_page(1, 1, 3)))
            .mount(&server)
            .await;
        // The next window holds ISO 19110 records: ISO cannot render it at any
        // size, and each size names a different record, as the live catalogue does.
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "2"))
            .and(query_param("typeNames", "gmd:MD_Metadata"))
            .respond_with(xml(concat!(
                r#"<ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows">"#,
                r#"<ows:Exception exceptionCode="NoApplicableCode"><ows:ExceptionText>"#,
                "InvalidParameterValueEx: code=InvalidParameterValue, locator=OutputSchema, ",
                "message=OutputSchema 'gmd' not supported for metadata with '70439986' (iso19110).",
                "</ows:ExceptionText></ows:Exception></ows:ExceptionReport>",
            )
            .to_string()))
            .mount(&server)
            .await;
        // Dublin Core serves the same window without complaint.
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "2"))
            .and(query_param("typeNames", "csw:Record"))
            .respond_with(xml(dublin_core_page(2, 2, 3)))
            .mount(&server)
            .await;

        let endpoint = format!("{}/csw", server.uri());
        let client = OgcRecordsClient::new(&server.uri(), "en", Some(&endpoint))
            .unwrap()
            .without_backoff();
        let records = client.search_all_datasets().await.unwrap();
        let ids: Vec<&str> = records.iter().map(|r| r.identifier.as_str()).collect();
        assert_eq!(
            ids,
            ["record-1", "dc-2", "dc-3"],
            "the unrenderable window should be read in Dublin Core, not skipped"
        );
        // The rest of the catalogue is still richer in ISO, so the settled
        // profile is not switched by one awkward window.
        assert_eq!(client.profile.get(), Some(&CswProfile::Iso));
    }

    /// A `\` continuation inside a string literal has silently produced a
    /// message with 37 spaces in the middle of it once already, and the only
    /// place that showed was a harvest log.
    #[test]
    fn diagnostic_messages_do_not_carry_source_indentation() {
        let flat = format!(
            concat!(
                "CSW catalogue failed {} windows in a row ",
                "around record {} in both profiles; stopping ",
                "rather than stepping through it one record at a time",
            ),
            3, 4826
        );
        assert!(!flat.contains("  "), "{flat}");
    }

    /// One unreadable record is a record; several in a row is the service.
    ///
    /// `geocatalogue.fr` began returning `ElasticsearchStatusException` for
    /// nearly every window after an hour of harvesting. Skipping forward one
    /// record at a time would have issued four requests each, indefinitely,
    /// against a backend already failing — so the walk stops and reports a
    /// truncated harvest instead.
    #[tokio::test]
    async fn a_failing_service_stops_the_walk_rather_than_being_stepped_through() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path, query_param},
        };

        let server = MockServer::start().await;
        let xml = |body: String| {
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/xml")
                .set_body_string(body)
        };
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("request", "GetCapabilities"))
            .respond_with(xml(capabilities_for(&server.uri())))
            .mount(&server)
            .await;
        // The first window works; everything after it fails, as a degraded
        // backend does.
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "1"))
            .respond_with(xml(iso_page(1, 1, 10_000)))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("request", "GetRecords"))
            .respond_with(xml(concat!(
                r#"<ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows">"#,
                r#"<ows:Exception exceptionCode="NoApplicableCode"><ows:ExceptionText>"#,
                "java.lang.RuntimeException: ElasticsearchStatusException",
                "</ows:ExceptionText></ows:Exception></ows:ExceptionReport>",
            )
            .to_string()))
            .mount(&server)
            .await;

        let endpoint = format!("{}/csw", server.uri());
        let client = OgcRecordsClient::new(&server.uri(), "en", Some(&endpoint))
            .unwrap()
            .without_backoff();

        let mut stream = client.paginate_stream();
        let mut pages = 0usize;
        let mut ended_with_error = false;
        while let Some(page) = stream.next().await {
            pages += 1;
            if page.is_err() {
                ended_with_error = true;
                break;
            }
            // A degraded service must not be walked forever.
            assert!(pages < 20, "the walk kept going through a failing service");
        }
        assert!(
            ended_with_error,
            "a failing service should end the walk as an error, so the harvest \
             reports truncated rather than complete"
        );
        // Bounded: one good window, then at most MAX_CONSECUTIVE_SKIPS skips.
        assert!(pages <= 1 + MAX_CONSECUTIVE_SKIPS, "pages = {pages}");
    }

    /// The same exception means two different things depending on when it
    /// arrives. Once pages have been read in a profile, the catalogue plainly
    /// supports it, so a later `OutputSchema` rejection is about one record —
    /// `nationaalgeoregister.nl` serves 2,600 ISO records and then a single ISO
    /// 19110 feature catalogue. Treating that as catalogue-wide cost the
    /// remaining 7,000 records.
    #[tokio::test]
    async fn a_rejection_after_the_profile_is_settled_is_one_record_not_the_catalogue() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path, query_param},
        };

        let server = MockServer::start().await;
        let xml = |body: String| {
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/xml")
                .set_body_string(body)
        };
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("request", "GetCapabilities"))
            .respond_with(xml(capabilities_for(&server.uri())))
            .mount(&server)
            .await;

        // Record 1 settles the profile as ISO.
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "1"))
            .respond_with(xml(iso_page(1, 1, 3)))
            .mount(&server)
            .await;
        // Record 2 is an ISO 19110 feature catalogue: it cannot be rendered as
        // gmd at any window size, and the message names the record.
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "2"))
            .respond_with(xml(concat!(
                r#"<ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows">"#,
                r#"<ows:Exception exceptionCode="NoApplicableCode"><ows:ExceptionText>"#,
                "org.fao.geonet.csw.common.exceptions.InvalidParameterValueEx: ",
                "code=InvalidParameterValue, locator=OutputSchema, message=OutputSchema ",
                "'gmd' not supported for metadata with '70502292' (iso19110).",
                "</ows:ExceptionText></ows:Exception></ows:ExceptionReport>",
            )
            .to_string()))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("startPosition", "3"))
            .respond_with(xml(iso_page(3, 1, 3)))
            .mount(&server)
            .await;

        let endpoint = format!("{}/csw", server.uri());
        let client = OgcRecordsClient::new(&server.uri(), "en", Some(&endpoint))
            .unwrap()
            .without_backoff();
        let records = client.search_all_datasets().await.unwrap();
        let ids: Vec<&str> = records.iter().map(|r| r.identifier.as_str()).collect();
        assert_eq!(
            ids,
            ["record-1", "record-3"],
            "the unrenderable record should be skipped, not end the catalogue"
        );
        assert_eq!(client.profile.get(), Some(&CswProfile::Iso));
    }

    /// Narrowing is for windows. A rejected profile is a property of the whole
    /// catalogue, so retrying it three times smaller wastes requests and buries
    /// the reason.
    #[tokio::test]
    async fn a_profile_rejection_is_not_retried_at_a_smaller_page_size() {
        use wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path, query_param},
        };

        let server = MockServer::start().await;
        let xml = |code: u16, body: String| {
            ResponseTemplate::new(code)
                .insert_header("content-type", "application/xml")
                .set_body_string(body)
        };
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("request", "GetCapabilities"))
            .respond_with(xml(200, capabilities_for(&server.uri())))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("typeNames", "gmd:MD_Metadata"))
            .respond_with(xml(
                400,
                concat!(
                    r#"<ExceptionReport xmlns="http://www.opengis.net/ows">"#,
                    r#"<Exception exceptionCode="InvalidParameterValue" locator="typeNames">"#,
                    "<ExceptionText>CSW: The typeNames parameter must be csw:Record",
                    "</ExceptionText></Exception></ExceptionReport>",
                )
                .to_string(),
            ))
            // Exactly one ISO attempt, at the full page size.
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/csw"))
            .and(query_param("typeNames", "csw:Record"))
            .respond_with(xml(200, DUBLIN_CORE.to_string()))
            .mount(&server)
            .await;

        let endpoint = format!("{}/csw", server.uri());
        let client = OgcRecordsClient::new(&server.uri(), "it", Some(&endpoint))
            .unwrap()
            .without_backoff();
        let page = client.page_resiliently(1).await.unwrap().unwrap().0;
        assert_eq!(page.records.len(), 3);
    }

    #[test]
    fn an_unrelated_exception_is_not_a_profile_rejection() {
        // Falling back on any failure would mask real outages behind a silent
        // downgrade to the shallower profile.
        let other = r#"<ows:ExceptionReport xmlns:ows="http://www.opengis.net/ows">
            <ows:Exception exceptionCode="NoApplicableCode">
              <ows:ExceptionText>Service temporarily unavailable</ows:ExceptionText>
            </ows:Exception>
          </ows:ExceptionReport>"#;
        let error = parse_get_records(other, "https://catalog.test", "en").unwrap_err();
        assert!(!is_profile_rejection(&error), "{error}");
    }

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
    fn bbox_coordinates_are_taken_from_one_complete_extent() {
        let xml = r#"<csw:GetRecordsResponse xmlns:csw="http://www.opengis.net/cat/csw/2.0.2" xmlns:gmd="http://www.isotc211.org/2005/gmd" xmlns:gco="http://www.isotc211.org/2005/gco"><csw:SearchResults numberOfRecordsMatched="1" numberOfRecordsReturned="1" nextRecord="0"><gmd:MD_Metadata><gmd:fileIdentifier><gco:CharacterString>multi-extent</gco:CharacterString></gmd:fileIdentifier><gmd:title><gco:CharacterString>Multiple extents</gco:CharacterString></gmd:title><gmd:EX_GeographicBoundingBox><gmd:westBoundLongitude><gco:Decimal>-20</gco:Decimal></gmd:westBoundLongitude><gmd:eastBoundLongitude><gco:Decimal>20</gco:Decimal></gmd:eastBoundLongitude></gmd:EX_GeographicBoundingBox><gmd:EX_GeographicBoundingBox><gmd:westBoundLongitude><gco:Decimal>-5</gco:Decimal></gmd:westBoundLongitude><gmd:southBoundLatitude><gco:Decimal>40</gco:Decimal></gmd:southBoundLatitude><gmd:eastBoundLongitude><gco:Decimal>10</gco:Decimal></gmd:eastBoundLongitude><gmd:northBoundLatitude><gco:Decimal>52</gco:Decimal></gmd:northBoundLatitude></gmd:EX_GeographicBoundingBox></gmd:MD_Metadata></csw:SearchResults></csw:GetRecordsResponse>"#;
        let page = parse_get_records(xml, "https://example.test", "en").unwrap();
        assert_eq!(
            page.records[0].metadata["spatial"]["bbox"],
            json!([-5.0, 40.0, 10.0, 52.0])
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

    /// Italy's RNDT advertises the ISO profile and rejects it — the live case
    /// the fallback exists for.
    ///
    /// ```text
    /// cargo test -p ceres-client ogc_records::tests::rndt_dublin_core_smoke -- --ignored --exact
    /// ```
    #[tokio::test]
    #[ignore = "requires network access to the Italian RNDT catalogue"]
    async fn rndt_dublin_core_smoke() {
        let endpoint = std::env::var("CERES_CSW_DUBLIN_CORE_SMOKE_URL")
            .unwrap_or_else(|_| "https://geodati.gov.it/RNDT/CSW".into());
        let client =
            OgcRecordsClient::new("https://geodati.gov.it", "it", Some(&endpoint)).unwrap();
        let page = client.page(1, PAGE_SIZE_LADDER[0]).await.unwrap();
        assert_eq!(client.profile.get(), Some(&CswProfile::DublinCore));
        assert!(!page.records.is_empty());
        assert!(page.matched > 20_000, "matched {}", page.matched);
        assert!(
            page.records
                .iter()
                .any(|record| !record.metadata["online_resources"]
                    .as_array()
                    .unwrap()
                    .is_empty()),
            "no record carried a dct:references link"
        );
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
