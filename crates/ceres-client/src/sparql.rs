//! SPARQL DCAT client for harvesting datasets from SPARQL-backed open data portals.
//!
//! Supports portals that expose DCAT-AP metadata via a SPARQL endpoint, such as:
//! - `data.europa.eu` (EU Open Data Portal)
//! - `data.norge.no` (Norwegian Data Portal)
//!
//! # API
//!
//! Endpoint: `POST {base}/sparql` with `Accept: application/sparql-results+json`
//!
//! Dataset discovery uses standard DCAT vocabulary:
//! - `dcat:Dataset` for dataset type
//! - `dct:title`, `dct:description`, `dct:identifier`, `dct:modified` for fields
//!
//! Pagination uses `LIMIT`/`OFFSET` in the SPARQL query.

use std::collections::HashMap;
use std::time::Duration;

use ceres_core::HttpConfig;
use ceres_core::error::AppError;
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::dcat::DcatDataset;

/// Delay between paginated SPARQL requests.
const PAGE_DELAY: Duration = Duration::from_millis(300);

/// Per-request timeout for SPARQL queries.
///
/// SPARQL queries on large catalogs can be slow — especially deep LIMIT/OFFSET
/// queries on data.europa.eu (~1.6M datasets). 300s accommodates worst-case pages.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(300);

/// Maximum backoff delay for rate-limited retries.
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Base delay for page-level retries (on top of per-request retries).
///
/// When all per-request retries fail, we wait longer before retrying the whole
/// page to give overloaded SPARQL endpoints time to recover.
const PAGE_RETRY_BASE_DELAY: Duration = Duration::from_secs(10);

/// Maximum number of page-level retries before giving up on a page.
const MAX_PAGE_RETRIES: u32 = 3;

/// Maximum consecutive skipped pages before stopping the stream entirely.
/// If this many pages in a row fail even after retries, the endpoint is
/// considered down rather than just struggling at specific offsets.
const MAX_CONSECUTIVE_SKIPS: usize = 3;

/// Default number of results per SPARQL page.
const DEFAULT_PAGE_SIZE: usize = 1000;

/// Minimum page size for adaptive reduction. Below this we give up rather than
/// issuing very small queries.
const MIN_PAGE_SIZE: usize = 50;

/// Number of dataset URIs to look up per VALUES batch in two-phase harvest.
/// Tested at 500 URIs → ~400ms on data.europa.eu.
const VALUES_BATCH_SIZE: usize = 200;

// =============================================================================
// SPARQL JSON Results Format (W3C standard)
// =============================================================================

/// Top-level SPARQL JSON results response.
#[derive(Debug, Deserialize)]
struct SparqlResponse {
    results: SparqlResults,
}

/// The `results` object containing bindings.
#[derive(Debug, Deserialize)]
struct SparqlResults {
    bindings: Vec<HashMap<String, SparqlValue>>,
}

/// A single SPARQL value in a binding row.
#[derive(Debug, Deserialize)]
struct SparqlValue {
    /// The RDF term value.
    value: String,
    /// Language tag for literals (e.g., `"en"`, `"nb"`).
    #[serde(rename = "xml:lang")]
    lang: Option<String>,
}

// =============================================================================
// SparqlDcatClient
// =============================================================================

/// HTTP client for harvesting DCAT-AP catalogs via SPARQL endpoints.
#[derive(Clone)]
pub struct SparqlDcatClient {
    client: Client,
    endpoint_url: Url,
    base_url: Url,
    language: String,
    page_size: usize,
}

impl SparqlDcatClient {
    /// Creates a new SPARQL DCAT client for the specified portal.
    ///
    /// The SPARQL endpoint is derived by convention as `{base_url}/sparql`,
    /// unless a custom endpoint is provided via `sparql_endpoint`.
    ///
    /// # Arguments
    ///
    /// * `base_url_str` - The base URL of the portal (e.g., `https://data.europa.eu`)
    /// * `language` - Preferred language for resolving multilingual fields (e.g., `"en"`, `"nb"`)
    /// * `sparql_endpoint` - Optional custom SPARQL endpoint URL (overrides `{base_url}/sparql`)
    pub fn new(
        base_url_str: &str,
        language: &str,
        sparql_endpoint: Option<&str>,
    ) -> Result<Self, AppError> {
        // Validate language as a BCP47-like tag to prevent SPARQL injection
        if language.is_empty()
            || !language
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(AppError::ConfigError(format!(
                "Invalid language tag '{language}'. Expected a BCP47 tag (e.g., 'en', 'nb', 'fr')."
            )));
        }

        let base_url = Url::parse(base_url_str)
            .map_err(|_| AppError::InvalidPortalUrl(base_url_str.to_string()))?;

        let endpoint_url = if let Some(ep) = sparql_endpoint {
            Url::parse(ep)
                .map_err(|_| AppError::ConfigError(format!("Invalid sparql_endpoint URL: {ep}")))?
        } else {
            base_url.join("sparql").map_err(|e| {
                AppError::Generic(format!("Failed to build SPARQL endpoint URL: {e}"))
            })?
        };

        let client = Client::builder()
            .user_agent("Ceres/0.1 (semantic-search-bot)")
            .timeout(REQUEST_TIMEOUT)
            .build()
            .map_err(|e| AppError::ClientError(e.to_string()))?;

        Ok(Self {
            client,
            endpoint_url,
            base_url,
            language: language.to_string(),
            page_size: DEFAULT_PAGE_SIZE,
        })
    }

    /// Returns the portal type identifier.
    pub fn portal_type(&self) -> &'static str {
        "dcat"
    }

    /// Returns the base URL of the portal.
    pub fn base_url(&self) -> &str {
        self.base_url.as_str()
    }

    /// Returns all dataset identifiers via a lightweight SPARQL query.
    pub async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        let mut ids = Vec::new();
        let mut offset = 0usize;

        loop {
            let query = format!(
                "SELECT DISTINCT ?dataset \
                 WHERE {{ ?dataset a <http://www.w3.org/ns/dcat#Dataset> }} \
                 LIMIT {} OFFSET {}",
                self.page_size, offset
            );

            let bindings = self.execute_query(&query).await?;
            if bindings.is_empty() {
                break;
            }

            let count = bindings.len();
            for binding in bindings {
                if let Some(v) = binding.get("dataset") {
                    ids.push(extract_last_segment(&v.value));
                }
            }

            if count < self.page_size {
                break;
            }
            offset += self.page_size;
            sleep(PAGE_DELAY).await;
        }

        Ok(ids)
    }

    /// Not implemented: SPARQL single-dataset lookup is not used by the harvest pipeline.
    pub async fn get_dataset(&self, _id: &str) -> Result<DcatDataset, AppError> {
        Err(AppError::Generic(
            "get_dataset is not implemented for SPARQL DCAT portals. \
             Use search_all_datasets() instead."
                .to_string(),
        ))
    }

    /// Searches for datasets modified since the given timestamp.
    pub async fn search_modified_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<DcatDataset>, AppError> {
        let filter = format!(
            "FILTER (?modified > \"{}\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)",
            since.to_rfc3339()
        );
        self.paginate_sparql(Some(&filter)).await
    }

    /// Fetches all datasets from the portal using paginated SPARQL queries.
    pub async fn search_all_datasets(&self) -> Result<Vec<DcatDataset>, AppError> {
        self.paginate_sparql(None).await
    }

    /// Returns the total number of datasets in the catalog.
    pub async fn dataset_count(&self) -> Result<usize, AppError> {
        let query = "SELECT (COUNT(DISTINCT ?dataset) AS ?count) \
             WHERE { ?dataset a <http://www.w3.org/ns/dcat#Dataset> }";

        let bindings = self.execute_query(query).await?;
        bindings
            .first()
            .and_then(|b| b.get("count"))
            .and_then(|v| v.value.parse::<usize>().ok())
            .ok_or_else(|| AppError::Generic("Failed to parse dataset count".to_string()))
    }

    /// Streams catalog datasets using a two-phase approach:
    ///
    /// 1. **URI collection** — lightweight `SELECT DISTINCT ?dataset` pages that
    ///    work reliably even at deep offsets (tested to 1.9M on data.europa.eu).
    /// 2. **Detail fetch** — for each batch of URIs, a `VALUES`-scoped query
    ///    retrieves title/description/identifier/modified without OFFSET.
    ///
    /// This avoids the query-plan explosion that LIMIT/OFFSET causes on large
    /// Virtuoso catalogs when combined with OPTIONAL clauses.
    pub fn paginate_sparql_stream(
        &self,
        _modified_since: Option<String>,
    ) -> BoxStream<'_, Result<Vec<DcatDataset>, AppError>> {
        /// State machine for the two-phase unfold.
        struct TwoPhaseState {
            /// Current offset into the lightweight URI enumeration.
            uri_offset: usize,
            /// URIs collected but not yet detail-fetched.
            pending_uris: Vec<String>,
            /// Total datasets yielded so far.
            fetched: usize,
            /// URI enumeration finished?
            uris_done: bool,
            /// Whole stream finished?
            done: bool,
            /// URI pages that failed after retries.
            skipped_uri_pages: usize,
            /// Detail batches that failed after retries.
            skipped_detail_batches: usize,
            /// Consecutive URI-page failures (stop after MAX_CONSECUTIVE_SKIPS).
            consecutive_uri_skips: usize,
        }

        let initial = TwoPhaseState {
            uri_offset: 0,
            pending_uris: Vec::new(),
            fetched: 0,
            uris_done: false,
            done: false,
            skipped_uri_pages: 0,
            skipped_detail_batches: 0,
            consecutive_uri_skips: 0,
        };

        Box::pin(futures::stream::unfold(initial, move |mut state| {
            async move {
                if state.done {
                    // Yield a final error if any work was skipped, so the
                    // harvest records a failure (prevents stale-marking).
                    let total_skipped = state.skipped_uri_pages + state.skipped_detail_batches;
                    if total_skipped > 0 {
                        return Some((
                            Err(AppError::Generic(format!(
                                "SPARQL harvest incomplete: {} URI pages and {} detail batches skipped, fetched {} datasets",
                                state.skipped_uri_pages,
                                state.skipped_detail_batches,
                                state.fetched
                            ))),
                            TwoPhaseState {
                                skipped_uri_pages: 0,
                                skipped_detail_batches: 0,
                                ..state
                            },
                        ));
                    }
                    return None;
                }

                // -- Fill the URI buffer until we have a full VALUES batch ----
                while !state.uris_done && state.pending_uris.len() < VALUES_BATCH_SIZE {
                    match self.fetch_uri_page(state.uri_offset).await {
                        Ok(uris) => {
                            state.consecutive_uri_skips = 0;
                            let page_full = uris.len() >= self.page_size;

                            tracing::debug!(
                                uri_offset = state.uri_offset,
                                page_uris = uris.len(),
                                "SPARQL URI page fetched"
                            );

                            state.pending_uris.extend(uris);
                            state.uri_offset += self.page_size;

                            if !page_full {
                                state.uris_done = true;
                            } else {
                                sleep(PAGE_DELAY).await;
                            }
                        }
                        Err(e) => {
                            // First page failure: propagate immediately.
                            if state.uri_offset == 0 && state.pending_uris.is_empty() {
                                return Some((
                                    Err(e),
                                    TwoPhaseState {
                                        done: true,
                                        ..state
                                    },
                                ));
                            }
                            state.skipped_uri_pages += 1;
                            state.consecutive_uri_skips += 1;
                            tracing::warn!(
                                uri_offset = state.uri_offset,
                                skipped = state.skipped_uri_pages,
                                error = %e,
                                "SPARQL URI page failed; skipping"
                            );
                            state.uri_offset += self.page_size;

                            if state.consecutive_uri_skips >= MAX_CONSECUTIVE_SKIPS {
                                tracing::warn!(
                                    "SPARQL URI enumeration: {} consecutive failures; stopping",
                                    MAX_CONSECUTIVE_SKIPS
                                );
                                state.uris_done = true;
                            } else {
                                sleep(PAGE_RETRY_BASE_DELAY).await;
                            }
                        }
                    }
                }

                // -- Nothing left to detail-fetch? We're done. ----------------
                if state.pending_uris.is_empty() {
                    state.done = true;
                    // The done-check at the top of the next tick will emit the
                    // incomplete-harvest error if any pages/batches were skipped.
                    return if state.skipped_uri_pages + state.skipped_detail_batches > 0 {
                        Some((Ok(vec![]), state))
                    } else {
                        None
                    };
                }

                // -- Phase 2: detail-fetch a VALUES batch ---------------------
                let batch_size = state.pending_uris.len().min(VALUES_BATCH_SIZE);
                let batch_uris: Vec<String> = state.pending_uris.drain(..batch_size).collect();

                // Retry detail batch with backoff before giving up.
                let mut last_err = None;
                for attempt in 0..=MAX_PAGE_RETRIES {
                    match self.fetch_details_batch(&batch_uris).await {
                        Ok(datasets) => {
                            state.fetched += datasets.len();
                            tracing::debug!(
                                batch_uris = batch_uris.len(),
                                datasets = datasets.len(),
                                total_fetched = state.fetched,
                                "SPARQL detail batch fetched"
                            );

                            if state.uris_done && state.pending_uris.is_empty() {
                                state.done = true;
                            }
                            sleep(PAGE_DELAY).await;
                            return Some((Ok(datasets), state));
                        }
                        Err(e) => {
                            if attempt < MAX_PAGE_RETRIES {
                                let delay = PAGE_RETRY_BASE_DELAY * 2_u32.pow(attempt);
                                tracing::warn!(
                                    batch_uris = batch_uris.len(),
                                    attempt = attempt + 1,
                                    delay_secs = delay.as_secs(),
                                    error = %e,
                                    "SPARQL detail batch failed; retrying after backoff"
                                );
                                sleep(delay).await;
                            }
                            last_err = Some(e);
                        }
                    }
                }

                // All retries exhausted for this detail batch.
                let e = last_err.unwrap();
                tracing::warn!(
                    batch_uris = batch_uris.len(),
                    error = %e,
                    "SPARQL detail batch failed after retries; skipping batch"
                );
                state.skipped_detail_batches += 1;
                if state.uris_done && state.pending_uris.is_empty() {
                    state.done = true;
                }
                // Back off before continuing to next batch.
                sleep(PAGE_RETRY_BASE_DELAY).await;
                Some((Ok(vec![]), state))
            }
        }))
    }

    // =========================================================================
    // Internal helpers
    // =========================================================================

    /// Phase 1: fetch one page of dataset URIs via the lightweight enumeration query.
    ///
    /// Per-request retries are handled inside `execute_query` → `request_with_retry`.
    /// Higher-level skip/continue logic lives in the caller (`paginate_sparql_stream`).
    async fn fetch_uri_page(&self, offset: usize) -> Result<Vec<String>, AppError> {
        let query = format!(
            "SELECT DISTINCT ?dataset \
             WHERE {{ ?dataset a <http://www.w3.org/ns/dcat#Dataset> }} \
             LIMIT {} OFFSET {}",
            self.page_size, offset
        );

        let bindings = self.execute_query(&query).await?;
        Ok(bindings
            .into_iter()
            .filter_map(|mut b| b.remove("dataset").map(|v| v.value))
            .collect())
    }

    /// Phase 2: fetch full metadata for a batch of dataset URIs using a VALUES query.
    ///
    /// The VALUES clause scopes the query to specific URIs — no OFFSET needed,
    /// so there's no query-plan explosion on large catalogs.
    async fn fetch_details_batch(&self, uris: &[String]) -> Result<Vec<DcatDataset>, AppError> {
        if uris.is_empty() {
            return Ok(vec![]);
        }

        // Validate URIs before interpolating into SPARQL — reject anything that
        // could break the `<...>` IRI syntax or inject query fragments.
        let values_clause: String = uris
            .iter()
            .filter(|u| {
                let ok = u.starts_with("http://") || u.starts_with("https://");
                let safe = !u.contains('>') && !u.contains('<') && !u.contains('}');
                if !ok || !safe {
                    tracing::warn!(uri = u.as_str(), "Skipping invalid URI in VALUES batch");
                }
                ok && safe
            })
            .map(|u| format!("(<{u}>)"))
            .collect::<Vec<_>>()
            .join(" ");

        if values_clause.is_empty() {
            return Ok(vec![]);
        }

        let title_filter = self
            .lang_filter_alternatives()
            .iter()
            .map(|l| format!("lang(?title) = \"{l}\""))
            .collect::<Vec<_>>()
            .join(" || ");

        let query = format!(
            "PREFIX dcat: <http://www.w3.org/ns/dcat#>\n\
             PREFIX dct:  <http://purl.org/dc/terms/>\n\
             \n\
             SELECT ?dataset ?title ?description ?identifier ?modified\n\
             WHERE {{\n\
               VALUES (?dataset) {{ {values_clause} }}\n\
               ?dataset dct:title ?title .\n\
               FILTER ({title_filter})\n\
               OPTIONAL {{ ?dataset dct:description ?description }}\n\
               OPTIONAL {{ ?dataset dct:identifier ?identifier }}\n\
               OPTIONAL {{ ?dataset dct:modified ?modified }}\n\
             }}",
        );

        let bindings = self.execute_query(&query).await?;
        Ok(self.bindings_to_datasets(bindings))
    }

    /// Core paginated SPARQL fetcher (non-streaming, used by search_modified_since).
    async fn paginate_sparql(
        &self,
        extra_filter: Option<&str>,
    ) -> Result<Vec<DcatDataset>, AppError> {
        let mut all_datasets = Vec::new();
        let mut offset = 0usize;
        let mut current_page_size = self.page_size;

        'pages: loop {
            let query =
                self.build_dataset_query_with_limit(offset, current_page_size, extra_filter);

            let bindings = 'retry: {
                for page_attempt in 0..=MAX_PAGE_RETRIES {
                    match self.execute_query(&query).await {
                        Ok(b) => break 'retry b,
                        Err(e) => {
                            // Adaptive page size reduction on server overload
                            if is_server_overload(&e) && current_page_size > MIN_PAGE_SIZE {
                                let new_size = (current_page_size / 2).max(MIN_PAGE_SIZE);
                                tracing::warn!(
                                    offset,
                                    old_page_size = current_page_size,
                                    new_page_size = new_size,
                                    error = %e,
                                    "SPARQL server overload; reducing page size and retrying"
                                );
                                current_page_size = new_size;
                                sleep(PAGE_RETRY_BASE_DELAY).await;
                                continue 'pages;
                            }

                            if all_datasets.is_empty() && page_attempt == MAX_PAGE_RETRIES {
                                return Err(e);
                            }
                            if page_attempt < MAX_PAGE_RETRIES {
                                let delay = PAGE_RETRY_BASE_DELAY * 2_u32.pow(page_attempt);
                                tracing::warn!(
                                    offset,
                                    attempt = page_attempt + 1,
                                    delay_secs = delay.as_secs(),
                                    error = %e,
                                    "SPARQL page fetch failed; retrying after backoff"
                                );
                                sleep(delay).await;
                                continue;
                            }
                            tracing::warn!(
                                offset,
                                collected = all_datasets.len(),
                                error = %e,
                                "SPARQL page fetch failed after retries; returning partial results"
                            );
                            break 'pages;
                        }
                    }
                }
                // Unreachable: the loop above always breaks or returns.
                break 'pages;
            };

            if bindings.is_empty() {
                break;
            }

            let count = bindings.len();
            let datasets = self.bindings_to_datasets(bindings);

            tracing::debug!(
                offset,
                page_size = current_page_size,
                page_datasets = datasets.len(),
                total_fetched = all_datasets.len() + datasets.len(),
                "SPARQL page fetched"
            );

            all_datasets.extend(datasets);

            if count < current_page_size {
                break;
            }
            offset += current_page_size;
            sleep(PAGE_DELAY).await;
        }

        Ok(all_datasets)
    }

    /// Builds the SPARQL SELECT query for dataset discovery.
    ///
    /// The language filter is applied only to `dct:title` (mandatory) to keep the
    /// query plan simple. Description language selection is handled client-side in
    /// [`bindings_to_datasets`](Self::bindings_to_datasets) via [`best_value`](Self::best_value).
    /// Filtering inside `OPTIONAL { dct:description }` causes HTTP 500 on large
    /// catalogs (e.g., data.europa.eu with ~2M datasets).
    #[cfg(test)]
    fn build_dataset_query(&self, offset: usize, extra_filter: Option<&str>) -> String {
        self.build_dataset_query_with_limit(offset, self.page_size, extra_filter)
    }

    fn build_dataset_query_with_limit(
        &self,
        offset: usize,
        limit: usize,
        extra_filter: Option<&str>,
    ) -> String {
        let extra = extra_filter.unwrap_or("");

        let title_filter = self
            .lang_filter_alternatives()
            .iter()
            .map(|l| format!("lang(?title) = \"{l}\""))
            .collect::<Vec<_>>()
            .join(" || ");

        format!(
            "PREFIX dcat: <http://www.w3.org/ns/dcat#>\n\
             PREFIX dct:  <http://purl.org/dc/terms/>\n\
             \n\
             SELECT ?dataset ?title ?description ?identifier ?modified\n\
             WHERE {{\n\
               ?dataset a dcat:Dataset .\n\
               ?dataset dct:title ?title .\n\
               FILTER ({title_filter})\n\
               OPTIONAL {{ ?dataset dct:description ?description }}\n\
               OPTIONAL {{ ?dataset dct:identifier ?identifier }}\n\
               OPTIONAL {{ ?dataset dct:modified ?modified }}\n\
               {extra}\n\
             }}\n\
             LIMIT {limit} OFFSET {offset}",
        )
    }

    /// Executes a SPARQL query and returns the result bindings.
    async fn execute_query(
        &self,
        query: &str,
    ) -> Result<Vec<HashMap<String, SparqlValue>>, AppError> {
        let resp = self.request_with_retry(query).await?;

        let body: SparqlResponse = resp.json().await.map_err(|e| {
            AppError::ClientError(format!("SPARQL endpoint returned non-JSON response: {e}"))
        })?;

        Ok(body.results.bindings)
    }

    /// HTTP POST with exponential backoff retry on transient errors and rate limits.
    async fn request_with_retry(&self, query: &str) -> Result<reqwest::Response, AppError> {
        let http_config = HttpConfig::default();
        let max_retries = http_config.max_retries;
        let base_delay = http_config.retry_base_delay;
        let mut last_error = AppError::Generic("No attempts made".to_string());

        for attempt in 1..=max_retries {
            let body = url::form_urlencoded::Serializer::new(String::new())
                .append_pair("query", query)
                .finish();
            match self
                .client
                .post(self.endpoint_url.clone())
                .header("Accept", "application/sparql-results+json")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .body(body)
                .send()
                .await
            {
                Ok(resp) => {
                    let status = resp.status();

                    if status.is_success() {
                        return Ok(resp);
                    }

                    if status == StatusCode::TOO_MANY_REQUESTS {
                        last_error = AppError::RateLimitExceeded;
                        if attempt < max_retries {
                            let delay = resp
                                .headers()
                                .get("retry-after")
                                .and_then(|v| v.to_str().ok())
                                .and_then(|v| v.parse::<u64>().ok())
                                .map(Duration::from_secs)
                                .unwrap_or_else(|| {
                                    (base_delay * 2_u32.pow(attempt)).min(MAX_RETRY_DELAY)
                                });
                            sleep(delay).await;
                            continue;
                        }
                        return Err(AppError::RateLimitExceeded);
                    }

                    if status.is_server_error() {
                        last_error = AppError::ClientError(format!(
                            "Server error: HTTP {}",
                            status.as_u16()
                        ));
                        if attempt < max_retries {
                            sleep(base_delay * attempt).await;
                            continue;
                        }
                    }

                    return Err(AppError::ClientError(format!(
                        "HTTP {} from {}",
                        status.as_u16(),
                        self.endpoint_url
                    )));
                }
                Err(e) => {
                    if e.is_timeout() {
                        last_error = AppError::Timeout(REQUEST_TIMEOUT.as_secs());
                    } else if e.is_connect() {
                        last_error = AppError::NetworkError(format!("Connection failed: {e}"));
                    } else {
                        last_error = AppError::ClientError(e.to_string());
                    }
                    if attempt < max_retries && (e.is_timeout() || e.is_connect()) {
                        sleep(base_delay * attempt).await;
                        continue;
                    }
                }
            }
        }

        Err(last_error)
    }

    /// Returns the SPARQL lang() filter alternatives for the preferred language.
    ///
    /// Norwegian is special: `"nb"` (Bokmål), `"nn"` (Nynorsk), and `"no"`
    /// (generic Norwegian) are used interchangeably across portals. When the
    /// preferred language is any of these, all three variants are matched.
    fn lang_filter_alternatives(&self) -> Vec<&str> {
        let lang = self.language.as_str();
        let mut alts = vec![lang];
        match lang {
            "nb" | "nn" | "no" => {
                for &sibling in &["nb", "nn", "no"] {
                    if sibling != lang {
                        alts.push(sibling);
                    }
                }
            }
            _ => {}
        }
        alts.push("en");
        alts.push(""); // untagged literals
        alts
    }

    /// Checks whether two language tags are siblings (same macro-language).
    fn are_sibling_languages(a: &str, b: &str) -> bool {
        matches!((a, b), ("nb" | "nn" | "no", "nb" | "nn" | "no"))
    }

    /// Returns a language-preference rank for selecting the best value.
    /// Higher rank = better match: preferred language > sibling > "en" > untagged/other.
    fn lang_rank(&self, lang: Option<&str>) -> u8 {
        match lang {
            Some(l) if l == self.language => 3,
            Some(l) if Self::are_sibling_languages(l, &self.language) => 2,
            Some("en") => 1,
            _ => 0,
        }
    }

    /// Selects the best value for a field from grouped bindings based on language preference.
    fn best_value<'a>(
        &self,
        bindings: &'a [HashMap<String, SparqlValue>],
        key: &str,
    ) -> Option<&'a SparqlValue> {
        bindings
            .iter()
            .filter_map(|b| b.get(key))
            .filter(|v| !v.value.is_empty())
            .max_by(|a, b| {
                self.lang_rank(a.lang.as_deref())
                    .cmp(&self.lang_rank(b.lang.as_deref()))
            })
    }

    /// Converts SPARQL bindings into `DcatDataset` objects, deduplicating by dataset URI.
    ///
    /// SPARQL may return multiple rows per dataset (e.g., different language tags).
    /// For each dataset, the title and description with the best language match are selected
    /// (preferred language > sibling > "en" > untagged).
    ///
    /// After URI-based grouping, results are also deduplicated by identifier to
    /// prevent `ON CONFLICT` errors during batch upsert (different URIs can map
    /// to the same identifier via `dct:identifier` or URI last-segment extraction).
    fn bindings_to_datasets(
        &self,
        bindings: Vec<HashMap<String, SparqlValue>>,
    ) -> Vec<DcatDataset> {
        let mut grouped: HashMap<String, Vec<HashMap<String, SparqlValue>>> = HashMap::new();

        for binding in bindings {
            let Some(dataset_uri) = binding.get("dataset").map(|v| v.value.clone()) else {
                continue;
            };
            grouped.entry(dataset_uri).or_default().push(binding);
        }

        let mut datasets = Vec::new();
        let mut seen_identifiers: std::collections::HashSet<String> =
            std::collections::HashSet::new();

        for (dataset_uri, rows) in &grouped {
            let Some(title_val) = self.best_value(rows, "title") else {
                continue; // Skip datasets without a title
            };
            let title = title_val.value.clone();

            let description = self
                .best_value(rows, "description")
                .map(|v| v.value.clone());

            let identifier = rows
                .iter()
                .filter_map(|b| b.get("identifier"))
                .map(|v| v.value.clone())
                .find(|s| !s.is_empty())
                .unwrap_or_else(|| extract_last_segment(dataset_uri));

            // Skip duplicate identifiers (different URIs can resolve to the same id)
            if !seen_identifiers.insert(identifier.clone()) {
                continue;
            }

            // Build raw metadata from the row that provided the selected title
            let raw_binding = rows
                .iter()
                .find(|b| b.get("title").map(|v| v.value.as_str()) == Some(title.as_str()))
                .unwrap_or(&rows[0]);
            let raw = binding_to_json(raw_binding);

            datasets.push(DcatDataset {
                id_uri: dataset_uri.clone(),
                identifier,
                title,
                description,
                raw,
            });
        }

        datasets
    }
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Returns `true` if the error looks like a server-side overload (HTTP 500, 502,
/// 503, 504, or a client-side request timeout). Used to trigger adaptive page
/// size reduction — SPARQL endpoints commonly return 500 when queries are too
/// heavy, not just 504.
fn is_server_overload(e: &AppError) -> bool {
    let msg = e.to_string();
    msg.contains("HTTP 500")
        || msg.contains("HTTP 502")
        || msg.contains("HTTP 503")
        || msg.contains("HTTP 504")
        || msg.contains("timed out")
        || msg.contains("timeout")
}

/// Extracts the last non-empty path segment from a URI.
fn extract_last_segment(uri: &str) -> String {
    uri.trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or(uri)
        .to_string()
}

/// Converts a SPARQL binding row into a `serde_json::Value` for raw metadata storage.
fn binding_to_json(binding: &HashMap<String, SparqlValue>) -> Value {
    let mut map = serde_json::Map::new();
    for (key, val) in binding {
        let mut entry = serde_json::Map::new();
        entry.insert("value".to_string(), json!(val.value));
        if let Some(ref lang) = val.lang {
            entry.insert("xml:lang".to_string(), json!(lang));
        }
        map.insert(key.clone(), Value::Object(entry));
    }
    Value::Object(map)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ---- SPARQL response parsing -------------------------------------------

    fn sample_sparql_json() -> &'static str {
        r#"{
            "head": { "vars": ["dataset", "title", "description", "identifier", "modified"] },
            "results": {
                "bindings": [
                    {
                        "dataset": { "type": "uri", "value": "https://data.europa.eu/dataset/1234" },
                        "title": { "type": "literal", "value": "Air Quality Data", "xml:lang": "en" },
                        "description": { "type": "literal", "value": "Air quality measurements", "xml:lang": "en" },
                        "identifier": { "type": "literal", "value": "air-quality-1234" }
                    },
                    {
                        "dataset": { "type": "uri", "value": "https://data.europa.eu/dataset/5678" },
                        "title": { "type": "literal", "value": "Population Census", "xml:lang": "en" }
                    }
                ]
            }
        }"#
    }

    #[test]
    fn parse_sparql_response() {
        let resp: SparqlResponse = serde_json::from_str(sample_sparql_json()).unwrap();
        assert_eq!(resp.results.bindings.len(), 2);

        let first = &resp.results.bindings[0];
        assert_eq!(
            first["dataset"].value,
            "https://data.europa.eu/dataset/1234"
        );
        assert_eq!(first["title"].value, "Air Quality Data");
        assert_eq!(first["title"].lang.as_deref(), Some("en"));
        assert_eq!(first["identifier"].value, "air-quality-1234");

        let second = &resp.results.bindings[1];
        assert_eq!(
            second["dataset"].value,
            "https://data.europa.eu/dataset/5678"
        );
        assert!(second.get("description").is_none());
    }

    // ---- bindings_to_datasets ----------------------------------------------

    #[test]
    fn bindings_to_datasets_basic() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en", None).unwrap();
        let resp: SparqlResponse = serde_json::from_str(sample_sparql_json()).unwrap();
        let datasets = client.bindings_to_datasets(resp.results.bindings);

        assert_eq!(datasets.len(), 2);

        // Find by URI (order is not guaranteed from HashMap)
        let air = datasets
            .iter()
            .find(|d| d.id_uri == "https://data.europa.eu/dataset/1234")
            .unwrap();
        assert_eq!(air.title, "Air Quality Data");
        assert_eq!(air.description.as_deref(), Some("Air quality measurements"));
        assert_eq!(air.identifier, "air-quality-1234");

        let pop = datasets
            .iter()
            .find(|d| d.id_uri == "https://data.europa.eu/dataset/5678")
            .unwrap();
        assert_eq!(pop.title, "Population Census");
        assert!(pop.description.is_none());
        assert_eq!(pop.identifier, "5678"); // fallback to last URI segment
    }

    #[test]
    fn bindings_deduplicates_by_uri() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en", None).unwrap();
        let json = r#"{
            "results": {
                "bindings": [
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/d/1" },
                        "title": { "type": "literal", "value": "First Title", "xml:lang": "en" }
                    },
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/d/1" },
                        "title": { "type": "literal", "value": "Duplicate Title", "xml:lang": "en" }
                    }
                ]
            }
        }"#;
        let resp: SparqlResponse = serde_json::from_str(json).unwrap();
        let datasets = client.bindings_to_datasets(resp.results.bindings);
        assert_eq!(datasets.len(), 1);
    }

    #[test]
    fn bindings_prefers_requested_language() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "nb", None).unwrap();
        let json = r#"{
            "results": {
                "bindings": [
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/d/1" },
                        "title": { "type": "literal", "value": "English Title", "xml:lang": "en" },
                        "description": { "type": "literal", "value": "English desc", "xml:lang": "en" }
                    },
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/d/1" },
                        "title": { "type": "literal", "value": "Norsk Tittel", "xml:lang": "nb" },
                        "description": { "type": "literal", "value": "Norsk beskrivelse", "xml:lang": "nb" }
                    }
                ]
            }
        }"#;
        let resp: SparqlResponse = serde_json::from_str(json).unwrap();
        let datasets = client.bindings_to_datasets(resp.results.bindings);
        assert_eq!(datasets.len(), 1);
        assert_eq!(datasets[0].title, "Norsk Tittel");
        assert_eq!(
            datasets[0].description.as_deref(),
            Some("Norsk beskrivelse")
        );
    }

    #[test]
    fn bindings_skip_empty_title() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en", None).unwrap();
        let json = r#"{
            "results": {
                "bindings": [
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/d/1" },
                        "title": { "type": "literal", "value": "" }
                    },
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/d/2" },
                        "title": { "type": "literal", "value": "Valid Title" }
                    }
                ]
            }
        }"#;
        let resp: SparqlResponse = serde_json::from_str(json).unwrap();
        let datasets = client.bindings_to_datasets(resp.results.bindings);
        assert_eq!(datasets.len(), 1);
        assert_eq!(datasets[0].title, "Valid Title");
    }

    // ---- extract_last_segment -----------------------------------------------

    #[test]
    fn extract_last_segment_basic() {
        assert_eq!(
            extract_last_segment("https://data.europa.eu/dataset/1234"),
            "1234"
        );
    }

    #[test]
    fn extract_last_segment_trailing_slash() {
        assert_eq!(
            extract_last_segment("https://data.europa.eu/dataset/1234/"),
            "1234"
        );
    }

    // ---- query building ----------------------------------------------------

    #[test]
    fn build_dataset_query_basic() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en", None).unwrap();
        let query = client.build_dataset_query(0, None);
        assert!(query.contains("dcat:Dataset"));
        assert!(query.contains("dct:title"));
        assert!(!query.contains("ORDER BY"));
        assert!(query.contains("LIMIT 1000 OFFSET 0"));
        assert!(query.contains("lang(?title) = \"en\""));
        // Description filter should NOT be in the query (moved to client-side)
        assert!(!query.contains("lang(?description)"));
    }

    #[test]
    fn build_dataset_query_with_offset() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "nb", None).unwrap();
        let query = client.build_dataset_query(2000, None);
        assert!(query.contains("LIMIT 1000 OFFSET 2000"));
        assert!(query.contains("lang(?title) = \"nb\""));
        // Norwegian language siblings should all be in the filter
        assert!(query.contains("lang(?title) = \"nn\""));
        assert!(query.contains("lang(?title) = \"no\""));
    }

    #[test]
    fn build_dataset_query_with_filter() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en", None).unwrap();
        let filter = "FILTER (?modified > \"2026-01-01T00:00:00Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>)";
        let query = client.build_dataset_query(0, Some(filter));
        assert!(query.contains(filter));
    }

    // ---- SparqlDcatClient::new ---------------------------------------------

    #[test]
    fn sparql_client_new_valid() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en", None).unwrap();
        assert_eq!(client.portal_type(), "dcat");
        assert_eq!(client.base_url(), "https://data.europa.eu/");
        assert_eq!(
            client.endpoint_url.as_str(),
            "https://data.europa.eu/sparql"
        );
    }

    #[test]
    fn sparql_client_new_with_custom_endpoint() {
        let client = SparqlDcatClient::new(
            "https://data.norge.no",
            "nb",
            Some("https://sparql.fellesdatakatalog.digdir.no"),
        )
        .unwrap();
        assert_eq!(client.base_url(), "https://data.norge.no/");
        assert_eq!(
            client.endpoint_url.as_str(),
            "https://sparql.fellesdatakatalog.digdir.no/"
        );
    }

    #[test]
    fn sparql_client_new_invalid_url() {
        let result = SparqlDcatClient::new("not-a-url", "en", None);
        assert!(matches!(result, Err(AppError::InvalidPortalUrl(_))));
    }

    #[test]
    fn sparql_client_rejects_invalid_language() {
        let result = SparqlDcatClient::new("https://data.europa.eu", "en\"}", None);
        assert!(matches!(result, Err(AppError::ConfigError(_))));

        let result = SparqlDcatClient::new("https://data.europa.eu", "", None);
        assert!(matches!(result, Err(AppError::ConfigError(_))));
    }

    #[test]
    fn bindings_deduplicates_by_identifier() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en", None).unwrap();
        // Two different URIs that resolve to the same identifier via last-segment extraction
        let json = r#"{
            "results": {
                "bindings": [
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/a/shared-id" },
                        "title": { "type": "literal", "value": "First", "xml:lang": "en" }
                    },
                    {
                        "dataset": { "type": "uri", "value": "https://example.org/b/shared-id" },
                        "title": { "type": "literal", "value": "Second", "xml:lang": "en" }
                    }
                ]
            }
        }"#;
        let resp: SparqlResponse = serde_json::from_str(json).unwrap();
        let datasets = client.bindings_to_datasets(resp.results.bindings);
        // Both URIs extract "shared-id" as identifier — only the first should survive
        assert_eq!(datasets.len(), 1);
        assert_eq!(datasets[0].identifier, "shared-id");
    }

    // ---- binding_to_json ---------------------------------------------------

    #[test]
    fn binding_to_json_preserves_fields() {
        let mut binding = HashMap::new();
        binding.insert(
            "dataset".to_string(),
            SparqlValue {
                value: "https://example.org/d/1".to_string(),
                lang: None,
            },
        );
        binding.insert(
            "title".to_string(),
            SparqlValue {
                value: "Test Title".to_string(),
                lang: Some("en".to_string()),
            },
        );

        let json = binding_to_json(&binding);
        let obj = json.as_object().unwrap();
        assert_eq!(obj["dataset"]["value"], "https://example.org/d/1");
        assert_eq!(obj["title"]["value"], "Test Title");
        assert_eq!(obj["title"]["xml:lang"], "en");
    }

    // ---- Integration smoke test (requires network) -------------------------

    #[tokio::test]
    #[ignore = "requires network access to data.europa.eu"]
    async fn test_sparql_smoke_europa() {
        let client = SparqlDcatClient::new("https://data.europa.eu", "en", None).unwrap();
        let count = client.dataset_count().await.unwrap();
        assert!(count > 100, "Expected >100 datasets, got {}", count);

        let datasets = client.search_all_datasets().await.unwrap();
        assert!(
            !datasets.is_empty(),
            "Expected at least some datasets from data.europa.eu"
        );
        let first = &datasets[0];
        assert!(!first.title.is_empty(), "First dataset title is empty");
        assert!(!first.id_uri.is_empty(), "First dataset id_uri is empty");
    }
}
