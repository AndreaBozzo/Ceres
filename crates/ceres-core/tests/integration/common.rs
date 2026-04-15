//! Test utilities and mock implementations for integration tests.
//!
//! Provides mock implementations of the core traits for testing
//! `HarvestService` and `SearchService` in isolation.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use ceres_core::job::{CreateJobRequest, HarvestJob, JobStatus};
use ceres_core::job_queue::JobQueue;
use ceres_core::models::NewDataset;
use ceres_core::traits::{DatasetStore, EmbeddingProvider, PortalClient, PortalClientFactory};
use ceres_core::{AppError, Dataset, SearchResult, SyncStats};
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use uuid::Uuid;

// =============================================================================
// MockEmbeddingProvider
// =============================================================================

/// Mock embedding provider that returns deterministic vectors.
///
/// Returns vectors based on the input text length, making embeddings
/// reproducible for testing. Dimension is configurable for testing
/// different provider configurations.
#[derive(Clone)]
pub struct MockEmbeddingProvider {
    dimension: usize,
}

impl MockEmbeddingProvider {
    /// Creates a new mock provider with 768 dimensions (Gemini default).
    pub fn new() -> Self {
        Self { dimension: 768 }
    }

    /// Creates a mock provider with a custom dimension.
    ///
    /// # Examples
    ///
    /// ```
    /// // Simulate OpenAI text-embedding-3-small (1536 dimensions)
    /// let provider = MockEmbeddingProvider::with_dimension(1536);
    /// ```
    #[allow(dead_code)]
    pub fn with_dimension(dimension: usize) -> Self {
        Self { dimension }
    }
}

impl Default for MockEmbeddingProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl EmbeddingProvider for MockEmbeddingProvider {
    fn name(&self) -> &'static str {
        "mock"
    }

    fn dimension(&self) -> usize {
        self.dimension
    }

    fn max_batch_size(&self) -> usize {
        100
    }

    async fn generate(&self, text: &str) -> Result<Vec<f32>, AppError> {
        // Generate a deterministic embedding based on text length
        let seed = text.len() as f32;
        let embedding: Vec<f32> = (0..self.dimension)
            .map(|i| (seed + i as f32) / 1000.0)
            .collect();
        Ok(embedding)
    }
}

// =============================================================================
// MockPortalClient
// =============================================================================

/// Mock portal data for testing.
#[derive(Clone)]
pub struct MockPortalData {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
}

/// Mock portal client with configurable datasets.
#[derive(Clone)]
pub struct MockPortalClient {
    #[allow(dead_code)]
    portal_url: String,
    datasets: Vec<MockPortalData>,
}

impl MockPortalClient {
    pub fn new(portal_url: &str, datasets: Vec<MockPortalData>) -> Self {
        Self {
            portal_url: portal_url.to_string(),
            datasets,
        }
    }
}

impl PortalClient for MockPortalClient {
    type PortalData = MockPortalData;

    fn portal_type(&self) -> &'static str {
        "mock"
    }

    fn base_url(&self) -> &str {
        &self.portal_url
    }

    async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        Ok(self.datasets.iter().map(|d| d.id.clone()).collect())
    }

    async fn get_dataset(&self, id: &str) -> Result<Self::PortalData, AppError> {
        self.datasets
            .iter()
            .find(|d| d.id == id)
            .cloned()
            .ok_or_else(|| AppError::Generic(format!("Dataset not found: {}", id)))
    }

    fn into_new_dataset(
        data: Self::PortalData,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
    ) -> NewDataset {
        let content_hash = NewDataset::compute_content_hash_with_language(
            &data.title,
            data.description.as_deref(),
            language,
        );

        // Mock uses `id` for both `{id}` and `{name}` since MockPortalData
        // has no separate name/slug field. This is sufficient for testing
        // the template plumbing; CKAN-specific substitution is tested in ckan.rs.
        let url = match url_template {
            Some(template) => template
                .replace("{id}", &data.id)
                .replace("{name}", &data.id),
            None => format!("{}/dataset/{}", portal_url.trim_end_matches('/'), data.id),
        };

        NewDataset {
            original_id: data.id.clone(),
            source_portal: portal_url.to_string(),
            url,
            title: data.title,
            description: data.description,
            embedding: None,
            metadata: serde_json::json!({}),
            content_hash,
        }
    }

    async fn search_modified_since(
        &self,
        _since: DateTime<Utc>,
    ) -> Result<Vec<Self::PortalData>, AppError> {
        // For testing, return all datasets as "modified"
        Ok(self.datasets.clone())
    }

    async fn search_all_datasets(&self) -> Result<Vec<Self::PortalData>, AppError> {
        // For testing, return all datasets
        Ok(self.datasets.clone())
    }

    async fn dataset_count(&self) -> Result<usize, AppError> {
        Ok(self.datasets.len())
    }
}

// =============================================================================
// MockPortalClientFactory
// =============================================================================

/// Factory for creating mock portal clients.
#[derive(Clone)]
pub struct MockPortalClientFactory {
    datasets: Vec<MockPortalData>,
}

impl MockPortalClientFactory {
    pub fn new(datasets: Vec<MockPortalData>) -> Self {
        Self { datasets }
    }
}

impl PortalClientFactory for MockPortalClientFactory {
    type Client = MockPortalClient;

    fn create(
        &self,
        portal_url: &str,
        _portal_type: ceres_core::config::PortalType,
        _language: &str,
        _profile: Option<&str>,
        _sparql_endpoint: Option<&str>,
    ) -> Result<Self::Client, AppError> {
        Ok(MockPortalClient::new(portal_url, self.datasets.clone()))
    }
}

// =============================================================================
// MockDatasetStore
// =============================================================================

/// Records a call to record_sync_status
#[derive(Clone, Debug)]
pub struct SyncStatusRecord {
    pub portal_url: String,
    #[allow(dead_code)]
    pub sync_mode: String,
    pub sync_status: String,
    pub datasets_synced: i32,
}

/// In-memory dataset store for testing.
///
/// Stores datasets in a `HashMap` and provides full implementation of
/// `DatasetStore` trait for verifying harvest/search logic.
#[derive(Clone)]
pub struct MockDatasetStore {
    /// Stored datasets keyed by (source_portal, original_id)
    datasets: Arc<Mutex<HashMap<(String, String), StoredDataset>>>,
    /// History of sync status records
    pub sync_history: Arc<Mutex<Vec<SyncStatusRecord>>>,
}

/// Internal representation of a stored dataset.
#[derive(Clone)]
struct StoredDataset {
    id: Uuid,
    dataset: NewDataset,
}

impl MockDatasetStore {
    pub fn new() -> Self {
        Self {
            datasets: Arc::new(Mutex::new(HashMap::new())),
            sync_history: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns the number of stored datasets.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.datasets.lock().unwrap().len()
    }

    /// Returns true if no datasets are stored.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Checks if a dataset exists for the given portal and original_id.
    #[allow(dead_code)]
    pub fn contains(&self, portal_url: &str, original_id: &str) -> bool {
        let key = (portal_url.to_string(), original_id.to_string());
        self.datasets.lock().unwrap().contains_key(&key)
    }

    /// Gets a dataset by portal URL and original ID.
    #[allow(dead_code)]
    pub fn get(&self, portal_url: &str, original_id: &str) -> Option<NewDataset> {
        let key = (portal_url.to_string(), original_id.to_string());
        self.datasets
            .lock()
            .unwrap()
            .get(&key)
            .map(|s| s.dataset.clone())
    }
}

impl Default for MockDatasetStore {
    fn default() -> Self {
        Self::new()
    }
}

impl DatasetStore for MockDatasetStore {
    async fn get_by_id(&self, id: Uuid) -> Result<Option<Dataset>, AppError> {
        let datasets = self.datasets.lock().unwrap();
        for stored in datasets.values() {
            if stored.id == id {
                return Ok(Some(Dataset {
                    id: stored.id,
                    original_id: stored.dataset.original_id.clone(),
                    source_portal: stored.dataset.source_portal.clone(),
                    url: stored.dataset.url.clone(),
                    title: stored.dataset.title.clone(),
                    description: stored.dataset.description.clone(),
                    embedding: stored.dataset.embedding.clone(),
                    metadata: stored.dataset.metadata.clone(),
                    first_seen_at: chrono::Utc::now(),
                    last_updated_at: chrono::Utc::now(),
                    content_hash: Some(stored.dataset.content_hash.clone()),
                    is_stale: false,
                }));
            }
        }
        Ok(None)
    }

    async fn get_hashes_for_portal(
        &self,
        portal_url: &str,
    ) -> Result<HashMap<String, Option<String>>, AppError> {
        let datasets = self.datasets.lock().unwrap();
        let hashes: HashMap<String, Option<String>> = datasets
            .iter()
            .filter(|((portal, _), _)| portal == portal_url)
            .map(|((_, original_id), stored)| {
                (
                    original_id.clone(),
                    Some(stored.dataset.content_hash.clone()),
                )
            })
            .collect();
        Ok(hashes)
    }

    async fn update_timestamp_only(
        &self,
        _portal_url: &str,
        _original_id: &str,
    ) -> Result<(), AppError> {
        // No-op for mock - we don't track timestamps
        Ok(())
    }

    async fn batch_update_timestamps(
        &self,
        _portal_url: &str,
        original_ids: &[String],
    ) -> Result<u64, AppError> {
        // Return count as if all were updated
        Ok(original_ids.len() as u64)
    }

    async fn mark_stale_datasets(
        &self,
        _portal_url: &str,
        _sync_start: DateTime<Utc>,
    ) -> Result<u64, AppError> {
        Ok(0)
    }

    async fn mark_stale_by_exclusion(
        &self,
        _portal_url: &str,
        _seen_ids: &[String],
    ) -> Result<u64, AppError> {
        Ok(0)
    }

    async fn upsert(&self, dataset: &NewDataset) -> Result<Uuid, AppError> {
        let mut datasets = self.datasets.lock().unwrap();
        let key = (dataset.source_portal.clone(), dataset.original_id.clone());

        let id = if let Some(existing) = datasets.get(&key) {
            existing.id
        } else {
            Uuid::new_v4()
        };

        datasets.insert(
            key,
            StoredDataset {
                id,
                dataset: dataset.clone(),
            },
        );

        Ok(id)
    }

    async fn batch_upsert(&self, datasets: &[NewDataset]) -> Result<Vec<Uuid>, AppError> {
        let mut ids = Vec::with_capacity(datasets.len());
        for dataset in datasets {
            ids.push(self.upsert(dataset).await?);
        }
        Ok(ids)
    }

    async fn search(
        &self,
        _query_vector: Vec<f32>,
        limit: usize,
    ) -> Result<Vec<SearchResult>, AppError> {
        // Simple mock: return first `limit` datasets with fake similarity scores
        let datasets = self.datasets.lock().unwrap();
        let results: Vec<SearchResult> = datasets
            .values()
            .take(limit)
            .enumerate()
            .map(|(i, stored)| {
                // Create a minimal Dataset for SearchResult
                SearchResult {
                    dataset: ceres_core::Dataset {
                        id: stored.id,
                        original_id: stored.dataset.original_id.clone(),
                        source_portal: stored.dataset.source_portal.clone(),
                        url: stored.dataset.url.clone(),
                        title: stored.dataset.title.clone(),
                        description: stored.dataset.description.clone(),
                        embedding: stored.dataset.embedding.clone(),
                        metadata: stored.dataset.metadata.clone(),
                        first_seen_at: chrono::Utc::now(),
                        last_updated_at: chrono::Utc::now(),
                        content_hash: Some(stored.dataset.content_hash.clone()),
                        is_stale: false,
                    },
                    similarity_score: 1.0 - (i as f32 * 0.1),
                }
            })
            .collect();
        Ok(results)
    }

    fn list_stream<'a>(
        &'a self,
        portal_filter: Option<&'a str>,
        limit: Option<usize>,
    ) -> BoxStream<'a, Result<Dataset, AppError>> {
        // Simple mock: collect all matching datasets and stream them
        let datasets = self.datasets.lock().unwrap();
        let results: Vec<Result<Dataset, AppError>> = datasets
            .iter()
            .filter(|((portal, _), _)| portal_filter.is_none_or(|filter| portal == filter))
            .take(limit.unwrap_or(usize::MAX))
            .map(|((_, _), stored)| {
                Ok(Dataset {
                    id: stored.id,
                    original_id: stored.dataset.original_id.clone(),
                    source_portal: stored.dataset.source_portal.clone(),
                    url: stored.dataset.url.clone(),
                    title: stored.dataset.title.clone(),
                    description: stored.dataset.description.clone(),
                    embedding: stored.dataset.embedding.clone(),
                    metadata: stored.dataset.metadata.clone(),
                    first_seen_at: chrono::Utc::now(),
                    last_updated_at: chrono::Utc::now(),
                    content_hash: Some(stored.dataset.content_hash.clone()),
                    is_stale: false,
                })
            })
            .collect();
        Box::pin(futures::stream::iter(results))
    }

    async fn get_last_sync_time(
        &self,
        _portal_url: &str,
    ) -> Result<Option<DateTime<Utc>>, AppError> {
        // Always return None to simulate first sync (full sync)
        Ok(None)
    }

    async fn record_sync_status(
        &self,
        portal_url: &str,
        _sync_time: DateTime<Utc>,
        sync_mode: &str,
        sync_status: &str,
        datasets_synced: i32,
    ) -> Result<(), AppError> {
        self.sync_history.lock().unwrap().push(SyncStatusRecord {
            portal_url: portal_url.to_string(),
            sync_mode: sync_mode.to_string(),
            sync_status: sync_status.to_string(),
            datasets_synced,
        });
        Ok(())
    }

    async fn get_duplicate_titles(&self) -> Result<std::collections::HashSet<String>, AppError> {
        // Find titles that appear in more than one portal
        let datasets = self.datasets.lock().unwrap();
        let mut title_portals: HashMap<String, std::collections::HashSet<String>> = HashMap::new();
        for ((portal, _), stored) in datasets.iter() {
            title_portals
                .entry(stored.dataset.title.to_lowercase())
                .or_default()
                .insert(portal.clone());
        }
        Ok(title_portals
            .into_iter()
            .filter(|(_, portals)| portals.len() > 1)
            .map(|(title, _)| title)
            .collect())
    }

    async fn list_pending_embeddings(
        &self,
        portal_filter: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<Dataset>, AppError> {
        let datasets = self.datasets.lock().unwrap();
        let results: Vec<Dataset> = datasets
            .iter()
            .filter(|((portal, _), stored)| {
                stored.dataset.embedding.is_none()
                    && portal_filter.is_none_or(|filter| portal == filter)
            })
            .take(limit.unwrap_or(usize::MAX))
            .map(|((_, _), stored)| Dataset {
                id: stored.id,
                original_id: stored.dataset.original_id.clone(),
                source_portal: stored.dataset.source_portal.clone(),
                url: stored.dataset.url.clone(),
                title: stored.dataset.title.clone(),
                description: stored.dataset.description.clone(),
                embedding: stored.dataset.embedding.clone(),
                metadata: stored.dataset.metadata.clone(),
                first_seen_at: chrono::Utc::now(),
                last_updated_at: chrono::Utc::now(),
                content_hash: Some(stored.dataset.content_hash.clone()),
                is_stale: false,
            })
            .collect();
        Ok(results)
    }

    async fn count_pending_embeddings(&self, portal_filter: Option<&str>) -> Result<i64, AppError> {
        let datasets = self.datasets.lock().unwrap();
        let count = datasets
            .iter()
            .filter(|((portal, _), stored)| {
                stored.dataset.embedding.is_none()
                    && portal_filter.is_none_or(|filter| portal == filter)
            })
            .count();
        Ok(count as i64)
    }

    async fn health_check(&self) -> Result<(), AppError> {
        // Mock is always healthy
        Ok(())
    }
}

// =============================================================================
// MockJobQueue
// =============================================================================

/// In-memory job queue for testing worker state machine logic.
#[derive(Clone)]
pub struct MockJobQueue {
    jobs: Arc<Mutex<HashMap<Uuid, HarvestJob>>>,
    claim_error: Arc<Mutex<Option<String>>>,
}

impl MockJobQueue {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(Mutex::new(HashMap::new())),
            claim_error: Arc::new(Mutex::new(None)),
        }
    }

    /// Insert a pending job and return its ID.
    pub fn with_pending_job(self, portal_url: &str) -> (Self, Uuid) {
        self.with_pending_job_config(portal_url, 0, 3)
    }

    /// Insert a pending job with specific retry configuration.
    pub fn with_pending_job_config(
        self,
        portal_url: &str,
        retry_count: u32,
        max_retries: u32,
    ) -> (Self, Uuid) {
        let id = Uuid::new_v4();
        let job = HarvestJob {
            id,
            portal_url: portal_url.to_string(),
            portal_name: None,
            portal_type: ceres_core::config::PortalType::default(),
            status: JobStatus::Pending,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            started_at: None,
            completed_at: None,
            retry_count,
            max_retries,
            next_retry_at: None,
            error_message: None,
            sync_stats: None,
            worker_id: None,
            force_full_sync: false,
            url_template: None,
            language: None,
            profile: None,
        };
        self.jobs.lock().unwrap().insert(id, job);
        (self, id)
    }

    /// Get the current status of a job.
    pub fn job_status(&self, job_id: Uuid) -> Option<JobStatus> {
        self.jobs.lock().unwrap().get(&job_id).map(|j| j.status)
    }

    /// Get the job by ID (for inspecting full state in tests).
    #[allow(dead_code)]
    pub fn get_job_snapshot(&self, job_id: Uuid) -> Option<HarvestJob> {
        self.jobs.lock().unwrap().get(&job_id).cloned()
    }

    /// Inject an error for the next claim_job call.
    #[allow(dead_code)]
    pub fn set_claim_error(&self, error: Option<String>) {
        *self.claim_error.lock().unwrap() = error;
    }
}

impl Default for MockJobQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl JobQueue for MockJobQueue {
    async fn create_job(&self, request: CreateJobRequest) -> Result<HarvestJob, AppError> {
        let id = Uuid::new_v4();
        let job = HarvestJob {
            id,
            portal_url: request.portal_url,
            portal_name: request.portal_name,
            portal_type: request.portal_type,
            status: JobStatus::Pending,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            started_at: None,
            completed_at: None,
            retry_count: 0,
            max_retries: request.max_retries.unwrap_or(3),
            next_retry_at: None,
            error_message: None,
            sync_stats: None,
            worker_id: None,
            force_full_sync: request.force_full_sync,
            url_template: request.url_template,
            language: request.language,
            profile: request.profile,
        };
        self.jobs.lock().unwrap().insert(id, job.clone());
        Ok(job)
    }

    async fn claim_job(&self, worker_id: &str) -> Result<Option<HarvestJob>, AppError> {
        // Check for injected error
        if let Some(err_msg) = self.claim_error.lock().unwrap().take() {
            return Err(AppError::DatabaseError(err_msg));
        }

        let mut jobs = self.jobs.lock().unwrap();
        // Find first pending job that is ready to be claimed
        // (no next_retry_at, or next_retry_at <= now)
        let now = Utc::now();
        let pending_id = jobs
            .values()
            .find(|j| {
                j.status == JobStatus::Pending
                    && j.next_retry_at.is_none_or(|retry_at| retry_at <= now)
            })
            .map(|j| j.id);

        if let Some(id) = pending_id {
            let job = jobs.get_mut(&id).unwrap();
            job.status = JobStatus::Running;
            job.worker_id = Some(worker_id.to_string());
            job.started_at = Some(Utc::now());
            job.updated_at = Utc::now();
            Ok(Some(job.clone()))
        } else {
            Ok(None)
        }
    }

    async fn complete_job(&self, job_id: Uuid, stats: SyncStats) -> Result<(), AppError> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&job_id) {
            job.status = JobStatus::Completed;
            job.completed_at = Some(Utc::now());
            job.sync_stats = Some(stats);
            job.updated_at = Utc::now();
        }
        Ok(())
    }

    async fn fail_job(
        &self,
        job_id: Uuid,
        error: &str,
        next_retry_at: Option<DateTime<Utc>>,
    ) -> Result<(), AppError> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&job_id) {
            if next_retry_at.is_some() {
                // Retryable: reset to pending
                job.status = JobStatus::Pending;
                job.next_retry_at = next_retry_at;
                job.retry_count += 1;
            } else {
                // Permanent failure
                job.status = JobStatus::Failed;
                job.completed_at = Some(Utc::now());
            }
            job.error_message = Some(error.to_string());
            job.worker_id = None;
            job.updated_at = Utc::now();
        }
        Ok(())
    }

    async fn cancel_job(&self, job_id: Uuid, stats: Option<SyncStats>) -> Result<(), AppError> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&job_id) {
            job.status = JobStatus::Cancelled;
            job.completed_at = Some(Utc::now());
            job.sync_stats = stats;
            job.updated_at = Utc::now();
        }
        Ok(())
    }

    async fn get_job(&self, job_id: Uuid) -> Result<Option<HarvestJob>, AppError> {
        Ok(self.jobs.lock().unwrap().get(&job_id).cloned())
    }

    async fn list_jobs(
        &self,
        status: Option<JobStatus>,
        limit: usize,
    ) -> Result<Vec<HarvestJob>, AppError> {
        let jobs = self.jobs.lock().unwrap();
        let mut result: Vec<HarvestJob> = jobs
            .values()
            .filter(|j| status.is_none_or(|s| j.status == s))
            .cloned()
            .collect();
        result.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        result.truncate(limit);
        Ok(result)
    }

    async fn release_job(&self, job_id: Uuid) -> Result<(), AppError> {
        let mut jobs = self.jobs.lock().unwrap();
        if let Some(job) = jobs.get_mut(&job_id)
            && job.status == JobStatus::Running
        {
            job.status = JobStatus::Pending;
            job.worker_id = None;
            job.updated_at = Utc::now();
        }
        Ok(())
    }

    async fn release_worker_jobs(&self, worker_id: &str) -> Result<u64, AppError> {
        let mut jobs = self.jobs.lock().unwrap();
        let mut released = 0u64;
        for job in jobs.values_mut() {
            if job.status == JobStatus::Running && job.worker_id.as_deref() == Some(worker_id) {
                job.status = JobStatus::Pending;
                job.worker_id = None;
                job.updated_at = Utc::now();
                released += 1;
            }
        }
        Ok(released)
    }

    async fn count_by_status(&self, status: JobStatus) -> Result<i64, AppError> {
        let jobs = self.jobs.lock().unwrap();
        Ok(jobs.values().filter(|j| j.status == status).count() as i64)
    }
}
