-- Migration: Add harvest_jobs table for persistent job queue
-- Enables recoverable, distributed job processing for portal harvesting
-- Related: https://github.com/AndreaBozzo/Ceres/issues/56

CREATE TABLE IF NOT EXISTS harvest_jobs (
    -- Primary identifier
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Job target
    portal_url VARCHAR NOT NULL,
    portal_name VARCHAR,

    -- Job status tracking
    -- Values: 'pending', 'running', 'completed', 'failed', 'cancelled'
    status VARCHAR(20) NOT NULL DEFAULT 'pending',

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,

    -- Retry handling (exponential backoff: 1min, 5min, 30min)
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    next_retry_at TIMESTAMPTZ,

    -- Error tracking
    error_message TEXT,

    -- Results (stored as JSONB for flexibility)
    -- Example: {"created": 10, "updated": 5, "unchanged": 100, "failed": 2}
    sync_stats JSONB,

    -- Worker identification (for distributed processing)
    worker_id VARCHAR(255),

    -- Sync configuration
    force_full_sync BOOLEAN NOT NULL DEFAULT FALSE,

    -- Constraints
    CONSTRAINT chk_harvest_jobs_status CHECK (
        status IN ('pending', 'running', 'completed', 'failed', 'cancelled')
    )
);

-- Index for efficient job claiming (pending jobs sorted by creation)
CREATE INDEX idx_harvest_jobs_pending
ON harvest_jobs(created_at)
WHERE status = 'pending';

-- Index for retry scheduling
CREATE INDEX idx_harvest_jobs_retry
ON harvest_jobs(next_retry_at)
WHERE status = 'pending' AND next_retry_at IS NOT NULL;

-- Index for worker's running jobs (useful for graceful shutdown)
CREATE INDEX idx_harvest_jobs_worker
ON harvest_jobs(worker_id)
WHERE status = 'running';

-- Index for listing jobs by status
CREATE INDEX idx_harvest_jobs_status
ON harvest_jobs(status, created_at DESC);

-- Index for portal-specific job lookup
CREATE INDEX idx_harvest_jobs_portal
ON harvest_jobs(portal_url, created_at DESC);
