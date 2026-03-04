-- Add is_stale column for soft-deletion of datasets removed from source portals.
-- Stale datasets are excluded from search results but retained for audit.
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS is_stale BOOLEAN NOT NULL DEFAULT FALSE;

-- Partial index: only index non-stale rows (the common case for search).
CREATE INDEX IF NOT EXISTS idx_datasets_not_stale ON datasets (is_stale) WHERE is_stale = FALSE;
