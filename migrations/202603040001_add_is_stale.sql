-- Add is_stale column for soft-deletion of datasets removed from source portals.
-- Stale datasets are excluded from search results but retained for audit.
ALTER TABLE datasets ADD COLUMN IF NOT EXISTS is_stale BOOLEAN NOT NULL DEFAULT FALSE;

-- Partial index: optimize for listing datasets that need embedding
CREATE INDEX IF NOT EXISTS idx_datasets_pending_embedding ON datasets (source_portal, last_updated_at DESC) WHERE embedding IS NULL AND NOT is_stale;
