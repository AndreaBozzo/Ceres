-- Migration: Add content_hash column for delta harvesting
-- This enables tracking content changes to avoid unnecessary embedding regeneration

-- Add the content_hash column (nullable for backward compatibility with existing records)
ALTER TABLE datasets ADD COLUMN content_hash VARCHAR(64);

-- Create index for efficient hash lookups by portal
-- Supports the get_hashes_for_portal() query pattern
CREATE INDEX idx_datasets_portal_hash ON datasets(source_portal, content_hash);

-- Comment explaining the column purpose
COMMENT ON COLUMN datasets.content_hash IS 'SHA-256 hash of (title + description) for delta detection. NULL means hash not yet computed.';
