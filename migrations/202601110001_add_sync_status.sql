-- Migration: Add sync_status column to portal_sync_status table
-- This column tracks whether a sync completed or was cancelled,
-- enabling graceful shutdown support and partial progress tracking.

ALTER TABLE portal_sync_status
ADD COLUMN IF NOT EXISTS sync_status VARCHAR(20) DEFAULT 'completed';

-- Add index for querying by status
CREATE INDEX IF NOT EXISTS idx_portal_sync_status_status
ON portal_sync_status(sync_status);

-- Update existing rows to have explicit 'completed' status
UPDATE portal_sync_status
SET sync_status = 'completed'
WHERE sync_status IS NULL;
