-- Migration: Add portal_sync_status table for incremental harvesting
-- This table tracks the last successful sync timestamp per portal,
-- enabling time-based incremental harvesting via CKAN's package_search API.

CREATE TABLE IF NOT EXISTS portal_sync_status (
    portal_url VARCHAR PRIMARY KEY,
    last_successful_sync TIMESTAMPTZ,
    last_sync_mode VARCHAR(20),  -- 'full' or 'incremental'
    datasets_synced INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
