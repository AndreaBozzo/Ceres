-- Add portal_type column to harvest_jobs to support non-CKAN portal types.
-- This column records the type of remote portal (for example CKAN, Socrata, or DCAT-based portals).
ALTER TABLE harvest_jobs ADD COLUMN portal_type TEXT NOT NULL DEFAULT 'ckan'
    CHECK (portal_type IN ('ckan', 'dcat', 'socrata'));
