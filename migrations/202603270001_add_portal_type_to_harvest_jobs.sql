-- Add portal_type column to harvest_jobs to support non-CKAN portal types.
-- Resolves TODO in worker.rs: "Add portal_type to HarvestJob when Socrata/DCAT support is added"
ALTER TABLE harvest_jobs ADD COLUMN portal_type TEXT NOT NULL DEFAULT 'ckan';
