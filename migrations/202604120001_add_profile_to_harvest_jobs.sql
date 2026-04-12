-- Add profile column to harvest_jobs for DCAT sub-dispatch.
-- Records the access profile (e.g., 'sparql' for SPARQL endpoints).
-- NULL means the default profile for the portal type.
ALTER TABLE harvest_jobs ADD COLUMN profile TEXT;
