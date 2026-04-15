-- Add profile and optional SPARQL endpoint override columns to harvest_jobs
-- for DCAT sub-dispatch.
-- `profile` records the access profile (e.g., 'sparql' for SPARQL endpoints).
-- NULL means the default profile for the portal type.
-- `sparql_endpoint` stores an optional per-job endpoint override; NULL means
-- use the default endpoint for the selected portal/profile.
ALTER TABLE harvest_jobs ADD COLUMN profile TEXT;
ALTER TABLE harvest_jobs ADD COLUMN sparql_endpoint TEXT;
