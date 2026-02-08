-- Migration: Add url_template column to harvest_jobs
-- Supports custom URL templates for portals with non-standard frontends (e.g. dati.gov.it)
-- Related: https://github.com/AndreaBozzo/Ceres/issues/87

ALTER TABLE harvest_jobs ADD COLUMN IF NOT EXISTS url_template TEXT;
