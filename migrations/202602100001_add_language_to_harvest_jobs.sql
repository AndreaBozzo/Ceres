-- Migration: Add language column to harvest_jobs
-- Supports preferred language for multilingual portals (e.g., ckan.opendata.swiss)
-- Related: https://github.com/AndreaBozzo/Ceres/issues/40

ALTER TABLE harvest_jobs ADD COLUMN IF NOT EXISTS language TEXT;
