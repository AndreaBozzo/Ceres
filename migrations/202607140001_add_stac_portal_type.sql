-- Allow collection-level STAC harvests to be queued by background workers.
ALTER TABLE harvest_jobs DROP CONSTRAINT harvest_jobs_portal_type_check;
ALTER TABLE harvest_jobs ADD CONSTRAINT harvest_jobs_portal_type_check
    CHECK (portal_type IN ('ckan', 'dcat', 'socrata', 'opendatasoft', 'arcgis', 'ogc_records', 'stac'));
