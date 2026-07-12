ALTER TABLE datasets
    ADD COLUMN record_kind VARCHAR(16) NOT NULL DEFAULT 'dataset';

ALTER TABLE datasets
    ADD CONSTRAINT datasets_record_kind_check
    CHECK (record_kind IN ('dataset', 'series', 'service', 'map', 'other'));

CREATE INDEX idx_datasets_searchable_embedding
    ON datasets (source_portal, last_updated_at DESC)
    WHERE embedding IS NULL AND NOT is_stale
      AND record_kind IN ('dataset', 'series');

ALTER TABLE harvest_jobs ADD COLUMN ogc_endpoint TEXT;
