--- Enables the vector extension and creates the datasets table ---
CREATE EXTENSION IF NOT EXISTS vector;

--- Main table to store datasets with embeddings ---
CREATE TABLE IF NOT EXISTS datasets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Core Metadata
    original_id VARCHAR NOT NULL,      -- Original ID on the CKAN portal
    source_portal VARCHAR NOT NULL,    -- e.g. "dati.gov.it"
    url VARCHAR NOT NULL,              -- URL of the dataset page
    
    -- Content
    title TEXT NOT NULL,
    description TEXT,
    
    -- Embedding (768 dimensions for Gemini text-embedding-004)
    embedding vector(768),
    
    -- Technical metadata (JSONB format for future flexibility)
    metadata JSONB DEFAULT '{}'::jsonb,
    
    -- Audit
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Uniqueness constraint: same ID on the same portal = same record
    CONSTRAINT uk_portal_original_id UNIQUE (source_portal, original_id)
);

-- Index for vector search (HNSW is the fastest for approximate queries)
-- Note: requires data in the table to be created effectively, 
-- but we define it here for completeness.
--
-- TODO(performance): Tune HNSW parameters for production scale
-- For millions of datasets, consider explicit parameters:
--   CREATE INDEX ON datasets USING hnsw (embedding vector_cosine_ops)
--   WITH (m = 16, ef_construction = 64);
-- Higher values = better recall, slower build time.
-- See: https://github.com/pgvector/pgvector#hnsw
--
-- TODO(performance): Dynamic ef_search tuning in search queries
-- Before executing ORDER BY embedding <-> query, set:
--   SET LOCAL hnsw.ef_search = 40;  -- or higher for better precision
-- This should be done per-transaction in the search service
-- to balance precision vs speed dynamically.
-- Named index with IF NOT EXISTS to prevent duplicate anonymous indexes when
-- this migration is re-applied on dev resets (see migration 202605210001).
CREATE INDEX IF NOT EXISTS idx_datasets_embedding_hnsw
    ON datasets USING hnsw (embedding vector_cosine_ops);