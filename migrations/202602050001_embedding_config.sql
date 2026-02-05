-- Track embedding provider configuration for dimension validation.
--
-- This table stores the active embedding provider configuration.
-- It ensures consistency between the configured provider and the
-- stored vector dimensions in the datasets table.
--
-- Only one row is allowed (singleton pattern via CHECK constraint).
-- Changing providers requires running a migration to:
-- 1. Clear existing embeddings
-- 2. Alter the vector column dimension
-- 3. Update this configuration
-- 4. Re-harvest all datasets

CREATE TABLE IF NOT EXISTS embedding_config (
    -- Singleton constraint: only id=1 is allowed
    id INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),

    -- Provider identifier (e.g., "gemini", "openai")
    provider_name VARCHAR(50) NOT NULL,

    -- Model name (e.g., "text-embedding-004", "text-embedding-3-small")
    model_name VARCHAR(100) NOT NULL,

    -- Embedding vector dimension (must match datasets.embedding column)
    dimension INTEGER NOT NULL CHECK (dimension > 0),

    -- Timestamps
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Insert default configuration for existing Gemini installations.
-- This ensures backward compatibility - existing databases will
-- continue to work with Gemini embeddings.
INSERT INTO embedding_config (provider_name, model_name, dimension)
VALUES ('gemini', 'text-embedding-004', 768)
ON CONFLICT (id) DO NOTHING;

COMMENT ON TABLE embedding_config IS
'Stores the active embedding provider configuration. Only one row allowed.
Changing providers requires clearing embeddings and running a migration.';

COMMENT ON COLUMN embedding_config.dimension IS
'Must match the dimension of datasets.embedding vector column (currently 768).';
