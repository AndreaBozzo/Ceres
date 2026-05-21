--- HNSW index dedup + tuning for production scale ---
--
-- Context: inspection of the production DB (1.79M datasets, ~362k embeddings)
-- found THREE duplicate HNSW indexes on datasets.embedding — datasets_embedding_idx,
-- datasets_embedding_idx1, datasets_embedding_idx2 — ~3.8 GB wasted, all with default
-- parameters and 0 scans. Root cause: the original migration used an unnamed
-- `CREATE INDEX ON datasets USING hnsw (...)` (no name, no IF NOT EXISTS), so each
-- re-application produced a new anonymous index (_idx, _idx1, _idx2).
--
-- This migration consolidates them into a single, explicitly-named, tuned index.
--
-- IMPORTANT — execution model:
--   Migrations are applied by the Makefile `migrate` target via `psql -f` WITHOUT
--   --single-transaction, so each statement autocommits. This is what allows
--   CREATE/DROP INDEX CONCURRENTLY (which cannot run inside a transaction block).
--   Do NOT wrap this file in BEGIN/COMMIT.
--
-- Memory: HNSW build for 362k × 768d vectors needs ~1.3 GB. The container defaults
--   (maintenance_work_mem=64MB) force a slow, low-quality on-disk build. We raise it
--   for this session only. See compose.yml notes for persistent production values.
--
-- Shared memory: parallel index builds allocate Postgres shared-memory segments in
--   /dev/shm, which defaults to 64 MB in the Docker container — far too small, causing
--   "could not resize shared memory segment ... No space left on device". We therefore
--   build SERIALLY (max_parallel_maintenance_workers = 0); maintenance_work_mem lives in
--   private backend memory and spills to disk (ample free space), so it is safe to raise.
--   To re-enable parallel builds, raise /dev/shm via `shm_size` in compose.yml first.

-- Raise build memory for the index creation in THIS session only (resets on disconnect).
SET maintenance_work_mem = '1GB';
SET max_parallel_maintenance_workers = 0;

-- Drop the duplicate anonymous indexes if present (existing production DBs).
DROP INDEX CONCURRENTLY IF EXISTS datasets_embedding_idx2;
DROP INDEX CONCURRENTLY IF EXISTS datasets_embedding_idx1;
DROP INDEX CONCURRENTLY IF EXISTS datasets_embedding_idx;

-- Drop any pre-existing default-parameter named index so we can rebuild it tuned.
-- (On fresh installs init.sql creates idx_datasets_embedding_hnsw with default params.)
DROP INDEX CONCURRENTLY IF EXISTS idx_datasets_embedding_hnsw;

-- Recreate a single tuned HNSW index.
--   m = 16              -> graph connectivity (recall vs memory/build time)
--   ef_construction = 64 -> index build quality
-- CONCURRENTLY avoids an exclusive table lock so harvesting/search keep working.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_datasets_embedding_hnsw
    ON datasets USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
