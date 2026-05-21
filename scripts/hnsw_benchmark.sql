-- HNSW search benchmark — latency at varying hnsw.ef_search values.
--
-- Usage:
--   PGPASSWORD=... psql -h localhost -p 5432 -U ceres_user -d ceres_db \
--       -f scripts/hnsw_benchmark.sql
--
-- Picks a real embedding from the table as the probe vector (deterministic: the
-- row with the smallest id that has an embedding), then runs the production search
-- query under several ef_search settings, reporting planner timing via EXPLAIN ANALYZE.
--
-- Read the "Execution Time" line under each \echo header. Higher ef_search = better
-- recall, higher latency. The app default is 40 (CERES_HNSW_EF_SEARCH).
--
-- Note: the first query is cold (loads ~1.2GB of index from disk; with a small
-- shared_buffers it can take seconds). Run a warm-up query first for steady-state
-- numbers. Reference run on the local catalog (~362k embeddings, 768d), warm cache:
--   ef_search=40  -> ~1.1 ms
--   ef_search=100 -> ~2.8 ms
--   ef_search=200 -> ~3.5 ms

\set ON_ERROR_STOP on
\timing on

-- Confirm we are hitting the tuned index, not a sequential scan.
\echo '=== index in use ==='
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'datasets' AND indexdef LIKE '%hnsw%';

-- Capture a probe vector into a temp table (psql can't easily inline a 768-dim literal).
DROP TABLE IF EXISTS _probe;
CREATE TEMP TABLE _probe AS
SELECT embedding AS v
FROM datasets
WHERE embedding IS NOT NULL AND NOT is_stale
ORDER BY id
LIMIT 1;

-- Each section runs inside an explicit transaction: psql is in autocommit mode,
-- so a bare `SET LOCAL` would be committed on its own and never reach the
-- following EXPLAIN. BEGIN/COMMIT keeps the SET LOCAL scoped to the query that
-- uses it, which is exactly the per-search behavior the app relies on.

\echo ''
\echo '=== ef_search = 40 (default) ==='
BEGIN;
SET LOCAL hnsw.ef_search = 40;
SET LOCAL hnsw.iterative_scan = relaxed_order;
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT id, 1 - (embedding <=> (SELECT v FROM _probe)) AS score
FROM datasets
WHERE embedding IS NOT NULL AND NOT is_stale
ORDER BY embedding <=> (SELECT v FROM _probe)
LIMIT 10;
COMMIT;

\echo ''
\echo '=== ef_search = 100 ==='
BEGIN;
SET LOCAL hnsw.ef_search = 100;
SET LOCAL hnsw.iterative_scan = relaxed_order;
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT id, 1 - (embedding <=> (SELECT v FROM _probe)) AS score
FROM datasets
WHERE embedding IS NOT NULL AND NOT is_stale
ORDER BY embedding <=> (SELECT v FROM _probe)
LIMIT 10;
COMMIT;

\echo ''
\echo '=== ef_search = 200 ==='
BEGIN;
SET LOCAL hnsw.ef_search = 200;
SET LOCAL hnsw.iterative_scan = relaxed_order;
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT id, 1 - (embedding <=> (SELECT v FROM _probe)) AS score
FROM datasets
WHERE embedding IS NOT NULL AND NOT is_stale
ORDER BY embedding <=> (SELECT v FROM _probe)
LIMIT 10;
COMMIT;
