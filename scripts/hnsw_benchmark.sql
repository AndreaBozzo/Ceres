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

\echo ''
\echo '=== ef_search = 40 (default) ==='
SET LOCAL hnsw.ef_search = 40;
SET LOCAL hnsw.iterative_scan = relaxed_order;
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT id, 1 - (embedding <=> (SELECT v FROM _probe)) AS score
FROM datasets
WHERE embedding IS NOT NULL AND NOT is_stale
ORDER BY embedding <=> (SELECT v FROM _probe)
LIMIT 10;

\echo ''
\echo '=== ef_search = 100 ==='
SET LOCAL hnsw.ef_search = 100;
SET LOCAL hnsw.iterative_scan = relaxed_order;
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT id, 1 - (embedding <=> (SELECT v FROM _probe)) AS score
FROM datasets
WHERE embedding IS NOT NULL AND NOT is_stale
ORDER BY embedding <=> (SELECT v FROM _probe)
LIMIT 10;

\echo ''
\echo '=== ef_search = 200 ==='
SET LOCAL hnsw.ef_search = 200;
SET LOCAL hnsw.iterative_scan = relaxed_order;
EXPLAIN (ANALYZE, BUFFERS, TIMING)
SELECT id, 1 - (embedding <=> (SELECT v FROM _probe)) AS score
FROM datasets
WHERE embedding IS NOT NULL AND NOT is_stale
ORDER BY embedding <=> (SELECT v FROM _probe)
LIMIT 10;
