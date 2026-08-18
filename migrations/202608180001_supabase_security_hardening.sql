-- Keep Ceres tables private when PostgreSQL is hosted behind Supabase's Data API.
--
-- Ceres connects through a trusted, direct PostgreSQL connection as the table
-- owner. It does not expose these operational tables through PostgREST, so the
-- safe API policy is explicit default-deny. Table owners and roles with
-- BYPASSRLS retain the direct access used by the CLI, server, and scheduler.

ALTER TABLE IF EXISTS public.datasets ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.embedding_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.harvest_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.portal_sync_status ENABLE ROW LEVEL SECURITY;
ALTER TABLE IF EXISTS public.schema_migrations ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS ceres_server_only ON public.datasets;
CREATE POLICY ceres_server_only ON public.datasets
    AS RESTRICTIVE FOR ALL TO PUBLIC USING (false) WITH CHECK (false);

DROP POLICY IF EXISTS ceres_server_only ON public.embedding_config;
CREATE POLICY ceres_server_only ON public.embedding_config
    AS RESTRICTIVE FOR ALL TO PUBLIC USING (false) WITH CHECK (false);

DROP POLICY IF EXISTS ceres_server_only ON public.harvest_jobs;
CREATE POLICY ceres_server_only ON public.harvest_jobs
    AS RESTRICTIVE FOR ALL TO PUBLIC USING (false) WITH CHECK (false);

DROP POLICY IF EXISTS ceres_server_only ON public.portal_sync_status;
CREATE POLICY ceres_server_only ON public.portal_sync_status
    AS RESTRICTIVE FOR ALL TO PUBLIC USING (false) WITH CHECK (false);

DROP POLICY IF EXISTS ceres_server_only ON public.schema_migrations;
CREATE POLICY ceres_server_only ON public.schema_migrations
    AS RESTRICTIVE FOR ALL TO PUBLIC USING (false) WITH CHECK (false);

-- Supabase provides an `extensions` schema and includes it in the database
-- search path. Relocate pgvector there to keep extension-owned objects out of
-- the exposed `public` schema. Plain PostgreSQL installations without that
-- platform schema deliberately keep their existing extension layout.
DO $$
BEGIN
    IF to_regnamespace('extensions') IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM pg_extension AS extension
           JOIN pg_namespace AS namespace
             ON namespace.oid = extension.extnamespace
           WHERE extension.extname = 'vector'
             AND namespace.nspname = 'public'
             AND extension.extrelocatable
       )
    THEN
        ALTER EXTENSION vector SET SCHEMA extensions;
    END IF;
END
$$;
