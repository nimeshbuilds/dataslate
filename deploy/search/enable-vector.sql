-- Run explicitly as the database owner after installing the pinned extension
-- files in the PostgreSQL server image. DataSlate never installs extensions.
\set ON_ERROR_STOP on
CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public VERSION '0.8.7';
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
    WHERE e.extname = 'vector' AND e.extversion = '0.8.7' AND n.nspname = 'public'
  ) THEN
    RAISE EXCEPTION 'Expected public.vector version 0.8.7; upgrades require a reviewed operator migration';
  END IF;
END $$;
-- The application role requires USAGE on the public schema. It must not own
-- this extension or receive CREATE on public. Apply grants to your actual role.
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
