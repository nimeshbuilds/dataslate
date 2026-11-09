-- Run with psql as the migration role, after the exact API image has migrated
-- a dedicated database. Supply NON-SECRET psql variables:
--   -v database=dataslate -v migration_role=dataslate_migrator -v runtime_role=dataslate_runtime
-- This grants existing objects only. Rerun after each schema migration.
-- It never creates a role/database, assigns passwords, or changes ownership.
-- PostgreSQL 17 \quit accepts no exit-code argument. Raise a SQL error instead:
-- ON_ERROR_STOP makes psql exit 3 and an open transaction is rolled back.
\set ON_ERROR_STOP on
\if :{?database}
\else
  \echo 'Missing database variable'
  DO $dataslate_guard$ BEGIN RAISE EXCEPTION 'DataSlate runtime role safety check failed'; END $dataslate_guard$;
\endif
\if :{?migration_role}
\else
  \echo 'Missing migration_role variable'
  DO $dataslate_guard$ BEGIN RAISE EXCEPTION 'DataSlate runtime role safety check failed'; END $dataslate_guard$;
\endif
\if :{?runtime_role}
\else
  \echo 'Missing runtime_role variable'
  DO $dataslate_guard$ BEGIN RAISE EXCEPTION 'DataSlate runtime role safety check failed'; END $dataslate_guard$;
\endif

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SELECT current_database() = :'database'
   AND current_user = :'migration_role'
   AND :'migration_role' <> :'runtime_role'
   AND EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'runtime_role'
               AND rolcanlogin AND NOT (rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication OR rolbypassrls))
   AND NOT EXISTS (SELECT 1 FROM pg_database d JOIN pg_roles r ON d.datdba = r.oid
                   WHERE d.datname = current_database() AND r.rolname = :'runtime_role')
   AND NOT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
                   JOIN pg_roles r ON r.oid = c.relowner
                   WHERE n.nspname = 'public' AND r.rolname = :'runtime_role')
   AND NOT EXISTS (SELECT 1 FROM pg_namespace n JOIN pg_roles r ON n.nspowner = r.oid
                   WHERE n.nspname = 'public' AND r.rolname = :'runtime_role')
   AND to_regclass('public.dataslate_schema_migrations') IS NOT NULL
   AS valid_boundary \gset
\if :valid_boundary
\else
  \echo 'Wrong database/migration identity, unsafe runtime role/ownership, or missing DataSlate schema'
  DO $dataslate_guard$ BEGIN RAISE EXCEPTION 'DataSlate runtime role safety check failed'; END $dataslate_guard$;
\endif

-- Membership also permits SET ROLE even when NOINHERIT is configured.
SELECT NOT pg_has_role(:'runtime_role', :'migration_role', 'MEMBER')
   AND NOT EXISTS (SELECT 1 FROM pg_roles r
                   WHERE r.rolname <> :'runtime_role'
                     AND pg_has_role(:'runtime_role', r.oid, 'MEMBER'))
   AS distinct_authority \gset
\if :distinct_authority
\else
  \echo 'Dedicated runtime role must not be a member of any other role'
  DO $dataslate_guard$ BEGIN RAISE EXCEPTION 'DataSlate runtime role safety check failed'; END $dataslate_guard$;
\endif

REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM :"runtime_role";
REVOKE CREATE ON DATABASE :"database" FROM :"runtime_role";
GRANT CONNECT ON DATABASE :"database" TO :"runtime_role";
GRANT USAGE ON SCHEMA public TO :"runtime_role";
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO :"runtime_role";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO :"runtime_role";
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
  ON TABLE public.dataslate_schema_migrations FROM :"runtime_role";

-- Check effective permissions, including unexpected PUBLIC/inherited grants.
SELECT NOT has_schema_privilege(:'runtime_role', 'public', 'CREATE')
   AND NOT has_database_privilege(:'runtime_role', :'database', 'CREATE')
   AND has_table_privilege(:'runtime_role', 'public.dataslate_schema_migrations', 'SELECT')
   AND NOT has_table_privilege(:'runtime_role', 'public.dataslate_schema_migrations', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
   AS effective_boundary \gset
\if :effective_boundary
\else
  \echo 'Effective runtime permissions remain unsafe; all grants in this transaction will roll back'
  DO $dataslate_guard$ BEGIN RAISE EXCEPTION 'DataSlate runtime role safety check failed'; END $dataslate_guard$;
\endif
COMMIT;
\echo 'DataSlate runtime grants applied; migration ledger remains read-only'
