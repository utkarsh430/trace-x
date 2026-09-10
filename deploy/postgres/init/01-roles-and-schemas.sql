-- ============================================================================
-- TRACE-X — schema and role bootstrap (ADR-0004)
--
-- GROUND-TRUTH ISOLATION IS A SECURITY CONTROL, NOT ORGANIZATION.
--
-- The application and every agent connect as `trace_app`. `trace_app` has NO
-- GRANT on the `groundtruth` schema. Ground truth is not hidden by convention
-- -- it is unreachable. A release-blocking test asserts that `trace_app`
-- receives "permission denied for schema groundtruth".
--
-- Leakage of ground truth into agent reasoning would silently invalidate every
-- metric in this project, which is why the control lives in the database rather
-- than in application code.
--
-- This file creates SCHEMAS and ROLES only. Tables arrive via Alembic migrations.
-- ============================================================================

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------- roles ----
-- Passwords come from the container environment, never from this file
-- (CLAUDE.md §9: no secret in code, image, log, or prompt). docker compose
-- injects them from .env; `make doctor` warns when .env is missing.
\getenv app_pw      TRACE_APP_DB_PASSWORD
\getenv stream_pw   TRACE_STREAM_DB_PASSWORD
\getenv eval_pw     TRACE_EVAL_DB_PASSWORD
\getenv auditor_pw  TRACE_AUDITOR_DB_PASSWORD

-- Fail loudly rather than silently creating a passwordless role.
-- NOTE: psql does not interpolate :'vars' inside dollar-quoted blocks, so the
-- guard and the CREATE statements are plain SQL driven by \gset and \gexec.
SELECT (coalesce(:'app_pw', '') = '' OR coalesce(:'stream_pw', '') = ''
     OR coalesce(:'eval_pw', '') = '' OR coalesce(:'auditor_pw', '') = '') AS pw_missing
\gset
\if :pw_missing
\echo '!! Role passwords are not set.'
\echo '!! docker compose must pass TRACE_APP_DB_PASSWORD, TRACE_STREAM_DB_PASSWORD,'
\echo '!! TRACE_EVAL_DB_PASSWORD and TRACE_AUDITOR_DB_PASSWORD into the container.'
\echo '!! Run `cp .env.example .env` (or `make setup`) and retry.'
\quit
\endif

-- Create each role only if absent. \gexec runs the generated statement, so no
-- row is produced (and nothing runs) when the role already exists.
SELECT format('CREATE ROLE trace_app LOGIN PASSWORD %L', :'app_pw')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'trace_app')
\gexec

SELECT format('CREATE ROLE trace_stream LOGIN PASSWORD %L', :'stream_pw')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'trace_stream')
\gexec

SELECT format('CREATE ROLE trace_eval LOGIN PASSWORD %L', :'eval_pw')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'trace_eval')
\gexec

SELECT format('CREATE ROLE trace_auditor LOGIN PASSWORD %L', :'auditor_pw')
 WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'trace_auditor')
\gexec

-- -------------------------------------------------------------- schemas ----
CREATE SCHEMA IF NOT EXISTS app         AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS audit       AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS groundtruth AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS eval        AUTHORIZATION CURRENT_USER;
CREATE SCHEMA IF NOT EXISTS external    AUTHORIZATION CURRENT_USER;

COMMENT ON SCHEMA groundtruth IS
  'Fraud labels, patterns and causal_evidence_keys. trace_app MUST NOT have any '
  'grant here. Readable only by trace_eval. See ADR-0004 and docs/EVALUATION.md.';

-- Nobody gets anything by default.
REVOKE ALL ON SCHEMA app, audit, groundtruth, eval, external FROM PUBLIC;
-- Database-level grants need a literal identifier, so build them dynamically.
DO $$
DECLARE db text := current_database();
BEGIN
  EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC', db);
  EXECUTE format(
    'GRANT CONNECT ON DATABASE %I TO trace_app, trace_stream, trace_eval, trace_auditor', db);
END
$$;

-- --------------------------------------------------------------- grants ----
-- trace_app: read/write app; INSERT-only audit (append-only log, ADR-0020).
GRANT USAGE ON SCHEMA app   TO trace_app;
GRANT USAGE ON SCHEMA audit TO trace_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA app
  GRANT SELECT, INSERT, UPDATE ON TABLES TO trace_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA app
  GRANT USAGE, SELECT ON SEQUENCES TO trace_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit
  GRANT INSERT ON TABLES TO trace_app;          -- INSERT only: never UPDATE or DELETE
ALTER DEFAULT PRIVILEGES IN SCHEMA audit
  GRANT USAGE, SELECT ON SEQUENCES TO trace_app;

-- trace_stream: read-only on app; writes land in Delta, not Postgres.
GRANT USAGE ON SCHEMA app, external TO trace_stream;
ALTER DEFAULT PRIVILEGES IN SCHEMA app      GRANT SELECT ON TABLES TO trace_stream;
ALTER DEFAULT PRIVILEGES IN SCHEMA external GRANT SELECT ON TABLES TO trace_stream;

-- trace_eval: the ONLY role that may read groundtruth.
GRANT USAGE ON SCHEMA app, audit, groundtruth, eval, external TO trace_eval;
ALTER DEFAULT PRIVILEGES IN SCHEMA app         GRANT SELECT ON TABLES TO trace_eval;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit       GRANT SELECT ON TABLES TO trace_eval;
ALTER DEFAULT PRIVILEGES IN SCHEMA groundtruth GRANT SELECT ON TABLES TO trace_eval;
ALTER DEFAULT PRIVILEGES IN SCHEMA eval        GRANT SELECT, INSERT ON TABLES TO trace_eval;
ALTER DEFAULT PRIVILEGES IN SCHEMA external    GRANT SELECT, INSERT ON TABLES TO trace_eval;
ALTER DEFAULT PRIVILEGES IN SCHEMA eval        GRANT USAGE, SELECT ON SEQUENCES TO trace_eval;
ALTER DEFAULT PRIVILEGES IN SCHEMA external    GRANT USAGE, SELECT ON SEQUENCES TO trace_eval;

-- trace_auditor: read-only, including audit. No groundtruth.
GRANT USAGE ON SCHEMA app, audit, eval TO trace_auditor;
ALTER DEFAULT PRIVILEGES IN SCHEMA app   GRANT SELECT ON TABLES TO trace_auditor;
ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT SELECT ON TABLES TO trace_auditor;
ALTER DEFAULT PRIVILEGES IN SCHEMA eval  GRANT SELECT ON TABLES TO trace_auditor;

-- ============================================================================
-- DELIBERATELY ABSENT: any GRANT of `groundtruth` to trace_app, trace_stream or
-- trace_auditor. Do not add one. tests/integration/test_groundtruth_isolation.py
-- asserts the denial and is a release blocker.
-- ============================================================================
