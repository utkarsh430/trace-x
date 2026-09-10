"""Schemas, service roles and grants -- the ground-truth isolation control.

Revision ID: 0001
Revises:
Create Date: 2026-09-10

THIS MIGRATION IS THE PROJECT'S HIGHEST-SEVERITY SECURITY CONTROL (ADR-0004).

`trace_app` -- the role the application, the workers and every agent connect as
-- has NO GRANT on the `groundtruth` schema. Ground truth is not hidden by
convention; it is unreachable. Leakage would silently invalidate every metric in
the project, so the control is a database grant rather than a code convention.

`tests/integration/test_groundtruth_isolation.py` asserts the denial and is a
RELEASE BLOCKER. Do not add a grant here to make something work.
"""

from __future__ import annotations

import os
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMAS = ("app", "audit", "groundtruth", "eval", "external")

# role -> environment variable holding its password
ROLES = {
    "trace_app": "TRACE_APP_DB_PASSWORD",
    "trace_stream": "TRACE_STREAM_DB_PASSWORD",
    "trace_eval": "TRACE_EVAL_DB_PASSWORD",
    "trace_auditor": "TRACE_AUDITOR_DB_PASSWORD",
}


def upgrade() -> None:
    conn = op.get_bind()

    # --- schemas ----------------------------------------------------------
    for schema in SCHEMAS:
        op.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')

    op.execute(
        """
        COMMENT ON SCHEMA groundtruth IS
          'Fraud labels, patterns and causal_evidence_keys. trace_app MUST NOT have '
          'any grant here. Readable only by trace_eval. See ADR-0004 and '
          'docs/EVALUATION.md. A release-blocking test asserts the denial.'
        """
    )

    # --- roles ------------------------------------------------------------
    for role, env_var in ROLES.items():
        password = os.getenv(env_var)
        if not password:
            raise RuntimeError(
                f"{env_var} is not set, so role '{role}' would be created without a "
                f"password. Run `cp .env.example .env` (or `make setup`) and export it."
            )
        # Bound parameters -> server-side quoting. No string interpolation of secrets.
        conn.execute(sa.text("SELECT set_config('tracex.role_name', :r, false)"), {"r": role})
        conn.execute(sa.text("SELECT set_config('tracex.role_pw', :p, false)"), {"p": password})
        conn.execute(
            sa.text(
                """
                DO $$
                BEGIN
                  IF NOT EXISTS (
                      SELECT FROM pg_roles WHERE rolname = current_setting('tracex.role_name')
                  ) THEN
                    EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L',
                                   current_setting('tracex.role_name'),
                                   current_setting('tracex.role_pw'));
                  END IF;
                END
                $$;
                """
            )
        )
    # Do not leave the passwords readable in the session's settings.
    conn.execute(sa.text("SELECT set_config('tracex.role_pw', '', false)"))

    # --- default-deny ------------------------------------------------------
    quoted = ", ".join(f'"{s}"' for s in SCHEMAS)
    op.execute(f"REVOKE ALL ON SCHEMA {quoted} FROM PUBLIC")
    op.execute(
        """
        DO $$
        DECLARE db text := current_database();
        BEGIN
          EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC', db);
          EXECUTE format(
            'GRANT CONNECT ON DATABASE %I TO trace_app, trace_stream, trace_eval, trace_auditor',
            db);
        END
        $$;
        """
    )

    # --- trace_app: read/write app; INSERT-only audit (append-only, ADR-0020)
    op.execute("GRANT USAGE ON SCHEMA app, audit TO trace_app")
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT, INSERT, UPDATE ON TABLES TO trace_app"
    )
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT USAGE, SELECT ON SEQUENCES TO trace_app"
    )
    # INSERT only: never UPDATE or DELETE. The audit log is append-only at the
    # database level, not by application convention.
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT INSERT ON TABLES TO trace_app")
    op.execute(
        "ALTER DEFAULT PRIVILEGES IN SCHEMA audit GRANT USAGE, SELECT ON SEQUENCES TO trace_app"
    )

    # --- trace_stream: read-only on app; its writes land in Delta ----------
    op.execute("GRANT USAGE ON SCHEMA app, external TO trace_stream")
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT ON TABLES TO trace_stream")
    op.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA external GRANT SELECT ON TABLES TO trace_stream")

    # --- trace_eval: the ONLY role that may read groundtruth ---------------
    op.execute("GRANT USAGE ON SCHEMA app, audit, groundtruth, eval, external TO trace_eval")
    for schema in ("app", "audit", "groundtruth"):
        op.execute(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO trace_eval"
        )
    for schema in ("eval", "external"):
        op.execute(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} "
            f"GRANT SELECT, INSERT ON TABLES TO trace_eval"
        )
        op.execute(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} "
            f"GRANT USAGE, SELECT ON SEQUENCES TO trace_eval"
        )

    # --- trace_auditor: read-only including audit; NO groundtruth ----------
    op.execute("GRANT USAGE ON SCHEMA app, audit, eval TO trace_auditor")
    for schema in ("app", "audit", "eval"):
        op.execute(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO trace_auditor"
        )

    # ======================================================================
    # DELIBERATELY ABSENT: any grant of `groundtruth` to trace_app,
    # trace_stream or trace_auditor. Do not add one.
    # ======================================================================


def downgrade() -> None:
    """Drop the schemas and roles this migration created.

    Roles are dropped only if they own nothing else, so a downgrade cannot
    silently destroy objects a later migration created.
    """
    for schema in reversed(SCHEMAS):
        if schema == "app":
            # `app` holds alembic_version; dropping it would erase migration
            # history mid-downgrade. Leave the schema, drop its contents.
            op.execute("DROP TABLE IF EXISTS app.alembic_version_placeholder")
            continue
        op.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')

    conn = op.get_bind()
    for role in ROLES:
        # Role name is bound as a parameter and quoted server-side by format(%I),
        # so no SQL is built by string interpolation here.
        #
        # DROP OWNED BY is required before DROP ROLE: it removes the role's
        # default-ACL references (pg_default_acl) and any granted privileges.
        # Without it DROP ROLE fails with dependent_objects_still_exist and the
        # downgrade silently leaves the roles behind.
        conn.execute(sa.text("SELECT set_config('tracex.role_name', :r, false)"), {"r": role})
        conn.execute(
            sa.text(
                """
                DO $$
                DECLARE r text := current_setting('tracex.role_name');
                BEGIN
                  IF EXISTS (SELECT FROM pg_roles WHERE rolname = r) THEN
                    EXECUTE format('REVOKE ALL ON DATABASE %I FROM %I', current_database(), r);
                    EXECUTE format('DROP OWNED BY %I', r);
                    BEGIN
                      EXECUTE format('DROP ROLE %I', r);
                    EXCEPTION WHEN dependent_objects_still_exist THEN
                      -- The role owns objects created outside this migration.
                      -- Refuse to destroy them; surface it instead.
                      RAISE WARNING 'role % still owns objects and was not dropped', r;
                    END;
                  END IF;
                END
                $$;
                """
            )
        )
