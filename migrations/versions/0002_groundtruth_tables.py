"""Ground-truth tables and the write-only trace_generator role.

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-12

Extends the control in 0001 rather than altering it (ADR-0004 is immutable; this
is ADR-0031). Two things land here.

**The tables.** `groundtruth` had no tables until now -- 0001 created the schema
and its grants. Labels, fraud patterns and causal_evidence_keys now have
somewhere to live.

**A fifth role, `trace_generator`.** None of the four existing roles may write
ground truth: trace_app has no grant at all, trace_eval reads it, and the others
are irrelevant. The generator needs to write it, so the alternative to a new role
is running the seed as the migration owner -- which would put owner credentials
in the ordinary development loop and turn "who may write ground truth" from a
grant into a convention. That is exactly the failure ADR-0004 exists to prevent.

trace_generator gets INSERT on the ground-truth tables and SELECT on the dataset
registry ONLY. It cannot read the labels it writes. That is a real property, not
a flourish: it means no credential in the development loop can read ground truth.

DELIBERATELY ABSENT, as in 0001: any grant of groundtruth to trace_app,
trace_stream or trace_auditor. Do not add one.
"""

from __future__ import annotations

import os
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

GENERATOR_ROLE = "trace_generator"
GENERATOR_PW_ENV = "TRACE_GENERATOR_DB_PASSWORD"

# Tables trace_generator may INSERT into but never SELECT from.
WRITE_ONLY_TABLES = ("transaction_labels", "causal_evidence", "scenario_instances")


def upgrade() -> None:
    conn = op.get_bind()

    # --- the role ----------------------------------------------------------
    password = os.getenv(GENERATOR_PW_ENV)
    if not password:
        raise RuntimeError(
            f"{GENERATOR_PW_ENV} is not set, so role '{GENERATOR_ROLE}' would be created "
            f"without a password. Run `cp .env.example .env` (or `make setup`) and export it."
        )
    # Bound parameters -> server-side quoting. No credential is interpolated.
    conn.execute(sa.text("SELECT set_config('tracex.gen_role', :r, false)"), {"r": GENERATOR_ROLE})
    conn.execute(sa.text("SELECT set_config('tracex.gen_pw', :p, false)"), {"p": password})
    conn.execute(
        sa.text(
            """
            DO $$
            BEGIN
              IF NOT EXISTS (
                  SELECT FROM pg_roles WHERE rolname = current_setting('tracex.gen_role')
              ) THEN
                EXECUTE format('CREATE ROLE %I LOGIN PASSWORD %L',
                               current_setting('tracex.gen_role'),
                               current_setting('tracex.gen_pw'));
              END IF;
            END
            $$;
            """
        )
    )
    conn.execute(sa.text("SELECT set_config('tracex.gen_pw', '', false)"))
    op.execute(
        """
        DO $$
        DECLARE db text := current_database();
        BEGIN
          EXECUTE format('GRANT CONNECT ON DATABASE %I TO trace_generator', db);
        END
        $$;
        """
    )

    # --- tables ------------------------------------------------------------
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS groundtruth.datasets (
            dataset_id                   bigserial PRIMARY KEY,
            dataset_version              text        NOT NULL UNIQUE,
            seed                         bigint      NOT NULL,
            generator_version            text        NOT NULL,
            fraud_scenario_config_digest text        NOT NULL,
            dataset_digest               text        NOT NULL,
            row_count                    bigint      NOT NULL CHECK (row_count >= 0),
            created_at                   timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        COMMENT ON TABLE groundtruth.datasets IS
          'Registry of generated datasets. dataset_version is UNIQUE so re-seeding an '
          'existing version is refused by the database rather than silently duplicating '
          'labels. The only ground-truth table trace_generator may SELECT.'
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS groundtruth.transaction_labels (
            dataset_id     bigint  NOT NULL REFERENCES groundtruth.datasets(dataset_id)
                                   ON DELETE CASCADE,
            transaction_id text    NOT NULL,
            is_fraud       boolean NOT NULL,
            fraud_pattern  text,
            scenario_instance_id text,
            PRIMARY KEY (dataset_id, transaction_id),
            -- A fraud label with no pattern is unscoreable; a legitimate label
            -- with one is a contradiction. Enforced here as well as in Python,
            -- because this table outlives any particular writer.
            CONSTRAINT label_pattern_consistency CHECK (
                (is_fraud AND fraud_pattern IS NOT NULL)
                OR (NOT is_fraud AND fraud_pattern IS NULL)
            )
        )
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS groundtruth.causal_evidence (
            dataset_id     bigint NOT NULL REFERENCES groundtruth.datasets(dataset_id)
                                  ON DELETE CASCADE,
            transaction_id text   NOT NULL,
            evidence_kind  text   NOT NULL,
            PRIMARY KEY (dataset_id, transaction_id, evidence_kind)
        )
        """
    )
    op.execute(
        """
        COMMENT ON TABLE groundtruth.causal_evidence IS
          'The facts that actually explain each injected fraud. Evidence precision and '
          'recall are set operations against this table, which is why Track A needs no '
          'LLM judge (docs/EVALUATION.md). Readable only by trace_eval.'
        """
    )
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS groundtruth.scenario_instances (
            dataset_id        bigint NOT NULL REFERENCES groundtruth.datasets(dataset_id)
                                     ON DELETE CASCADE,
            instance_id       text   NOT NULL,
            fraud_pattern     text   NOT NULL,
            participants      jsonb  NOT NULL,
            transaction_count integer NOT NULL CHECK (transaction_count >= 0),
            PRIMARY KEY (dataset_id, instance_id)
        )
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS ix_labels_fraud ON groundtruth.transaction_labels "
        "(dataset_id, is_fraud)"
    )

    # --- grants ------------------------------------------------------------
    # 0001 set ALTER DEFAULT PRIVILEGES for trace_eval, so these tables are
    # already SELECT-able by it. Asserted by the integration suite rather than
    # assumed, because default privileges apply only to the creating role.
    op.execute(f"GRANT USAGE ON SCHEMA groundtruth TO {GENERATOR_ROLE}")
    for table in WRITE_ONLY_TABLES:
        op.execute(f"GRANT INSERT, DELETE ON groundtruth.{table} TO {GENERATOR_ROLE}")
    # The registry, and only the registry, is readable: the generator must be
    # able to check whether a dataset_version already exists before writing one.
    op.execute(f"GRANT SELECT, INSERT, DELETE ON groundtruth.datasets TO {GENERATOR_ROLE}")
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE groundtruth.datasets_dataset_id_seq TO {GENERATOR_ROLE}"
    )

    # ======================================================================
    # DELIBERATELY ABSENT:
    #   * SELECT for trace_generator on transaction_labels / causal_evidence /
    #     scenario_instances -- the writer must not be able to read ground truth
    #     back, so no credential in the development loop can.
    #   * ANY grant of groundtruth to trace_app, trace_stream or trace_auditor.
    # Do not add either.
    # ======================================================================


def downgrade() -> None:
    for table in ("causal_evidence", "scenario_instances", "transaction_labels", "datasets"):
        op.execute(f"DROP TABLE IF EXISTS groundtruth.{table} CASCADE")

    conn = op.get_bind()
    conn.execute(sa.text("SELECT set_config('tracex.gen_role', :r, false)"), {"r": GENERATOR_ROLE})
    conn.execute(
        sa.text(
            """
            DO $$
            DECLARE r text := current_setting('tracex.gen_role');
            BEGIN
              IF EXISTS (SELECT FROM pg_roles WHERE rolname = r) THEN
                EXECUTE format('REVOKE ALL ON DATABASE %I FROM %I', current_database(), r);
                EXECUTE format('DROP OWNED BY %I', r);
                BEGIN
                  EXECUTE format('DROP ROLE %I', r);
                EXCEPTION WHEN dependent_objects_still_exist THEN
                  RAISE WARNING 'role % still owns objects and was not dropped', r;
                END;
              END IF;
            END
            $$;
            """
        )
    )
