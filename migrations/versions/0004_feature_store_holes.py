"""The online feature store's hole ledger.

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-13

**Why a table.** An observation the online store failed to record withdraws its completeness
(ADR-0046 §5). When the store is unreachable the withdrawal has to wait for it to come back, and a
gateway that restarts in the meantime must not forget it is owed. The only durable state the
gateway already depends on for readiness is PostgreSQL, so the debt is written here: once per hole
episode, never once per transaction (docs/PHASE3_PLAN.md §4.1), and cleared when the store's epoch
has been moved past it.

DELIBERATELY ABSENT, as in every migration: any grant of `groundtruth` to `trace_app`. `trace_app`
may record a hole and set its `cleared_at` once, and nothing else: it has no DELETE, its UPDATE
grant
covers only `cleared_at`, and a trigger refuses clearing a hole twice or un-clearing it. The record
of an outage cannot be rewritten by the process whose outage it records.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app.feature_store_holes (
            hole_id      bigserial   PRIMARY KEY,
            -- Processing time. A hole is a fact about the deployment, not about any event.
            opened_at    timestamptz NOT NULL DEFAULT now(),
            reason       text        NOT NULL
                           CHECK (reason IN ('UNREACHABLE', 'BREAKER_OPEN', 'REFUSED')),
            instance_id  text        NOT NULL,
            cleared_at   timestamptz,
            CONSTRAINT hole_cleared_after_opened
                CHECK (cleared_at IS NULL OR cleared_at >= opened_at)
        )
        """
    )
    op.execute(
        """
        COMMENT ON TABLE app.feature_store_holes IS
          'Episodes in which the online feature store failed to record an observation. While a '
          'row has no cleared_at, no feature read may claim completeness (ADR-0046 5). One row per '
          'episode, never per transaction.'
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS feature_store_holes_open_idx "
        "ON app.feature_store_holes (opened_at) WHERE cleared_at IS NULL"
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION app.feature_store_holes_clear_once() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF OLD.cleared_at IS NOT NULL THEN
                RAISE EXCEPTION 'hole % is already cleared; its record is immutable', OLD.hole_id;
            END IF;
            IF NEW.cleared_at IS NULL
               OR NEW.hole_id IS DISTINCT FROM OLD.hole_id
               OR NEW.opened_at IS DISTINCT FROM OLD.opened_at
               OR NEW.reason IS DISTINCT FROM OLD.reason
               OR NEW.instance_id IS DISTINCT FROM OLD.instance_id THEN
                RAISE EXCEPTION 'hole % may only be updated by clearing it', OLD.hole_id;
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER feature_store_holes_clear_once BEFORE UPDATE ON app.feature_store_holes "
        "FOR EACH ROW EXECUTE FUNCTION app.feature_store_holes_clear_once()"
    )
    # REVOKE first: migration 0001's default privileges give trace_app table-wide UPDATE on new
    # `app` tables, and a column grant does not narrow a table grant.
    op.execute("REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON app.feature_store_holes FROM trace_app")
    op.execute("GRANT SELECT ON app.feature_store_holes TO trace_app")
    # A hole is recorded by its reason and instance alone: its id, opening time and clearing are
    # the database's, so trace_app cannot insert a hole already cleared or back-dated.
    op.execute("GRANT INSERT (reason, instance_id) ON app.feature_store_holes TO trace_app")
    op.execute("GRANT UPDATE (cleared_at) ON app.feature_store_holes TO trace_app")
    op.execute("GRANT SELECT ON app.feature_store_holes TO trace_stream")
    op.execute("GRANT SELECT ON app.feature_store_holes TO trace_eval")
    op.execute("GRANT USAGE, SELECT ON SEQUENCE app.feature_store_holes_hole_id_seq TO trace_app")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.feature_store_holes CASCADE")
    op.execute("DROP FUNCTION IF EXISTS app.feature_store_holes_clear_once()")
