"""Producer sessions: the fenced writer behind the gateway's observation log.

Revision ID: 0006
Revises: 0005
Create Date: 2026-09-14

**Why a table** (docs/PHASE3_PLAN.md §4.1, ADR-0051). Every observation the gateway publishes
carries its session id and a contiguous sequence number, and coverage is judged against this row. A
session that closed cleanly vouches for sequences 1..last_seq. An unclosed one is a gap bounded by
its last heartbeat. A session id in Bronze with no row here is itself a gap.

**One writer at a time.** A process becomes the writer only by holding a session-scoped advisory
lock and committing its row here. It heartbeats every few seconds, never once per transaction, and
closes only after a confirmed flush.

**Clocks are the database's.** `started_at`, `heartbeat_at` and `closed_at` come from PostgreSQL
`now()`; the trigger overwrites whatever a client sends. The coverage rule measures gaps against one
declared clock.

DELIBERATELY ABSENT, as in every migration: any grant of `groundtruth` to `trace_app`. `trace_app`
may open a session by naming it, heartbeat an open session, and close it once. It cannot rewrite a
session's identity, reopen one, or delete the record of a session that ended badly.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app.producer_sessions (
            session_id   text        PRIMARY KEY
                                     CHECK (char_length(session_id) BETWEEN 1 AND 64),
            producer     text        NOT NULL CHECK (char_length(producer) BETWEEN 1 AND 128),
            instance_id  text        NOT NULL CHECK (char_length(instance_id) BETWEEN 1 AND 128),
            -- PROCESSING time, the database's clock (plan §4.1 point 5).
            started_at   timestamptz NOT NULL DEFAULT now(),
            heartbeat_at timestamptz NOT NULL DEFAULT now(),
            closed_at    timestamptz,
            -- The highest sequence number ASSIGNED, written only with a confirmed flush.
            last_seq     bigint      CHECK (last_seq >= 0),
            CONSTRAINT producer_session_closed_together
                CHECK ((closed_at IS NULL) = (last_seq IS NULL)),
            CONSTRAINT producer_session_heartbeat_after_start
                CHECK (heartbeat_at >= started_at),
            CONSTRAINT producer_session_closed_after_heartbeat
                CHECK (closed_at IS NULL OR closed_at >= heartbeat_at)
        )
        """
    )
    op.execute(
        """
        COMMENT ON TABLE app.producer_sessions IS
          'One row per fenced producer session of the observation log (ADR-0051). A closed row '
          'vouches for sequences 1..last_seq; an unclosed row is a gap bounded by heartbeat_at. '
          'Times are the database clock; trace_app may heartbeat an open session and close it once.'
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS producer_sessions_open_idx "
        "ON app.producer_sessions (producer, heartbeat_at) WHERE closed_at IS NULL"
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION app.producer_sessions_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                NEW.started_at := now();
                NEW.heartbeat_at := NEW.started_at;
                NEW.closed_at := NULL;
                NEW.last_seq := NULL;
                RETURN NEW;
            END IF;
            IF NEW.session_id IS DISTINCT FROM OLD.session_id
               OR NEW.producer IS DISTINCT FROM OLD.producer
               OR NEW.instance_id IS DISTINCT FROM OLD.instance_id
               OR NEW.started_at IS DISTINCT FROM OLD.started_at THEN
                RAISE EXCEPTION 'producer session % may not change its identity',
                    OLD.session_id;
            END IF;
            IF OLD.closed_at IS NOT NULL THEN
                RAISE EXCEPTION 'producer session % is closed; its record is immutable',
                    OLD.session_id;
            END IF;
            IF NEW.closed_at IS NULL AND NEW.last_seq IS NOT NULL THEN
                RAISE EXCEPTION 'producer session % records last_seq only when it closes',
                    OLD.session_id;
            END IF;
            NEW.heartbeat_at := greatest(now(), OLD.heartbeat_at);
            IF NEW.closed_at IS NOT NULL THEN
                NEW.closed_at := NEW.heartbeat_at;
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER producer_sessions_guard BEFORE INSERT OR UPDATE ON app.producer_sessions "
        "FOR EACH ROW EXECUTE FUNCTION app.producer_sessions_guard()"
    )
    # REVOKE first: 0001's default privileges give trace_app table-wide UPDATE on new `app` tables,
    # and a column grant does not narrow a table grant (as in 0004).
    op.execute("REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON app.producer_sessions FROM trace_app")
    op.execute("GRANT SELECT ON app.producer_sessions TO trace_app")
    op.execute(
        "GRANT INSERT (session_id, producer, instance_id) ON app.producer_sessions TO trace_app"
    )
    op.execute(
        "GRANT UPDATE (heartbeat_at, closed_at, last_seq) ON app.producer_sessions TO trace_app"
    )
    op.execute("GRANT SELECT ON app.producer_sessions TO trace_stream")
    op.execute("GRANT SELECT ON app.producer_sessions TO trace_eval")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.producer_sessions CASCADE")
    op.execute("DROP FUNCTION IF EXISTS app.producer_sessions_guard()")
