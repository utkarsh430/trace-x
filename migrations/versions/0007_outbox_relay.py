"""The outbox relay's grants, and the outbox's immutability (ADR-0051 §7; ADR-0007).

Revision ID: 0007
Revises: 0006
Create Date: 2026-09-15

**The relay publishes what the transaction committed, and only marks it.** A relay claims
unpublished rows with `FOR UPDATE SKIP LOCKED` and publishes them. It records `published_at` only
after the broker confirmed the delivery; a failed attempt records `attempts` and `last_error`.
Nothing else about a row may change once it is committed:
- `topic`, `partition_key`, `idempotency_key`, `payload` and `created_at` are immutable, so the
  event the transaction committed is the event relayed;
- a row is inserted unpublished, with no attempts and no error, whatever a client sends, so an
  insert cannot pre-mark its own event published;
- `published_at` is stamped once from the database clock and never cleared or moved;
- `attempts` never decreases.

**What this does not guarantee.** Only the insert is forced unpublished. The relay must set
`published_at`, so `trace_app` holds UPDATE on it, and any `trace_app` connection can mark an
unpublished row published without relaying it. The trigger then only stamps the time. That a row
marked published was delivered rests on the relay being the only code that sets it (ADR-0051 §7),
not on the database.

`trace_app` keeps INSERT, because the gateway writes rows in its own transactions. Its table-wide
UPDATE narrows to the relay's three columns. `SELECT ... FOR UPDATE` needs UPDATE on at least one
column, which that grant gives. It has no DELETE or TRUNCATE.

DELIBERATELY ABSENT, as in every migration: any grant of `groundtruth` to `trace_app`.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0007"
down_revision: str | None = "0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION app.outbox_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'INSERT' THEN
                NEW.created_at := now();
                NEW.published_at := NULL;
                NEW.attempts := 0;
                NEW.last_error := NULL;
                RETURN NEW;
            END IF;
            IF NEW.outbox_id IS DISTINCT FROM OLD.outbox_id
               OR NEW.topic IS DISTINCT FROM OLD.topic
               OR NEW.partition_key IS DISTINCT FROM OLD.partition_key
               OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
               OR NEW.payload IS DISTINCT FROM OLD.payload
               OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
                RAISE EXCEPTION 'outbox row % may not change what it relays', OLD.outbox_id;
            END IF;
            IF OLD.published_at IS NOT NULL
               AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
                RAISE EXCEPTION 'outbox row % was published; its publication is immutable',
                    OLD.outbox_id;
            END IF;
            IF NEW.attempts < OLD.attempts THEN
                RAISE EXCEPTION 'outbox row % may not forget an attempt', OLD.outbox_id;
            END IF;
            IF OLD.published_at IS NULL AND NEW.published_at IS NOT NULL THEN
                NEW.published_at := now();
            END IF;
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER outbox_guard BEFORE INSERT OR UPDATE ON app.outbox "
        "FOR EACH ROW EXECUTE FUNCTION app.outbox_guard()"
    )
    op.execute(
        """
        COMMENT ON COLUMN app.outbox.last_error IS
          'The last failed attempt. Prefixed "refused: " when the row itself cannot be published '
          '(its content or stored key is invalid); such a row is never claimed again, because its '
          'content is immutable. Any other error is a delivery failure and is retried.'
        """
    )
    # A column grant does not narrow a table grant, so the table-wide UPDATE is revoked first
    # (as in 0004 and 0006).
    op.execute("REVOKE UPDATE, DELETE, TRUNCATE ON app.outbox FROM trace_app")
    op.execute("GRANT UPDATE (published_at, attempts, last_error) ON app.outbox TO trace_app")


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS outbox_guard ON app.outbox")
    op.execute("DROP FUNCTION IF EXISTS app.outbox_guard()")
    op.execute("COMMENT ON COLUMN app.outbox.last_error IS NULL")
    op.execute("GRANT UPDATE ON app.outbox TO trace_app")
