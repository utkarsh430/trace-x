"""The authorization outcomes' delivery watermark (ADR-0051 §5; user decision, 2026-09-15).

Revision ID: 0008
Revises: 0007
Create Date: 2026-09-15

**Why.** Authorization outcomes change online state but are not sequenced in a writer session.
Their durable record is PostgreSQL, relayed to Kafka through the transactional outbox (ADR-0049 §4;
ADR-0051 §1). So history that reads them is complete only through the time by which every outcome
recorded earlier has a confirmed delivery. This table holds that time per topic. The outbox relay
advances it, in the same transaction that marks rows published.

- `delivered_through`: every outbox row of the topic created before it has a confirmed delivery.
  It never moves backwards and never lies in the future; the trigger refuses both.
- `advanced_at`: when it was last written, from the database clock.

`trace_app` may create a topic's row and advance it. It may not move it back, delete it or truncate
the table.

DELIBERATELY ABSENT, as in every migration: any grant of `groundtruth` to `trace_app`.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0008"
down_revision: str | None = "0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app.outbox_delivery_watermark (
            topic             text        PRIMARY KEY CHECK (char_length(topic) BETWEEN 1 AND 128),
            delivered_through timestamptz NOT NULL,
            advanced_at       timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        COMMENT ON TABLE app.outbox_delivery_watermark IS
          'Per topic, the time before which every outbox row has a confirmed Kafka delivery. '
          'Advanced only by the outbox relay, only forward (ADR-0051 section 5).'
        """
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION app.outbox_delivery_watermark_guard() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF NEW.delivered_through > now() THEN
                RAISE EXCEPTION 'the % delivery watermark may not lie in the future', NEW.topic;
            END IF;
            IF TG_OP = 'UPDATE' THEN
                IF NEW.topic IS DISTINCT FROM OLD.topic THEN
                    RAISE EXCEPTION 'a delivery watermark may not change its topic';
                END IF;
                IF NEW.delivered_through < OLD.delivered_through THEN
                    RAISE EXCEPTION 'the % delivery watermark may not move backwards', OLD.topic;
                END IF;
            END IF;
            NEW.advanced_at := now();
            RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        "CREATE TRIGGER outbox_delivery_watermark_guard BEFORE INSERT OR UPDATE "
        "ON app.outbox_delivery_watermark FOR EACH ROW "
        "EXECUTE FUNCTION app.outbox_delivery_watermark_guard()"
    )
    # REVOKE first: 0001's default privileges give trace_app table-wide rights on new `app` tables,
    # and a column grant does not narrow a table grant (as in 0006).
    op.execute(
        "REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON app.outbox_delivery_watermark FROM trace_app"
    )
    op.execute("GRANT SELECT ON app.outbox_delivery_watermark TO trace_app")
    op.execute(
        "GRANT INSERT (topic, delivered_through) ON app.outbox_delivery_watermark TO trace_app"
    )
    op.execute("GRANT UPDATE (delivered_through) ON app.outbox_delivery_watermark TO trace_app")
    op.execute("GRANT SELECT ON app.outbox_delivery_watermark TO trace_stream")
    op.execute("GRANT SELECT ON app.outbox_delivery_watermark TO trace_eval")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.outbox_delivery_watermark CASCADE")
    op.execute("DROP FUNCTION IF EXISTS app.outbox_delivery_watermark_guard()")
