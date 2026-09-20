"""Authorization outcomes: the system of record behind `tx.authorization.v1`.

Revision ID: 0005
Revises: 0004
Create Date: 2026-09-14

**The first delivery is the observation** (ADR-0049 §2, §4). `transaction_id` is the primary key,
so a second delivery for one transaction is settled by the database -- a duplicate when its content
matches, a conflict when it does not -- whatever an online cache holds or has forgotten. `trace_app`
may insert and read, and nothing else: with no UPDATE and no DELETE, a conflicting delivery cannot
overwrite the outcome recorded first.

**The revoke is explicit, and it has to be.** Migration 0001 set `ALTER DEFAULT PRIVILEGES IN SCHEMA
app GRANT SELECT, INSERT, UPDATE ON TABLES TO trace_app`, so every table created in `app` arrives
with UPDATE for `trace_app`. A migration that only grants what it wants leaves that UPDATE in place.
Found by `tests/integration/test_authorization_store.py`, which tried the rewrite and succeeded.

**Written with its outbox row, in one transaction.** The outcome and the event that relays it to
`tx.authorization.v1` commit together, as triage's case and its `investigation.requested.v1` row do
(ADR-0007). The relay drains `app.outbox`; nothing here publishes.

DELIBERATELY ABSENT, as in every migration: any grant of `groundtruth` to `trace_app`. An outcome is
what an authorization system decided, not whether a transaction was fraud.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app.authorization_outcomes (
            transaction_id          text        PRIMARY KEY
                                    CHECK (char_length(transaction_id) BETWEEN 1 AND 64),
            account_id              text        NOT NULL,
            authorization_outcome   text        NOT NULL
                                    CHECK (authorization_outcome IN ('APPROVED', 'DECLINED')),
            -- EVENT time: when the authorization system decided, at the millisecond published.
            decided_at              timestamptz NOT NULL,
            -- The transaction's event time, as the outcome's producer reported it.
            transaction_occurred_at timestamptz NOT NULL,
            -- The published event's id, so an identical redelivery is answered with it.
            event_id                text        NOT NULL,
            -- PROCESSING time. Never used for business logic (ADR-0026).
            received_at             timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT authorization_decided_not_before_transaction
                CHECK (decided_at >= transaction_occurred_at)
        )
        """
    )
    op.execute(
        """
        COMMENT ON TABLE app.authorization_outcomes IS
          'One authorization outcome per transaction, the first delivery (ADR-0049). The '
          'primary key decides duplicates and conflicts; trace_app has no UPDATE or DELETE, so a '
          'conflicting delivery cannot overwrite the observation. decided_at is EVENT time; '
          'received_at is processing time; they are never interchanged (ADR-0026).'
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS authorization_outcomes_account_idx "
        "ON app.authorization_outcomes (account_id, decided_at)"
    )
    # 0001's default privileges already granted UPDATE: take back everything but read and insert.
    op.execute("REVOKE UPDATE, DELETE, TRUNCATE ON app.authorization_outcomes FROM trace_app")
    op.execute("GRANT SELECT, INSERT ON app.authorization_outcomes TO trace_app")
    op.execute("GRANT SELECT ON app.authorization_outcomes TO trace_stream")
    op.execute("GRANT SELECT ON app.authorization_outcomes TO trace_eval")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.authorization_outcomes CASCADE")
