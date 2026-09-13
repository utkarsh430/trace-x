"""Cases, the durable investigation queue, and the transactional outbox.

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-12

The tables triage writes, and the reason it can be correct.

**One transaction, three rows.** ADR-0007 states the requirement plainly: the
case row and the queue row must move together. A case marked TRIAGED whose queue
entry was never written is a silently stuck investigation; a queue entry with no
case is an orphan that fails forever. The outbox row joins them, so the event a
consumer will eventually see is committed with the state it describes rather
than published before it or lost after it.

**`cases.trigger_transaction_id` is UNIQUE, and that is the real idempotency
control.** "Duplicate transaction_id yields one effect" (docs/SECURITY.md §9) is
enforced by the database, not by an application check and not by a cache -- the
same reasoning that makes re-seeding a dataset version impossible in 0002.
Redis holds a response-replay cache in front of it, and Redis may be down.

DELIBERATELY ABSENT, as in 0001 and 0002: any grant of `groundtruth` to
`trace_app`. Nothing here reads a label, and nothing here should.

RLS is also deliberately absent. `docs/SECURITY.md` §4 puts row-level security on
`app.cases` for analyst visibility, and analysts arrive with `trace-api` in Phase
5. Adding policies now would mean writing them against roles that do not exist,
which is how a security control comes to be untested.
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # --- cases -------------------------------------------------------------
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app.cases (
            case_id                 text        PRIMARY KEY,
            -- The natural idempotency key. UNIQUE is the control: a duplicate
            -- submission is refused by the database under concurrency, whatever
            -- the application or the cache believes.
            trigger_transaction_id  text        NOT NULL UNIQUE,
            account_id              text        NOT NULL,
            status                  text        NOT NULL,
            risk_band               text        NOT NULL,
            score                   double precision NOT NULL
                                      CHECK (score >= 0.0 AND score <= 1.0),
            -- Provenance of the BEHAVIOUR that opened this case. Rules hot-reload,
            -- so without these a case cannot be explained after the fact.
            rule_pack_id            text        NOT NULL,
            rule_pack_digest        text        NOT NULL,
            threshold_config_digest text        NOT NULL,
            feature_set_version     text        NOT NULL,
            feature_source          text        NOT NULL,
            degraded                boolean     NOT NULL DEFAULT false,
            occurred_at             timestamptz NOT NULL,
            created_at              timestamptz NOT NULL DEFAULT now(),
            updated_at              timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT case_band_opens_investigation
                CHECK (risk_band IN ('HIGH', 'CRITICAL'))
        )
        """
    )
    op.execute(
        """
        COMMENT ON TABLE app.cases IS
          'One case per triaged transaction. trigger_transaction_id is UNIQUE, which is '
          'what makes "duplicate transaction_id yields one effect" a database guarantee '
          'rather than an application convention (docs/SECURITY.md 9, ADR-0033). '
          'occurred_at is EVENT time; created_at is processing time; they are never '
          'interchanged (ADR-0026).'
        """
    )
    op.execute("CREATE INDEX IF NOT EXISTS cases_account_idx ON app.cases (account_id)")
    op.execute("CREATE INDEX IF NOT EXISTS cases_status_idx ON app.cases (status, created_at DESC)")

    # --- case transition history ------------------------------------------
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app.case_transitions (
            transition_id bigserial   PRIMARY KEY,
            case_id       text        NOT NULL REFERENCES app.cases(case_id) ON DELETE CASCADE,
            from_status   text,
            to_status     text        NOT NULL,
            event         text        NOT NULL,
            actor         text        NOT NULL,
            occurred_at   timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        COMMENT ON TABLE app.case_transitions IS
          'Append-only history of the case state machine (ADR-0027). from_status is NULL '
          'only for the opening transition. A silently-dropped transition produces a '
          'plausible-looking case history, which is far harder to find than a crash.'
        """
    )
    op.execute(
        "CREATE INDEX IF NOT EXISTS case_transitions_case_idx "
        "ON app.case_transitions (case_id, transition_id)"
    )

    # --- durable work queue (ADR-0007) -------------------------------------
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app.investigation_queue (
            queue_id         bigserial   PRIMARY KEY,
            -- One queue entry per case. Without UNIQUE, a retry could enqueue a
            -- second entry and two workers would investigate the same case,
            -- spending its budget twice.
            case_id          text        NOT NULL UNIQUE
                                         REFERENCES app.cases(case_id) ON DELETE CASCADE,
            priority         smallint    NOT NULL DEFAULT 0,
            available_at     timestamptz NOT NULL DEFAULT now(),
            leased_by        text,
            lease_expires_at timestamptz,
            attempts         integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            created_at       timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT lease_fields_agree CHECK (
                (leased_by IS NULL AND lease_expires_at IS NULL)
                OR (leased_by IS NOT NULL AND lease_expires_at IS NOT NULL)
            )
        )
        """
    )
    op.execute(
        """
        COMMENT ON TABLE app.investigation_queue IS
          'Postgres-backed durable queue consumed with SELECT ... FOR UPDATE SKIP LOCKED '
          '(ADR-0007). Chosen over Celery/Redis Streams because the enqueue must be '
          'transactional with the case write; a queue entry without a case is an orphan '
          'and a case without an entry is silently stuck.'
        """
    )
    # The claim order for SKIP LOCKED: highest priority first, then oldest.
    # Partial index on unleased rows only -- a leased row is never a claim
    # candidate, and indexing it would grow the hot index for nothing.
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS investigation_queue_claim_idx
        ON app.investigation_queue (priority DESC, available_at)
        WHERE leased_by IS NULL
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS investigation_queue_lease_idx
        ON app.investigation_queue (lease_expires_at)
        WHERE leased_by IS NOT NULL
        """
    )

    # --- transactional outbox ----------------------------------------------
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS app.outbox (
            outbox_id       bigserial   PRIMARY KEY,
            topic           text        NOT NULL,
            partition_key   text        NOT NULL,
            -- The envelope's content hash. UNIQUE with the topic, so a retry of
            -- the same semantic event produces one row rather than a duplicate
            -- a consumer must deduplicate later.
            idempotency_key text        NOT NULL,
            payload         jsonb       NOT NULL,
            created_at      timestamptz NOT NULL DEFAULT now(),
            published_at    timestamptz,
            attempts        integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            last_error      text,
            CONSTRAINT outbox_topic_idempotency UNIQUE (topic, idempotency_key)
        )
        """
    )
    op.execute(
        """
        COMMENT ON TABLE app.outbox IS
          'Events committed in the SAME transaction as the state they describe, relayed '
          'to Kafka in Phase 3. Writing to a broker inside a database transaction is not '
          'atomic: publishing before commit can announce a case that never existed, and '
          'publishing after can lose one. partition_key is stored rather than rederived '
          'so the relay cannot pick a different key than the producer intended '
          '(docs/EVENT_CONTRACTS.md 3).'
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
        ON app.outbox (created_at)
        WHERE published_at IS NULL
        """
    )

    # --- grants -------------------------------------------------------------
    # trace_app only. trace_stream reads app read-only per docs/SECURITY.md 4;
    # the relay that will publish the outbox is a Phase 3 deliverable and gains
    # its UPDATE grant then, in the migration that introduces it.
    for table in ("cases", "case_transitions", "investigation_queue", "outbox"):
        op.execute(f"GRANT SELECT, INSERT, UPDATE ON app.{table} TO trace_app")
        op.execute(f"GRANT SELECT ON app.{table} TO trace_stream")
        op.execute(f"GRANT SELECT ON app.{table} TO trace_eval")
    for sequence in (
        "case_transitions_transition_id_seq",
        "investigation_queue_queue_id_seq",
        "outbox_outbox_id_seq",
    ):
        op.execute(f"GRANT USAGE, SELECT ON SEQUENCE app.{sequence} TO trace_app")


def downgrade() -> None:
    # Reverse dependency order: children before parents, so the FKs drop cleanly.
    for table in ("outbox", "investigation_queue", "case_transitions", "cases"):
        op.execute(f"DROP TABLE IF EXISTS app.{table} CASCADE")
