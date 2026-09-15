# ADR-0051: The gateway's durable observation log — fenced sessions, contiguous sequences, detectable gaps

- **Status:** Proposed (Phase 3 Step 4). The chaos tests confirm or correct it before acceptance
  (docs/PHASE3_PLAN.md §4.1).
- **Date:** 2026-09-14
- **Phase:** 3
- **Supersedes / Superseded by:** None. Implements `docs/PHASE3_PLAN.md` §4.1 and the "as-served
  order" row of §4.3. No accepted ADR mandates the "bounded local WAL" that `docs/ARCHITECTURE.md` §18
  and `docs/OPERATIONS.md` describe for a Kafka outage: ADR-0035 names a local WAL only as a rejected
  alternative for case creation. Those documentation rows change when publishing lands.

## Context

- **Nothing durable records what the gateway observed.** The online feature store is the only
  record. If Redis is lost, nothing can rebuild the store (Step 9 needs a durable log), and nothing
  can prove what was lost.
- **The approved invariant (§4.1):** anything lost by a crash, by shedding or by a Kafka outage
  becomes a detectable gap, and completeness cannot be claimed across one. No PostgreSQL write
  happens per transaction.
- **What exists:**
  - one producer factory, `trace_core.contracts.publish` (ADR-0047). `publish(block=False)` sheds on a
    full buffer, and its ledger records what was accepted, failed, shed and refused;
  - a single gateway process on the event loop, whose repository calls are bounded (ADR-0039,
    ADR-0035);
  - the feature store's receipts, carrying a store-wide `position` and an epoch;
  - `app.outbox`, written in-transaction by triage and by authorization outcomes, which nothing
    drains;
  - `tx.scored.v1`, PLANNED, with a disk reservation in `deploy/kafka/topics.yaml`.

## Decision

### 1. Scope
- **Covered:** `tx.scored.v1` for every scored transaction, and `identity.events.v1` for every
  identity event whose stream changes online state.
- **Not covered today:** device events change no online state (ADR-0046 §4), so they are not
  sequenced until they do.
- **Not covered, by design:** authorization outcomes and investigations keep the transactional outbox
  (ADR-0007, ADR-0049). Their durability comes from PostgreSQL, not from a session.

### 2. Producer sessions (fencing)
- **Migration 0006, `app.producer_sessions`:** `session_id`, `producer`, `instance_id`, `started_at`,
  `heartbeat_at`, `closed_at`, `last_seq`.
  - A trigger stamps every time from PostgreSQL `now()`, whatever a client sends.
  - It refuses rewriting a session's identity, recording `last_seq` without closing, and any change
    to a closed session.
  - `trace_app` may insert a session's identity and update only its heartbeat and its one close. It
    has no DELETE or TRUNCATE.
- **The lock:** a process becomes the writer only by holding the session-scoped advisory lock
  `WRITER_LOCK_KEY` on one dedicated autocommit connection, and committing its session row on that
  connection.
  - The lock belongs to that backend, so a process that dies loses it with its socket.
  - Without the lock the gateway is not ready and does not score, which is the accepted
    PostgreSQL-down behaviour (ARCHITECTURE §18).
- **Heartbeat:** every few seconds, never per transaction. It confirms that the lock is still this
  backend's (`pg_locks`) and that the row is still open. If either fails, or the database cannot be
  reached, the session is lost, and the process stops online writes and produce at once.
- **Lease:** the request path does not wait for a heartbeat to report a loss. `ready` holds only
  while the fence was confirmed within the lease (6 s, three heartbeat intervals), so a heartbeat
  stuck on a silent network expires the process's claim by itself.
- **Takeover grace:** PostgreSQL can release a lock before its holder learns of it (a restart, a
  terminated backend).
  - A process that acquires the lock while another session is unclosed, and was heartbeated
    within the grace, writes nothing until the grace has passed.
  - The grace is the lease plus a 2 s margin, for a request already past its readiness check.
  - A predecessor that closed cleanly is not waited out, and one that cannot be ruled out is.
  - Both guards compare durations: the process's monotonic clock for its own lease and grace, and
    the database clock for a predecessor's heartbeat age.
- **Connection bounds:** the dedicated connection has a 2 s connect timeout, a 2 s statement
  timeout, TCP keepalives, and `tcp_user_timeout` where the platform supports it.
- **Gateway surface:** while the process is not the writer:
  - `/readyz` returns 503, and `checks.writer_session` names the reason;
  - scoring, identity events a released feature reads, and authorization outcomes are refused
    with 503 and `Retry-After`, before the rate limiter and before anything is recorded;
  - events that change no online state are accepted;
  - `writer_refused_total{surface}` counts the refusals.
  The start-up writes to online state (inheriting holes, dating the epoch) run only after
  acquisition and any grace.

### 3. Write order, per observation
1. Assign the next contiguous sequence number, in process memory.
2. Write the online store, with the atomic `score` or `observe` (Step 1).
3. Produce, even when step 2 failed, recording the observe outcome, the store position and the store
   epoch in the event. The produce uses `block=False`, so a shed record leaves its sequence number
   missing.
- **Session headers:** `tracex-session-id` and `tracex-seq`, never payload fields. A record's semantic
  content and `idempotency_key` must not depend on which session carried it.
- **Failures change no decision.** A shed or failed delivery makes the session unclosable. Scoring
  fails open (CLAUDE.md §3.7).

### 4. Close
The session closes only after a confirmed flush: nothing outstanding, no failed delivery report and
nothing shed in the session. The close records `last_seq`, the highest sequence number assigned. Any
other outcome leaves the session unclosed.

### 5. Coverage rule
Bronze (Step 5) applies it; it is stated here because the producer must make it computable.
- Every integer from 1 to `max(seen, last_seq)` must be present in Bronze for the session.
- An unclosed session's gap runs from its last Bronze event to its last heartbeat, plus the heartbeat
  interval, the producer's delivery timeout and the broker-to-PostgreSQL clock margin.
- A session id in Bronze with no session row is a gap.
- A gateway record without session headers is a gap.
- A live session certifies only its contiguous prefix below Bronze's high-water mark.
- Arrival-time gaps map to event time conservatively (§4.1 point 6).

### 6. `tx.scored.v1`
- **Key:** `account_id`. **Dedup identity:** `payload.transaction_id`.
- **Payload:**
  - the transaction as served;
  - the decision summary: decision, band, score, degraded reasons, rule pack and threshold digests,
    feature set version;
  - `observe_outcome`, `store_position` and `store_epoch`.
- **Release:** with its producer: the schema, its RELEASED entry, the codegen model, the `topics.py`
  key, and a `topics.yaml` declaration replacing the reservation.
- **Measured before declaring:** the record size replaces the reservation's placeholder.
- **`identity.events.v1`** gains the gateway as a producer. That is ledger metadata, not a schema
  change.

### 7. Outbox relay
- **Drain:** `app.outbox` with `FOR UPDATE SKIP LOCKED`, in small batches. Publish through the
  factory, and set `published_at` only after a confirmed delivery report.
- **Delivery:** at least once. Consumers deduplicate by the per-topic identity.
- **Process: decided by measurement.**
  - Option A: a relay inside `trace-gateway`, ADR-0049's wording.
  - Option B: the `trace-worker` entrypoint.
  - Neither adds a network boundary (ADR-0001). ADR-0039 measured GIL-bound work contending with the
    hot path, so A is not assumed safe.
  - The deciding test is Step 4's controlled hot-path A/B. A is kept only if the scoring-core p99
    and achieved throughput stay within the no-relay control's run-to-run noise. Otherwise B.
    ADR-0049 is updated to match.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| A bounded local WAL, then shed (the current docs) | A local WAL is lost with its disk and host, and needs its own coverage proof; §4.1 makes loss detectable instead, with no new durable store |
| An outbox row per scored transaction | A PostgreSQL write per transaction on the hot path (ADR-0002; the release ledger's planned note rejects it) |
| Block-reservation sequence ranges in PostgreSQL | Not required to prove the invariant (§4.1), and adds a coordination write |
| Kafka transactions (exactly-once producer) | They do not include the Redis write, so the gap would still need detecting, at higher latency |
| Several unfenced writers | Out of scope for Phase 3 (§4.1 point 2); needs a cross-process sequence |
| A lease column instead of an advisory lock | A lease needs a clock-based expiry and a write to renew it; the advisory lock ends exactly with its connection, and the heartbeat already bounds the gap |

## Consequences
**Positive.**
- Every loss is a bounded, detectable gap.
- Step 9 can rebuild the store and claim completeness only where coverage holds.
- The outbox finally drains.

**Negative.**
- PostgreSQL becomes a readiness dependency for scoring through the writer's lock, as it already is
  for triage.
- Each gateway holds one dedicated connection and writes a heartbeat every few seconds.
- The event loop gains one produce per scored request, whose latency the controlled A/B must measure.
- An outage makes history incomplete across its gap.

**Risks.**
- Producer callbacks, served by `poll` on the event loop, could stall it under a slow broker. The
  signal is the A/B's scoring-core p99; the mitigation is measured, not assumed.
- A network partition can keep a stale backend's lock until PostgreSQL notices the dead connection,
  so no successor takes over until then. The partitioned process stops writing when its lease
  expires, and its gap is bounded by its last heartbeat.
- A process suspended between its readiness check and its write (a paused VM) can outlive both its
  lease and a successor's grace. The store checks no fencing token, so an overlap is not prevented.
  The session headers planned for published observations (slice 3) are what could make it
  detectable.
- A misestimated clock margin would misplace gap edges. The signal is the coverage tests at gap
  boundaries.

## Implementation status
- **Slice 1 (implemented):** migration 0006, `PostgresSessionLedger`, `PostgresWriterLock` and
  `trace_core.observation.session.WriterSession`, with unit tests and integration tests against
  PostgreSQL.
- **Slice 2 (implemented):** the gateway wiring:
  - `trace_core.observation.supervisor.WriterSupervisor`: acquire, heartbeat, lease, takeover grace,
    and re-acquisition with a new session;
  - readiness, the 503 refusals, and the start-up writes behind the fence;
  - unit tests, integration tests, and a chaos test that terminates the writer's backend.
- **Not yet built:**
  - sequencing and session headers;
  - `tx.scored.v1`;
  - the outbox relay;
  - the chaos tests;
  - the A/B.

## Status
Proposed
