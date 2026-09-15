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
- **Authorization outcomes still change online state**, so their history's completeness is covered
  separately, by the outbox delivery watermark in §5 (user decision, 2026-09-15). No second
  gateway sequencing or durability path is added for them.

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
- **Settled while implementing (slice 3):**
  - Topics are verified at start, and retried on the log's own thread. A topic not yet verified is
    not published to, so no publish waits on broker metadata.
  - A transaction refused for clock skew is refused before its number is assigned.
  - A transaction triage refuses with 503 is still published, with the decision the pipeline
    reached and no case id: the online store recorded it.
  - The gateway producer's delivery timeout is 30 s, because it bounds an unclosed session's gap
    (§5).
  - Without a configured broker nothing is published and no session closes. A `core`-only
    gateway scores normally.
  - A retried identity event under the same idempotency key keeps its envelope `event_id`, derived
    from `(token, key)` as the observation's own id is.

### 4. Close
The session closes only after a confirmed flush: nothing outstanding, no failed delivery report and
nothing shed in the session. The close records `last_seq`, the highest sequence number assigned. Any
other outcome leaves the session unclosed.

### 5. Coverage rule
Bronze (Step 5) applies it; it is stated here because the producer must make it computable.
`trace_core.observation.coverage.assess` implements it once, for Bronze and for the chaos tests.
- Every integer from 1 to `max(seen, last_seq)` must be present in Bronze for the session.
- An unclosed session's gap runs from its last Bronze event to its last heartbeat, plus the heartbeat
  interval, the producer's delivery timeout and the broker-to-PostgreSQL clock margin.
- A session id in Bronze with no session row is a gap.
- A gateway record without session headers is a gap.
- A live session certifies only its contiguous prefix below Bronze's high-water mark.
- Arrival-time gaps map to event time conservatively (§4.1 point 6).
- **Authorization outcomes: the outbox delivery watermark (user decision, 2026-09-15).**
  - `delivered_through` is the time before which every `tx.authorization.v1` outbox row has a
    confirmed Kafka delivery. It is stored per topic (migration 0008), moves only forward, never
    lies in the future, and so survives a restart.
  - The relay advances it in the transaction that marks confirmed rows published. The candidate
    is the least of that transaction's start, the start of the oldest other open `trace_app`
    transaction (read first), and the oldest unpublished row's `created_at`. So it never passes
    an unconfirmed, refused or uncommitted row. This relies on `created_at` being the inserting
    transaction's start (migration 0007) and on only `trace_app` inserting outbox rows.
  - It reads which rows are marked, never how many records Kafka holds, so at-least-once
    duplicates cannot move it.
  - **Feature history is COMPLETE over a horizon only when both paths vouch**
    (`trace_core.observation.history_completeness.assess_history`): no observation-coverage gap
    intersects it, the log was assessed through its end, and the watermark clears its end by the
    clock margin. Any unresolved gap on either path keeps it INCOMPLETE.

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
  - The measured size exceeded the placeholder. The local byte cap is 192 MiB per partition so the
    declaration fits the local disk cap; the numbers are recorded beside it in `topics.yaml`.
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
- **The A/B, pre-registered before any run (2026-09-15).** Written here before the first measurement,
  so neither the rule nor the workload is chosen after seeing a result:
  - **One image, three arms differing only in configuration**, all built from the same clean commit:
    - `log-off`: no broker configured, no relay (the publisher's control);
    - `log-on`: the observation log publishing, no relay (the no-relay control this section names);
    - `log-relay`: the observation log publishing and the relay running in the gateway (option A).
  - **Workload:** the load gate's `representative` profile, its seed, 500 TPS, 180 s per run, on this
    host with the compose limits unchanged.
  - **State:** before every run the gateway is stopped, both Redis instances flushed, the `app` case,
    queue, outbox and authorization tables truncated, and the covered topics deleted and recreated
    from their declarations. Each is verified empty before the gateway restarts.
  - **Order:** `log-off`, `log-on`, `log-relay`, `log-relay`, `log-on`, `log-off`, so drift over the
    session and run-to-run noise are both visible.
  - **Metrics:** the gateway's own decision-latency p99 (`RiskDecision.latency_ms`, the scoring core)
    and the achieved rate, both from the load harness's records. Each record also carries the live
    gateway's `/readyz` checks, so every run's arm is read from the gateway, not from a label.
  - **Rule for the relay's process.** For each metric, the arm's noise is the spread of its own two
    runs. Option A is kept only if, for both metrics, the difference between the `log-relay` and
    `log-on` means is no larger than the larger of the two arms' spreads. Otherwise the relay moves to
    option B, and ADR-0049 is updated to match.
  - **The publisher's cost** (`log-on` against `log-off`) is measured and reported the same way. No
    pass threshold is set for it here; the ROADMAP latency budget still applies to every run.
  - A run that fails the load harness's integrity conditions is recorded and repeated, never dropped.
- **Settled while implementing (slice 4):**
  - One pass is one transaction: claim the oldest unpublished batch, publish, flush, and mark
    published only the rows the broker confirmed. Delivery reports are counted per topic, so a
    batch is confirmed as a whole.
  - A row whose own content cannot be published (an invalid event, an unkeyed one, or a stored
    key its event contradicts) is marked `refused: ...` and never claimed again, so it cannot
    block the queue. Migration 0007 makes a row's content immutable, which is what makes the
    refusal final.
  - A delivery failure is never a refusal: a missing topic, a shed record or an unconfirmed flush
    records an attempt and is retried.
  - Migration 0007 also inserts every row unpublished whatever the writer sends, stamps
    `published_at` once from the database clock, and narrows `trace_app`'s UPDATE to the
    relay's three columns.
  - The relay has its own producer, with a 5 s delivery timeout and a 10 s flush, so every pass
    reaches a verdict.
  - Option A is wired into the gateway behind `TRACE_GATEWAY_OUTBOX_RELAY`, off by default
    until the A/B. Option B is built only if the A/B rules A out.

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
- An identity event that reuses an idempotency key with a different event time, while the replay
  cache is down, gets a different envelope `event_id`, but the store records a conflict under one
  observation id. History and the store then disagree about that event.

## Implementation status
- **Slice 1 (implemented):** migration 0006, `PostgresSessionLedger`, `PostgresWriterLock` and
  `trace_core.observation.session.WriterSession`, with unit tests and integration tests against
  PostgreSQL.
- **Slice 2 (implemented):** the gateway wiring:
  - `trace_core.observation.supervisor.WriterSupervisor`: acquire, heartbeat, lease, takeover grace,
    and re-acquisition with a new session;
  - readiness, the 503 refusals, and the start-up writes behind the fence;
  - unit tests, integration tests, and a chaos test that terminates the writer's backend.
- **Slice 3 (implemented):** `tx.scored.v1` released with its producer, and the observation log:
  - the schema, its RELEASED entry, the codegen model, the `topics.py` key, and a `topics.yaml`
    declaration replacing the reservation, with a measured record size;
  - `trace_core.observation.scored_event` and `trace_core.observation.log.ObservationLog`:
    sequence, write online, produce with `block=False` and session headers, and close only on a
    confirmed flush of every assigned number;
  - identity events that feed a stream are sequenced and published on `identity.events.v1`;
  - the gateway image gains `confluent-kafka`, and only it, from the `stream` extra.
- **Slice 4 (implemented):** `trace_core.observation.outbox_relay.OutboxRelay` and migration 0007,
  wired into the gateway as option A, off by default. Tested against PostgreSQL with a fake
  producer, and end to end against a real broker.
- **Slice 5 (implemented):** the coverage rule (`trace_core.observation.coverage`) and
  `tests/chaos/test_observation_log.py`. A child process composing the production writer, pipeline,
  Redis store and log is killed with SIGKILL at every point of plan §4.1's loss table, or sheds
  against a paused broker; every loss was a detected gap bounded by the last heartbeat.
  - A critic review (2026-09-15) then found the rule wrong in two cases, which the chaos checks
    could not catch: a loss after the ledger snapshot, and a loss before a buffered record's
    arrival. Both were reproduced, and fixes are in progress (docs/PROGRESS.md).
- **Not yet built:** the controlled hot-path A/B, which also decides where the relay runs.

## Status
Proposed
