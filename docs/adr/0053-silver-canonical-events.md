# ADR-0053: Silver: validated, exactly deduplicated, late-tagged canonical events

## Context

Bronze (ADR-0052) holds every record each released topic delivered, as raw bytes, and proves
conservation against its checkpoints. Nothing downstream can use it directly: a Bronze row may be a
poison value, a duplicate, a producer retry under a new `event_id`, a late arrival, or an event from
the future. Gold (Step 7), parity (Step 8) and reconstruction (Step 9) need one row per real event,
typed, with its lateness stated rather than inferred.

The approved plan fixes the constraints (`docs/PHASE3_PLAN.md`):
- **§4.2.** The physical canonical table holds at most one row per declared identity. There is no
  fallback in which Silver holds duplicates and readers deduplicate. If exact dedup cannot meet the
  Phase 3 rate, that is surfaced as an architecture conflict, never solved by weakening uniqueness.
- **§4.3, timing semantics v1.** `is_late` is arrival delay (LogAppendTime − `occurred_at`) over
  600 s. There is no watermarked dedup pre-filter. Future-skew quarantine thresholds are declared.
  A compressed (backfill) replay has `is_late` null. The constants are versioned in code.
- **Q2.** Per-topic dedup identities (`deploy/kafka/topics.yaml`). Silver never deduplicates on
  `idempotency_key` for topics whose identity is something else (eval-v1 shares envelope ids).
- **Q9.** Rows carry `trust_tier`; no tokenization is claimed.
- **ADR-0048.** Delta inserts every duplicate a MERGE source carries, so canonical uniqueness needs
  two mechanisms: deterministic in-batch dedup, then an insert-only MERGE, with a post-write
  uniqueness assertion. Every table is created from its declaration before a query writes it, and is
  written only through `OpenedCheckpoint`.

Two questions were left open for this step:
- how Silver treats a value outside a released enum (from Step 2);
- where the mirrored §4.3 timing constants live.

## Decision

### 1. Tables

One canonical Silver table per released topic, plus two shared tables, all snake_case (ADR-0048, U9):

| Table | Holds | Write |
|---|---|---|
| `silver.<topic_name>` (e.g. `silver.tx_raw_v1`) | one row per declared identity: the envelope and payload as typed columns, the Kafka coordinates of the first record seen, `is_late`, `arrival_delay_ms`, `trust_tier`, lineage | insert-only MERGE on the identity |
| `silver.late_events` | a copy of every canonical row whose `is_late` is true, keyed by topic and identity | insert-only MERGE on (topic, identity) |
| `silver.quarantine` | every Bronze row Silver did not admit: raw bytes, topic, topic id, partition, offset, reason code, detail, consumer version, batch | append |

A late event stays in its canonical table. `late_events` is a copy with a metric, never a removal.

### 2. What becomes a canonical row, in order

For each Bronze row of a micro-batch:
1. **Decode and validate** the value with the topic's generated contract model
   (`model_validate_json`: strict, `extra="forbid"`, enums and patterns). Silver validates with the
   same models producers validate with, so the contract is never re-implemented in Spark.
   Failure: quarantine, reason `invalid_event`.
   - A value outside a released enum fails validation and is quarantined. Adding an enum value is a
     breaking change (`docs/EVENT_CONTRACTS.md` §4), so Silver never guesses at it.
2. **Future skew** (§4.3): quarantine, reason `future_skew`.
   - Gateway-produced events: `occurred_at` more than 86,400 s after the envelope's `ingested_at`
     (the gateway's receipt clock, the one its acceptance check used).
   - Other producers: more than 86,400 s plus a 300 s clock margin after LogAppendTime.
3. **Identity**: the topic's declared dedup identity, read from the validated event.
4. **In-batch dedup**: among rows with one identity, keep the first by (LogAppendTime, partition,
   offset). The rest are duplicates.
   - A duplicate whose canonical content differs from the kept row's is an identity conflict:
     quarantine, reason `identity_conflict`, with both content digests. Content excludes fields a
     producer retry may legitimately change (the envelope's `event_id` and `ingested_at` where the
     identity is `payload.transaction_id`).
   - An identical duplicate is counted, not stored.
5. **Insert-only MERGE** into the canonical table on the identity. A row whose identity already
   exists is not inserted; if its content differs from the stored row, it is an identity conflict
   and quarantined in the same batch.
6. **Lateness** (§4.3): `arrival_delay_ms` = LogAppendTime − `occurred_at`; `is_late` = delay over
   600,000 ms. `is_late` is null when the record carries the backfill replay header
   (`eval/replay/faults.py`), and for batch adapter loads.
7. **After the commit**: assert that the canonical table holds no identity twice. A failure stops
   the query.

### 3. Timing constants

`trace_core.stream.timing` holds timing semantics v1 as versioned constants
(`TIMING_SEMANTICS_VERSION = 1`), with a unit test pinning each value to §4.3. Changing a value
means a new version and an ADR, never a response to a result.

### 4. Queries and checkpoints

One Structured Streaming query per topic, `silver_transform_<topic_name>`, reading that topic's Bronze
table through `OpenedCheckpoint.delta_source` (loss-refusing), with a `foreachBatch` sink making one
idempotent commit per target per batch: the canonical MERGE, the `late_events` MERGE and the
quarantine append.

### 5. Conservation Bronze → Silver

A check, as Bronze's is: for the Bronze range a Silver checkpoint consumed, every Bronze row is
exactly one of an admitted canonical row, a counted identical duplicate, or a quarantine row.
Missing or double-counted rows fail the check.

### 6. Ownership

- Lead: this ADR, the table declarations and registry, the timing module, integration, critic
  review, `make verify` and the commit.
- Spark agent (isolated worktree): the transforms, the `foreachBatch` sink, `services/stream/silver.py`,
  stream and integration tests, the conservation check.

## Acceptance

`P3.event-time`: duplicates (including producer retries with new envelope ids), out-of-order and late
events handled exactly, and future skew quarantined, proved against real Delta and a real broker.

## Alternatives Considered

| Alternative | Why not |
|---|---|
| `dropDuplicatesWithinWatermark`, the original DATA_ENGINEERING §2 | Drops non-duplicates as late after a long backlog or replay (§4.3), and bounds dedup by a watermark instead of the table |
| Spark-native validation from a declared StructType | Re-implements the contract, and diverges from the producers' validation on patterns, enums and extra fields |
| Per-topic quarantine tables | More tables to declare and maintain, with no reader that needs the split; a topic column serves |
| A Silver that keeps duplicates, deduplicated by readers | Forbidden by §4.2: every reader would have to repeat exact dedup, and correctly |

## Consequences

**Positive.**
- One row per real event, typed and validated, for Gold, parity and reconstruction.
- Every Bronze row is accounted for: a canonical row, a counted duplicate, or a quarantine row with
  its reason.
- Lateness is a stored, versioned fact that no reader has to infer.

**Negative.**
- Validation runs the producers' Python models inside Spark, which costs throughput. Step 13
  measures whether Silver still meets the throughput target.
- A MERGE per topic per micro-batch adds commit latency, and joins against a canonical table that
  keeps growing.
- More queries and checkpoints to operate: one per released topic.

**Risks.**
- A producer change to a field inside the conflict digest would quarantine legitimate retries as
  conflicts.
- Insert-only MERGE cost grows with table size until the Step 14 layout benchmark decides clustering.

## Status

Proposed
