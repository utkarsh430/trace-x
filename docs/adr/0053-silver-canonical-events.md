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

Revised after implementation and critic review, before acceptance. The revisions:
- a `silver.duplicates` table;
- `tx.scored.v1`'s preference order and content digest;
- `silver.late_events` as a projection of the canonical tables;
- the `replayed` and `superseded` dispositions;
- the full set of quarantine reasons;
- the recorded limits (§7).

## Decision

### 1. Tables

One canonical Silver table per released topic, plus three shared tables, all snake_case (ADR-0048, U9):

| Table | Holds | Write | `delta.appendOnly` |
|---|---|---|---|
| `silver.<topic_name>` (e.g. `silver.tx_raw_v1`) | one row per declared identity: the envelope and payload as typed columns, the Kafka coordinates of the delivery it keeps, its content digest, `is_late`, `arrival_delay_ms`, `trust_tier`, lineage | MERGE on the identity: insert-only, except `silver.tx_scored_v1`, where a preferred delivery of the same content replaces the stored one (§2.5) | yes; `silver.tx_scored_v1` no |
| `silver.duplicates` | every admitted Bronze row that is not its identity's canonical row but carries its content: coordinates, topic, identity, digest, disposition (`duplicate` or `superseded`), consumer version, batch | append | yes |
| `silver.late_events` | exactly the canonical rows whose `is_late` is true, keyed by topic and identity; partitioned by `silver_topic` | MERGE from the committed canonical rows (§2.8) | no |
| `silver.quarantine` | every Bronze row Silver did not admit: raw bytes, topic, topic id, partition, offset, reason, detail, consumer version, batch | append | yes |

- A late event stays in its canonical table; `late_events` never removes one.
- `silver.duplicates` and `silver.quarantine` rows belong to the checkpoint version that wrote them. A
  reset checkpoint reprocesses Bronze and records them again under its new version, so readers
  filter by consumer version.
- The two tables without `appendOnly` are the ones whose rows legitimately change. Delta cannot
  enforce their shape, so the uniqueness assertion and conservation (§5) do.
- **`silver.late_events` is partitioned by `silver_topic`, for correctness, not layout.**
  - *Why.* Every topic's query MERGEs into it, concurrently under `silver run`. For an unpartitioned
    table Delta's conflict check has no partition predicate to separate those writers.
  - *Reproduced on Delta 4.0.1.* Before the partition, 7 of 16 barrier-released concurrent sink
    batches of two topics failed with `ConcurrentAppendException` ("Files were added to the root of
    the table by a concurrent update"), which stops the job. Every conflict was the other topic's
    `late_events` MERGE. The appends to `silver.duplicates` and `silver.quarantine` never
    conflicted.
  - *How it is kept.* The MERGE condition carries the literal topic predicate, so each writer
    records one partition. The Step 14 layout benchmark may not remove the partition.

### 2. What becomes a canonical row, in order

For each Bronze row of a micro-batch:
1. **Decode and validate** the value with the topic's generated contract model
   (`model_validate_json`: strict, `extra="forbid"`, enums and patterns). Silver validates with the
   same models producers validate with, so the contract is never re-implemented in Spark.
   Quarantine reasons, checked in this order:
   - `null_value`: the record has no value.
   - `not_log_append_time`: the record's timestamp is not LogAppendTime, so its arrival delay
     cannot be measured. Every released topic is declared LogAppendTime (ADR-0052).
   - `invalid_event`: the model rejects the value.
   - `unrepresentable`: a valid event holds an integer outside its column's 64-bit range, which
     Spark would otherwise store as null without a word.
   - A value outside a released enum fails validation and is quarantined. Adding an enum value is a
     breaking change (`docs/EVENT_CONTRACTS.md` §4), so Silver never guesses at it.
   - `detail` names only field paths the model declares, and error types. A key the input supplied
     is written `<extra>`, so field names an attacker chose never reach the table.
2. **Future skew** (§4.3): quarantine, reason `future_skew`.
   - Gateway-produced events: `occurred_at` more than 86,400 s after the envelope's `ingested_at`
     (the gateway's receipt clock, the one its acceptance check used).
   - Other producers: more than 86,400 s plus a 300 s clock margin after LogAppendTime.
3. **Identity**: the topic's declared dedup identity, read from the validated event.
4. **In-batch dedup**: among rows with one identity, keep the first in the topic's preference
   order. The rest go to `silver.duplicates` as `duplicate`.
   - Every topic but one: (LogAppendTime, partition, offset, topic id).
   - `tx.scored.v1`: `observe_outcome = RECORDED` first, then (`store_epoch`, `store_position`) with
     nulls last, then (LogAppendTime, partition, offset, topic id).
     - Why: a gateway retry republishes a scored transaction as `REDELIVERY`, carrying the store
       counter at the retry. The as-served order (§4.3) is the recorded delivery's position, and a
       retry can arrive first.
     - More than one RECORDED delivery can exist: a store restart starts a new epoch, and an expired
       identity key records again. The earliest epoch and position wins.
   - **Content digest**: the validated event minus fields a retry may legitimately change.
     - Every topic: `ingested_at`, `trace_id`, and the producer's `@version` suffix (a retry can
       cross a redeploy). `event_id` too, unless the identity is `envelope.event_id`.
     - `tx.scored.v1` also: the scoring results the gateway recomputes on each delivery
       (`observe_outcome`, `store_epoch`, `store_position`, `decision_summary`, `served_features`) and
       the envelope `idempotency_key`.
   - A duplicate whose digest differs from the kept row's is an identity conflict: quarantine,
     reason `identity_conflict`, with both digests.
5. **MERGE** into the canonical table on the identity, for rows whose identity is already stored:
   - Same coordinates as the stored row: `replayed`, the batch re-run after a crash that followed
     the commit. Nothing is written. A replay is recognised only against a committed row, so a
     coordinate that appears twice within one batch is a duplicate, in the pure rules as in Spark.
   - Different digest: `identity_conflict`, quarantined in the same batch.
   - Same digest: not inserted, recorded as `duplicate`. In `silver.tx_scored_v1` only, a delivery
     that precedes the stored row in the preference order replaces it, and the stored row goes to
     `silver.duplicates` as `superseded`.
   - Because in-batch and cross-batch use one total order, a `tx.scored.v1` identity's canonical row
     does not depend on where micro-batches fall. For every other topic the first batch's row stays.
     That is independent of batch boundaries only while an identity's deliveries share a partition
     (§7).
6. **Lateness** (§4.3):
   - `arrival_delay_ms` = LogAppendTime − `occurred_at`, floored to whole milliseconds.
   - `is_late` = `arrival_delay_ms` over 600,000 (`timing.is_late_ms`), judged on the stored
     milliseconds so the two stored facts never disagree.
   - `is_late` is null when the record carries the backfill replay header (`eval/replay/faults.py`).
     There is no batch adapter load path yet; when one is added, it writes null too.
7. **After the commit**: assert that the canonical table holds no identity twice. A failure stops
   the query.
8. **Late events**: after the canonical commit, the committed canonical rows for the batch's
   identities are MERGEd into `silver.late_events` on (topic, identity). A late row is inserted or,
   when its coordinates changed, updated; a row no longer late is deleted. A replay or a reset
   checkpoint changes nothing, and a crash between the two commits is re-derived on replay.

### 3. Timing constants

`trace_core.stream.timing` holds timing semantics v1 as versioned constants
(`TIMING_SEMANTICS_VERSION = 1`) and the functions that apply them (`is_late_ms`, `future_skewed`),
with a unit test pinning each value to §4.3. Changing a value
means a new version and an ADR, never a response to a result.

### 4. Queries and checkpoints

- **Queries.** One Structured Streaming query per topic, `silver_transform_<topic_name>`. Each reads that
  topic's Bronze table through `OpenedCheckpoint.delta_source` (loss-refusing), starting at Bronze
  version 0 on a new checkpoint version. A source offset naming a negative version is refused.
- **Sink.** A `foreachBatch` sink makes one idempotent commit per target per batch, each through the
  checkpoint's Delta app id and batch version. The targets commit in this order, and the order is
  load-bearing: the `silver.quarantine` append, the `silver.duplicates` append, the canonical
  MERGE, then the `late_events` MERGE. A crash between two commits therefore leaves a *prefix* of
  that sequence -- the five reachable states the Step 10 resume cases enumerate, never a state in
  which the canonical MERGE landed alone.
- **CLI.** `python -m services.stream.silver run` exits 3 when any query failed. It reads each
  query's liveness before its failure, because Spark records a failure before it marks the query
  terminated. A requested stop stops every query before judging them.

### 5. Conservation Bronze → Silver

A check, as Bronze's is (`python -m services.stream.silver conservation`).
- **Per Bronze row.** For the Bronze versions a Silver checkpoint version consumed, every Bronze row
  is exactly one of: the canonical row (by coordinates), a `silver.duplicates` row, or a quarantine
  row.
- **Across tables.**
  - Every `silver.duplicates` row has a canonical row with the same identity and digest.
  - Every `silver.late_events` row equals a canonical row's identity and coordinates, and that row
    is late.
  - No identity is canonical twice.
- Missing, double-counted or dangling rows fail the check, and so does a Bronze coordinate that
  appears twice in Bronze itself (`bronze_repeated`).

### 6. Ownership

- Lead: this ADR, the table declarations and registry, the timing module, integration, critic
  review, `make verify` and the commit.
- Spark agent (isolated worktree): the transforms, the `foreachBatch` sink, `services/stream/silver.py`,
  stream and integration tests, the conservation check.

### 7. Recorded limits

Known and not solved in Step 6:
- **Partly consumed Bronze version.** A micro-batch that stops inside one Bronze version cannot be
  conserved exactly; the check fails closed rather than passing.
- **Starting from version 0 — amended by ADR-0052 Amendment 1, point 6 (Step 11).** Before any
  retention floor exists, a new Silver checkpoint starts at Bronze version 0, as above. Once the
  audited local retention floor has run, it does not: `tables.retention_start` gives the lowest
  Bronze version that added a data file still live, read from one snapshot, and refuses when a live
  file was added before the retained log or by a `dataChange=false` rewrite. A resumed checkpoint
  keeps its recorded start (`silver.silver_start_version`); a reset publishes the new start
  (`maintenance.reset_silver`). `delta_source` refuses a Bronze reader that has committed no batch
  when floors exist and its start is any other version, so version 0 is then refused, as is a start
  that would skip live rows. A Silver checkpoint must exist before any floor advances. Silver's
  Bronze reader sets `ignoreDeletes=true` (Amendment 1, point 5), which passes removes-only commits
  and never a rewrite.
- **Boundary-dependent canonical row.** Outside `tx.scored.v1`, an identity's canonical row
  depends on micro-batch boundaries when its deliveries land on different partitions. Topic keys
  put a retry on its original's partition, so this needs a producer that changed the key.
- **One writer per canonical table.** Queries for different topics are tested concurrently: sink
  batches released together, and every topic's query in one session. Two writers to the same
  canonical table, such as two jobs for one topic, are not supported and not tested. Their MERGEs
  would conflict, and nothing retries.
- **Private model mapping.** Silver reads the generated models through `publish._models`, a
  private mapping; a public accessor is deferred.

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
| Identical duplicates counted, not stored | Conservation could not account for each Bronze row by its coordinates, so a restart that re-counted would pass unnoticed |
| `tx.scored.v1` keeps its first arrival, like every other topic | A retry arriving first would become canonical, with a retry's store position and outcome, so as-served replay (Steps 8, 9) would place the transaction wrongly |
| `late_events` as an append-only log with an anti-join | A superseding row leaves a stale or missing late copy, and every table stays appendOnly only by giving up that consistency |
| Unpartitioned `late_events`, retrying a MERGE that hits a concurrent-write conflict | Reproduced conflicts on 7 of 16 concurrent batches; retrying adds a loop to every sink and still makes topics wait on each other, where a topic partition removes the conflict |

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
- `silver.tx_scored_v1` and `silver.late_events` are not `appendOnly`. Delta no longer refuses an
  update or delete there; the uniqueness assertion and conservation are the guard.

**Risks.**
- A producer change to a field inside the conflict digest would quarantine legitimate retries as
  conflicts. Conversely, a field excluded from `tx.scored.v1`'s digest that is really content would
  hide a genuine conflict.
- Insert-only MERGE cost grows with table size until the Step 14 layout benchmark decides clustering.

## Status

Proposed

## Amendment 1 (2026-09-18): the canonical and `late_events` MERGEs are bounded on `occurred_at`

- **Status of this amendment:** Proposed. The user approved bounding the MERGE (option (a)) after
  the Step 13 stream benchmark (`bench-20260918-141621-stream-throughput-50aad177`, FAIL). The Step 13
  record holds what that run measured; this amendment adds no measurement of its own.
- **What it does not change:** §4.2 uniqueness, the dispositions, the commit order (§4), and timing
  semantics v1. No lateness bound is added. A record of any age is admitted and deduplicated.

### Context

The canonical MERGE's condition was `t.silver_identity = s.silver_identity` and nothing else, so
Delta read every file of the canonical table on every micro-batch (the Consequences above already
named this growing join). The `late_events` MERGE read its topic's whole partition, and so did the
read of the committed rows that feeds it. Per-batch cost therefore grew with the table.

### Why no lateness or arrival bound exists, and why none is needed

- **Nothing bounds how far apart one identity's deliveries are in arrival or event time.** `is_late`
  is a tag, not a filter (§2.6). The only time rule that rejects a record is future skew (§2.2). It
  bounds `occurred_at` above the record's own arrival, and never how far one delivery is from
  another. A retry days later, a replay, or a checkpoint reset reaches the same identity at any
  distance. Pruning on `kafka_timestamp`, `silver_admitted_at` or any lateness window would
  therefore break exact deduplication.
- **One identity's rows share one `occurred_at`.** This is an equality, not a window:
  - The content digest covers `envelope.occurred_at` for every topic
    (`silver_rules.content_digest`; `silver_rules.DIGESTED_EVENT_TIME`, which the module refuses at
    import to exclude).
  - An insert-only canonical table never replaces a row.
  - A `tx.scored.v1` supersede requires the stored row's digest, in classification
    (`silver.classify_frame`, `eligible`) and again in the MERGE's update condition
    (`_canonical_merge`, `t.content_digest = s.content_digest`).
  - So every row an identity has had, and the `late_events` row copied from one of them, carry one
    `occurred_at`.

### Decision

1. **Canonical MERGE.** The condition is `t.silver_identity = s.silver_identity AND` a target-only
   range on `t.occurred_at` (`silver.canonical_merge_condition`):
   - The range is the batch's superseding rows' `occurred_at`, or `FALSE` when the batch supersedes
     nothing, which is every batch of every topic but `tx.scored.v1` and most of its batches.
   - It is exact. The source holds `admit` and `supersede` rows only.
     - An `admit` row's identity has no committed row. Classification read, by identity and
       unbounded, the table this MERGE writes, and §7 requires one writer per canonical table. So
       the row matches nothing under any predicate.
     - A `supersede` row matches its identity's committed row, whose `occurred_at` is its own.
2. **`late_events`.** After the canonical commit, each batch identity has one committed row:
   - the batch's `admit` or `supersede` row;
   - or the row classification found. Classification now also returns `existing_occurred_at_us`,
     and a `duplicate` or `replayed` row equals that row's `occurred_at`.

   Both the read of the committed rows and the MERGE's target (`late_events_merge_condition`,
   after the partition literal of §1) are bounded by the range of those values. A `conflict` row's
   own `occurred_at` is left out: it differs by definition and never becomes canonical. Its
   identity's stored row stays in the range through `existing_occurred_at_us`. The set of
   identities re-derived is unchanged.
3. **Literals Delta can skip files with.**
   - The bounds are `TIMESTAMP '… +00:00'` literals, compared with each file's min/max statistics.
     `occurred_at` is the fourth column of every canonical table and the third of `late_events`,
     inside Delta's default 32 statistics columns. A unit test holds this.
   - Each bound is widened by 1 ms (`STATS_SLACK_US`), because Delta records timestamp statistics
     at millisecond precision. Widening only adds files to read.
   - Silver appends roughly in arrival order, so file statistics are roughly time-clustered. A file
     that holds a very late event, or one up to the 24 h future-skew limit, has wide statistics and
     is read more often. That costs performance, never correctness.
4. **Guards: a failed premise is refused, never deduplicated against part of the table.**
   - *Before the batch's first commit* (`batch_merge_bounds`): the batch is refused with
     `SilverPruningError` when any of these holds:
     - a `supersede` row's `occurred_at` differs from the row it replaces;
     - a bound is half-open;
     - the superseding range falls outside the committed range.
   - *Before the `late_events` MERGE*: the batch is refused when any batch identity has no committed
     row inside the range.
   - *After the writes*: `_assert_unique` still reads the canonical table and the topic's
     `late_events` partition by identity, unbounded. So a duplicate that pruning could only create
     if the premise broke stops the query.

### Consequences

- The two MERGEs, and the `late_events` source read, no longer grow with the table. For a batch
  that supersedes nothing, the canonical MERGE reads no target file.
- **Still O(table) per batch, and not bounded here:**
  - classification's identity lookup (`classify_frame`, the `canonical.join(identities)` semi-join);
  - the post-write uniqueness assertion (`_assert_unique`), which reads the canonical table by
    identity and the topic's whole `late_events` partition.

  They cannot be bounded on `occurred_at` exactly. A `conflict`, by definition, may carry any
  `occurred_at`. Finding it, and proving that an `admit` identity has no row at all, needs a lookup
  by identity. Bounding them needs a user decision: either an identity-organised layout or index
  (Step 14 / ADR-0015 territory), or a declared dedup horizon, which §4.2 forbids.
- **Two writers to one canonical table** (unsupported, §7): the unbounded MERGE read every file, so
  Delta's conflict check refused the second writer's commit. With the MERGE reading no file, both
  commits can land. The uniqueness assertion then stops the query after the fact, and
  conservation fails.
- **Evidence status.**
  - The stream tests in `tests/stream/test_silver_bounded_merge.py` are written: exact dedup on a
    multi-file table, a supersede at both edges of the range, and file-level pruning. Pruning is
    shown by deleting every out-of-range data file: the bounded MERGE succeeds, and the unbounded
    control fails with `FAILED_READ_FILE.FILE_NOT_EXIST`.
  - Those stream tests have not been run as of this amendment.
  - Whether the bounded MERGEs meet `P3.stream-throughput` is for a new benchmark run to say.
