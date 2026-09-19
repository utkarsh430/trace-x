# ADR-0055: Gold is a batch build of primitive-shaped state and event-time-complete point-in-time context from Silver

- **Status:** Proposed (Phase 3 Step 7; drafted by the parity agent for the lead's review and integration)
- **Date:** 2026-09-15
- **Phase:** 3 (Step 7 of `docs/PHASE3_PLAN.md`)
- **Supersedes / Superseded by:** — . Implements B4 of `docs/PHASE3_PLAN.md` §2 on the conventions of
  ADR-0048 and the canonical tables of ADR-0053. Replaces `docs/DATA_ENGINEERING.md` §2's description of
  Gold as "authoritative feature values, upsert", which the approved plan already withdrew.

## Context

The approved plan makes Gold batch (B4): **primitive-shaped state**, the state the online store keeps
(`trace_core.features.state_plan.PLAN`), so reconstruction (Step 9) can rebuild Redis primitives rather
than write values; and **per-transaction point-in-time features**. The plan rejected Gold writing feature
values back to Redis: a written value cannot age, and a store that evaluates every feature at an
arbitrary `as_of` holds primitives.

Gold claims the `EVENT_TIME_COMPLETE` mode only (ADR-0046 §1): what a complete history says at each
transaction's `occurred_at`, with arrival order deciding only which delivery of an identity came first
and ties at the scored millisecond broken by identity. The as-served mode belongs to the Redis store and
the reference; Gold does not claim it.

What Gold inherits:
- **Silver** (ADR-0053) holds one canonical row per identity, validated, with `is_late` stored. A
  `silver.tx_scored_v1` row can be replaced by a preferred delivery of the same content, so a build must
  read each Silver table at one Delta version and record the versions it covered.
- **Lake conventions** (ADR-0048): tables created from declarations, writes through `OpenedCheckpoint`,
  snake_case identifiers, minimum protocol, no loss-tolerant reader or session settings.
- **The oracle:** `tests/conformance/feature_semantics_suite.py`'s literal fixtures, which Gold must pass
  by subclassing `EventTimeCompleteConformanceSuite` unmodified, on a real JVM.
- **Freshness target** (PHASE3_PLAN §4.3): p95 build lag behind the Silver commit it covers within
  15 minutes, no build over 60 minutes, a stall at 30 minutes without progress while Silver advances.
  Step 13 measures it; Gold records the lag of every build.

## Decision

### 1. Sources

A build reads `silver.tx_scored_v1`, `silver.identity_events_v1` and `silver.tx_authorization_v1`, the
observation log (ADR-0051) and the outcome stream (ADR-0049), each at one pinned Delta version. It
never reads `silver.duplicates`, `silver.quarantine`, `silver.late_events` or `is_late`.

- **Transactions:** every canonical `tx.scored.v1` row, whatever `observe_outcome` says. A complete
  history holds every transaction the gateway accepted, including those the online store failed to
  record. A transaction's own `authorization_outcome` field is never carried (ADR-0049 §3).
- **Identity events:** those whose `identity_event_type` feeds a stream by
  `features.semantics.identity_stream`, applied to every released enum value. The rest change no online
  state and are not observations.
- **Outcomes:** `APPROVED` and `DECLINED` only, each with its verification against the complete
  history's transactions: `VERIFIED` (same id and account), `REJECTED` (same id, another account),
  `PENDING` (no such transaction).

`silver.tx_raw_v1` is not a source. It carries the generator's transactions before scoring, and a history
that read both it and the observation log would count a replayed transaction twice.

### 2. Tables

All snake_case in the `gold` tier, created from `TableDeclaration`s, unpartitioned and unclustered (the
layout is Step 14's, ADR-0015), at protocol (1, 2).

| Table | Holds | Key | Serves |
|---|---|---|---|
| `gold.observations` | every observation once, under its namespaced identity, in `Event`'s shape, with an outcome's verification | `observation_identity` | PLAN velocity, exact distinct, previous observations and profiles (the online store computes all of these from raw observations, ADR-0046 §5) |
| `gold.minute_buckets` | per entity, currency and minute: count, sum and sum of squares as `DECIMAL(38,0)` | entity, id, currency, minute | PLAN `buckets` |
| `gold.distinct_buckets` | each value once per entity, dimension and five-minute bucket | entity, id, dimension, bucket, value | PLAN `approx_distinct` (the estimand the HyperLogLogs approximate) |
| `gold.tx_windows` | per transaction, every declared window its context holds | transaction, entity, stream, window | `FeatureContext.windows` |
| `gold.tx_profiles` | per transaction with a profile, the account lifetime strictly before `as_of`, reduced | transaction | `FeatureContext.profiles` |
| `gold.tx_previous` | per transaction, the previous observation per PLAN `previous` stream | transaction, entity, stream | `FeatureContext.previous` |
| `gold.builds` | one row per committed build: pins, Gold versions and row counts, lag | append-only | readers, the check, Step 13 |

The primitive tables and PLAN are derived from the same declarations: `minute_buckets` and
`distinct_buckets` are built for exactly PLAN's entries, so a PLAN change reaches Gold without an edit.

### 3. Point-in-time context, not finished values

The per-transaction tables hold what `FeatureContext` holds, so the 26 shared feature definitions
evaluate a Gold context exactly as they evaluate the reference's or Redis's. This is the context
snapshot's stated purpose (`features/context.py`): parity compares the feature logic on one snapshot
shape rather than two retrieval paths.

- **Which windows.** `gold_plan.compile_windows` folds the released `WindowedAggregate`s per
  `(entity, stream, window)`. For each it computes exactly the `WindowState` fields the declared
  aggregations read (`gold_plan.READS`); NULL in a column means "not computed for this window".
  `gold_plan.unsupported_declarations` must be empty: a shape Gold does not compile is refused before any
  job runs, never read as absent.
- **Presence.** A window row exists exactly when `reference.window_state` returns a state: always for an
  `INCLUDED` transaction window (the scored transaction is a member), otherwise when it has a member.
- **No completeness claim.** A context is built by `gold_plan.feature_context(..., complete_since=...)`;
  the claim is the caller's. Gold vouches for no period until reconstruction supplies evidence
  (Step 9, plan §4.1). Its source is `ONLINE_ONLY`, as the reference's event-time-complete context
  says: `RECONCILED` needs hydration-verified history (Q4g).

### 4. The build protocol

A build runs through the `gold_build` checkpoint (ADR-0048 §6). Its sources are the three Silver tables
(start `snapshot`); its targets are the seven Gold tables.

1. **Refuse before writing:** a missing Silver table, a loss-tolerant session setting, or an uncompilable
   declaration.
2. **Create** every Gold table from its declaration; **open** the checkpoint, which refuses a replaced
   Silver table or a Gold table that lost delivered commits.
3. **Plan.** Builds are numbered 0, 1, 2 ... The next build's plan -- each source's table id, pinned
   version and that version's commit time -- is published to the checkpoint's `offsets/<n>` before any
   target is touched, atomically and never over an existing file. A build planned and not committed is
   **replayed with its own plan**, however far Silver has advanced.
4. **Replace** each Gold table by one MERGE through `OpenedCheckpoint.merge`: matched and changed,
   update; not matched, insert; not matched by source, delete. It is the build's one idempotent,
   stamped commit to that table. A key held twice stops the build before the write. On a replay,
   Delta skips the tables the crashed attempt already committed, by the checkpoint's transaction
   identity, and the rest are computed from the same pins.
5. **Record** the build in `gold.builds` through `OpenedCheckpoint.append`, then publish
   `commits/<n>`.

**Gold writes Spark's progress layout itself.** `offsets/<n>` and `commits/<n>` are the names
`checkpoints.SparkProgress` reads, so `decide_start` judges a Gold build by the same rule as a streaming
batch: a target that recorded a build neither committed nor exactly the next planned one refuses the
start. The offsets file is not a Spark offset log. No Spark query runs on this checkpoint, and none
may.

Readers take a build's Gold tables at the versions its `gold.builds` row records
(`gold.read_context_rows`), so a build in progress is never half-read.

### 5. The computation

Spark SQL only -- joins, window functions and higher-order functions. No Python UDF, and no call into
the reference. Each reduction mirrors one in `trace_core.features.reference` for meaning.

- **Bounded windows.** A subject is joined only with the rows inside its widest declared window for that
  `(entity, stream)`: both sides carry a bucket as wide as the window, the subject is exploded over the
  at most two buckets its range touches, and an equi-join on the entity and bucket is followed by the
  exact predicate. The approximate estimand joins `gold.distinct_buckets`, and the merchant CV joins
  `gold.minute_buckets`, the same way.
- **Unbounded lifetime.** The lifetime's run is a running sum of 30-day gaps per account. The latest
  transaction, same-currency transaction and located transaction strictly before `as_of` are found by
  as-of lookups: one sort per account, with the subject unioned in ahead of every row at its own
  millisecond. The 128-amount and 20-point samples are equi-joins on row numbers. A merchant or
  category is habitual once its third visit in the run precedes `as_of`.
- **Exact arithmetic.** Sums and sums of squares are `DECIMAL(38,0)`, and ANSI mode makes an overflow
  fail the build. Event time is floored to the millisecond with `pmod`. An even-count median of integer
  amounts goes through an exact decimal and its string, so it is the correctly rounded double Python
  computes. The medoid sums whole-metre distances, rounded half up, as `profile_math` declares.

### 6. Freshness

`gold.builds.lag_ms` is the build's finish minus the newest commit time among its pinned Silver versions
(PHASE3_PLAN §4.3), logged as `gold_build_committed` with the pins and row counts. The p95, the 60-minute
ceiling and the stall rule are measured over the stream benchmark's window in Step 13. No figure is
claimed here.

### 7. Check and entrypoint

`python -m services.stream.gold build` runs the next build (or the replay) and prints its record.
`python -m services.stream.gold check` judges the latest committed build:
- every Gold table is still the table, version and row count it recorded;
- every Silver source is still the table it pinned;
- `gold.observations` holds exactly one row per Silver observation at the pins;
- no Gold table holds a key twice;
- every transaction has context windows, and profiles and previous observations name only transactions.

Exit codes follow Bronze and Silver: 0 ok, 1 not consistent, 2 refused, 3 failed on Spark.

### 8. Conformance on a real JVM

`tests/stream/test_feature_semantics_gold.py` subclasses `EventTimeCompleteConformanceSuite` unmodified.
Each context comes from a Gold build over the log's first deliveries, written as Silver canonical rows.

- **One build for many fixtures.** A recorder subclass runs each fixture only to note which logs it asks
  about; it judges nothing and its empty contexts are discarded. The logs are then written into one
  lake, with every entity and observation id prefixed per log so they cannot meet, and built once. A
  log the recorder missed is built alone.
- **The prefixing is checked.** Several logs are rebuilt alone, unprefixed, and must give identical rows.
- **A collection guard** fails unless every collected fixture read a Gold context. The `stream` marker's
  session guard fails a run in which no stream test executed.

### 9. Recorded limits

- **Full rebuild.** Every build recomputes and rewrites every Gold table from all of Silver. That is
  correct, and simple to replay. Its cost grows with history, so the §4.3 freshness target is at risk as
  history grows; an incremental build needs its own design, because a late Silver row changes later
  transactions' contexts back to the start of their lifetimes.
- **One writer.** Two concurrent builds cannot both plan one number, but two replays of one planned build
  would race on the same transaction identity; Delta's conflict check stops one. Not tested.
- **Unread `WindowState` fields.** A context holds the fields the released features read. A field no
  declared aggregation reads keeps its default, so a Gold context is valid for the released feature set
  only. A new aggregation is refused until `READS` names its fields.
- **Identity-event ids.** Silver identifies a gateway identity event by its envelope `event_id`; the
  online store by the `idev_` id it carries as `correlation_id`. They are derived from the same
  `(token, key)`, so they deduplicate alike and no feature value depends on which is used. Gold keys by
  Silver's and carries `correlation_id`, which hydration (Step 9) needs.
- **Transactions only from the observation log.** A dataset the generator emitted straight to
  `tx.raw.v1`, without the gateway, has no Gold contexts. Phase 4 training on such data needs a declared
  source choice, never both at once.

## Acceptance

Three-way conformance: the Spark subclass of `EventTimeCompleteConformanceSuite` passes every literal
fixture on a real JVM with Delta 4.0.1, as the reference does. The build's stream tests cover
declarations, primitives, replay with pins, `is_late` independence and refusals.

## Alternatives Considered

| Alternative | Why not |
|---|---|
| Gold writes finished feature values (the original DATA_ENGINEERING §2) | Rejected by the approved plan: a value cannot age. Values also need a completeness claim Gold cannot make before Step 9 |
| Store the 26 feature values per transaction, computed in Spark | Re-implements every compute function in SQL. The context shape shares them, and parity then compares one snapshot |
| Store every `WindowState` field for every declared window | The merchant 24-hour state would need each transaction joined with a day of that merchant's transactions, for fields nothing reads |
| A structured streaming query with `foreachBatch` recomputing Gold | Its micro-batch offsets would have to be translated into pinned Silver versions. A replay without pins would recompute from newer Silver than the tables Delta skipped, mixing versions in one build |
| Overwrite each Gold table instead of MERGE | `OpenedCheckpoint` offers append and MERGE only, and an overwrite outside it has no transaction identity, so a replay would rewrite rather than skip |
| Append each build's rows and filter readers by build id | Storage grows by a full copy per build, and every reader must filter correctly |
| One `reset_checkpoint` per build instead of progress files | A reset is an audited exception with a reason, not a per-build step |
| A Gold ledger outside the checkpoint directory | `decide_start` would not read it, so the second build's targets would look like commits from an unplanned batch and the start would be refused |
| Read `silver.tx_raw_v1` as well as the observation log | A replayed transaction appears on both and would count twice |
| Python UDFs calling the reference reductions | The brief forbids calling the reference from Spark. UDFs also run row at a time outside the JVM, and each worker would need the driver's interpreter |
| Exact joins over all history per entity | The equi-join on the entity alone fans each subject out to the entity's whole history before the range predicate. Bucketing bounds it by the window |
| Approximate distinct counts with Spark's HLL sketches | The declared estimand is the exact bucket union; an estimate of it would add a second error source to the parity bound |
| Run each fixture's log through its own Gold build in the conformance test | Correct, but it multiplies the minutes a build takes by the number of fixture checks. The batched build is cross-checked against single-log builds instead |

## Consequences

**Positive.**
- Gold's point-in-time contexts are held to the same literal fixtures as the reference and the Redis
  store, through the same feature definitions.
- A build's tables always describe one pinned set of Silver versions, including after a crash, and the
  record says which.
- The primitive tables are derived from PLAN, so reconstruction has exactly the state the store's plan
  declares, with the minute and distinct buckets exact.
- Nothing Gold computes depends on `is_late`, on arrival order beyond first delivery, or on
  `silver.duplicates` and `silver.quarantine`.

**Negative.**
- Every build rewrites every Gold table. Build time and write volume grow with history, and Step 13 may
  show the freshness target missed as history grows.
- The profile computation sorts each account's whole history per build, and the merchant CV joins up to
  a day of minute buckets per transaction. Neither is benchmarked here.
- Gold writes files named as Spark's progress files into its checkpoint. That is a convention a later
  reader must know, and a Spark query must never be pointed at the directory.
- `gold.tx_windows` holds NULL for fields no declared feature reads, so the table is not a general
  `WindowState` store.
- A Gold context is valid for the released feature set only (§9).
- Seven more tables and one more checkpoint to operate.

**Risks.**
- **The JVM's trigonometry is not bit-equal to the platform libm.** Distances agree within the float
  parity tolerance, and whole-metre rounding agrees on the tested points, but an exact half-metre tie
  could round differently and pick another medoid.
- **Decimal overflow fails the build loudly** if a merchant's minute sums of squares ever exceed
  `DECIMAL(38,0)`. That is correct, but an adversarial amount stream could stop Gold.
- **Silver log retention.** A replay needs its pinned versions to still be readable, so the Step 11
  retention floor must cover the longest time a build can stay planned.

## Status

Proposed
