# ADR-0056: Feature parity: exact implementation parity on the store's own order, and arrival skew on a frozen partition

- **Status:** Proposed (Phase 3 Step 8, `P3.feature-parity`; drafted by the parity agent for the
  lead's review). §5 freezes the partitions, the lateness model, the strata and the feature sets
  **before any measured run**. No measured run exists, and none may be cited: the evidence recorded
  here is diagnostic, and the lead executes the measured runs on a clean commit.
- **Date:** 2026-09-15
- **Phase:** 3 (Step 8 of `docs/PHASE3_PLAN.md`)
- **Supersedes / Superseded by:** — . Implements PHASE3_PLAN §2 B7 and the approximate-parity bound
  and arrival-skew target of §4.3, under §4.4. Builds on ADR-0046 (§5 comparisons, §8 bounded reads),
  ADR-0047 §5 (the fault overlay), ADR-0051 (`tx.scored.v1`), ADR-0053 (Silver) and ADR-0055 (Gold).

## Context

B7 splits parity into two questions that a single number would confuse:

- **Implementation parity on identical ordered input.** Two implementations of one declaration,
  given the same observations in the same order, must give the same answer. Exact features exactly,
  floating-point features at the declared relative tolerance, approximate features within the §4.3
  bound per cardinality stratum.
- **Arrival skew.** The as-served answer sees only what had arrived; the event-time-complete answer
  sees everything. How often they differ on windowed counters is a property of the stream's
  lateness, not a defect, and is gated below 1% only on a representative partition declared before
  measurement.

What exists: the reference answers both modes (`ReferenceFeatureStore`,
`event_time_complete_context`); every `tx.scored.v1` delivery carries what was served, the observe
outcome and the store's position and epoch; Gold stores event-time-complete contexts the shared
feature definitions evaluate; the overlay schedules seeded faults.

Three facts found while designing constrain everything below:

1. **Only transactions publish their store position.** The online store's counter moves for every
   newly recorded observation -- transactions, identity events and authorization outcomes
   (`redis_features.record`). `tx.scored.v1` carries it; an identity event carries only its
   session sequence number; an outcome, relayed from the outbox, carries neither. The durable log
   alone cannot reconstruct the order in which the store saw observations.
2. **The reference never folds or trims.** ADR-0046 §8 declares four situations in which the Redis
   store serves an absence the reference does not. Any other difference is a finding.
3. **A compressed replay is excluded from arrival skew** (PHASE3_PLAN §4.3), and eval-v2's recorded
   ingestion lag is at most a quarter of a second at the start of the dataset, so a paced eval-v2
   replay's arrival skew comes almost entirely from its lateness model.

## Decision

### 1. Three comparisons

| Comparison | Implementation | Reference | Input |
|---|---|---|---|
| As-served implementation parity | each served entry of every scored delivery (Bronze `tx.scored.v1`, redeliveries included) | `build_context(..., AS_SERVED)` | the store's order (§2) |
| Event-time-complete implementation parity | Gold's context for each scored transaction | `build_context(..., EVENT_TIME_COMPLETE)` | Silver at Gold's pins (§2) |
| Arrival skew | the served entry of each transaction's first RECORDED delivery | Gold's value for it | windowed counters (§4) |

All three evaluate one `CanonicalTransaction`, rebuilt from the served payload, so field coverage
is identical on both sides of every comparison.

### 2. Identical ordered input

**The as-served order is the driver's order, verified by the store.** A parity run drives one gateway
with one sequential client (`eval.parity.driver`), and records each request, the gateway's answer and
what it applied: SCORED, REPLAYED from the replay cache, OBSERVED (an identity event feeding a
stream, an outcome, including one the store records as REJECTED), NO_STATE, or REFUSED. The order is
then checked against the store's own evidence and **refused, never compared, when they disagree**:
- scored deliveries pair with scored requests per transaction, and their session sequence numbers
  must increase in the driver's order; one writer session per run (`LinkError`, `OrderEvidenceError`);
- at every scored delivery the reference's receipt -- recorded, conflicting and position -- must
  equal the store's observe outcome and published position (`OrderEvidenceError`);
- the store's epoch must not move during a run.

**Gold's identity events are the gateway's only** (lead's decision, 2026-09-15; ADR-0055 amended at
integration). `identity.events.v1` carries two kinds of producer: the generator publishes events
directly, and the gateway republishes what it ingests, with the online store's observation id in
`correlation_id`. The store observes only the gateway's, so Gold and this step's oracle read only
rows whose producer name, before `@`, is `trace-gateway`, keyed by `correlation_id`. Without that,
Gold would count an event the store never saw, and would count one replayed event twice under two
identities. Directly produced identity events are recorded as not observed online in Phase 3. A
stream fixture writes both for one activity and requires exactly one observation.

**The complete history is Silver at exactly the versions Gold read.** Rows are mapped to
observations in `eval.parity.history` from the declarations, not from Gold's SQL, so Gold and its
oracle cannot share a mapping defect. An identity event is identified by its `correlation_id`, the
online store's `idev_` id (ADR-0055 §9). Before any event-time-complete comparison,
`verify_history` requires Silver's observations to be exactly the ones the store recorded, with the
same recorded form; transactions the store failed to record are the declared exception.

**The reference at scale.** `eval.parity.replay` calls the reference's own `build_context` on the
union of the observations sharing an entity id with the subject; entities other than the account
only within the widest window, previous-observation lookback and approximate bucket. The docstring
states why that is equivalent (the one cross-entity read, outcome verification, cannot change a
value) and `tests/unit/test_parity_replay.py` checks equality with the unmodified reference, read
for read, on generated streams reaching every place the subset could differ, including an account
capped by §8.

### 3. How values compare

- The declared `ParityComparison` only (ADR-0046 §5): EXACT by equality; FLOAT at `1e-9` relative
  with no absolute tolerance; APPROXIMATE by the §4.3 bound where one side is an estimator.
- **Gold against the reference compares approximate features by equality**: both compute the
  declared estimand exactly. Stricter than the declaration, never looser.
- **Absences match** when the state matches, and, as served, the lookback completeness too. A capped
  reference value reports INCOMPLETE, as `served_features` does.
- **Strata and bound.** True cardinality 0 must be 0 exactly. Otherwise per feature and stratum
  (1-9, 10-99, 100-999, 1000+) the RMS relative error must be at most 1%. Every stratum a run
  exercises needs at least 200 comparisons, and each approximate feature 1,000 at non-zero
  cardinality; an unexercised stratum is recorded as such and the bound is not claimed there.
- **ADR-0046 §8's declared situations**, counted apart as declared exceptions and never as
  divergences, only when the store served INSUFFICIENT_HISTORY against the reference's number:
  - `late_read_after_fold`: an earlier write to the account, dated later, folded its raw transactions
    past `as_of` minus the raw history (25 h), and something was folded -- for features over the
    account's transactions;
  - `scored_ahead_of_wall_clock`: the transaction is dated after the gateway received it -- all
    features;
  - `cap_at_scored_millisecond`: at least `SCORE_READ_CAP` account transactions at the scored
    millisecond -- features over the account's transactions;
  - `snapshot_with_unfolded_history`: never compared, since a snapshot means the store did not record
    the transaction (skipped and counted by observe outcome).
  A different number is always a divergence, and so is any other absence. Each divergence records how
  far the read was behind the store's newest write.

### 4. Arrival skew

- **Features: the windowed counters** -- every windowed COUNT and every windowed exact DISTINCT_COUNT,
  derived from the declarations (today eleven, pinned by a test). Their served values are exact, so a
  difference is attributable to arrival. HyperLogLog counts are bounded by implementation parity;
  sums, ratios and the coefficient of variation are not counters.
- **Denominator:** comparisons where either side has a value. **Gate:** strictly below 1%, on the
  representative partition, in a measured run. Other partitions and diagnostic runs are recorded.
- **Subjects:** each slice transaction's first RECORDED delivery. Warm-up transactions (a compressed
  replay) and transactions the store did not record are excluded and counted.
- **Capped windows** are excluded and counted. Which were capped is the reference's AS_SERVED
  evaluation of the same read (`FeatureValue.depth_capped`), which implementation parity has already
  required the store to match -- not inferred from a decision-level reason string.
- **Completeness claim.** Both sides claim completeness from the store's single epoch, which the run
  vouches at the start of its history on an empty store. A difference of claim would otherwise be
  counted as skew.

### 5. The frozen partitions and lateness model

Declared in `eval/parity/partition.py`; `tests/unit/test_parity_partition.py` pins the digests.

| | Representative (gated) | Adversarial ingress (recorded) |
|---|---|---|
| Dataset | eval-v2, `sha256:88f2142a…0485185` (`eval/track_a/eval-v2.manifest.json`), every stream file verified by SHA-256 | same |
| Streams | `tx.raw.v1`, `identity.events.v1`, `tx.authorization.v1` (device events feed no feature) | same |
| Slice (event time) | `2026-02-04T12:00:00.000Z` to `13:00:00.000Z` | `2026-02-11T12:00:00.000Z` to `13:00:00.000Z` |
| Warm-up | the 24 h before, compressed, fault-free, same uniform shift | same |
| Timing | PACED (§4.3): one uniform shift; on-time records at their recorded ingestion lag | PACED |
| Seed | 20260915 | 20260916 |
| Faults (basis points of slice base events) | reordered 50 (pairs), exact duplicate 50, retry with a new event id 20, late 20, future within 24 h 5 | reordered 500, exact duplicate 300, retry 300, late 300, future within 24 h 100, future beyond 24 h 50 |
| Bands | late 1,800-7,200 s; future lead 3,600-43,200 s; beyond 93,600-259,200 s; duplicates 1-120 s; reorder gap ≤ 120 s | same |
| Digest | `sha256:7b1237e9…bc21f8f`; lateness `sha256:95b79e09…aa57e4d` | `sha256:f686f127…58604aa`; lateness `sha256:e44ce346…596f72b` |

- **Chosen, not measured.** No production lateness data exists.
- **Counts** round half up from basis points.
- **Excluded classes.**
  - *Invalid records* have no HTTP analogue. The request contract carries no schema version, so a
    wrong-version copy would be scored as a fresh transaction.
  - *Conflicting duplicates* cannot be built for an outcome base event: the overlay's `_conflict`
    reads `payload.score` for any topic other than transactions, identity and device events. This is
    a defect in `eval/replay/faults.py`, which the Kafka agent owns.
- **Density caveat, stated before measuring.** eval-v2 averages about 12 transactions a minute over
  40,000 accounts. Within one hour most accounts appear once, so arrival skew on the representative
  partition is structurally small, and the metric's power to detect a lateness problem is bounded
  by that density. The warm-up gives every slice window real 24-hour history, but does not raise
  the slice's own density.

### 6. The run

`python -m eval.parity.run`:
1. verify the dataset and load the partition;
2. build the overlay;
3. vouch the empty store's epoch at the shifted start of history;
4. post the warm-up, then publish the overlay through the driver on its schedule
   (`publish_overlay`, arrival classes checked at hand-over);
5. wait until Kafka holds exactly what the posts imply (outcomes at least once);
6. run Bronze, Silver (available-now, per topic) and one Gold build on real Spark and Delta;
7. read scored deliveries from Bronze, the history from Silver at the build's pins, and Gold's
   contexts;
8. evaluate, guard, and write the PARITY record.

- **Diagnostic mode** runs anywhere, writes to a directory the caller names, and is never
  publishable.
- **Measured mode** refuses:
  - a dirty worktree;
  - an unverified dataset;
  - non-empty observation topics;
  - an existing lake;
  - an unknown gateway image digest.

  It writes to `eval/manifest/`.
- **Collection guard** (`eval.parity.guard`). A run fails when:
  - a pairing compared nothing, or skipped everything;
  - Spark did not demonstrably write Bronze, Silver and a Gold build;
  - a gated partition classified no arrival-skew comparison;
  - a feature more than half of whose offered comparisons were excluded as unvouched, in either
    pairing (`MAX_NOT_VOUCHED_FRACTION_PER_FEATURE`, 0.5).

  A measured run also fails on an exercised stratum below 200 or an approximate feature below 1,000.

  **On the exclusion bound.** §5's exclusion rule subtracts each unvouched absence from `compared`,
  F1 expects that to be material on the representative partition's late band, and no verdict read
  `not_vouched_fraction` -- so a run could exclude nearly every comparison of an EXACT feature,
  report zero divergences and pass. The volume floors above do not cover this: they bind only the
  approximate features. The bound is per feature rather than a whole-run ceiling because the
  failure it catches is one feature going uncompared while the run's overall fraction stays
  healthy. **Chosen, not derived, and frozen before any measured run**, like the lateness model
  itself: a feature more than half of whose offered comparisons were excluded was not meaningfully
  compared, whatever its divergence count says. It is a guard constant, not part of the partition
  declaration, so it changes no frozen digest in §5.
- **PARITY record** (`eval.parity.record`) fields:
  - run_id, git SHA and dirty flag, environment lock;
  - partition and lateness-model digests, eval-v2 manifest digest, overlay version, seed and
    provenance;
  - Spark, Delta, Hadoop, Scala, Java and Python versions;
  - the gateway target and image digest;
  - counts per post class, Kafka topic, feature and stratum;
  - every result, the guard and the verdict.
- **Report.** `eval.parity.report` renders records only, one section per run_id, with the standing
  caveat.

### 7. Recorded limits

- **One sequential driver, one writer session, one store epoch per run.** A concurrent or multi-session
  run's as-served order is not recoverable from the log, and is refused rather than approximated.
- **Wall-clock cost.** A paced partition costs its slice in real time, plus the warm-up's posting.
- **Memory.** Gold's context rows for every transaction are collected to the driver.
- **Claim linter.** `make check-claims` knows record type PARITY: `scripts/check_claims.py` defines
  `PARITY_REQUIRED`, mirroring `eval/parity/record.py`'s own `REQUIRED_FIELDS` (so the two cannot
  drift), and registers it in `REQUIRED_BY_TYPE`. Added by the lead at integration.

### 8. Findings while building, and the lead's decisions on them

**F1. Late reads behind held history: ADR-0046 §5 as declared, and a measurement rule here**
(lead's decision, 2026-09-16; not a new semantic exception).
- *What happens.* Each online structure holds its widest window plus the late-arrival margin. A read
  reaching behind that serves absence and stops vouching for the window (`history_incomplete`). The
  reference models no retention, and must not: a second implementation of retention in the oracle
  would be the drift parity exists to detect.
- *Reproduced,* by a controlled probe varying only how far a later-dated write is ahead of a late
  read: no difference at 30 and 55 minutes; six features differ at 70, 125 and 150 minutes
  (`card_tx_count_5m`, `device_distinct_accounts_24h`, `failed_logins_1h`, both HyperLogLog counts
  and `merchant_amount_cv_24h`). The 70-minute threshold fits the card window's retention.
  DIAGNOSTIC, no run_id, NOT publishable.
- *The rule (§3).* A window is compared when the as-served read vouches for it
  (`lookback_completeness = COMPLETE`), and when both sides are absent -- those must agree exactly.
  When the as-served side is absent and NOT vouched for while the reference has a number, the
  comparison is **excluded and counted**, per feature and window, and reported as a fraction of
  every comparison offered. A value served while unvouched is still compared: serving a number is a
  claim. Nothing is dropped silently.
- *Reading the fraction.* It measures how far the corpus reaches behind what the store still held,
  not a divergence. On the frozen representative partition, whose late band (1,800-7,200 s) exceeds
  the shortest non-account retention, it is expected to be material, and is reported plainly.
- *Arrival skew keeps these comparisons.* An absence against an event-time-complete number is a
  difference, and the read was unvouched because it arrived late, so excluding them would understate
  the gate. Recorded here as a deliberate choice.

**F2. One undeliverable outbox row blocks every later row, on every topic, with nothing logged.**
- *How it showed.* Two end-to-end runs posted every authorization outcome successfully -- each
  RECORDED with its outbox row -- and `tx.authorization.v1` stayed empty until the run's delivery
  wait expired. The relay logs only a pass that *raises* (`outbox_relay_pass_failed`), and none was.
- *The cause, from the outbox itself* (the run now dumps it rather than timing out silently):

      pending  <none>                                                         638 rows, 0 attempts
      pending  not handed over: EventPublishError: topic
               investigation.requested.v1 does not exist on the broker          1 row, 497 attempts

  The gateway's triage writes `investigation.requested.v1` rows to the same outbox as the outcome
  events. The relay claims oldest-first and **stops the batch at the first hand-over failure**, so
  that one row -- the oldest -- was re-claimed and re-failed every idle interval, deferring all 638
  outcome rows behind it, none of which was ever attempted.
- *Classification.* The trigger was this step's harness: it created only the three topics the
  medallion reads. Fixed here -- the run now creates every declared topic. The relay is not edited.
- *A real defect, found by this run and fixed by the lead at `5cba28a`.* Correctness was never at
  risk -- the delivery watermark never passes an undelivered row -- but delivery stalled invisibly
  across topics. The fix confines a failed row to its own topic and partition key, the only ordering
  Kafka guarantees, so every other lane keeps draining, and a pass that cannot publish everything it
  claimed logs `outbox_relay_rows_not_published` with its claimed, published, failed, refused and
  deferred counts, the blocked-lane count and the first error. Three unit tests cover lane
  isolation, an integration test fails a single partition key as an unavailable partition leader
  does, and the OPERATIONS runbook gains "Outbox not draining". This step keeps its harness fix --
  creating every declared topic -- and the instrumentation that surfaced the stall; it does not
  touch the relay.

**F3. Checked and disproved: the outcome ingress does bound future skew.**
`POST /v1/events/authorization` calls `_future_skew_problem(request, body.decided_at)` before it
records anything (`services/gateway/app.py`), which applies the same `ScoringPipeline.check_clock`
bound transactions use and answers 422 beyond it -- one bound for every recorded stream, so the
completeness guard's 24-hour margin stays sufficient (ADR-0046 §5). An outcome dated beyond the
bound is therefore refused, never applied online, and never in Silver, which is what the run's
classification already expects. The hypothesis that it was unbounded is withdrawn (lead, 2026-09-16).

## Acceptance

`P3.feature-parity` requires, on a clean commit:
- the unit and mutation self-tests;
- the end-to-end integration test;
- a measured PARITY record on the representative partition with no divergence in either pairing,
  every exercised stratum within the bound at its minimum, a guard with no violation, and arrival
  skew below 1%;
- a measured record on the adversarial partition, recorded, not gated.

## Evidence so far (diagnostic; no measured run exists)

Every figure below is a diagnostic result on the working tree, NOT publishable: no `run_id`, and a
measured run is the lead's, on a clean commit.

| What | Result |
|---|---|
| Parity unit and mutation self-tests (`-m "parity and not integration and not stream"`) | 143 passed |
| Gold stream tests on Temurin 17, including the gateway-only identity fixture and the unmodified event-time-complete conformance suite | 88 passed |
| `ruff`, `mypy` over `eval/parity` and the two Gold modules, `make check-claims` | clean |
| ADR-0046 §5 probe (F1), varying only how far a later-dated write is ahead of a late read | no difference at 30 and 55 minutes; six features differ at 70, 125 and 150 |
| End-to-end diagnostic run (synthetic partition, gateway, Kafka, Bronze, Silver, Gold) | passed; its PARITY record is diagnostic and is not retained in `eval/manifest/`, so no run_id is cited -- an id that cannot resolve is worse than none (CLAUDE.md §13.2) |

The diagnostic run, on feature set 5.0.0 with Spark 4.0.1, Delta 4.0.1, Hadoop 3.4.1, Scala 2.13.16
and Java 17.0.18 (DIAGNOSTIC, NOT publishable, dirty worktree):
- **as-served implementation parity:** 15,652 comparisons, 0 divergent, 0 skipped;
- **event-time-complete implementation parity:** 15,600 comparisons, 0 divergent -- **withdrawn,
  and not to be cited.** This was measured before the defect the Phase 3 adversarial review found
  was fixed: `ParityTally.add` applied ADR-0046 §5's unvouched exclusion in *both* pairings, and
  because neither side of this pairing records lookback completeness (both are built with
  `Observed.of`, leaving it `None`), the `!= COMPLETE` test held for every Gold absence. Each one
  was downgraded to NOT_VOUCHED and subtracted from `compared`. The detector for exactly the
  missing-row defect ADR-0055 §3 rules out by construction was therefore disabled, so "0 divergent"
  is not evidence of parity here and the comparison count is not comparable to a corrected run.
  Fixed, with a regression test that fails against the old behaviour, before any measured run. This
  pairing must be re-measured;
- **arrival skew:** 11 of 634 comparisons differ, with 20 capped windows excluded and 540 warm-up
  subjects excluded as the compressed replay §4 requires. Recorded, not gated: this is a synthetic
  partition, not the frozen representative one;
- **no declared exceptions and no unvouched exclusions fired**, because this partition's late band
  (1,000-3,000 s) stays inside the shortest non-account retention by construction. The frozen
  representative band (1,800-7,200 s) is expected to exercise both (F1);
- **inputs proved identical before values were compared:** Kafka held exactly the 602 scored, 16
  identity and 78 outcome records the posts implied; Silver's 694 observations are exactly the 694
  the store recorded, with the same content; the overlay injected exactly what it was asked for;
- **the guard fired only where it should:** both HyperLogLog features reached 602 comparisons at
  non-zero cardinality, below the 1,000 minimum, so a run this small cannot claim the approximate
  bound. Their measured RMS relative error in the exercised stratum was 0.
- **Tenure and the negative memberships were absent on both sides throughout**, as feature set
  5.0.0 requires of a corpus that begins inside each account's lifetime: matched absences, not
  divergences.

The comparator's non-vacuity is shown by the mutation self-tests, not asserted: a window edge off by
one millisecond, a duplicate counted twice, a reordered pair, an approximate value outside its
stratum's bound, an absent value served as 0, and the same edge and duplicate planted on Gold's side
each fail the comparison, while the unmutated control compares clean.

## Alternatives Considered

| Option | Why not |
|---|---|
| Read served values back from Redis | The store's state at a past read is gone; `tx.scored.v1` already carries exactly what was served |
| Order observations by LogAppendTime or offsets | Partition interleaving and asynchronous relay make arrival order at Kafka differ from the store's order; §4.3 names the store's counter |
| Order by session sequence numbers alone | Outcomes carry none; identity events' numbers would still leave outcomes unplaced |
| Pool the approximate bound across cardinalities | §4.3 forbids it: low-cardinality strata, where the estimator is exact, would hide high-cardinality error |
| Measure arrival skew on a compressed replay | §4.3 excludes it; arrival delay has no meaning there |
| Infer capped windows from `history_depth_capped` and INCOMPLETE | A decision-level reason cannot say which window was capped; the reference's flag can, and parity already binds the store to it |
| Use the reference's event-time-complete value for arrival skew | The metric describes the system; Gold is the system's event-time-complete answer, and Gold-versus-reference parity is required anyway |

## Consequences

**Positive.**
- A divergence is attributable. Inputs are proven identical before values are compared, and every
  refusal names the evidence that failed.
- The comparator is not vacuous: planted divergences in both pairings fail, and the declared
  exceptions excuse only the absences they declare.
- The frozen partition cannot be quietly swapped for a friendlier one: its digest is pinned.

**Negative.**
- Parity needs a dedicated, quiet stack per run: an empty feature store, empty topics and one
  writer.
- Representative arrival skew is weak evidence at eval-v2's density (§5).
- Every limit in §7.

## Status

Proposed. Becomes Accepted when the lead has reviewed §5's frozen declarations, the measured runs
exist, and `P3.feature-parity` records their run_ids.
