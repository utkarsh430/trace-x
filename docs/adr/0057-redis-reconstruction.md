# ADR-0057: Redis reconstruction: replay history through the store's own write path, and claim completeness by compare-and-set under the writer fence

- **Status:** Proposed (Phase 3 Step 9, `P3.redis-hydration`). The lead reviewed the epoch and
  completeness semantics and confirmed D1-D6 before implementation; the module, its tests and the
  acceptance run recorded below are complete.
- **Date:** 2026-09-15
- **Phase:** 3 (Step 9 of `docs/PHASE3_PLAN.md`)
- **Supersedes / Superseded by:** — . Implements plan §2 B1 under §4.1 points 2, 5, 6 and 7. It
  builds on ADR-0046 §1, §5 and §8, ADR-0049, ADR-0051, ADR-0052 (and Step 11's amendment 1),
  ADR-0053 and ADR-0055.

## Context

- **B1.** Reconstruction rebuilds primitives, never values. It writes through the same idempotent
  path the gateway uses. It claims completeness only by compare-and-set on the epoch, and only with
  evidence.
- **The store.** `RedisOnlineFeatureStore.observe` runs the same Lua `record` as `score`.
  - Its first write sets `epoch` to the script's wall clock, with `NX`.
  - `withdraw_completeness` keeps the later of the stored epoch and `resume_at`.
  - A read vouches for a window only when the window starts at or after the epoch.
  - Beyond 25 hours (`raw_tx_ms` = `raw_ie_ms` = 90,000,000 ms), transactions are folded into the
    profile prefix and identity events are dropped, and their `obs:` keys are deleted.
  - A folded observation is no longer recognised as a redelivery. A fold that goes out of
    `(ms, identity)` order marks the lifetime inexact, and the profile then reads as absent.
  - A raw record that interleaves with the prefix makes the profile absent
    (`account_profile`), never double counted.
- **The evidence available.**
  - `assess_bronze_coverage`: the ADR-0051 §5 rule, over Bronze at stated versions and one ledger
    read.
  - `silver_conservation.consumed_bronze`: the Bronze version a Silver checkpoint has read whole.
  - `app.feature_store_holes`.
  - `app.authorization_outcomes`: the system of record for outcomes (ADR-0049 §4).
  - `app.producer_sessions` and the writer lock (ADR-0051 §2).
- **Identity events.** The store identifies a gateway identity event by the `idev_…` id in
  `correlation_id`, not by the Silver envelope `event_id` (Step 7 finding). Only the gateway's
  identity events were ever observed online. Bronze coverage counts the generator's as "not
  observations".
- **Clock bounds.** Transactions, identity and device events, and authorization outcomes
  (`decided_at`, `_future_skew_problem` in `services/gateway/app.py`) are refused at ingress when
  dated more than `MAX_CLOCK_SKEW_FUTURE_S` (F = 24 h) ahead. Anything backdated up to
  `MAX_BACKDATE_S` (D = 90 d) is accepted.
- **Coverage vouches only below `Coverage.through`.** `through` is at most Bronze's high-water
  mark: the earliest, over partitions, of each partition's newest arrival. Once writes stop, that
  mark stays at the quietest partition's last record. So "wait until `through` passes the last
  write" never completes.

## Decision

### 1. Target, and the one-writer fence
- **Hydration is the writer while it runs.** `services/stream/hydrate.py` starts a
  `WriterSupervisor` with producer `trace-hydrator`, on `WRITER_LOCK_KEY`.
  - While it holds the lock, every gateway is not ready and refuses online-state writes (503).
  - It reads no evidence until two things hold: the supervisor reports ready (the takeover grace
    has passed when a predecessor may still be writing), and a further `2 × clock_margin` has
    passed. Every other session is then closed, or its lease ran out before the ledger read. So
    under the ADR-0051 §5 rule its tail is bounded.
  - The lock held elsewhere is a refusal (exit 2). Nothing is written.
- **The fence is checked before every batch and before the claim**, with
  `ready_as(initial_session_id)`. A lost or re-acquired session aborts the run without a claim.
- **Foreign writes are also caught by the store's own counter.** Each receipt's `position` must
  equal the previous one plus 1 when recorded, and unchanged on a redelivery. A mismatch means
  someone else wrote, and the run aborts. The claim script (§5) compares the position as well. This
  closes the "paused process after its fence check" residual for everything written before the
  claim.
- **Close.** The session closes confirmed, with `last_seq = 0`. The coverage ledger query selects
  only the gateway producer, so hydration never appears in observation coverage.
- **Target precondition, checked before any write.** The namespace holds no `epoch` and no
  `position`, or it holds an unfinished hydration marker (§5) whose epoch is still that marker's.
  Anything else is refused:
  - a store with another epoch, since (d) forbids moving it earlier;
  - a store written since the marker, detected by its position, or by a gateway session opened
    after the marker's start.

### 2. Inputs, read in this order, all after the fence (§1)
1. **Bronze coverage.** `assess_bronze_coverage(clock_margin_s=M)` gives Bronze versions `V_b` per
   covered topic, the ledger rows, and the ledger read time `R`. M is required, and has no default.
2. **Silver checkpoints.** For `tx.scored.v1` and `identity.events.v1`, the current Silver
   checkpoint must have read Bronze whole through `V_b` (`consumed_bronze(...).through_version ≥
   V_b`), with no problem. Silver's conservation report over those versions must be conserved.
   Otherwise the run is refused ("Silver has not consumed the evidence").
3. **Silver pins `S`.** `silver.tx_scored_v1` and `silver.identity_events_v1` are pinned at their
   current versions, read after step 2, so they include every committed batch step 2 relied on.
   `silver.quarantine` is pinned too.
4. **Outcomes.** One statement reads `app.authorization_outcomes`, all rows, with its
   `statement_timestamp()`. The row count is read again just before the claim, and a change is a
   refusal. Under the fence a not-ready gateway records no outcome (ADR-0051 §2).
5. **Holes.** Read at begin (§5) and again immediately before the claim.

**Data.**
- *Transactions:* `gold_features.observations` applied to the pins `S`, so the projection is
  Gold's, not a second one.
  - Every canonical `tx.scored.v1` row, whatever `observe_outcome`.
- *Identity events:* the same projection, but only rows whose envelope `producer` is the gateway.
  Their `event_id` is replaced by `correlation_id`.
- *Outcomes:* from `app.authorization_outcomes`, built with `authorization_observation`, the
  gateway's own mapping.

**Gold's committed build is not an input** (decision D1).
- `gold.observations` is exactly this projection of Silver at older pins.
- Its other tables cannot be written through `observe`.
- Pinning Silver directly never needs an old Silver version. So "a tail beyond lake retention" is
  a failure that cannot happen, not a refusal to maintain.
- If a pinned Silver version is unreadable, Delta raises, and the run fails with no claim.

### 3. Replay order, and why the profile comes out as the live fold would
- **The order.** `(occurred_ms, namespace, event_id)`, ADR-0046 §1's declared total order, with
  identity events on their `idev_` ids.
- **Streaming.** A global sort, then `toLocalIterator()`. Batches are flushed at 1,000
  observations, or earlier when the next one would take a batch's event-time span to
  `min(raw_tx_ms, raw_ie_ms) − LATE_ARRIVAL_MARGIN_S` or beyond. The bound is computed from
  `LAYOUT` at start, and asserted to be positive.
- **Why the prefix is exact.** In this order each `fold` call's cutoff, `min(ms, now) −
  raw_tx_ms`, is non-decreasing. An observation written after a fold has
  `ms ≥ that fold's reference > its cutoff`. So folds proceed in strictly increasing
  `(ms, identity)` order, and `inexact` is never set.
  - `reset_run` fires on exactly the 30-day gaps of the event-time sequence.
  - The folded prefix therefore equals the prefix of a live store that received the same
    observations never more than the raw horizon out of order.
  - **The fifth declared difference: `folded_out_of_order`** (named for Step 8's parity list,
    beside ADR-0046 §8's four). *Where:* an account whose live store folded an observation out of
    `(occurred_ms, identity)` order, because it arrived more than the raw horizon (25 h) behind
    that account's newest observation, which marks its lifetime inexact. *What differs:* the live
    store serves the profile features as absent (`INSUFFICIENT_HISTORY`); the hydrated store, which
    replayed in event-time order, serves the event-time-complete value. *Direction:* an absence
    against a value, never two different numbers, and never the reverse.
- **Retention trims** use `min(ms, now)` per key and range reads, so they agree.
  - Key expiry is relative to hydration's clock, and only lengthens a key's life.
  - A future-dated observation's trim reference differs, which is §8's declared case 2.

### 4. What may be claimed: the earliest `T`
All times are UTC, compared at the millisecond. `F` = 24 h, `M` = the clock margin, and
`after(x) = floor_ms(x) + 1`.

**Refused outright (no claim; the epoch stays at `B`, §5):**
- the coverage is not `consistent` (any anomaly);
- any gap is open;
- any hole is open at the claim;
- the outcome count changed;
- the fence was lost;
- a position mismatch;
- `T ≥ B`: nothing is claimable.

**Otherwise `T` is the maximum of:**
- **(a) History coverage: each gap.** For every gap `g`, including `unknown` gaps:
  `after(g.end + F)`.
  - A lost write at write time `w ∈ [g.start, g.end]` carries `occurred_at ∈ [w − D, w + F]`
    (plan §4.1 point 6). The gap's full event-time image `[g.start − D, g.end + F]` is computed and
    logged.
  - An epoch claim vouches for `[T, ∞)`, so only the image's upper edge can cross it.
  - `g.end` already carries the margin.
- **(a) History coverage: the origin.** `after(min(started_at) + F + M)` over the ledger's gateway
  sessions, or `after(R + F + M)` when there are none.
  - Writes made before the ledger existed are invisible to the rule, so the claim starts no earlier
    than a day after its first session.
- **(b) Silver presence: quarantined records.** For every `silver.quarantine` row at `S` on
  `tx.scored.v1`, and on `identity.events.v1` when it carries session headers (`identity_conflict`
  included): `after(kafka_timestamp + M + F)`.
  - Bronze coverage counted such a record present, but Silver holds no canonical row for it, or
    holds a different content under its identity.
  - Its write preceded its arrival, within the margin.
- **(b) Silver presence: identity collisions.** Two replayed rows under one store identity with
  different content (a `conflicting` receipt, or two Silver identities sharing one `idev_` id) give
  `after(max(occurred_ms of both))`. The lost store may hold either.
- **(c) Holes, at begin.** Holes open at begin are withdrawn and cleared by the guard protocol on
  the target (§5). They are about the replaced store, whose losses coverage and the outcome table
  already bound.
- **(d) No existing later epoch.** Enforced by §1's precondition and §5's compare-and-set.

**Why this is enough: the quiescence lemma** (decision D2).
- ADR-0051 §5 limits its guarantee to writes before `through` for three reasons:
  - sessions opened after the ledger read;
  - heartbeats committed after it;
  - records not yet read.
- Under §1's fence and wait, the first two cannot happen.
- Unread records are missing sequence numbers, which the rule places inside that session's runs or
  tail. Those are bounded by present stamps, the close, or the lease bound.
- So when the coverage is consistent and every gap is closed, every lost write of the old regime
  lies in a reported gap, whatever `through` says.
- Hydration uses only `Coverage`'s public fields. Property tests over generated quiescent histories
  test the lemma.

**Outcomes need no coverage.** The store applies an outcome only after its durable row committed,
and hydration replays every row. So the hydrated store holds a superset of what the lost store
applied. This replaces ADR-0051 §5's watermark and Bronze high-water recipe, whose mark likewise
stops advancing at quiescence (decision D3).

### 5. The epoch protocol
1. **Marker.** Hydration writes a hash `<ns>:hydration` = `{run_id, begun_at, B, pins S, cursor,
   count, position, digest, state=replaying}`. It is not a feature primitive. `digest` is a running
   SHA-256 over replayed identities.
2. **Begin.** It runs `CompletenessGuard(store, ledger, instance_id="hydrate:<run>").resume()` and
   `.reconcile()`, then `withdraw_completeness(resume_at = B)`, where `B = after(now + F + M)`.
   - The store then vouches for nothing a lost or not-yet-replayed observation could carry. Every
     old-regime write happened before the fence, and is dated at most `F` ahead.
   - `observe`'s `NX` can no longer date the epoch.
   - Holes recorded before the reconcile are cleared exactly as the guard clears them.
**The exception to ADR-0046 §5, stated precisely.** That section says hydration must check the
ledger before restoring any earlier epoch, and the guard's rule is that an epoch only ever moves
forward. Both still hold, with one exception this ADR defines:
- **Foreign epochs never move earlier.** An epoch this run did not write -- a gateway's `NX`, a
  guard's withdrawal, another hydration's claim -- refuses the run before it writes (§1), and the
  claim's compare-and-set applies only while the epoch is still exactly the `B` this run withdrew
  to. So the rule holds for every epoch except hydration's own marker.
- **Holes open at begin are withdrawn, not crossed.** They are about the store hydration replaces.
  `B` is past every event time their lost observations could carry, so withdrawing to `B` and
  clearing them through the guard's own protocol is exactly what the guard would have done.
- **What the hole never does is excuse the loss.** An observation the old store missed is either in
  history, and replayed, or missing from the log -- and then its sequence number is missing, the
  coverage rule reports a gap, and `T` starts after it (§4). Clearing the hole never clears the gap.
- **Holes opened after begin refuse the claim.** They are about the store this run is building.

3. **Replay** (§3). After each batch the marker records `cursor`, `count`, `position` and `digest`.
   Nothing re-observed after a crash is folded or dropped, because batches span less than the raw
   horizon. So resuming at the cursor re-delivers only unfolded observations, which are no-ops.
4. **Claim**, in one script: if `GET epoch == B` and `GET position == expected`, then `SET epoch T`
   and `HSET marker state=claimed T`. It returns whether it set.
   - It runs only after the §4 refusals are re-checked: holes, outcome count, fence.
   - A withdrawal meanwhile (a stale gateway's `reconcile`) makes the epoch later than `B`, so the
     claim does not apply and hydration exits "not claimable".
5. **Re-run.**
   - *`state=claimed`, epoch still `T`:* exit 0 with nothing written ("hydrating twice equals
     hydrating once").
   - *`state=replaying`, epoch `B`, no foreign write:* resume with the marker's pins `S`, and
     re-read all evidence. `B` is withdrawn to the new `B' > B`, which only moves it later.
     - If the new pins add observations at or below the cursor (count or digest differ), resuming
       would replay them late. The run is refused unless `--discard-unfinished` is given.
     - That flag deletes the namespace only while the marker proves it holds nothing but this
       unfinished hydration.

The claim and marker scripts belong in `RedisOnlineFeatureStore` (decision D4). Hydration then
reaches Redis only through the store.

### 6. Failure and crash model

| Point | State left | Re-run |
|---|---|---|
| Before the fence | nothing | fresh |
| Fence held elsewhere | nothing; exit 2 | fresh |
| After the marker, before `B` | marker, no epoch; nothing vouched | adopts the marker, sets `B'` |
| During replay | epoch `B`: vouches only from a time no unreplayed observation can reach, like a withdrawn store; partial primitives | resumes at the cursor (§5.5) |
| Evidence insufficient | epoch `B`, all data; exit 1 | re-run once evidence improves |
| After the claim, before the marker update | impossible: one script | — |
| After the claim | epoch `T`, `state=claimed` | exit 0, no writes |
| Redis refuses a write (`noeviction`) or is unreachable | epoch `B` (or none); run fails, exit 3 | resumes |
| PostgreSQL lost | the fence lapses; abort, epoch `B`; a gateway may take over on `B` | refused as a foreign write if a gateway wrote |
| A gateway starts after a crash | its `establish_epoch` is a no-op against `B`; it serves soundly from `B` | the marker detects its writes and refuses |

Exit codes follow Bronze, Silver and Gold: 0 claimed or already claimed, 1 not claimable, 2 refused,
3 failed.

**Observability.** Structured logs, carrying no entity ids:
- `hydration_started`
- `hydration_evidence`, carrying every `T` component and refusal reason
- `hydration_batch`
- `hydration_claimed` or `hydration_not_claimed`

Also counters of observations replayed, redeliveries and conflicts.

## Acceptance

`pytest -m 'stream and integration' tests/integration/test_redis_hydration.py`, on a real broker, a
real Redis, a real PostgreSQL and a real JVM. The suite writes its history through the gateway's own
pieces -- the fenced `WriterSupervisor`, `ScoringPipeline` over the Redis store, and `ObservationLog`
over the producer factory -- then ingests it through Bronze and Silver, and rebuilds a second store
from that history.

- **Equality.** A hydrated store answers every probe as the live store did: windows, profiles and
  previous observations, for every entity the history touched, at an `as_of` past its newest
  observation. Completeness is compared separately, because the two stores date their epochs
  differently by construction.
- **Every refusal** in §4 and §1: an unknown session (an open gap), Silver behind the coverage's
  Bronze version, a store another writer has touched, a fence held elsewhere, a withdrawal during
  the run, and a foreign write during the run.
- **The lost observation** that is in neither the store nor the log: its hole is withdrawn exactly
  as the guard withdraws one, and the claim still starts after the event time it could have
  carried.
- **Idempotence**, and a crash then a re-run: a child process is killed with SIGKILL between two
  `observe` calls, and the resumed run converges on the same store, position and claim as a clean
  hydration.
- **The pure decisions** are unit-tested (`tests/unit/test_hydration_claim.py`), and D2 is held to
  property tests over generated quiescent histories
  (`tests/unit/test_hydration_quiescence_properties.py`), covering sessions that closed cleanly,
  were left unclosed, lost their heartbeat, or lost records to a delivery timeout.
- **Evidence isolation, deliberately.** The suite migrates a database of its own inside the
  compose PostgreSQL:
  the coverage rule reads every gateway session in one ledger, and a running compose gateway's open
  session would otherwise make every claim environment-dependent. Advisory locks are per database,
  so the fence is the real one. No compose service is reconfigured, and the database is dropped
  afterwards.
- **The memory spike** is recorded as a diagnostic (Redis `INFO memory`), not as a target, and
  what it does and does not measure is stated with it.
- **The fifth declared difference** (`folded_out_of_order`, D6) has its own history: one account's
  last observation is dated past the raw horizon behind its own newest, so the live store folds out
  of `(occurred_ms, identity)` order and serves no profile for it, while hydration -- replaying in
  event-time order -- serves the event-time-complete value. The test asserts *containment*, not
  merely a difference: windows and previous observations must agree, every profile the live store
  serves must equal the hydrated one, and the extra profiles must be the hydrated store's. A test
  that only checked "they differ" would pass if hydration were the side losing a profile, which is
  the case this difference is declared to rule out.

**Measured, 2026-09-16.** The ten tests executed together and passed, none skipped, against a
throwaway broker, a real Redis, this suite's own migrated PostgreSQL database and Delta 4.0.1 on
Temurin 17. The diagnostic from the equality test: the hydrated namespace holds 82 keys against the
live store's 81, and instance `used_memory` moved by less than a hundred kilobytes across the
hydration. **This does not measure the spike the Negative consequences predict.** That spike is a
function of replaying a history longer than the primitives' retention -- HyperLogLog buckets for
every five-minute bucket of the replayed span coexisting for their retention -- and this history
spans about 37 hours, not 30 days. The instance-wide `used_memory_peak` the run reported is a
figure since the Redis server started, shared with everything else on that instance; it is not
attributable to this run and is not quoted as hydration's cost. Measuring the spike needs a
history longer than the widest retention, which belongs with the Step 12 memory model.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| `gold.observations` for the prefix plus a Silver tail (the plan's picture) | It adds a Gold-to-Silver consistency obligation and a dependency on Gold's freshness and old Silver versions, without adding evidence: under B1 only `gold.observations` is usable, and it is this projection of Silver. Kept as D1's alternative if the lead prefers it |
| Write Gold's `minute_buckets` and `distinct_buckets` into Redis keys | B1 forbids direct key writes. The keys would also not be idempotent against the Lua `record` |
| Replay in the lost store's as-served order `(store_epoch, store_position)` | Only recorded scored transactions carry a position. Identity events, outcomes and unrecorded transactions have none, and positions restart with every store |
| Refuse while a live session exists, without taking the lock | Racy: a gateway can acquire during hydration and interleave writes the claim then crosses |
| Hydrate a second namespace while the gateway serves a cold store, then switch | It needs continuous tail-following and an atomic switch of the gateway's namespace. Out of scope for Phase 3's single writer |
| Let `observe`'s `NX` date the epoch, then compare-and-set from that value | It cannot be told from a gateway's `NX`. A crash would leave a claim from the start time that ignores history dated up to 24 h ahead |
| A far-future sentinel epoch during replay | `from_millis` and readiness (`since + widest lookback`) overflow near year 9999, and an abandoned run leaves a store that never warms |
| Wipe and restart on every re-run | A destructive delete per crash. Kept only as the explicit `--discard-unfinished` |
| Wait until `Coverage.through` passes the last write | Unattainable at quiescence: the high-water mark stays at the quietest partition's last arrival |
| Outcomes from Silver, certified by the delivery watermark and Bronze's high-water mark (ADR-0051 §5) | Both need traffic to advance. The system-of-record read is exact and needs no delivery path |
| Refuse while any hole is open, and never clear one | Deadlock: only a guard withdrawal on the target clears a hole about the lost store, and that withdrawal dates the target's epoch, which (d) then forbids hydration to move earlier |

## Consequences

**Positive.**
- **Gold's primitive-state tables are not a hydration input** (D1). `gold.minute_buckets`,
  `gold.distinct_buckets` and the per-transaction context tables are written by no path `observe`
  offers, and `gold.observations` is this projection of Silver at older pins. Hydration pins Silver
  directly, so it never depends on a Gold build's freshness or on an old Silver version, and Gold
  keeps its own consumers (Step 8's parity oracle) unchanged.
- A rebuilt store claims completeness only from evidence, and never across a gap, an open hole or a
  foreign write. Every failure leaves it claiming at most what a withdrawn store claims.
- Primitives are written by the gateway's own script, so no second implementation of the online
  semantics exists.
- A crash at any point converges on re-run. A completed run is a no-op when repeated.

**Negative.**
- **Scoring is not ready (503) for the whole hydration**, which costs O(history) script calls.
- **A memory spike.** Every replayed key's expiry is relative to hydration's clock. HyperLogLog,
  card, device and outcome keys for the whole replayed history coexist for up to their retention,
  about 2 hours for HyperLogLogs. The spike is unmeasured, and the Step 12 memory model does not
  cover it.
- **Claims come a day later than a live store's after any gap**, because of the 24 h future-skew
  widening. A recent gap leaves the 30-day profile features unvouched for 30 days after the claim.
- **Correctness rests on a lemma that extends ADR-0051 §5 past `through`** (D2), proven by tests
  rather than by the coverage module.
- **After Step 11 retention, retired Bronze offsets read as missing numbers**, so as gaps. `T` then
  moves past the retained horizon until coverage treats RETIRED offsets as present.
- **Hydration targets only a fresh store.** After a Redis loss the gateway must not write before
  hydration runs, or the store must be discarded explicitly.
- **A hydrated store's positions do not replay the lost store's as-served order**, and its epoch
  value does not identify a store instance.
- **The store module gains a marker and a claim script.**

**Risks.**
- **The resume seeds its counter from the store, not from the marker.** A crash inside a batch
  leaves the store ahead of the last marker update, and those writes are the run's own. Seeding
  from the marker would fail every legitimate resume as a foreign write. What still catches a real
  race is `prepare`'s two guards -- the store no more than one batch ahead, and no gateway session
  opened since -- and the per-write continuity check during the replay.
  - *The batch in "one batch ahead" is the crashed run's, recorded in the marker, not the resuming
    run's.* Reading it from the resuming run let a crash at `--batch-size 2`, resumed at the 1,000
    default, tolerate 1,000 observations written by someone else. Found in the Phase 3 adversarial
    review and fixed; `_foreign_sessions` never depended on it.
- **`_foreign_sessions` is widened by the clock margin towards reporting one.** Session times are
  the database's and the marker's are this host's, so a session opened within a margin of the run's
  start is treated as a writer and refuses the claim. Erring the other way would let a racing
  gateway through on a clock difference.
- **An unfenced writer** (a gateway with no writer session) violates the premise that every write
  happens in a ledger session, and nothing detects it.
- **The fold equivalence covers arrival disorder within the raw horizon.** Beyond it, the declared
  fifth difference applies.
- **`clock_margin_s` is an operator input.** Misestimated, it misplaces every bound, as for
  coverage.

## Decisions confirmed by the lead

Confirmed before implementation, and listed here so the record shows what was decided rather than
what was asked:
- **D1:** Silver at fresh pins, not Gold's build.
- **D2:** the quiescence lemma, and its use in place of `through`.
- **D3:** outcomes from `app.authorization_outcomes`.
- **D4:** the marker and claim scripts in `redis_features.py`.
- **D5:** hydration clears holes through the guard protocol at begin.
- **D6:** the fifth declared difference.

## Implementation status

- **`packages/trace_core/stream/hydration.py`:** the pure claim rules (`compute_claim`,
  `event_time_image`, `replay_span_bound_ms`, the cursor and digest), the evidence read
  (`read_evidence`), the Silver-to-observation projection (`observation_frame`, built on Gold's own
  `observations`), and `Hydrator` with its four phases.
- **`services/stream/hydrate.py`:** `run` and `status`, with the exit codes the Bronze, Silver and
  Gold jobs use.
- **`packages/trace_core/repositories/redis_features.py` (additive only):** the hydration marker
  and the three scripts -- start, update and the compare-and-set claim -- plus the guarded discard
  and two small reads. No existing script or read path changed.
- **Tests:** `tests/unit/test_hydration_claim.py` (the pure decisions),
  `tests/unit/test_hydration_quiescence_properties.py` (D2), and
  `tests/integration/test_redis_hydration.py` with `tests/integration/hydration_harness.py`, the
  child process killed between two `observe` calls.
- **Acceptance evidence:** the ten tests passed together in one run, with the memory diagnostic
  above. The lead records the run beside it at integration.
- **Corrected at integration, by the Phase 3 adversarial review** (all three found by reading, and
  none of them reachable by the ten tests above):
  - *A crash between the marker and the withdrawal wedged the namespace.* `prepare` writes the
    marker and only then withdraws, so that window leaves a marker with no epoch -- §6's "adopts the
    marker, sets `B'`" row. `_adopt` refused it as a foreign epoch and the `--discard-unfinished`
    branch sat below the raise, so the documented escape hatch was unreachable and recovery meant
    deleting keys by hand. It now adopts that state **and withdraws to a fresh `B'`**: `_adopt`
    never called `_withdraw_to_begin`, so relaxing the refusal alone would have adopted a store with
    no epoch and let `observe`'s `NX` date it from the first replayed observation (§5.2).
  - *Identity conflicts did not survive a resume.* `_flush` found them, the marker did not carry
    them, and `_ReplayState` began empty -- so a resumed run dropped the `identity_conflicts`
    component and claimed `T` **earlier** than the evidence allows. They are now in the marker, and
    an unreadable list is refused rather than read as "no conflicts".
  - *The resume's foreign-write tolerance was the resuming run's batch size*, not the crashed run's
    (see Risks).

## Status
Proposed
