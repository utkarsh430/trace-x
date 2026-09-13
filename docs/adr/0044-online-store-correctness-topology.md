# ADR-0044: Feature state cannot silently evict — two Redis instances, a declaration-driven write plan, and a completeness epoch

- **Status:** Accepted
- **Date:** 2026-09-13
- **Phase:** 2
- **Supersedes / Superseded by:** Supersedes ADR-0038's decision to leave the write path un-pruned. Resolves the exposure ADR-0042 recorded and did not fix. ADR-0003 (Redis as the online store) and ADR-0034 (hybrid distinct-count storage) are unchanged.

## Context

A passing Phase 2 acceptance run (`run_id: load-20260912-gateway-550789bb`) met every latency and
throughput target while its Redis discarded 346,067 feature keys under `allkeys-lru`. Nothing flagged
it. An evicted feature key reads back **empty**, and an empty key is exactly what an account with no
history looks like — so the rules over it abstained, the score fell, and a transaction that should have
been CRITICAL was approved with `degraded=false`. ADR-0042 named this as a correctness defect rather
than a capacity one. This ADR is the fix, and it has four parts because the defect has four causes.

**Cause 1 — one instance, one policy, two kinds of state.** Redis eviction is instance-wide. The
replay cache (1,579 bytes per entry, 37% of the keyspace) and the feature state shared one pool, and
LRU cannot tell that losing one costs nothing and losing the other costs a decision.

**Cause 2 — the write path stored state for features nobody had released.** ADR-0038 pruned the
reads to the declarations and deliberately left the writes alone so that history would accumulate
ahead of a future feature. Measured, that meant: five-minute card velocity retained for twenty-five
hours; velocity sets for DEVICE and IP that no feature reads; previous-observation hashes for five
entities, one of them read; minute buckets for entities with no bucket-derived feature; and a bucket
hash that never trimmed its fields, growing one field set per active minute for as long as the entity
stayed active. The memory that correctness-relevant state needed was spent on hypothetical features.

**Cause 3 — an absent key meant the same thing in three different situations.** A genuinely new
account, a store that had been recording for an hour, and a store that had just evicted the account's
history all produced the same `INSUFFICIENT_HISTORY`, with `degraded=false`. Worse, for the profile
features the situation was inverted: `account_tenure_days` reported a fresh store's first sighting as
the account's age, so every restart turned every returning customer into a new one for the tenure and
new-device rules.

**Cause 4 — nobody had derived the requirement.** The 512 MB limit was inherited. ADR-0041 sized the
store from a keyspace that was already evicting and was wrong by 2.8×; ADR-0042 corrected it from ten
minutes of traffic, which says nothing about steady state.

## Decision

### 1. Two Redis instances, classified by what their loss costs

| class | keys | instance | policy | loss costs | authority elsewhere |
|---|---|---|---|---|---|
| **A. correctness-critical feature state** | `f:*` | `redis` | **`noeviction`**, AOF off | a wrong decision | Delta via Phase 3 reconciliation (not yet) |
| **B. disposable acceleration** | `rl:*` rate-limit windows | `redis-cache` | `allkeys-lru` | a token briefly over budget; fails open (CLAUDE.md §3.7) | none needed |
| **C. idempotency replay** | `idem:*` | `redis-cache` | `allkeys-lru` | a retry is re-scored instead of replayed; **never** a duplicate case | **Postgres**: `cases.trigger_transaction_id` UNIQUE (ADR-0007) |

Class A refuses writes when full and says so. Classes B and C may lose anything, because nothing in
them changes a decision — proven by the chaos test that pauses the cache instance and asserts the
same transaction gets the same band, score and fired rules. The gateway holds two clients and two
breakers; an outage on one is never reported as the other. Correctness outranks the cost of one more
lightweight local container (CLAUDE.md §12 still holds: `make up` on a laptop with no cloud account).

**The bounded replay guarantee, stated.** At 500 TPS the 128 MiB cache holds roughly two and a half
minutes of responses before LRU recycles them. A retry outside that window is re-scored against
newer features and returns the *existing* `case_id` from Postgres rather than the cached body. That
is the same limit the shared instance had — while also destroying feature state to reach it — and
it is now a documented property of class C rather than a surprise.

### 2. A refused write is loud

`redis.OutOfMemoryError` becomes the typed `FeatureWriteFailedError`, surfaced on the decision as
`feature_write_failed`, counted in `degraded_mode_total`, and it does **not** trip the breaker: the
store is answering reads, and opening the circuit would blind scoring to punish a write that did not
happen. The write also **deletes the store's completeness epoch** (§4 below), because an observation
that was not recorded is a hole in the history and no window spanning the hole is complete. Proven
against a throwaway 2 MB `noeviction` container: `evicted_keys` stays at zero under pressure, the
refusal is typed, the breaker stays closed.

### 3. The declarations decide what is stored

`trace_core.features.state_plan` folds every released `FeatureSpec` into a `StatePlan`: for each
storage primitive, which entities and streams it is written for, which windows read it, its retention
(widest reader plus the late-arrival margin, **per primitive**), and how far back the store must have
been recording for each feature to be complete. The Redis store consumes it for reads and writes both.
`PairwiseWithPrevious` now declares its lookback (24 h) and `ProfileAttribute` its horizon (30 d) —
the bounds they already effectively had, derived rather than accidental.

| primitive | before | after |
|---|---|---|
| velocity sets | 5 entities × every stream, 25 h | ACCOUNT (TX 25 h, failed-login 2 h), CARD (TX **1 h 5 m**), MERCHANT (TX 25 h) |
| previous-observation hashes | 5 entities × every stream, 25 h | ACCOUNT × TX and IDENTITY_CHANGE, 25 h |
| minute buckets | 5 entities, 25 h, never trimmed | ACCOUNT (**2 h**), MERCHANT (25 h); trimmed per write |
| distinct counts | already declaration-driven (ADR-0034) | unchanged; retention per declared window |
| profile / amount sample | 31.25 d | 30 d, declared |

**No feature computes anything differently.** The conformance suite runs unmodified against the
reference implementation and against Redis, and that is the check on the claim.

**The backfill contract.** A feature that is not released has no claim on the store. A feature
released later declares what it needs, and its history begins when its state does:

```
new feature → required state declared in FeatureSpec.semantics → PLAN derives its primitives and retention
           → backfill (Phase 3: durable replay from the medallion) or warm-up (serve traffic for the lookback)
           → INSUFFICIENT_HISTORY, and decisions carry history_incomplete, until the lookback is complete
           → readiness reports "complete since …" → feature enabled
```

It never pretends its history exists. Phase 3 is not pre-built here; until it exists, warm-up is the
only mechanism, and its cost is stated in §4.

### 4. A missing key means zero only when the store would know

The feature store keeps one **epoch** key: when it began recording continuously, set with `NX` by
every write and by the gateway at start-up, deleted by a refused write, and read by every snapshot in
the same round trip. `FeatureContext` gains `complete_since`, and every feature read now has three
answers instead of two:

| the store … | windowed feature over an absent key | negative profile claim (`not known`, `N days old`) | decision |
|---|---|---|---|
| has recorded since before the lookback began (**COMPLETE**) | a measured zero: the entity did nothing | sound | clean |
| began recording inside the lookback (**INCOMPLETE**) | `INSUFFICIENT_HISTORY` | `INSUFFICIENT_HISTORY` | `degraded=true`, reason `history_incomplete` |
| did not say (**UNKNOWN** — no epoch) | `INSUFFICIENT_HISTORY` | `INSUFFICIENT_HISTORY` | `degraded=true`, reason `history_incomplete` |

A *positive* profile claim ("this device is known") is sound the moment it is observed on any store.
The asymmetry is the point: "unknown device" on a store that started last week is the restart false
positive, and it is now refused rather than reported.

**What a restart costs, stated.** With AOF off a restart empties the store. The epoch goes with it,
the first write re-establishes one, and every decision carries `history_incomplete` until the store
has recorded for the widest declared lookback — **24 hours** for the windowed features, **30 days**
for the profile features. During that period the tenure, habitual-merchant and new-device rules cannot
fire, because their inputs are honestly unknown. Readiness reports `feature_history: warming since …;
complete for every feature at …`. This is not a regression: before this ADR the same restart produced
a day of confident wrong answers and thirty days of false new-device alarms, and neither was flagged.
It is the cost the backfill contract exists to remove, and the strongest argument for Phase 3's
reconciliation being on the mandatory path.

### 5. The memory model, and the budget that follows from it — `run_id: bench-20260913-memory-model-5c770259`

`benchmarks/features/memory_model.py` measures the bytes each primitive costs in Redis (`MEMORY
USAGE` on synthetic keys, base plus per-member, HyperLogLog as a sparse→dense curve by cardinality)
and projects every primitive in `PLAN` across the representative population and rate (ADR-0040:
669,767 accounts, 50,233 merchants, 803,720 devices, 334,884 IPs, 500 TPS).

| projection | total | what it answers |
|---|---|---|
| **ten-minute acceptance run** | **548.0 MiB** | what the gate needs to run `noeviction` with zero refused writes |
| **steady state at 500 TPS, declared retention** | **26,440.8 MiB (25.82 GiB)** | what serving the target indefinitely actually requires |

**Configured feature-store limit: 704 MiB** (`maxmemory`), container 832M; cache 128 MiB, container
160M. The ten-minute projection with 1.25× headroom, rounded up to 64 MiB. Not more, because
`docs/ARCHITECTURE.md` §14 budgets the whole `core` profile at 2.7 GB and `api`, `worker` and `ui`
have not yet drawn against it; at these limits 621 MiB remains for them. The headroom is validated by
the acceptance run, which records the store's actual end-of-run memory and eviction count under its
own `run_id` — if the real working set crowds the limit, the budget conflict is surfaced rather than
the limit quietly raised. At this limit the local store holds **~13 minutes of 500 TPS** before it
refuses writes; it refuses loudly, and `make up` on a laptop still yields a working product that says
exactly what it cannot do.

**The steady state does not fit a laptop, and this ADR does not change any feature to make it.** The
structures responsible, from the model, are named so the decision can be made with them in view:

| structure | steady state | why it is large | the decision it would take |
|---|---|---|---|
| merchant minute-buckets (`f:b`, MERCHANT) | 14,367.6 MiB | `merchant_amount_cv_24h` needs 24 h of per-minute sums for every merchant; a hot merchant has ~900 active minutes × 5 integer fields | coarser buckets or a running estimator — a **precision** change to AMOUNT_CV at the window boundary, which is exactly what ADR-0032's parity tolerance exists to measure, not to absorb silently |
| amount sample (`f:pa`) | 3,970.4 MiB | 128 recent amounts per account for the robust z-score, over 30 d | a smaller sample — an **estimator** change, declared APPROXIMATE already |
| account velocity (`f:v`, ACCOUNT/TX) | 2,449.6 MiB | one member per transaction for 25 h, ~67 per account | none without changing what a 24 h count means |
| merchant velocity (`f:v`, MERCHANT/TX) | 2,411.8 MiB | as above, ~900 per merchant | same |
| device→accounts exact distinct (`f:dx`) | 1,254.3 MiB | exact per ADR-0034, 24 h | APPROXIMATE storage — an **accuracy** change ADR-0034 forbids making at runtime and requires an ADR to make at all |

Each is a feature-semantics decision. None is made here.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Raise `maxmemory` and keep `allkeys-lru` | The instruction and the evidence agree: eviction of feature state is a correctness defect at any size. A bigger cap moves the day it happens and keeps it silent when it does |
| `noeviction` on a single shared instance | The replay cache would then fill the store and *feature writes* would be refused to protect *cache entries* — the wrong class protected. Two instances is the only way each class gets the policy its semantics allow |
| `volatile-lru` | Every key already carries a TTL, so the volatile set is the whole keyspace; no effect |
| Keep writing state for undeclared features (ADR-0038's choice) | Measured against a representative workload it cost the memory correctness needed, for features that do not exist. The backfill contract replaces it with an honest lifecycle |
| Treat an absent key as zero everywhere ("the store is complete") | The exact silent failure being fixed. A store that cannot say when it began cannot vouch for anything, and the conformance suite's outranking invariant — nothing is ever silently zero on an empty store — still holds because an empty store has no epoch |
| Treat an absent key as `INSUFFICIENT_HISTORY` everywhere (the previous behaviour) | Conflates a new account with a lost one, and for profile negatives was actively wrong: a fresh store reported first-sighting as tenure. A warm store also reported every quiet account as "insufficient history" forever |
| Flag warm-up on a separate response field instead of `degraded` | Additive and precise, but `degraded` already means "made on fewer inputs than designed" (ARCHITECTURE §18) and every consumer — the header, the counter by reason, the runbook — is already wired to it. The reason string is the discriminator, and it was there already |
| Shorten the profile horizon so restarts warm faster | Changes what "tenure" and "known device" mean to make a number look better. The horizon is the feature's declared semantics; the fix for slow warm-up is Phase 3 backfill |
| Reduce the steady-state footprint now (coarser buckets, smaller sample, approximate device counts) | Each is a feature-semantics change with a parity or accuracy consequence. Surfaced above with its cost, per the instruction, and not made |

## Consequences

**Positive.** Feature state can no longer disappear silently: a full store refuses, says so on every
decision, and withdraws its completeness claim. A new account and a lost one are different answers.
A restart is visible for as long as it actually matters. The store holds what the released features
declare and nothing else — the ten-minute working set fell from a measured 1.32 GB to a projected
548 MiB — and the memory requirement is a model traceable to declarations and measurements rather
than a number inherited from a compose file. Every claim above has a test: `noeviction` declared in
compose and proven on a real full container; missing-but-expected state not read as zero; genuine
new entities measured as zero; `FLUSHALL` producing `history_incomplete`; the cache paused with the
decision unchanged; and the conformance suite unmodified against both stores.

**Negative.** One more container in the `core` profile, and the `core` budget is now 2,144 MiB of
2.7 GB with `api`, `worker` and `ui` still to come. The local feature store holds ~13 minutes of
500 TPS. A cold ten-minute acceptance run carries `history_incomplete` on every decision, because a
thirty-day horizon cannot warm in ten minutes; the harness counts it separately from real
degradation, and the report says so, but a reader who stops at "degraded" will misread it. A restart
costs a 30-day warm-up flag on profile features until Phase 3 provides backfill — honest, and
operationally heavy. The replay guarantee is bounded by cache residency.

**Risks.** The model is a model: its ten-minute projection is validated by the acceptance run's
recorded memory, but the steady-state figure is a projection of a projection and is quoted to guide a
decision, not to size a production deployment. The epoch is a single key; a process that deleted it
would make the store look fresh, which fails toward over-caution, never toward a fabricated zero. And
the profile horizon gate means that during warm-up the new-device and tenure rules are blind — a
detection gap that is now *visible*, where before it was a false-positive storm that was not.

## Status

Accepted
