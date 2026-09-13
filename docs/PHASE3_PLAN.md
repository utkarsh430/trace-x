# Phase 3 — Approved Plan: Streaming, Medallion and Durable Feature-State Reconstruction

> **Status:** approved by the user on 2026-09-13, with the amendments recorded in §3 and §4.
> This document is authoritative for the **scope, order and decisions** of Phase 3 work.
> `docs/ROADMAP.md` remains authoritative for the phase gate, `docs/PROGRESS.md` for live state, and
> each ADR for its own decision once it lands. ADR numbers are assigned in landing order (the control
> plane forbids gaps), so this plan names the ADRs it requires by subject, not by number.

---

## 1. Why the original Phase 3 plan changed

Phase 2 left four facts Phase 3 had to absorb: feature state must never silently evict; the feature
store reports history completeness explicitly; fully-warmed exact state cannot fit a laptop; and
Phase 3 must supply the durable history that reconstruction needs. The planning investigation (six
specialist reviews; load-bearing claims checked against the code, by experiment in a throwaway
container, or against cited upstream documentation — and a claim resting only on documentation is
re-verified by the step that depends on it) found that the original design could not deliver that as
written:

- **"Gold writes authoritative feature values back to Redis" is incoherent** with a store that holds
  primitives and evaluates every feature at an arbitrary `as_of`. A written value cannot age, and
  Redis HyperLogLog bytes are Redis-specific.
- **The online store and the naive reference already disagree** on inputs the conformance suite never
  supplies: boundary minutes of bucket-derived sums, duplicate delivery, out-of-order arrival of the
  previous observation and of first-seen, the home location, currency scope of the profile, event-time
  filtering of profile reads, future-dated trims, and backdated distinct counts. One class — the
  coefficient of variation dividing same-currency sums by an all-currency count — is wrong in *both*
  implementations, so implementation-versus-implementation parity would pass on it.
- **Nothing durably records what the gateway observed.** `tx.raw.v1` requires fields the API makes
  optional, and no runtime Kafka producer or outbox relay exists.
- **Events beyond a 10-minute watermark leaving Silver** would make Silver a different history from
  the one the online store was built from, since the gateway accepts backdated events.
- **The frozen `eval-v1` dataset carries a label proxy:** identity and device events are emitted only
  inside fraud scenarios. It also contains no duplicates, late or out-of-order events, so any test fed
  only `eval-v1` passes vacuously on those paths.
- **The steady-state memory projection is not a reliable architecture input** (encoding thresholds,
  per-observation bucket counting, uniform arrivals, a population held fixed across a 30-day horizon).
  "Does not fit a laptop" survives; "which structures dominate" does not.
- **The toolchain could not prove itself:** Java was a warning, Hadoop and Scala were never asserted,
  the lockfile was consumed by nothing, and Spark jars would resolve unhashed from Maven.

Exploratory measurements from that investigation are deliberately not quoted here: none has a
`run_id`. Each is re-established as a literal fixture or a recorded run by the step that owns it.

## 2. Approved architecture

| | Decision |
|---|---|
| B1 | **Reconstruction rebuilds primitives, never values.** A `StatePlan`-driven hydrator writes each online primitive through the same idempotent path the gateway uses; completeness is claimed only by compare-and-set on the epoch, and only with evidence |
| B2 | **Durable observation log.** The gateway publishes `tx.scored.v1` (contract in §3 Q1) with a coverage mechanism that makes every loss detectable (§4.1); the outbox relay ships as its own process |
| B3 | **Silver is the complete accepted history.** Lateness is a deterministic tag, late events are copied to `late_events` and counted, and no stateful operator on the durable path is watermarked on `occurred_at` |
| B4 | **Gold is batch:** primitive-shaped state plus per-transaction point-in-time features (as-served and event-time-complete) |
| B5 | **Feature semantics are declared precisely** and the online write path is idempotent and order-independent |
| B6 | **Explicit transport and dedup identity** per topic; deterministic `event_id`; declared partitioner and timestamp type; Spark rejects go to Delta quarantine |
| B7 | **Parity is split:** exact implementation parity on identical ordered input, and arrival skew as a separately recorded metric |
| B8 | **Single-JVM local Spark**, bounded local resources, snapshot tables for reproducibility |
| B9 | **Local Delta layout chosen by measurement** now; Databricks layout decided in Phase 12 |
| B10 | **`eval-v2` without label proxies**, implemented in Phase 3 (§5 Step E) |
| — | **Hashed dependency and toolchain contract** (Step 0) |
| — | **Seven additional acceptance capabilities** (§7), plus `P3.eval-v2` — a tracker the lead added for the approved Q5 requirements, not a separately approved capability |

## 3. Approved decisions

**Q1 — `tx.scored.v1` is the observation log, with a strengthened contract.** Before
`docs/contracts/RELEASED.json` freezes it, the schema must carry: canonical transaction fields
(nullable where the API allows) with `field_coverage`; a decision summary; the observe outcome; the
store's completeness epoch; `feature_set_version`; the rule-pack version or digest. Served features are
an **extensible collection** of entries — never one schema property per currently-known feature — and
each entry carries enough state and metadata to interpret the served result later (at minimum feature
id, state, value when available, approximation flag, source). A future feature-set version must be
representable without a new event schema. Each entry must also let a later reader explain an
absence: `missing_fields` for an `UNAVAILABLE` feature, and for `INSUFFICIENT_HISTORY` whether the
entity is genuinely new or the store could not vouch for the lookback (its completeness for that
feature). **Never** labels, causal evidence keys or any evaluation-only data.

**Q2 — Identity.** Per-topic dedup identities; deterministic `event_id`; an optional
`X-Idempotency-Key` on `/v1/events/*` (additive). Device events arriving without `platform` are **not**
given a fabricated platform and the API is not broken: they are recorded as a **typed coverage gap**,
and any reconstruction or completeness claim that depends on device history must recognise that gap
and refuse completeness.

**Q3 — Transport.** `murmur2_random` partitioner; `LogAppendTime` message timestamps.

**Q4 — Feature semantics** (bump `FEATURE_SET_VERSION`; rules re-validated):

| | Decision |
|---|---|
| Q4a | Account amount and declined-ratio features use **exact half-open** windows. `merchant_amount_cv_24h` uses an **explicitly declared minute alignment** over **same-currency** buckets |
| Q4b | Profile lifetime runs **since the last inactivity gap of at least 30 days** |
| Q4c | **Home location is the geodesic medoid of the last 20 located observations strictly before `as_of`**, with an explicitly declared minimum-history requirement, declared tie-breaking and boundary behaviour, and no component-wise latitude/longitude median (which misbehaves across the antimeridian). The exact definition is written into the feature-semantics ADR and into hand-derived literal fixtures **before** implementation. If repository evidence shows N = 20 is seriously inappropriate, work stops before that ADR is frozen and the evidence and an alternative are surfaced |
| Q4d | Robust z-score: median and MAD of the **last 128 amounts strictly before `as_of`** |
| Q4e | Identity change: only `PASSWORD_CHANGE`, `EMAIL_CHANGE`, `PHONE_CHANGE`, `ADDRESS_CHANGE` and `MFA_RESET` count. `LOGIN_SUCCEEDED`, `MFA_ENROLLED` and `UNKNOWN` do **not** (today's handler counts all three); `LOGIN_FAILED` remains its own stream. The mapping moves from the HTTP handler into the feature declarations, where the offline implementation can read it |
| Q4f | **Strict event-time** semantics for backdated scoring |
| Q4g | `RECONCILED` means "the lookback is covered by a hydration-verified history" |

Not decisions, because the declarations already settle them — defects to fix: per-currency profile
keys, declined-ratio currency scope, the CV count's currency, duplicate double counting, out-of-order
overwrite of the previous observation, first-seen by arrival order, trims driven by future-dated
events, and identity-event retries.

**Also a defect, found while reviewing this plan:** an observation the online store fails to record
because Redis is *unreachable* (rather than full) leaves the completeness epoch standing, so after the
outage the store claims completeness over a hole. Step 1 makes any unrecorded observation withdraw
completeness, including when the gateway process restarts before Redis is reachable again.

**Self-inclusion is a conflict, not a docstring fix.** ADR-0032 states that the scored transaction is
inside its own window; the gateway reads features before recording the transaction, so what it serves
excludes it, and the conformance suite pins the exclusion. Step 1 resolves it from evidence — the rule
pack's thresholds and how Phase 2 calibrated them — and records the answer in the feature-semantics
ADR with a literal fixture. If that evidence does not settle it, it is escalated before that ADR is
frozen.

**Q5 — `eval-v2` in Phase 3.** `eval-v1` stays immutable and none of its historical claims change.
`eval-v2` emits legitimate identity and device activity as well as fraud-scenario activity, has its
own manifest and digest, preserves lineage to generator, config and seed, and ships a test proving
that the mere presence of identity or device events is not a fraud-label proxy.

**Q6 — Physical lake contract.** Bronze stores raw Kafka value bytes plus transport metadata; tables
use the minimum Delta protocol features; no column mapping, deletion vectors or type widening by
default; clustering only if the layout benchmark earns it.

**Q7 — Acceptance.** The stricter split parity criterion. The outage-recovery test offers
**5,000 events/s** as its target rate; the rate is never lowered after measurement, and insufficient
recovery capacity is recorded as an honest failure.

**Q8 — Local-development resource overrides.** Kafka retention is capped by bytes and lake retention is
bounded **for local development only**. The conceptual production retention contract in
`docs/EVENT_CONTRACTS.md` is unchanged. Bronze is the durable local record, and every limitation
caused by laptop capacity stays documented.

**Q9 — Privacy.** The Silver PII-tokenization claim is removed. Rows carry `trust_tier`, and an
explicit tokenization and privacy design is required before any source containing real PII is
admitted.

## 4. Additional safeguards

### 4.1 Observation-log coverage survives crashes without invisible gaps

**Invariant:** anything lost by crash, shedding or Kafka outage ⇒ a detectable gap ⇒ completeness
cannot be claimed across it. No Postgres write happens per transaction.

The design below is the set of constraints the observation-log ADR must satisfy; the chaos tests
confirm or correct it before that ADR is accepted.

1. **Scope: every write that changes online state is covered** — scored transactions on
   `tx.scored.v1` and identity events on `identity.events.v1` today, device events whenever they begin
   to be observed. A write that changes online state and is not sequenced and published is itself a
   defect, because its loss would be invisible.
2. **One online-state writer per feature store in Phase 3, fenced.** The gateway is a single process
   (ADR-0039). A process becomes a writer only by holding a session-scoped Postgres advisory lock and
   committing a `producer_sessions` row (session id, start time from Postgres `now()`). A process without
   the lock is **not ready and does not serve scoring** — consistent with the accepted failure model in
   `docs/ARCHITECTURE.md` §18 (Postgres down ⇒ 503), so there is no path that serves decisions without a
   session. The writer records a heartbeat time on its session row every few seconds (never per
   transaction). If the heartbeat fails or the lock is lost, the process stops online writes and
   produce at once and turns not-ready. Multiple concurrent writers would need a cross-process
   mechanism and are out of scope for Phase 3.
3. **Write order is fixed, with two preconditions:** assign the next contiguous sequence number → write
   the online store → produce, **even when the online write failed**, with the observe outcome recorded
   in the event. Preconditions: (a) the online write is atomic (Step 1's single Lua observe script;
   today's non-transactional pipeline can leave a partial write), and (b) any observation the online
   store did not record withdraws its completeness (Step 1), which also covers a response replayed from
   the idempotency cache after its original observe failed. The handler writes the online store before
   it responds, so a process crash can leave history missing an event but never the online store missing
   an *answered* transaction; a case committed by triage just before a crash has no answer and no online
   record, and its history gap is covered by point 5.
4. **A confirmed flush** means `flush()` returned zero outstanding messages **and** no delivery report
   failed for any sequence number in the session **and** no event was shed. `last_seq` is the highest
   sequence number *assigned*. Only then is the close row written; any shed, failed delivery or failed
   flush leaves the session unclosed.
5. **Coverage rule, with declared clocks** (session rows and heartbeats: Postgres `now()`; events: Kafka
   `LogAppendTime`, with the broker-to-Postgres clock offset measured and applied as a margin):
   - every integer from 1 to `max(seen, last_seq)` must be present in Bronze for the session;
   - an unclosed session's gap starts at its last Bronze event and ends at its last recorded heartbeat
     plus the heartbeat interval plus the producer's delivery timeout plus the clock margin — after that
     point the fenced process could no longer write or produce, so the gap is bounded and can close;
   - a session id in Bronze with no session row is a gap;
   - a record whose producer is the gateway but that carries no session headers is a gap;
   - a live session certifies only its contiguous prefix below Bronze's high-water mark.
6. **Arrival-time gaps map to event time conservatively.** A lost event may carry an `occurred_at` up
   to the accepted backdate bound before its arrival or up to the future-skew bound after it, so a gap
   over arrival times `[g0, g1]` invalidates completeness for every `as_of` whose lookback intersects
   `[g0 − backdate bound, g1 + future-skew bound]`. Narrowing that would need a narrower acceptance
   policy at the API, which is a user decision.
7. **Two epochs, two claims.** The online store's epoch certifies the online store; history coverage
   certifies the durable log. A gateway crash does not withdraw the online store's epoch (point 3); a
   store rebuilt from history may claim completeness only where both hold.
8. **Bronze never skips unread data:** `failOnDataLoss=true`, and offsets are never reset to latest.
   Local byte caps (Q8) that evict unread segments therefore stop the query loudly.

| Loss | Why it is detectable |
|---|---|
| Crash before produce (sequence assigned or not) | session unclosed ⇒ gap, bounded by the last heartbeat |
| Crash after sequence assignment | same |
| Crash while buffered | same |
| Crash after broker acknowledgement | event is in Bronze; session unclosed ⇒ conservative bounded gap |
| Crash before or after the session-open write | before: the process was never ready and served nothing; after: unclosed ⇒ gap |
| Crash before or after a heartbeat or the close write | before: the previous heartbeat bounds the gap; after close: `last_seq` bounds the range |
| Shedding when the buffer is full | sequence missing inside the range, and the session cannot close cleanly |
| Kafka outage (delivery timeout) | sequence missing inside the range, or unclosed session |
| Postgres unavailable at start, or heartbeat or lock lost | no lock ⇒ not ready, no writes, no produce |
| Gateway record without session headers | counted as a gap |

A block-reservation scheme is **not** required to prove the invariant. Chaos tests kill the process at
each point in the table.

### 4.2 Canonical Silver stays exactly deduplicated

The physical canonical Silver table holds at most one row per declared identity. Insert-only MERGE is
benchmarked as planned. There is **no** fallback in which Silver holds duplicates and correctness depends
on readers applying exact dedup. If exact canonical dedup cannot meet the Phase 3 rate, the result is
surfaced as an **architecture conflict** with evidence and an exact alternative; the uniqueness
invariant is never weakened for performance.

### 4.3 Timing and measurement semantics are frozen before the first applicable run

Declared now as timing semantics **v1**, and encoded as versioned constants in code before any
benchmark or experiment that depends on them. Changing a value requires a new version and an ADR, and
never happens in response to a result.

| Semantic | v1 declaration | Basis |
|---|---|---|
| `is_late` | arrival delay (Kafka `LogAppendTime` − `occurred_at`) **greater than 600 s** | The value is inherited from the event contract's 10-minute watermark; the quantity is new — arrival delay includes producer buffering, where the watermark measured event-time progress. As-served features, arrival skew and quarantine **never read** `is_late` |
| Replayed history | a **real-time-paced** replay through Kafka publishes `occurred_at` with one uniform, recorded shift to the replay's wall clock (the ADR-0040 technique), so arrival delay reflects the dataset's recorded ingestion lag. A **compressed** replay is a backfill: `occurred_at` is not shifted, records carry a backfill header, `is_late` is null, and they are excluded from arrival skew. Batch adapter loads also carry `is_late = null`. Deterministic `event_id`s are always derived from unshifted content | Without rebasing every replayed event is "late" and the late-event tests would pass vacuously; a uniform shift on a compressed replay would push history into the future and trip future-skew quarantine |
| Streaming dedup pre-filter | **none in v1.** Exact uniqueness comes from §4.2 alone. Any pre-filter added later may drop only rows it has proven duplicate, and the replay and backlog tests assert `numRowsDroppedByWatermark == 0` | A watermarked dedup drops rows older than its horizon as late, which would lose non-duplicates on replay or after a long backlog |
| Future-skew quarantine | gateway-observed events: `occurred_at` more than **86,400 s** ahead of the gateway receipt time carried in the event (strictly greater); other producers: more than 86,400 s plus a **300 s** clock margin ahead of `LogAppendTime` | The gateway's own receipt clock is the one its acceptance check used, so nothing it accepted can be quarantined; the margin for other producers is chosen, and covers broker clock skew |
| As-served order | the store-wide observation counter returned by the atomic observe script and published with each event (paired with the store's epoch, since a restarted store restarts its counter); for events no gateway observed, `LogAppendTime` order | The order the online store actually saw, independent of process or partition interleaving |
| Gold freshness target | p95 of Gold build lag behind the Silver commit it covers **must stay within 15 minutes**, no single build may lag more than 60 minutes, and a Gold build making no progress for 30 minutes while Silver advances is a stall; measured over the stream benchmark's measured window | Values **chosen**, not derived: no online read depends on Gold. The Silver tail beyond Gold's high-water mark *is* correctness-bearing for hydration, so hydration refuses to claim completeness when that tail is beyond the bounded local lake retention (Q8) |
| Consumer lag (throughput and outage targets) | `freshness_lag(t) = t − min over partitions of (the newest LogAppendTime committed to Silver for that partition by t)`, so one stuck partition cannot hide behind a fast one; covering Kafka → Silver commit, sampled at each Silver commit, `processingTime` trigger over continuously produced traffic (never `availableNow` over preloaded data); the broker-to-host clock offset is recorded in the run manifest | Defines what "lag recovers to below 10 s" measures |
| Approximate-feature parity bound | per approximate feature and **per cardinality stratum**, RMS relative error **at most 1%** over **at least 200** comparisons in each stratum and at least 1,000 overall; at true cardinality zero both sides must equal zero exactly; no looser bound without a user decision | A pooled bound would let low-cardinality strata, where the estimator is exact, hide high-cardinality error |
| Arrival-skew target | pass/fail: the **fraction of comparisons whose as-served and event-time-complete values differ** must be **below 1%** on windowed counters, evaluated on the parity corpus's representative partition (an `eval-v2` slice with a lateness model whose parameters are frozen in the parity ADR before its first run); denominator: comparisons where either side has a value; adversarial partitions are recorded, not gated | Declared before measuring so a friendlier stream cannot be chosen afterwards |

### 4.4 Evidence discipline

Hand-derived literal fixtures remain an independent oracle: agreement between reference, Redis and
Spark is necessary and never sufficient, because implementations can share a bug. Mutation tests,
non-vacuous collection guards and `run_id`-backed acceptance evidence are kept throughout. Every
marker- or selector-based acceptance command must fail when it collects zero tests or when every
selected test is skipped.

## 5. Ordered steps

Every step runs its focused tests, is reviewed by the critic, integrates cleanly and leaves
`make verify` green before it is committed. Details of each step's tests, failure-path validation,
observability and resource impact are carried into its ADR and its acceptance evidence.

| Step | Purpose | Owner | Depends on | Evidence |
|---|---|---|---|---|
| **0** | Toolchain and dependency contract: hashed stream extra, verified JVM jars, Java 17 enforced everywhere, fail-fast Spark session factory, CI `test-stream` job | lead | approval | `P3.pin-failfast` |
| **1** | Feature semantics precision (including Q4c's medoid definition and fixtures) and online-store correctness: idempotent Lua observe, adversarial conformance and property tests; Phase 2 load gate and manual replay re-run | lead + parity agent | 0 | `P3.semantics-hardening` |
| **2** | Kafka platform: declared topics, producer factory, fault-injection publisher | Kafka agent | 0 | `P3.kafka-ingest` (part) |
| **3** | Lake conventions, Delta 4.0.1 capability spike, single-JVM stream runtime | Delta + Spark agents | 0 | spike recorded in its ADR |
| **E** | `eval-v2`: legitimate identity and device activity, own manifest and digest, lineage, label-proxy test | generator agent | 0 | `P3.eval-v2` |
| **4** | Durable observation log: `tx.scored.v1`, gateway publisher, session coverage (§4.1), outbox relay; controlled hot-path A/B | lead + Kafka agent | 1, 2 | `P3.observation-log` |
| **5** | Bronze | Spark agent | 2, 3, 4 | `P3.kafka-ingest` |
| **6** | Silver: validation, quarantine, exact canonical dedup (§4.2), deterministic lateness (§4.3) | Spark + Delta agents | 5 | `P3.event-time` |
| **7** | Gold (batch): primitive state and point-in-time features | parity agent | 1, 6 | three-way conformance |
| **8** | Parity framework: overlay, mutation self-tests, collection guard, PARITY manifests | parity + testing agents | 7 | `P3.feature-parity` |
| **9** | Redis reconstruction and evidence-based completeness | parity agent (lead reviews epoch semantics) | 1, 4, 7 | `P3.redis-hydration` |
| **10** | Recovery chaos and replay-from-offset | testing agent | 5, 6 (9) | `P3.checkpoint-resume` |
| **11** | Lake maintenance and resource guards | Delta agent | 5, 6 | `P3.resource-bounds` |
| **12** | Memory model correction | lead | 1 | recorded benchmark run |
| **13** | Stream throughput and outage benchmark (outage at the 5,000 events/s target rate) | testing + Spark agents | 5, 6 | `P3.stream-throughput` |
| **14** | Delta layout benchmark and the layout ADR (local) | Delta agent | 6, 7, 11 | `P3.layout-benchmark` |
| **15** | Databricks strategy (documented, not deployed), documentation, acceptance closure | lead | all | exit checklist |

**Waves:** 0 → {1, 2, 3, E} → {4, 5, 12} → {6, 11} → {7, 10, 13} → {8, 9, 14} → 15.

## 6. Ownership and process

| Owner | Owns | Never edits |
|---|---|---|
| Lead | toolchain; `features/semantics.py`, `spec.py`, `definitions.py`, `reference.py`; the conformance suite; event schemas and the release ledger; gateway integration; migrations; ADRs; `tests/acceptance/status.json` and `docs/PROGRESS.md`; the memory model; integration and `make verify` before every commit | — |
| Kafka agent | `deploy/kafka/`, the compose Kafka service, `contracts/publish.py`, `data/generator/emit.py`, `eval/replay/faults.py`, `services/relay/` | schemas, gateway handlers |
| Spark agent | Bronze and Silver transforms, `services/stream/`, the stream image | Gold, features |
| Delta agent | table registry, checkpoints, snapshots, optimize/vacuum tooling, `benchmarks/delta_layout/` | streaming transforms |
| Parity agent | Redis Lua (after the semantics commit), online key layout, Gold, hydration, `eval/parity/` | semantics files, the conformance suite |
| Generator agent | `data/generator/` except `emit.py`, the `eval-v2` manifest, generator tests | Kafka sink, features |
| Testing agent | Phase 3 chaos tests, collection guards, the `test-stream` workflow, the stream load harness | production modules |
| Critic | read-only review of every step before it lands | everything |

Parallel agents work in isolated git worktrees with non-overlapping ownership. Only the lead commits.

## 7. Acceptance capabilities

| Capability | What it proves |
|---|---|
| `P3.medallion` | every medallion table produced with its invariants |
| `P3.event-time` | duplicates (including retries with new envelope ids), out-of-order and late events handled exactly; future skew quarantined |
| `P3.checkpoint-resume` | kill/resume at injected points with no loss and no double count |
| `P3.feature-parity` | exact implementation parity on an adversarial stream with Spark actually executed; arrival skew recorded |
| `P3.layout-benchmark` | local layout chosen by a recorded benchmark |
| `P3.kafka-ingest` | declared topics, correct producer, conservation into Bronze |
| `P3.observation-log` | every loss mode in §4.1 detected under chaos |
| `P3.semantics-hardening` | the declared semantics pass literal fixtures on every implementation; Phase 2 gates re-met |
| `P3.redis-hydration` | reconstruction reaches completeness only through evidence, never across a gap |
| `P3.stream-throughput` | the throughput target and the outage-recovery target, recorded honestly |
| `P3.pin-failfast` | a wrong Java, Spark, Delta, Hadoop, Scala or jar fails fast with an actionable message |
| `P3.resource-bounds` | local resource caps asserted and unsafe maintenance refused |
| `P3.eval-v2` | the Q5 requirements, including the label-proxy test |

## 8. Exit checklist

The ROADMAP Phase 3 exit conditions, unweakened, plus: all thirteen capabilities above PASS with
executable evidence; Phase 2's load gate and manual replay re-met after hot-path changes; Redis, Kafka
and Spark tests running in CI with nothing silently skipped; documentation updated (including the
removal of the Silver tokenization claim and the local-override statement); the final adversarial
review recorded; and no Phase 4 ML, agent or cloud-deployment work.
