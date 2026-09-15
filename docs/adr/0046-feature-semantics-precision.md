# ADR-0046: Feature semantics made precise — one declared meaning per feature, and an online store that is idempotent and order-independent

- **Status:** Proposed. The semantic decisions are made. Self-inclusion was decided by the user on 2026-09-13, and Q4c's sample of 20 with a minimum of 3 is approved for now. The ADR becomes Accepted when Phase 3 Step 1 lands with every literal fixture passing on the naive reference and on the Redis store. §7 records an architecture conflict that is **not** decided here.
- **Date:** 2026-09-13
- **Phase:** 3
- **Supersedes / Superseded by:** Extends ADR-0032 by declaring the precision its shapes left implicit. It replaces ADR-0032's offline translations: `window()` and `lag()` produce neither per-transaction windows nor the declared previous observation. It amends ADR-0034's parity tolerance for approximate distinct counts, and ADR-0032's classification of the robust z-score as approximate. Implements `docs/PHASE3_PLAN.md` §3, Q4.

## Context

ADR-0032 made every feature carry a declared shape — entity, stream, window, aggregation — so that a
second implementation could be compiled from the declaration rather than re-derived from prose. It did
not declare the precision a compiler needs: what identifies an observation, which currency a figure
covers, how ties are broken, whether the scored transaction is part of its own window in practice, how
far back a profile reaches, which estimator a robust statistic uses, or what happens when observations
arrive out of event-time order.

Phase 3 planning showed that the gap was not theoretical. On inputs the conformance suite never
supplied, the Redis store and the naive reference returned different numbers — not absences — for
bucket-derived sums at a window's boundary minutes, for duplicate deliveries, for an older observation
arriving after a newer one, for first-seen, for the home location, for a profile keyed by currency, for
profile reads that included later-dated observations, for retention trims driven by a future-dated
event, and for a distinct count scored after a later occurrence of the same value. One defect — the
coefficient of variation dividing same-currency sums by an all-currency count — was present in **both**
implementations, so agreement between them would have passed on it.

One divergence was between the code and ADR-0032 itself. ADR-0032 declares windows half-open
`(t − W, t]` with "the scored transaction inside its own window". The gateway read features **before**
recording the transaction, so every value it served excluded it, and the conformance suite pinned the
exclusion. Nobody chose that; it followed from the order of two calls.

## Decision

Every rule below is written into `trace_core.features` as a declaration, implemented by the naive
reference, and pinned by a **hand-derived literal fixture** in
`tests/conformance/feature_semantics_suite.py`. Agreement between implementations is necessary and never
sufficient; the literals are the oracle.

### 1. Time, identity, order, and the two evaluation modes

- **Event time only**, compared at **millisecond** precision. Every `occurred_at`, and `as_of`, is
  floored to epoch milliseconds before any comparison.
- **Every observation has an identity**, unique per stream as plan §3 Q2 requires:
  `<namespace>:<event_id>`, where the namespace is `transaction` or `identity_event`
  (`observation.IdentityNamespace`) and `event_id` is required and non-empty.
  - A transaction's `event_id` is its `transaction_id`.
  - An identity event's `event_id` is minted by the gateway at ingress, and is derived from the
    caller's optional `X-Idempotency-Key` once that is supported (plan §3, Q2). It is never taken
    from `X-Request-Id`, a correlation header the caller chooses.
  - Without namespaces, a failed login whose id equalled a transaction id would be recorded first
    and swallow the transaction as a "redelivery".
  - Until `X-Idempotency-Key` is supported, a retried identity event is a new observation and counts
    again. That support is a precondition of serving these semantics (§5).
  - An observation contributes at most once per identity. **The first delivery of an identity is the
    observation**; a later delivery under the same identity contributes nothing, whatever it carries.
  - A store reports a later delivery that carries a different observation -- another account,
    amount, time or place, post-decision fields aside -- as **conflicting**
    (`ObserveReceipt.conflicting`). The context it serves is the first delivery's, so the gateway
    does not evaluate the new payload against it: it decides rules-only and says
    `observation_conflict`.
- **The declared total order** is `(occurred_ms, namespace, event_id)`, compared as strings after
  the millisecond. At one millisecond, every identity event therefore sorts before every
  transaction.
- **Strict event time** (Q4f): an observation dated after `as_of` never contributes, whatever order
  observations arrived in. The single exception is §2's approximate distinct counts.
- **Two evaluation modes** (`semantics.EvaluationMode`) answer two different questions of one
  declaration.
  - **`AS_SERVED`** is what the online store served. The scored transaction is recorded, and in the
    same atomic operation the read sees exactly the observations recorded up to and including it. The
    store reports the read's **position**, a counter that moves once per newly recorded observation,
    paired with the store's epoch (plan §4.3). An observation recorded after the scored transaction
    cannot contribute to it, **even one at the same millisecond**. An offline replay of served values
    must therefore follow the recorded order and treat the scored transaction as the current
    observation; a range frame over `occurred_at` is not an implementation of this mode.
  - **`EVENT_TIME_COMPLETE`** is what a complete history says: every observation, whatever order it
    arrived in, with the first delivery of each identity in arrival order. Gold computes this mode.

### 2. Windows, and whether the scored transaction is inside its own

- Half-open `(as_of − W, as_of]`, as ADR-0032 states. An observation exactly `W` old is outside.
- **Decision (user, 2026-09-13): the scored transaction is inside its own transactional windows.**
  - ADR-0032 is honoured. Phase 2's exclusion was implementation drift and is corrected in feature set
    `2.0.0`.
  - **Rule thresholds are not shifted by one to preserve Phase 2 behaviour.** The Phase 2
    representative load gate, the manual replay and rule validation are re-run on the declared
    thresholds, and any regression on legitimate traffic is surfaced with evidence. A threshold change
    would be a separate, explicit ruleset decision.
- **Self-inclusion is declared per feature** (`semantics.CurrentObservation`), never implied by write
  order. A registry refuses two features that read one window but declare it differently.
  - **`INCLUDED`** applies to every windowed count, sum, ratio, distinct count and the merchant
    coefficient of variation. The window's upper edge is the scored transaction's own place. As served,
    that means everything the read can see: everything recorded before it, and the transaction itself.
    Over a complete history, it means every observation at or before the scored transaction in the
    declared order. An observation at the same millisecond counts when it sorts at or before it.
  - **`EXCLUDED`** applies to baselines and to the previous observation: the robust z-score, home, the
    habitual and known sets, tenure, and the three pairwise features and `hours_since_identity_change`.
    These read only observations **strictly before `as_of`** in event time, in both modes. Other
    observations at the same millisecond are excluded too, so a baseline never depends on arrival order
    at a tie.
  - A field the scored transaction carries that is not known when it is scored never contributes from
    it, even to an `INCLUDED` feature (§7).
- **Counts and exact distinct counts** cover all currencies.
- **`account_amount_sum_1h`** is an exact window over the **same currency** as the scored transaction.
- **`declined_ratio_1h`** is an exact window over **all currencies**. Its required fields name no
  currency. The denominator is the observations whose outcome is known, excluding the scored
  transaction's own outcome (§7). Zero known outcomes is `INSUFFICIENT_HISTORY`.
- **`merchant_amount_cv_24h`** uses a **declared minute alignment** (Q4a).
  - An observation belongs to minute `m = floor(occurred_ms / 60 000)`.
  - The window is the whole minutes strictly between the minute containing `as_of − W` and the minute
    containing `as_of`: `floor((as_of_ms − W_ms) / 60 000) < m < floor(as_of_ms / 60 000)`.
  - The scored transaction itself is added once. It lies in the excluded minute containing `as_of`.
  - Count, sum and sum of squares all come from the **same currency** as the scored transaction.
  - `CV = sqrt(max(q/c − (s/c)², 0)) / |s/c|`, and `INSUFFICIENT_HISTORY` when `c < 2` or the mean is
    zero.
  - Nothing dated after `as_of` can contribute, so strict event time holds.
- **Approximate distinct counts** (`ip_distinct_accounts_1h`, `merchant_distinct_accounts_1h`) use the
  estimand ADR-0034 chose.
  - The estimand is the distinct count over **five-minute buckets inclusive at both edges**:
    `floor((as_of_ms − W_ms) / 300 000) ≤ b ≤ floor(as_of_ms / 300 000)`.
  - This is the one declared exception to strict event time. The bucket containing `as_of` may hold
    observations dated after `as_of`. As served, it holds those recorded before the read; over a
    complete history, all of them.
  - Excluding that bucket would blind both features to their most recent five minutes, which is where
    credential stuffing and a colluding merchant show.
- **A redelivered scored transaction is read as its first delivery.**
  - As-of, currency and entities come from the first delivery.
  - The read is against the store as it is at the redelivery, so observations recorded after the
    first delivery are visible to it.
  - Its own identity never enters its baselines or its previous observation. The fields the
    features compare against a baseline (amount, location, merchant, device) are the request's.
  - A redelivery whose content differs from the first delivery is a conflict. The gateway flags it
    in Step 1's gateway work.

### 3. Profiles (ACCOUNT)

- **Lifetime** (Q4b) is the account's transactions strictly before `as_of`, back to the most recent
  **inactivity gap of at least 30 days**. Walking back in the declared order, the lifetime
  ends before the first consecutive pair at least 30 days apart. If the latest observation is at least
  30 days before `as_of`, the lifetime is empty.
- **Currency**: every profile attribute covers all currencies except the amount sample, which is the
  scored transaction's currency.
- **Tenure** is `as_of` minus the earliest observation in the lifetime, in days. The completeness gate
  ADR-0044 declared for negative answers is kept.
- **Habitual merchant / category** needs at least three observations in the lifetime at that merchant
  or category. **Known device** needs at least one.
- **Robust z-score** (Q4d) uses the median and MAD of `|amount_minor|` over the **last 128**
  same-currency observations in the lifetime, in the declared order.
  - At least eight observations are required.
  - The median of an even count is the mean of the middle two.
  - `z = (|amount| − median) / (1.4826 × MAD)`, capped at ±50.
  - A zero MAD gives 0 when the deviation is zero, and otherwise ±50 **with the sign of the
    deviation**. Phase 2 returned +50 for a lower amount too, so R015, R009 and R016 read a small
    payment on a constant-amount account as a high-value anomaly.
  - Declared as the definition, so the feature is **exact** and parity is an equality.
- **Home location** (Q4c, N = 20 and minimum 3 **approved for now** by the user on 2026-09-13) is the
  **geodesic medoid of the last 20 located observations** in the lifetime, all currencies, in the
  declared order.
  - At least **three** located observations are required. Unlocated observations stay in the lifetime
    but take no place in the sample. Three is the smallest set whose medoid survives one outlier.
    Repository evidence from `eval-v1`: most transactions have at least three prior located
    observations and far fewer have twenty, so a larger minimum would remove the feature from most
    young accounts.
  - The medoid is the sample observation that minimises the sum of great-circle distances to the
    others. Distances use `trace_core.domain.geo.haversine_km` with the IUGG mean radius, 6 371.0088 km.
    Each is rounded to whole metres as `floor(d_m + 0.5)` before summing, so every implementation
    compares integers.
  - Ties go to the earliest in the declared order.
  - A medoid is always one of the account's own points and is defined on the sphere, so it cannot break
    across the antimeridian the way a component-wise median does.
  - `distance_from_account_home_km` is `haversine_km(medoid, transaction)`.

### 4. Pairwise and identity streams

- **Previous observation** is the latest observation in the declared order on the stream in
  `(as_of − L, as_of)`, where `L` is the declared 24-hour lookback. Both ends are open: an observation
  exactly 24 hours old is outside, and one at `as_of` is never the previous one.
- **Stream derivation** (Q4e) is declared in `trace_core.features.semantics.identity_stream`, not in
  the HTTP handler.
  - `PASSWORD_CHANGE`, `EMAIL_CHANGE`, `PHONE_CHANGE`, `ADDRESS_CHANGE` and `MFA_RESET` feed
    `IDENTITY_CHANGE`.
  - `LOGIN_FAILED` feeds `IDENTITY_FAILED_LOGIN`.
  - `LOGIN_SUCCEEDED`, `MFA_ENROLLED` and `UNKNOWN` feed no feature stream and change no online state.

### 5. Obligations on implementations

- **The score-time read is atomic.** The store records the scored transaction and reads its context in
  one operation, and returns the position.
  - The Redis store does both in one Lua script; `ReferenceFeatureStore.score` is the same contract.
    Redis refuses a `#!lua` script that may write before it starts when the instance is over
    `maxmemory`, so a refused score leaves nothing half-recorded
    (`tests/integration/test_online_store_capacity.py`).
  - Reading first and then adding the transaction in memory is not equivalent. A HyperLogLog cannot
    count a member without a write, and an exact distinct count would need a membership read.
  - Reading and writing in separate operations lets two concurrent transactions each miss the other
    while the recorded order says one preceded the other.
- **The online store is idempotent and order-independent.**
  - Every contribution is keyed by identity, and identity is checked across the whole store rather
    than per account: a redelivery naming another account is still a redelivery, read as its first
    delivery.
  - The previous observation, first-seen and lifetime boundaries are derived from event time, never
    from the last write.
  - Retention trims are computed from `min(occurred, now)`, so a future-dated observation cannot evict
    current state.
  - Beyond 25 hours an account's transactions are folded, in event-time order, into a bounded profile
    prefix: the lifetime's start, visit counters, known devices, the last 128 amounts per currency and
    the last 20 located points. A folded observation is no longer recognised as a redelivery, and one
    folded out of order marks that lifetime inexact, so its profile features read as absent until a
    30-day gap starts a new lifetime. Neither case produces a different number.
- **An observation the store did not record withdraws its completeness, durably.**
  - `features.completeness.CompletenessGuard` and its PostgreSQL ledger implement this, tested against
    a real database, and are wired into the gateway's scoring pipeline and its identity and device
    ingress.
  - A hole is opened when a write is refused, the store is unreachable, or the breaker is open.
  - The hole is recorded **once per episode** in `app.feature_store_holes` (migration 0004), never
    once per transaction. While it is open, no read may claim completeness.
  - Clearing covers only holes recorded before the withdrawal began. A hole another instance records
    during it stays open.
  - `trace_app` can record a hole and clear it once. It cannot delete or rewrite one, so the record of
    an outage survives the process whose outage it was.
  - Before the store is used again, its epoch is **moved forward** to the withdrawal time plus the
    24-hour future-skew bound. Deleting the epoch is not enough: a lost observation may be dated up
    to that bound ahead of its arrival, the same conservative mapping plan §4.1 point 6 uses for
    history.
  - The margin covers every recorded stream. Transactions, identity events and device events dated more
    than 24 hours ahead are all refused at ingress.
  - A process that starts with an open hole inherits it. A running process learns of another
    instance's hole only when that instance withdraws it, by the epoch moving in the shared store; if
    that instance stops first, its peers keep claiming completeness until one of them restarts. One
    gateway runs locally; closing this for several instances belongs with plan §4.1's writer fencing
    (Step 4).
  - A ledger that is configured but unreadable counts as holding one until it can be read. Read
    later and empty, it vouches as it would have at start-up, and nothing is withdrawn -- unless this
    process lost an observation in the meantime. The gateway opens its database pool before the
    guard first reads the ledger. A crash after a failed ledger write forgets the hole; plan §4.1's
    writer fencing, in Step 4, closes that window.
  - The conditions for wiring it, as met in Step 1b:
    - withdrawing keeps the later of the store's existing epoch and `resume_at`, in one
      compare-and-set script;
    - the store's refused-write path issues no command at all, so nothing re-creates the epoch at
      "now";
    - hydration (`P3.redis-hydration`) must still check the ledger before restoring any earlier epoch.
  - When the scored transaction itself is refused, the decision is made on a read-only snapshot that
    does not contain it, and is marked degraded. The published scored event carries that observe
    outcome, so an as-served replay reproduces what was served.
- **On an incomplete store, a window's value is a lower bound.** ADR-0044 already declares this for any
  present window. Because the scored transaction is now always inside its own windows, such lower
  bounds are served where Phase 2 served absences. The `history_incomplete` degraded reason still
  fires, because the profile features gated on the 30-day horizon are absent on any store younger than
  that.
- **A read behind what the store still holds is absent, not a lower bound.** Each structure keeps
  what its widest window reads, plus the late-arrival margin, behind the newest observation. A read
  further behind — a late arrival, or a redelivery read at its first delivery's time — would find the
  oldest part of a window trimmed and serve what is left. The store knows how far each structure has
  been trimmed, folded or dropped. A declared window reaching behind that is left out, and the context
  stops vouching for it, so it reads `INSUFFICIENT_HISTORY` and the decision carries
  `history_incomplete`. A previous observation is served only when nothing that could be later has
  been folded away (`tests/integration/test_redis_feature_store.py`).
- **Parity is declared per feature** (`FeatureSpec.parity`, a `semantics.ParityComparison`), so a
  comparison cannot quietly choose its own tolerance.
  - `EXACT`: counts, amount sums, exact distinct counts and the habitual and known sets. Compared
    by equality.
  - `FLOAT`: the declined ratio, the merchant CV, tenure, the robust z-score, the home distance, and
    every pairwise feature (distance, speed, and elapsed seconds or hours). Compared at a relative
    tolerance of `1e-9`, which is arithmetic, not estimation.
  - `APPROXIMATE`: the HyperLogLog distinct counts. Compared at the per-stratum bound frozen in
    plan §4.3.
  - The literal fixtures enforce this: an `EXACT` feature may not be given a tolerance.
- **`FEATURE_SET_VERSION` becomes `2.0.0`**, because served values change.
- **No run is recorded until the served values conform.**
  - `spec.SERVED_FEATURES_CONFORM` was false until Step 1b made the Redis store and the gateway's
    score-time read implement these semantics.
  - While it was false, the load harness and the gateway replay refused before doing anything, so no
    record could carry a version its values were not computed with.
  - Setting it required the Redis store to pass every fixture, the atomic score-time read, the wired
    completeness guard, and `X-Idempotency-Key` identities for identity events. All four hold as of
    Step 1b, and a change that breaks one must set it back.

### 6. Literal fixtures, and the tests that keep them honest

Every fixture is hand-derived and carries its arithmetic. Fixtures whose answer is the same in both
modes run in both; mode-dependent fixtures are written in pairs.

- **Windows and self-inclusion**:
  - the scored transaction in its own counts, sums and distinct counts;
  - a window's lower boundary at exactly `W` and one millisecond inside it;
  - millisecond flooring of both `as_of` and the observation;
  - out-of-order arrival;
  - a later-dated observation delivered first;
  - a future-dated observation delivered last.
- **Same-millisecond observations**:
  - recorded before and after the scored transaction, as served;
  - identity-ordered, in every arrival order, over a complete history;
  - the identity tie-break applying only at the upper edge;
  - failed logins at the scored millisecond in both modes.
- **Duplicates and identities**:
  - redelivery of a transaction with different content, of the scored transaction, and of an
    identity event;
  - a redelivered scored transaction read as its first delivery;
  - an identity event whose id equals a transaction's.
- **Currency and outcomes**:
  - mixed-currency amount sums and declined ratios;
  - the scored transaction's own outcome, declined or approved;
  - no earlier known outcome.
- **The merchant CV**: whole minutes, the minute containing `as_of`, the partial far-edge minute, other
  currencies, too few observations, and a zero mean.
- **`as_of` exactly on a minute and a five-minute bucket boundary**, with observations on both sides of
  both edges.
- **Distinct counts**:
  - a value also seen later in event time;
  - a value from another currency;
  - edge-inclusive buckets, including an observation after `as_of` recorded before and after the read.
- **The robust z-score**:
  - at 7, 8, 128 and 129 observations, with a non-saturating baseline;
  - excluding the scored transaction and its millisecond;
  - excluding other currencies;
  - the ±50 cap, and the sign of a zero-MAD deviation.
- **Profiles**:
  - a lifetime gap of exactly 30 days and one millisecond less;
  - an account quiet for exactly 30 days and one millisecond less;
  - the same history delivered newest first;
  - profiles built from another currency's transactions;
  - two eras across a gap, so the samples, home and habits read only the lifetime.
- **Habitual sets** with a redelivered visit.
- **Home**:
  - 0, 1 and 2 located observations, then exactly 3;
  - exactly 20;
  - more than 20, with unlocated observations interleaved;
  - an outlier;
  - ties by time and then identity;
  - whole-metre half-up rounding that decides the medoid, where unrounded and rounded-down sums pick a
    different point;
  - the antimeridian.
  - Every home fixture puts the scored point away from home, and home away from the last located
    observation, so a store that uses the last location cannot pass.
- **The previous observation**: the latest rather than the last delivered, never at the scored
  millisecond, the half-open lookback, and ties at one millisecond.
- **The whole feature set on an empty store**: every value is what the scored transaction alone
  contributes.

`test_feature_semantics_mutations.py` holds 58 mutants. Each breaks one rule listed above in a copy of
the reference, of the shared arithmetic (`profile_math`) or of the feature definitions. It requires the
fixture written for that rule to fail **on a feature expectation**; a crash or a harness assertion does
not count as a catch. The mutants include:

- profiles keyed by currency, or walked in arrival order;
- samples read beyond the lifetime;
- previous-observation ties going to either delivery;
- missing or wrong whole-metre rounding;
- identities shared across streams, and the order between namespaces;
- a redelivery read at its own time, or only up to its first delivery's position;
- transaction counts confined to one currency, and habitual categories read beyond the lifetime;
- the guards held in the definitions.

Three identity filters, in the aligned window, the profile and the previous observation, cannot change
any value once reads anchor at the first delivery. They are defence in depth, and no fixture tests them.

Its reach is the reference implementation. The Redis store and Spark are held to the same fixtures,
not to the mutants. It also swaps whole implementations, and requires the as-served fixtures to fail in
two cases:

- an as-served replay computed over a complete history;
- an as-served replay computed as a range over `occurred_at`.

The same test swaps them the other way and requires the complete-history fixtures to fail when the
complete history is computed as served.

This is how fixtures that cannot fail are found. The critic's first review disproved the claimed
coverage for nine rules, and its second for one more rule and three mutant gaps. Each now has a fixture
and a mutant.

Property tests tie the modes together. They cannot catch a rule broken identically in both modes;
only the fixtures and their mutants can. One property builds histories that reach, by construction and
with the reach asserted, a lifetime gap on either side of 30 days, a computed robust z-score and a home
sample that overflows:

- serving observations in the declared order and scoring the latest gives the complete answer;
- with the scored transaction at any position in that order, and observations tied at its millisecond
  on both sides, serving everything before it gives the complete answer for every non-approximate
  feature;
- redeliveries change nothing in either mode;
- on those constructed histories, the modes agree on every non-approximate feature.

### 7. Causality of the scored transaction's own fields — an architecture conflict

**Evidence.** TRACE-X's decision gates authorization:

- `docs/ARCHITECTURE.md` §1 shows the payment system sending the transaction synchronously and receiving
  the `RiskDecision`;
- ADR-0002 says "an authorization decision must return in tens of milliseconds";
- scoring fails open as "approve + flag" (CLAUDE.md §3.7).

`AuthorizationOutcome` is documented as "what the upstream authorization system did with the
transaction", and one of its values is `REVERSED`, which can only happen later. So a transaction's own
authorization outcome is **post-decision information**. Yet:

- `tx.raw.v1` — "a transaction as it arrived, before scoring" — **requires** `authorization_outcome`;
- `POST /v1/transactions` accepts it;
- the generator sets it on every transaction: always `APPROVED` for legitimate traffic, and `DECLINED`
  only inside the card-testing scenario.

The other fields a scored transaction contributes are available when it is scored: identifiers, amount,
currency, merchant, category and country, location, channel and entry mode.

**What Step 1 does, which introduces no leakage.** The scored transaction's own outcome never
contributes to its own features (`observation.POST_DECISION_FIELDS`). `declined_ratio_1h` treats it as
not yet known. This is pinned by a fixture and by a mutation.

**What remains unresolved, and is not decided here.**

1. Earlier transactions' outcomes are taken from their own scoring requests. They become visible to the
   very next scoring, even one millisecond later. In the documented topology an outcome is known only
   after the earlier decision, at a delay nothing models.
2. A pre-decision contract carries a post-decision field.
3. In `eval-v1`, `DECLINED` occurs only in fraud. A non-zero `declined_ratio_1h` is therefore close to a
   label proxy on Track A, and rule R002 reads it.

**Options.**

- **A — outcomes become their own dated observations.** An authorization-result event, dated when the
  outcome became known, feeds the declined ratio, which reads only outcomes known strictly before
  `as_of`. The request field is no longer read by any feature. The generator emits outcomes with a
  declared lag and legitimate declines. This needs a new event contract, so it is a user decision.
- **B — declare TRACE-X post-authorization.** This contradicts ADR-0002 and the §1 topology.
- **C — keep request-supplied outcomes but count an earlier outcome only after a declared minimum lag.**
  This is a heuristic whose delay nothing measures.

**Recommendation: A**, as its own decision and outside Step 1. Until it is decided, `declined_ratio_1h`
and R002 keep Phase 2's treatment of earlier outcomes, and `P3.semantics-hardening` discloses it.

**Decided (U7, 2026-09-14): option A**, designed in ADR-0049. Until ADR-0049 is implemented, the
treatment above stands.

### 8. Bounded score-time reads (Step 12; user decision 2026-09-15)

**Why.** The score-time read fetched an account's whole 25-hour raw set, and a device's members per
window, then decoded every observation (`run_id: bench-20260915-071647-score-latency-86fabdbf`).
- **The cost.** It grows about linearly with depth. At depth 8,192 the script alone outruns the
  gateway's 20 ms Redis timeout, so the account's scores go rules-only, blind to its features.
- **Who controls depth.** An adversary does: the rate limiter is per token.
- **The shared cost.** Redis runs scripts on one thread, so a long script also delays other accounts'
  calls.
- **The decision.** The user chose to cap the read and make depth a declared risk signal.

**Decision.**
1. **Counts and last observations stay exact at any depth.**
   - *Counts.* A window count is a sorted-set range count on `occurred_ms`, O(log N): `(as_of - W,
     as_of]` when the current observation is included, `(as_of - W, as_of)` when excluded. This
     covers `account_tx_count_*`, `card_tx_count_5m` and `failed_logins_1h`.
   - *Outcomes.* The outcome counts behind `declined_ratio_1h` are range counts too.
   - *A layout change this needs.* Identity events move from one account set to one set per
     identity stream, so failed logins can be counted without reading identity changes. A capped
     failed-login count would blind R012 during credential stuffing, the attack that makes it deep.
   - *Last observations.* The previous transaction and the latest identity change are bounded
     reverse ranges with `LIMIT 1`.
   - Nothing that is a count becomes a lower bound.
2. **Content is read up to a declared cap, `SCORE_READ_CAP = 512` observations per raw set.**
   - It covers the features that need observation content: `account_amount_sum_1h`,
     `account_distinct_merchants_1h`, `account_distinct_mcc_5m`, `account_distinct_devices_24h`,
     `account_distinct_countries_24h` and `device_distinct_accounts_24h`.
   - Already bounded, and unchanged: HyperLogLog distinct counts and the merchant CV's minute
     counters.
   - **The account profile reads the same capped raw set.** It is the folded prefix plus the raw
     25-hour lifetime. Found in implementation: an uncapped profile keeps the read O(N), and a
     profile built from a capped read is wrong rather than absent. When the raw read is capped:
     - *Positive memberships stay exact.* A device seen in the capped read or the prefix is known;
       a merchant or category with three or more visits there is habitual. More history can only
       confirm them.
     - *The robust z-score and the home point stay exact* when the capped read holds at least 128
       same-currency amounts, or at least 20 located points, after the lifetime's start. Those are
       all they read, and the most recent ones are inside the capped read. Otherwise they are
       absent.
     - *Tenure* is exact when the prefix holds the lifetime's start. Otherwise it is absent.
     - *Negative memberships are absent.* That covers an unknown device, and a merchant or category
       under three visits.
     - An absent profile feature reads `INSUFFICIENT_HISTORY` and `INCOMPLETE` with
       `history_depth_capped`, as in point 3.
   - The read takes the most recent 512 at or before `as_of`, by `(occurred_ms, identity)`.
   - The value is fixed now, from the recorded curve: at depth 512 the script took 2.6 ms and a
     whole score p99 20.2 ms (`run_id: bench-20260915-071647-score-latency-86fabdbf`), well inside
     the 20 ms timeout and the 100 ms budget.
   - It is not tuned after the load gate re-runs; changing it needs a new decision.
3. **A capped window is absent, not a lower bound**, consistent with §5's rule for unheld history.
   - *When a window counts as capped.* The set holds more than the cap inside the window's lookback,
     which the exact count shows.
   - *Its content features.* They read `INSUFFICIENT_HISTORY` with `lookback_completeness =
     INCOMPLETE`, and the decision carries the degraded reason `history_depth_capped`.
   - *Contract.* Both are within the released `tx.scored.v1` contract: the state and completeness
     enums already hold these values, and degraded reasons are free strings. No `.v2` is needed.
   - *Counts.* Stay AVAILABLE and exact.
4. **Depth is a declared risk signal.**
   - Weight 0.5 with no band floor, declared by analogy with the sustained-volume rule R006 (0.5, no
     floor). Alone it scores MEDIUM, which opens no investigation, and it adds to any rule that
     fires. A band floor is the policy lever if depth alone should escalate. Weights live in the
     rule pack.
   - Rule `R019_history_depth_capped`: `account_tx_count_24h >= 513`, the exact count at which the
     24-hour content read is capped.
   - The core pack goes to 1.1.0 with a new digest; decisions already carry the pack digest.
   - It is declared from the cap, not fitted.
   - Existing velocity rules still fire on the exact counts.
   - *Where it misses.* The profile is capped on the raw 25-hour history, and R019 counts 24 hours. An
     account with 512 transactions in 24 hours plus more in the 25th hour is capped without R019
     firing. A fixture pins this boundary, and `state_plan` declares the 25-hour horizon. It is
     accepted: R019 declares the dominant case, and adding a 25-hour count feature for this edge
     would be a new served feature.
5. **Versions and layout migration.**
   - `FEATURE_SET_VERSION` goes to 4.0.0, because served values change for capped accounts.
   - An account's legacy single identity-event set is read as not held (`history_incomplete`) until
     it expires within 25 hours. It is not migrated inside the write script: moving a deep legacy set
     in one atomic script is itself the O(N) stall this section removes.
   - The first new identity write to such an account sets the legacy set to expire and records its
     newest event time as dropped-through. Without that marker, a read reaching behind it after
     expiry would silently undercount instead of reading absent.
   - While a legacy set exists, other account features with lookbacks of an hour or more on that
     account also read INCOMPLETE, for at most 25 hours.
   - *Where the reference and Redis can still differ.* In four cases no fixture covers them, and
     Redis only ever serves an absence, never a different number:
     - a late read after a later-dated write folded past `as_of` minus 25 hours;
     - a scored transaction dated ahead of the wall clock;
     - a snapshot that still holds older, unfolded transactions;
     - 512 or more transactions at the exact scored millisecond.
     Parity (Step 8) expects these.
6. **Evaluation modes.**
   - *Where the cap applies.* It is an as-served obligation: the reference's AS_SERVED mode and the
     Redis store implement it against shared literal fixtures.
   - *Where it doesn't.* Event-time-complete reads (the reference's EVENT_TIME_COMPLETE mode, Gold)
     are uncapped and exact.
   - *Parity (Step 8).* A capped as-served value is compared as absent, and arrival-skew
     comparisons exclude capped windows and record their count.

**Fixtures** (literal, shared by the reference's as-served mode and the Redis store):
- A window at exactly 512 observations: not capped, every feature exact.
- At 513: counts exact, content absent and INCOMPLETE, reason present, R019 fires.
- A device with more than 512 members in 24 hours: its distinct-accounts count is absent.
- Idempotent redelivery at the cap boundary.
- A current observation excluded or included at the boundary millisecond.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Exclude the scored transaction from every window | The user's decision was to honour ADR-0032. Exclusion was never chosen; it followed from the order of two calls |
| Shift the velocity thresholds by one to keep Phase 2's firing | Thresholds are declared ruleset configuration. Moving them to hide a semantic change would be a threshold decision taken implicitly |
| Read the context, then add the scored transaction in memory | A HyperLogLog cannot count a member without writing it. Separate read and write operations race: two concurrent transactions can each miss the other while the recorded order says one preceded |
| A range frame over `occurred_at` for as-served replay | An observation at the same millisecond that arrived after the scored transaction would leak into its value; a mutation test pins this |
| Include the scored transaction in baselines too | The transaction would enter the baseline it is compared with, and hide the departure the feature measures |
| Leave precision implicit and let parity tests define it | Two implementations that share a defect agree; the CV denominator showed this is not hypothetical |
| Declare whatever the Redis store serves to be the truth | Its divergences are wrong numbers: a duplicate turned a dispersed merchant into a laundering signal |
| Component-wise median of latitude and longitude for home | Not a point the account visited, and wrong across the antimeridian |
| Last-seen location as home | One trip moves "home"; the declared description is a habitual location |
| Arrival order for backdated scoring | Rejected in Q4f. Even as served, a later-dated observation that arrived first never counts |
| Currency-keyed profiles | `required_fields` declare no currency for tenure, habitual, known-device or home; a purchase in another currency at a habitual merchant is not novel |
| Exact half-open CV for merchants | Rejected in Q4a: per-observation amounts for a busy merchant make the read unbounded on the hot path |
| Compare medoid distance sums as floats | A last-bit difference between libm implementations can flip a near-tie, making parity flaky on a correct implementation |
| Exclude the current five-minute bucket from approximate distinct counts | Keeps strict event time at the cost of the most recent five minutes of the two signals whose value is recency |
| A per-value 0.81% tolerance for approximate features | One standard deviation used as a hard bound fails large-cardinality comparisons routinely and invites a prohibited widening |
| Count the scored transaction's own authorization outcome | Post-decision information (§7) |
| One identity namespace across all streams | A failed-login event could share a transaction's id and swallow it; plan §3 Q2 declares identities per topic |
| Take an identity event's id from `X-Request-Id` | A caller-chosen correlation header: two distinct events sent under one would count once |
| A zero MAD scores every departure +50 | A lower amount on a constant-amount account would read as a high-value anomaly |
| Pre-aggregating account windows inside the score script (§8) | Exact at any depth, but a new store layout whose idempotency and order-independence must be re-proved, with a re-run memory model and load gate; the user chose the bounded read |
| A per-account admission cap at the gateway (§8) | Bounds depth, but refuses legitimate high-volume accounts: a product decision the user did not take |

## Consequences

**Positive.**

- Each feature has one meaning that a compiler can translate without guessing, including whether the
  transaction is inside its own window and what "before" means at a tie.
- The demonstrated divergences are literal fixtures that fail until fixed, and each listed rule's
  fixture is shown to fail by at least one of 58 mutants.
- A duplicate, a retry, a backdated transaction or an out-of-order arrival no longer changes a served
  value.
- The profile no longer depends on the currency of the transaction being scored.

**Negative.**

- An account above the score-time read cap (§8) loses its content features while it stays above:
  amount sums, distinct counts, and the profile's negative memberships and uncovered tenure,
  z-score and home. Its counts stay exact, and R019 declares the condition. Rules that read those
  features (R008, R009, R015 to R018) may abstain on such an account. Before §8, a deep account's
  score timed out and every feature was absent.
- Served values change, so `FEATURE_SET_VERSION` moves to `2.0.0`.
- Each velocity rule now fires one transaction earlier than it did in Phase 2 at the same threshold. For
  example, `account_tx_count_1m ≥ 5` fires on the fifth transaction in a minute rather than the sixth.
  The load gate, the manual replay and rule validation are re-run to measure what that does to
  legitimate traffic.
- Measured on the `eval-v1` replay, in a controlled comparison with the Phase 2 code: R010
  (`device_distinct_accounts_24h >= 5`) adds 9 legitimate and 3 fraud high-risk decisions, every one
  attributed to self-inclusion (`eval/replay/attribute_rule_changes.py`). This is expected semantic
  drift, not a regression. R010 stays at 5 (decision U10); its threshold is studied on `eval-v2` once
  that dataset is frozen and its label-proxy checks pass, and any change is a separate, versioned
  ruleset decision.
- The online store holds more: every observation raw for 25 hours under its own identity key, then a
  bounded folded profile per account. That costs memory and hot-path CPU: a score decodes its
  account's last 25 hours in the gateway, so the cost grows with the account's velocity. The Phase 2
  memory model describes the Phase 2 layout and does not bound this one; memory is re-measured in plan
  Step 12, and the medoid's cost must stay bounded on the hot path.
- The store's script reads and writes several entities' keys at once, so it runs on one Redis
  instance, not a Redis Cluster. Sharding is a Phase 12 decision.
- A late arrival or a late redelivery reads absent where the store no longer holds its windows, and
  carries `history_incomplete`, where Phase 2 served a lower bound.
- A zero MAD now scores a lower amount −50 instead of +50, so R015, R009 and R016 stop firing on small
  payments by constant-amount accounts. The re-validation records the change.
- Identity and device events dated more than 24 hours ahead are refused with 422, as transactions
  already were.
- The merchant CV's history window is up to two minutes short of its nominal length.
- The approximate distinct counts keep a declared exception to strict event time.
- Identity and device events sent without an idempotency key still cannot have their retries
  recognised. That limit is stated, not hidden.

**Risks.**

- Hand-derived fixtures can themselves be wrong. Each carries its arithmetic, and the mutation tests
  show each can fail.
- A rule tuned against Phase 2 values may fire more often. The re-validation records it rather than
  silently retuning thresholds.
- §7 leaves `declined_ratio_1h` reading earlier outcomes whose timing is unmodelled until the conflict
  is decided.

## Status

Proposed
