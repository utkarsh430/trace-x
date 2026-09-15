# ADR-0049: Authorization outcomes are their own dated events, and `declined_ratio_1h` reads only verified outcomes known before the score

- **Status:** Proposed (Phase 3). The user made the architecture decision, U7, on 2026-09-14, and
  amended this record the same day: the payload field is named `authorization_outcome`, unverified
  outcomes never count, and the online cache is not the correctness authority. No implementation exists
  yet.
- **Date:** 2026-09-14
- **Phase:** 3 (Step E Stage 2; the implementation is step 7 of the approved Stage 2 order)
- **Supersedes / Superseded by:** — .
  - Decides the conflict ADR-0046 §7 left open, taking option A.
  - Changes one feature declaration of ADR-0046, and with it the feature set version to 3.0.0.
  - Adds one topic under ADR-0047's declaration rules.

## Context

**TRACE-X's decision gates authorization** (ADR-0046 §7).
- The payment system sends a transaction synchronously and acts on the `RiskDecision`
  (`docs/ARCHITECTURE.md` §1; ADR-0002).
- So a transaction's own authorization outcome is post-decision information.

**Yet the contracts carry that outcome before the score.**
- `tx.raw.v1` — "a transaction as it arrived, before scoring" — requires `authorization_outcome`.
- `POST /v1/transactions` accepts it.
- `declined_ratio_1h` counts earlier transactions' outcomes taken from their own scoring requests. It
  sees them at the very next score, even one millisecond later.

**Step 1 closed only part of this.** It stopped a transaction's own outcome from entering its own
features (`POST_DECISION_FIELDS`).

**eval-v1 makes the leak a label proxy.** In eval-v1, `DECLINED` occurs only inside `CARD_TESTING`, so
the request field is also a label proxy (Stage 1d audit, LP-02 and LP-03). R002 reads it.

**U7, as decided by the user:**
- The current transaction's outcome is post-decision. It **must not** participate in its own features.
- **Architecture.** The transaction is scored, and its authorization result becomes known later. A
  separate authorization outcome event then updates historical outcome state, which later transactions
  may use.
- **Contract.** Introduce and version an explicit outcome event contract. Do not fabricate the outcome
  into the pre-score payload.
- **Replay.** A historical source with an outcome column is replayed transaction first, with the
  outcome event later through a declared deterministic delay model. Ground truth may know both; the
  runtime sees them only in causal order.
- **Feature.** `declined_ratio_1h` uses prior observed outcomes only and does not include the scored
  transaction's own. This is encoded in the FeatureSpec semantics as a causality-based exception to
  self-inclusive windows.
- **Fixtures.** Seven are required (§8, F1–F7).

**The user's amendments** (2026-09-14):
- The payload field is `authorization_outcome`, with the values `APPROVED` and `DECLINED`.
- An outcome that cannot yet be verified against its transaction and account:
  - is retained as pending;
  - affects no correctness feature;
  - is reconciled once transaction evidence exists.
- Duplicates stay idempotent, and conflicts stay conflicts.
- The disposable Redis cache must not be the sole correctness authority.

## Decision

### 1. The causal chain

```
payment system ── transaction ──▶ TRACE-X records it and scores it (one atomic read)
      ▲                                   │
      └──────────── RiskDecision ◀────────┘
      │
      │ authorizes or declines, later
      ▼
authorization outcome (event time = when the outcome was decided)
      │
      ├──▶ PostgreSQL: the durable record — identity, duplicates, conflicts — plus an outbox row
      │        └──▶ tx.authorization.v1 ──▶ warm path (reconciled against tx.raw.v1)
      ▼
online outcome state for the account: PENDING until its transaction is held, then VERIFIED
      │
      ▼
read by transactions scored AFTER it is verified, and only for outcomes decided strictly before
their as_of
```

### 2. The event contract: `tx.authorization.v1`

**Topic.**

| Setting | Value | Why |
|---|---|---|
| Name | `tx.authorization.v1` | One authorization outcome for one transaction |
| `event_type` / `schema_version` | `tx.authorization` / `1` | EVENT_CONTRACTS §2 |
| Key | `account_id` | The only reader is a per-account window, so per-account order is the order that matters |
| Partitions (local / cloud) | 6 / 24 | Equal to `tx.raw.v1`, so an account maps to the same partition number on both topics |
| Cleanup, timestamp, retention | `delete`, `LogAppendTime`, 7 d | Equal to `tx.raw.v1`, the stream it is reconciled with |
| Dedup identity | `payload.transaction_id` | At most one authorization outcome exists per transaction; a producer retry carries a new `event_id` |

**Envelope** (every `docs/EVENT_CONTRACTS.md` §2 field).
- **`occurred_at`** is the **outcome time**: when the upstream authorization system decided. It is the
  event time and drives every window.
- **`ingested_at`** is processing time only.
- **`event_id`** is a UUIDv7 minted at the outcome time.
- **`trace_id` and `correlation_id`** are the transaction's. A transaction and its outcome are one
  business flow. No other event shares these ids (LPC-5 §8, rule 5).
- **`idempotency_key`** is `sha256:` over the canonical JSON of `transaction_id`, `account_id`,
  `authorization_outcome`, and the outcome time and transaction time in epoch milliseconds.

**Payload.** `additionalProperties: false`; every field is required.

| Field | Type | Meaning |
|---|---|---|
| `transaction_id` | string, 1–64 | The transaction; the same value as its `tx.raw.v1` `payload.transaction_id` |
| `account_id` | `^acct_\d{6,}$` | The transaction's account, as the outcome's producer reports it |
| `authorization_outcome` | enum `APPROVED`, `DECLINED` | What the authorization system decided |
| `transaction_occurred_at` | date-time | The transaction's event time, copied |

**Nothing else is in the payload.** A reason code, amount, merchant or card is read by nothing:
- In synthetic data, every field nobody reads is one more place a label can leak.
- In a released schema, it is a contract nobody has exercised.

**Why the enum has two values, not the four of `AuthorizationOutcome`.**
- `UNKNOWN` carries no observation.
- `REVERSED` is a later event with other semantics: a reversal of an approval.

A reversal contract is a separate decision, made when something reads reversals.

**Rules for every producer and consumer.**
- **Validity.** An outcome time earlier than `transaction_occurred_at`, or later than 24 h after the
  producer's clock, is invalid.
  - Producers never publish one.
  - The ingress rejects one (§4).
- **Unrecognised values.** A consumer that meets an `authorization_outcome` value it does not
  recognise counts it in neither the numerator nor the denominator, and counts it as unrecognised. An
  added enum value is compatible only with such a branch (EVENT_CONTRACTS §4).
- **Duplicates and conflicts.** A delivery whose `transaction_id` was already recorded is handled by
  whether it matches:
  - **Identical content** is a duplicate and contributes nothing.
  - **Different content** — account, outcome, outcome time or transaction time — is **conflicting**:
    - the first delivery stays the observation (ADR-0046 §1);
    - the conflicting delivery is not recorded;
    - it is counted and reported.
- **Verification.** An outcome is **verified** when a transaction with the same `transaction_id` and
  the same `account_id` is known. Until then it is **pending**. A known transaction with a different
  account makes it **rejected**. Only verified outcomes ever reach a feature, online or offline.

**Release.**
- The topic becomes RELEASED in the step that gives it its first real producers: the eval-v2 generator
  and the outbox relay, which runs in `trace-worker` (ADR-0051 §7: the pre-registered hot-path A/B
  ruled out a relay on a gateway thread).
- Its schema file, its `RELEASED.json` entry, its `deploy/kafka/topics.yaml` declaration and the
  `docs/EVENT_CONTRACTS.md` §3 row are added in that step, not before.

**Disk budget.**
- **Local declaration:** six partitions, each with a 64 MiB retention cap and 8 MiB segments.
- **Design ingress:** the rate of `tx.raw.v1`, with an assumed mean record of 1024 bytes.
- **Fit:** the declaration test must show the local total stays within the 5.5 GiB cap. If it does not,
  the step stops and reports; the cap is not raised silently.

### 3. `tx.raw.v1` and the scoring request

- **`tx.raw.v1` is unchanged: released and immutable.** A producer that emits a transaction before its
  outcome writes `authorization_outcome: UNKNOWN`. That is every producer TRACE-X scores for.
  - `UNKNOWN` is a released value that means exactly "not known".
  - The eval-v2 generator writes `UNKNOWN` on every transaction.
  - eval-v1 is frozen and keeps its values.
- **No feature reads the outcome field.** No feature reads `CanonicalField.AUTHORIZATION_OUTCOME` from a
  transaction. `POST_DECISION_FIELDS` keeps the field, and a test asserts that no feature
  implementation reads it.
- **The request still accepts it.** `POST /v1/transactions` keeps accepting `authorization_outcome`,
  because removing it would break the API.
  - OpenAPI documents it as deprecated and ignored by scoring and features.
  - The online store stops recording it.
- **No `tx.raw.v2`.** No field is removed or re-typed. A `.v2` without the field — with its dual-write
  window — is the right change only when a consumer needs the field gone.

### 4. Ingress: `POST /v1/events/authorization`

**Request.**
- **Body:** `AuthorizationOutcomeRequest`, strict, extra fields forbidden:
  - `transaction_id`;
  - `account_id`;
  - `authorization_outcome`;
  - `decided_at` (the outcome time);
  - `transaction_occurred_at`.
- **Authentication:** as for every event route.

**The durable record comes first, and it is the authority on identity.** Before acknowledging
anything, the ingress writes PostgreSQL, the system of record (ADR-0004), in one transaction.
- **Tables.** A row in `authorization_outcomes`, whose primary key is `transaction_id` and which holds
  the payload and the receipt time; and an outbox row that relays the outcome to `tx.authorization.v1`,
  the transactional outbox `investigation.requested.v1` already uses (ADR-0007).
- **Duplicates.** An identical redelivery finds the row, changes nothing and gets `202`.
- **Conflicts.** Different content under the same `transaction_id` is a conflict: `409
  AUTHORIZATION_CONFLICT`. This row decides it — not the cache, and not whether the cache still holds
  anything.
- **PostgreSQL unavailable.** The answer is `503`, and nothing is applied online. An outcome the system
  of record did not keep never reaches a feature.

PostgreSQL is chosen over a direct Kafka produce because it is in the `core` profile, while Kafka runs
only under `streaming` (CLAUDE.md §12).

**Then the online store applies it** (§6).
- **Transaction already held, same account:** the outcome is VERIFIED; `202`.
- **Transaction already held, different account:** the outcome is REJECTED and never applied; `409
  AUTHORIZATION_ACCOUNT_MISMATCH`. The durable row stays as the audit of what was reported, and the
  warm path rejects it by the same rule.
- **Transaction not held** (the outcome arrived first, or the transaction has left online retention):
  the outcome is PENDING; `202`. It affects nothing until reconciliation verifies it.
- **Store unavailable or the write fails:**
  - `202`, because the durable record has the outcome;
  - the degraded counter is incremented;
  - the completeness guard records a hole (ADR-0046 §5), so no window claims completeness over the
    outcome.

**Identity is the `transaction_id`, not token-scoped.** Identity events are token-scoped, but
transaction identities are already store-wide (ADR-0046 §1). Token scoping would let two callers
record two outcomes for one transaction. `X-Idempotency-Key`, when sent, serves only the response
replay cache.

### 5. Feature semantics (feature set 3.0.0)

**New stream and namespace.**
- **Stream:** `Stream.AUTHORIZATION_OUTCOME`.
- **Namespace:** `transaction_authorization`, so `transaction_authorization:<transaction_id>` never
  collides with `transaction:<transaction_id>`.
- **Order at a tie.** The namespace sorts after `transaction`, so at one millisecond an outcome orders
  after a transaction in `(occurred_ms, namespace, event_id)`.

**New declaration: `CurrentObservation.PRIOR_KNOWN`.**
- **Scope.** The feature reads a post-decision stream: observations that exist only after a decision
  about the transaction they describe.
- **What it reads.** An observation counts only when all three hold:
  - it is **verified** (§2);
  - it was **known strictly before `as_of`**: its event time, the outcome time, is earlier than `as_of`
    in milliseconds and inside the window;
  - it **does not describe the scored transaction itself**, whatever its time.
- **Window.** Membership is `as_of_ms − W_ms < decided_ms < as_of_ms`. An outcome exactly one window old
  is outside, as for every window; one decided at `as_of` is not yet known.
- **Evaluation modes.**
  - `AS_SERVED` additionally requires the observation to have been recorded **and verified** before the
    score's atomic read.
  - `EVENT_TIME_COMPLETE` counts every outcome inside the window whose transaction, with the same
    account, exists anywhere in the complete history, whatever the arrival order.
  - The modes differ exactly by late deliveries and late verifications, as for every other stream.
- **Validation.** `FeatureRegistry` refuses:
  - a `WindowedAggregate` over `AUTHORIZATION_OUTCOME` that does not declare `PRIOR_KNOWN`;
  - `PRIOR_KNOWN` on any other stream;
  - `DECLINED_RATIO` on any other stream.

**`declined_ratio_1h`.** The feature becomes
`WindowedAggregate(ACCOUNT, 1h, DECLINED_RATIO, stream=AUTHORIZATION_OUTCOME,
current_observation=PRIOR_KNOWN)`.
- **Numerator:** the verified `DECLINED` outcomes in the window.
- **Denominator:** the verified `APPROVED` and `DECLINED` outcomes in the window. Pending and rejected
  outcomes count in neither.
- **No outcomes:** `INSUFFICIENT_HISTORY`, never zero.
- **Currency:** it spans currencies; outcomes carry none.
- **Required fields:** `account_id`, `occurred_at` and `authorization_outcome`. For this feature, a
  source covering `authorization_outcome` means the source supplies authorization outcomes — as a
  stream, or as a column a replay derives them from (§7). A source that supplies neither makes the
  feature `UNAVAILABLE`.
- **Description:** "Share of this account's verified authorization outcomes decided in the last hour,
  and known before this transaction was scored, that were declines."
- **Lookback and completeness:** one hour, as before. An outcome the store failed to record withdraws
  completeness like any other observation.

**Versioning.**
- **`FEATURE_SET_VERSION` becomes `3.0.0`.** One feature changed meaning: its stream, its time axis
  (outcome time rather than transaction time), its verification rule and its self-exclusion rule.
- **`SERVED_FEATURES_CONFORM`** becomes `False` in the commit that declares 3.0.0. It becomes `True`
  only when the reference implementation, the Redis store and the gateway all serve it and §8's
  fixtures pass as served.

**R002.**
- Its text and thresholds are unchanged, but its input changed meaning.
- It is re-validated on eval-v2 (Stage 2, step 13), never retuned on eval-v1.

### 6. Online store

**The store holds derived state.** PostgreSQL decides identity, duplicates and conflicts (§4). The
store decides only whether an outcome is verified yet, using the transaction observations it holds.

**Losing the store is safe.**
- Pending outcomes never counted, so their loss changes no value.
- Verified outcomes lost with the store are covered by the completeness guard: the epoch moves and no
  window claims completeness over the loss (ADR-0044; ADR-0046 §5).
- Rebuilding outcome state from `authorization_outcomes` after a loss is a recovery optimisation, and
  is deferred.

**Apply an outcome** — one atomic operation, after the durable write:
- If the identity is already recorded, the delivery is a duplicate (identical) or conflicting
  (different). The durable record has already answered this; the store repeats the check, so it never
  trusts a stale path.
- If the transaction observation `transaction:<transaction_id>` is held, compare accounts:
  - equal: add the outcome to the account's verified outcome state, scored by outcome time;
  - different: reject.
- Otherwise, keep it as pending, keyed by `transaction_id`.

**Reconcile** — inside the transaction's own atomic record-and-read.
- **When:** a transaction is recorded while a pending outcome for its `transaction_id` exists.
- **Same account:** the outcome is promoted to verified state.
- **Different account:** the outcome is rejected.
- **The promoting transaction's own read never counts the outcome** — it is its own outcome — so only
  scores recorded after the promotion can.

**Retention.**
- Verified state keeps the widest window reading it plus `LATE_ARRIVAL_MARGIN_S`.
- Pending entries expire after the same bound past their outcome time. By then they can no longer fall
  inside any window a future score reads, and the durable record keeps them.

**Recorded form.** For this stream the recorded form includes `authorization_outcome`, because the
outcome is the observation here, not a post-decision field of a transaction. `Event.recorded_form`
becomes stream-aware.

**Read (inside the score's atomic read).**
- Count the verified outcomes with `as_of_ms − 3,600,000 < decided_ms < as_of_ms`, and the `DECLINED`
  among them.
- Then remove the scored transaction's own outcome, if it is present.

### 7. Generation and replay

**Delay model DM-1 (declared, chosen, not measured).**
- `decided_ms = transaction_occurred_ms + 40 + int(abs(g))`, where
  `g = derive(seed, "authorization-latency", transaction_id).gauss(300.0, 150.0)`.
- One distribution applies to every transaction, whatever its outcome, label or scenario.
- It is keyed by `transaction_id`, so it is a pure function of the seed and the id: any tool holding the
  id reproduces it, independent of generation order.
- A separate delivery delay is not modelled: an event is emitted, and replayed, at its outcome time.

**eval-v2 generation (under the gate).**
- Every `tx.raw.v1` row carries `authorization_outcome: UNKNOWN`.
- Every transaction also yields exactly one `tx.authorization.v1` row:
  - its `authorization_outcome` comes from the generation plan — legitimate declines (T3) and planted
    card-testing declines — and is `APPROVED` otherwise;
  - its outcome time comes from DM-1;
  - its envelope follows §2, with millisecond timestamps.
- At an equal millisecond an outcome is emitted after transactions, and an outcome never precedes its
  own transaction. Every generated outcome is therefore verifiable.
- Outcome rows carry no label. Their scenario membership is derived on the evaluation side through
  `transaction_id`.

**Replay of a dataset with an outcome stream** (eval-v2). The replay posts each outcome to the §4
ingress at its position in the merged event-time order.

**Replay of a historical source with an outcome column and no outcome stream** (eval-v1, or an
external source).
- The replay derives one outcome for each transaction whose value is `APPROVED` or `DECLINED`, dated by
  DM-1 with the source manifest's seed.
- `REVERSED` and `UNKNOWN` derive nothing, and are counted.
- The transaction request is sent without `authorization_outcome`.
- The derivation happens in the replay tool; the frozen dataset is untouched.

**Replay report.** It records DM-1, the seed, the derived and omitted counts, and the feature set
version.

### 8. Fixtures

The fixtures live in the shared semantics suite and run against the reference implementation and the
Redis store as served. The gateway fixtures run against a real PostgreSQL.
- Times are relative to the scored transaction `s` at `T`.
- `O(x, v, τ)` is an outcome `v` for transaction `x`, decided at `τ`.
- Every outcome's transaction is held unless a fixture says otherwise.

| # | History, and order of recording | Expectation |
|---|---|---|
| F1 own outcome never counts | `O(d1, DECLINED, T−19.7 s)`, `O(a1, APPROVED, T−10 s)`, and `O(s, DECLINED, T)` all recorded before `s` is scored. A variant has `O(s, DECLINED, T−5 ms)`, a causally invalid time that is still present. | `declined_ratio_1h(s) = 1/2` in both variants and both modes. Mutating "count own outcome" fails. |
| F2 a known outcome affects a later score | `O(s, DECLINED, T+300 ms)` recorded; `s2` at `T+10 s` | `s2` counts it |
| F3 late delivery never changes an issued score | `s2` at `T+10 s` is scored AS_SERVED before `O(s, DECLINED, T+300 ms)` is recorded; then it is recorded | `s2`'s served value is unchanged; `EVENT_TIME_COMPLETE` for `s2` includes it. The difference between modes is asserted. |
| F4 duplicate is idempotent | the same outcome delivered twice, with a new `event_id` | second receipt `recorded = False`, `conflicting = False`; counted once |
| F5 conflicting outcome is detected | `O(x, APPROVED, τ)` then `O(x, DECLINED, τ)` | second receipt `conflicting = True`, not recorded; the ratio uses `APPROVED`; the gateway answers 409 `AUTHORIZATION_CONFLICT` |
| F6 replay preserves DM-1 and order | a merged replay stream | every outcome follows its transaction, at exactly DM-1 for its seed and `transaction_id`; a rerun is identical; DM-1's inputs are the seed and the id only |
| F7 a tie cannot leak | `O(s, DECLINED, T)` and `O(y, DECLINED, T)` for another transaction `y`, recorded before and, in a variant, after `s` | neither counts, in both modes and both recording orders |
| F8 window edges | outcomes at `T − 3,600,000 ms`, `T − 3,599,999 ms` and `T` | only the one at `T − 3,599,999 ms` counts |
| F9 account mismatch | transaction `m` held for account A; an outcome for `m` naming account B | rejected, never counted; the gateway answers 409 `AUTHORIZATION_ACCOUNT_MISMATCH`; the durable row exists |
| F10 pending never counts | an outcome for transaction `p`, decided at `T−20 s`, arrives while `p` is not held | `s` does not count it |
| F11 reconciliation | as F10, then `p` is recorded (same account) at `T−10 s`, before `s` | `p`'s own score does not count it; `s` does |
| F12 reconciliation mismatch | as F10, then `p` is recorded for a different account | rejected at `p`'s record; never counted |
| F13 duplicate and conflict while pending | as F10, then the identical outcome, then a different one | the duplicate changes nothing; the different one is a conflict (409), and neither counts until `p` is held |
| F14 the cache is not the authority | an accepted outcome; the online store is flushed; the identical outcome and then a different one are delivered | the identical one is a duplicate (202) and the different one a conflict (409), both decided by PostgreSQL |
| F15 no system of record, no feature | PostgreSQL unavailable at delivery | 503; nothing applied online; no score counts it |
| F16 the request field is never read | earlier transactions carry `DECLINED` in `tx.raw.v1`'s field; no outcomes recorded | `INSUFFICIENT_HISTORY` |
| F17 absence and currencies | no outcome in the window; outcomes for transactions in two currencies | `INSUFFICIENT_HISTORY`; and a ratio over both currencies |

**Mutations that must be caught:**
- counting the scored transaction's own outcome;
- counting a pending outcome;
- an inclusive upper edge;
- reading the transaction field;
- a conflicting delivery overwriting the first;
- a conflict decided by the cache alone;
- tie order decided by arrival.

### 9. What changes when this is implemented

| Area | Change |
|---|---|
| Contracts | `tx.authorization.v1.json`, generated models, `RELEASED.json`, `topics.yaml`, EVENT_CONTRACTS §3, OpenAPI (new route; the request field deprecated) |
| Persistence | A migration adding `authorization_outcomes`, and the outbox relay for the new topic |
| Features | `Stream.AUTHORIZATION_OUTCOME`, the namespace, `CurrentObservation.PRIOR_KNOWN`, registry validation, `declined_ratio_1h`, `FEATURE_SET_VERSION = "3.0.0"`, stream-aware recorded form |
| Implementations | Reference windows with verification; Redis apply, reconcile and read scripts; the completeness guard for the new stream |
| Gateway | The §4 route with its durable write, request model, conflict, mismatch and unavailable problems, and counters |
| Generator | eval-v2 only: `UNKNOWN` on transactions, the outcome stream, DM-1 keyed by `transaction_id` |
| Replay | The fourth stream; derivation for historical sources; the order at ties; the report |
| Evaluation | LPC-5 §5.2 and §8 rules 3–5 read the outcome stream |
| Tests | §8 fixtures and mutations; contract tests; the budget test; replay order tests |

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| **B — declare TRACE-X post-authorization** (ADR-0046 §7) | Contradicts ADR-0002 and the synchronous topology of `docs/ARCHITECTURE.md` §1. |
| **C — keep request-supplied outcomes, counted after a minimum lag** (ADR-0046 §7) | A heuristic lag nothing measures. The pre-score contract would still carry post-decision data. |
| **Window on transaction time, filtered by knowledge time** ("transactions of the last hour whose outcome was known before `as_of`") | Keeps the old wording, but needs two times per record and a filter no `WindowedAggregate` expresses. The warm path would need a stream-stream join with an inequality on a second time column. The two definitions differ only by the outcome latency, which is far below the window. |
| **Count an outcome whose transaction is not held** | An outcome naming the wrong account, or a transaction that never existed, would change another account's ratio with nothing to contradict it. |
| **The Redis store as the identity and conflict authority** | Feature state can be lost; the completeness guard exists because of that (ADR-0044). A duplicate delivered after a loss would count twice, and a conflict would pass as new. |
| **A direct Kafka produce as the durable record** | Kafka runs only in the `streaming` profile, so the `core` product could not accept outcomes. The outbox gives the same topic from PostgreSQL. |
| **`tx.raw.v2` without the field** | The breaking-change procedure and a dual-write window, for a field whose released value `UNKNOWN` already says "not known". |
| **Reuse the four-valued `AuthorizationOutcome` enum in the new event** | `UNKNOWN` is not an observation and `REVERSED` is a different, later event. Both would need consumer rules nothing exercises. |
| **Token-scoped identity, like identity events** | The business key is `transaction_id`. Token scoping would let two callers record two outcomes for one transaction. |
| **Carry the outcome on `tx.scored.v1`** | `tx.scored.v1` is TRACE-X's decision, not the payment system's, and it is PLANNED, with no producer. |

## Consequences

**Positive.**
- A transaction's own outcome cannot reach its own features, by construction, in every mode and at
  every tie.
- Earlier outcomes reach features only once they exist and are verified against their transaction.
- Duplicates and conflicts are decided durably, and do not depend on the cache still holding state.
- eval-v2's declines, legitimate and planted, reach features through one path. The request field that
  was a label proxy in eval-v1 is read by nothing.
- Replay order is declared and reproducible from the seed and the transaction id.

**Negative.**
- One more topic, route, table, outbox relay, stream, store structure and set of fixtures to maintain.
  The local Kafka disk headroom shrinks.
- Outcome ingestion writes PostgreSQL. This is not the scoring hot path, but an outcome cannot be
  accepted while PostgreSQL is down; the ratio then degrades to fewer outcomes, never to wrong ones.
- An outcome that arrives before its transaction counts only after the transaction is recorded.
- The feature now measures verified outcomes known in the last hour, not transactions of the last hour.
  At the start of a burst it sees fewer outcomes than before, so R002 fires later in a card-testing
  burst, by design.
- Replay values under feature set 3.0.0 are not comparable with Phase 2's or Step 1's without a
  manifest-diff note, including on eval-v1, whose outcomes become DM-1-derived.
- DM-1 is chosen, not measured. Its spread matters only at window edges.

**Risks.**
- **A producer that re-stamps the outcome time on retry** shows up as conflicts. The conflict counter is
  the signal.
- **Many outcomes stay pending** — because transactions arrive late, or a producer's accounts are
  wrong. This shows in the pending and rejected counters and in `AS_SERVED`/`EVENT_TIME_COMPLETE`
  parity; it is not hidden.
- **A consumer needs reversals.** A reversal contract is then a new decision, not an enum value slipped
  into this one.

## Status

Proposed
