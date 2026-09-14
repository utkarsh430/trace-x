# ADR-0049: Authorization decisions are their own dated events, and `declined_ratio_1h` reads only decisions known before the score

- **Status:** Proposed (Phase 3). The user made the architecture decision, U7, on 2026-09-14. This
  record fixes the design before any implementation.
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
- **Architecture.** The request is scored, the decision is returned, and the result becomes known. A
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

## Decision

### 1. The causal chain

```
payment system ── transaction ──▶ TRACE-X records it and scores it (one atomic read)
      ▲                                   │
      └──────────── RiskDecision ◀────────┘
      │
      │ decides (approve / decline), later
      ▼
tx.authorization.v1  (event time = when the decision was made)
      │
      ▼
online decision state for the account ──▶ read by transactions scored AFTER it is recorded,
                                           and only for decisions made strictly before their as_of
```

### 2. The event contract: `tx.authorization.v1`

**Topic.**

| Setting | Value | Why |
|---|---|---|
| Name | `tx.authorization.v1` | One authorization decision about one transaction |
| `event_type` / `schema_version` | `tx.authorization` / `1` | EVENT_CONTRACTS §2 |
| Key | `account_id` | The only reader is a per-account window, so per-account order is the order that matters |
| Partitions (local / cloud) | 6 / 24 | Equal to `tx.raw.v1`, so an account maps to the same partition number on both topics |
| Cleanup, timestamp, retention | `delete`, `LogAppendTime`, 7 d | Equal to `tx.raw.v1`, the stream it is joined with |
| Dedup identity | `payload.transaction_id` | At most one authorization decision exists per transaction; a producer retry carries a new `event_id` |

**Envelope** (every `docs/EVENT_CONTRACTS.md` §2 field).
- **`occurred_at`** is the **decision time**: when the upstream authorization system decided. It is
  the event time and drives every window.
- **`ingested_at`** is processing time only.
- **`event_id`** is a UUIDv7 minted at the decision time.
- **`trace_id` and `correlation_id`** are the decided transaction's. A transaction and its decision are
  one business flow. No other event shares these ids (LPC-5 §8, rule 5).
- **`idempotency_key`** is `sha256:` over the canonical JSON of `transaction_id`, `account_id`,
  `decision`, and the decision time and transaction time in epoch milliseconds.

**Payload.** `additionalProperties: false`; every field is required.

| Field | Type | Meaning |
|---|---|---|
| `transaction_id` | string, 1–64 | The decided transaction; the same value as its `tx.raw.v1` `payload.transaction_id` |
| `account_id` | `^acct_\d{6,}$` | The decided transaction's account |
| `decision` | enum `APPROVED`, `DECLINED` | What the authorization system decided |
| `transaction_occurred_at` | date-time | The decided transaction's event time, copied |

**Nothing else is in the payload.** A reason code, amount, merchant or card is read by nothing:
- In synthetic data, every field nobody reads is one more place a label can leak.
- In a released schema, it is a contract nobody has exercised.

**Why `decision` is not the four-valued `AuthorizationOutcome`.**
- `UNKNOWN` carries no observation.
- `REVERSED` is a later event with other semantics: a reversal of an approval.

A reversal contract is a separate decision, made when something reads reversals.

**Rules for every producer and consumer.**
- **Validity.** A decision time earlier than `transaction_occurred_at`, or later than 24 h after the
  producer's clock, is invalid.
  - Producers never publish one.
  - The ingress rejects one (§4).
- **Unrecognised decisions.** A consumer that meets a `decision` value it does not recognise counts it
  in neither the numerator nor the denominator, and counts it as unrecognised. An added enum value is
  compatible only with such a branch (EVENT_CONTRACTS §4).
- **Duplicates and conflicts.** A delivery whose `transaction_id` was already recorded is handled by
  whether it matches:
  - **Identical content** is a duplicate and contributes nothing.
  - **Different content** — account, decision, decision time or transaction time — is **conflicting**:
    - the first delivery stays the observation (ADR-0046 §1);
    - the conflicting delivery is not recorded;
    - it is counted and reported.

**Release.**
- The topic becomes RELEASED in the step that gives it its first real producer: the eval-v2 generator,
  together with the gateway ingress of §4.
- Its schema file, its `RELEASED.json` entry, its `deploy/kafka/topics.yaml` declaration and the
  `docs/EVENT_CONTRACTS.md` §3 row are added in that step, not before.

**Disk budget.**
- **Local declaration:** six partitions, each with a 64 MiB retention cap and 8 MiB segments.
- **Design ingress:** the rate of `tx.raw.v1`, with an assumed mean record of 1024 bytes.
- **Fit:** the declaration test must show the local total stays within the 5.5 GiB cap. If it does not,
  the step stops and reports; the cap is not raised silently.

### 3. `tx.raw.v1` and the scoring request

- **`tx.raw.v1` is unchanged: released and immutable.** A producer that emits a transaction before its
  decision writes `authorization_outcome: UNKNOWN`. That is every producer TRACE-X scores for.
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
- **Body:** `AuthorizationDecisionRequest`, strict, extra fields forbidden:
  - `transaction_id`;
  - `account_id`;
  - `decision`;
  - `decided_at` (event time);
  - `transaction_occurred_at`.
- **Authentication:** as for every event route.
- **Success:** `202 Accepted`, returning the observation identity as `event_id`.

**Refusals.**

| Status | When |
|---|---|
| 401 | Unauthenticated |
| 422 | `decided_at` more than 24 h ahead of the gateway clock; `decided_at` earlier than `transaction_occurred_at`; a pattern violation |
| 409 `AUTHORIZATION_CONFLICT` | The store reports the delivery as conflicting (§2). The first delivery stays. |
| 409 `AUTHORIZATION_ACCOUNT_MISMATCH` | The store still holds the observation `transaction:<transaction_id>` and its account differs from the body's. Checked inside the same atomic record operation; nothing is recorded. |

**Identity.**
- **An unknown transaction is still accepted.** A decision for a transaction the store does not hold is
  recorded and counted as **unverified**:
  - it arrived before its transaction, or after that transaction left retention;
  - it can affect only transactions scored after it that satisfy §5's time rule;
  - its own transaction is excluded by identity.
- **The identity is the `transaction_id`, not token-scoped.** Identity events are token-scoped, but
  transaction identities are already store-wide (ADR-0046 §1). Token scoping would let two callers
  record two decisions for one transaction.
  - `X-Idempotency-Key`, when sent, serves only the response replay cache.

**Store unavailable.**
- **Response:** best effort — `202`, with the degraded counter incremented, as for identity events.
- **Completeness:** the completeness guard records the hole, so a later window cannot claim
  completeness over the lost decision (ADR-0046 §5).

The HTTP ingress exists for the same reason the identity route does: the hot path's online store is fed
over HTTP. The warm path consumes the topic.

### 5. Feature semantics (feature set 3.0.0)

**New stream and namespace.**
- **Stream:** `Stream.AUTHORIZATION_DECISION`.
- **Namespace:** `transaction_authorization`, so `transaction_authorization:<transaction_id>` never
  collides with `transaction:<transaction_id>`.
- **Order at a tie.** The namespace sorts after `transaction`, so at one millisecond a decision orders
  after a transaction in `(occurred_ms, namespace, event_id)`.

**New declaration: `CurrentObservation.PRIOR_KNOWN`.**
- **Scope.** The feature reads a post-decision stream: observations that exist only after a decision
  about the transaction they describe.
- **What it reads.** An observation counts only when both hold:
  - it was **known strictly before `as_of`**: its event time, the decision time, is earlier than
    `as_of` in milliseconds and inside the window;
  - it **does not describe the scored transaction itself**, whatever its time.
- **Window.** Membership is `as_of_ms − W_ms < decided_ms < as_of_ms`. A decision exactly one window
  old is outside, as for every window; one made at `as_of` is not yet known.
- **Evaluation modes.**
  - `AS_SERVED` additionally requires the observation to have been recorded before the score's atomic
    read.
  - `EVENT_TIME_COMPLETE` counts every recorded decision inside the window, whatever order it arrived
    in.
  - The two modes differ exactly by late deliveries, as for every other stream.
- **Validation.** `FeatureRegistry` refuses:
  - a `WindowedAggregate` over `AUTHORIZATION_DECISION` that does not declare `PRIOR_KNOWN`;
  - `PRIOR_KNOWN` on any other stream;
  - `DECLINED_RATIO` on any other stream.

**`declined_ratio_1h`.** The feature becomes
`WindowedAggregate(ACCOUNT, 1h, DECLINED_RATIO, stream=AUTHORIZATION_DECISION,
current_observation=PRIOR_KNOWN)`.
- **Numerator:** the `DECLINED` decisions in the window.
- **Denominator:** the `APPROVED` and `DECLINED` decisions in the window.
- **No decisions:** `INSUFFICIENT_HISTORY`, never zero.
- **Currency:** it spans currencies; decisions carry none.
- **Required fields:** `account_id`, `occurred_at` and `authorization_outcome`. For this feature, a
  source covering `authorization_outcome` means the source supplies authorization decisions — as a
  stream, or as a column a replay derives them from (§7). A source that supplies neither makes the
  feature `UNAVAILABLE`.
- **Description:** "Share of this account's authorization decisions made in the last hour, and known
  before this transaction was scored, that were declines."
- **Lookback and completeness:** one hour, as before. An unrecorded decision observation withdraws
  completeness like any other observation.

**Versioning.**
- **`FEATURE_SET_VERSION` becomes `3.0.0`.** One feature changed meaning: its stream, its time axis
  (decision time rather than transaction time) and its self-exclusion rule.
- **`SERVED_FEATURES_CONFORM`** becomes `False` in the commit that declares 3.0.0. It becomes `True`
  only when the reference implementation, the Redis store and the gateway all serve it and §8's
  fixtures pass as served.

**R002.**
- Its text and thresholds are unchanged, but its input changed meaning.
- It is re-validated on eval-v2 (Stage 2, step 13), never retuned on eval-v1.

### 6. Online store

**Record.** One atomic operation does all of the following:
- checks the identity against earlier deliveries, which yields recorded, duplicate or conflicting;
- checks the account against the held transaction observation, if any, and refuses a mismatch;
- adds the decision to the account's decision state, scored by decision time.

**Retention.** The widest window reading the state plus `LATE_ARRIVAL_MARGIN_S`, derived like every
windowed state.

**Recorded form.** For this stream the recorded form includes `decision`, because the decision is the
observation here, not a post-decision field of a transaction. `Event.recorded_form` becomes
stream-aware.

**Read (inside the score's atomic read).**
- Count the decisions with `as_of_ms − 3,600,000 < decided_ms < as_of_ms`, and the `DECLINED` among
  them.
- Then remove the scored transaction's own decision, if it is present.

### 7. Generation and replay

**Delay model DM-1 (declared, chosen, not measured).**
- `decided_ms = transaction_occurred_ms + 40 + int(abs(g))`, where
  `g = derive(seed, "authorization-latency", transaction_id).gauss(300.0, 150.0)`.
- One distribution applies to every transaction, whatever its decision, label or scenario.
- It is keyed by `transaction_id`, so it is a pure function of the seed and the id: any tool holding the
  id reproduces it, independent of generation order.
- A separate delivery delay is not modelled: an event is emitted, and replayed, at its decision time.

**eval-v2 generation (under the gate).**
- Every `tx.raw.v1` row carries `authorization_outcome: UNKNOWN`.
- Every transaction also yields exactly one `tx.authorization.v1` row:
  - its decision comes from the generation plan — legitimate declines (T3) and planted card-testing
    declines — and is `APPROVED` otherwise;
  - its decision time comes from DM-1;
  - its envelope follows §2, with millisecond timestamps.
- At an equal millisecond a decision is emitted after transactions, and a decision never precedes its
  own transaction.
- Decision rows carry no label. Their scenario membership is derived on the evaluation side through
  `transaction_id`.

**Replay of a dataset with a decision stream** (eval-v2). The replay posts each decision to the §4
ingress at its position in the merged event-time order.

**Replay of a historical source with an outcome column and no decision stream** (eval-v1, or an
external source).
- The replay derives one decision for each transaction whose value is `APPROVED` or `DECLINED`, dated by
  DM-1 with the source manifest's seed.
- `REVERSED` and `UNKNOWN` derive nothing, and are counted.
- The transaction request is sent without `authorization_outcome`.
- The derivation happens in the replay tool; the frozen dataset is untouched.

**Replay report.** It records DM-1, the seed, the derived and omitted counts, and the feature set
version.

### 8. Fixtures

The fixtures live in the shared semantics suite and run against the reference implementation and the
Redis store as served.
- Times are relative to the scored transaction `s` at `T`.
- `D(x, v, τ)` is a decision `v` for transaction `x`, made at `τ`.

| # | History, and order of recording | Expectation |
|---|---|---|
| F1 own decision never counts | `D(d1, DECLINED, T−19.7 s)`, `D(a1, APPROVED, T−10 s)`, and `D(s, DECLINED, T)` all recorded before `s` is scored. A variant has `D(s, DECLINED, T−5 ms)`, a causally invalid time that is still present. | `declined_ratio_1h(s) = 1/2` in both variants and both modes. Mutating "count own decision" fails. |
| F2 a known decision affects a later score | `D(s, DECLINED, T+300 ms)` recorded; `s2` at `T+10 s` | `s2` counts it |
| F3 late delivery never changes an issued score | `s2` at `T+10 s` is scored AS_SERVED before `D(s, DECLINED, T+300 ms)` is recorded; then it is recorded | `s2`'s served value is unchanged; `EVENT_TIME_COMPLETE` for `s2` includes it. The difference between modes is asserted. |
| F4 duplicate is idempotent | the same decision delivered twice, with a new `event_id` | second receipt `recorded = False`, `conflicting = False`; counted once |
| F5 conflicting decision is detected | `D(x, APPROVED, τ)` then `D(x, DECLINED, τ)` | second receipt `conflicting = True`, not recorded; the ratio uses `APPROVED`; the gateway answers 409 `AUTHORIZATION_CONFLICT` |
| F6 replay preserves DM-1 and order | a merged replay stream | every decision follows its transaction, at exactly DM-1 for its seed and `transaction_id`; a rerun is identical; DM-1's inputs are the seed and the id only |
| F7 a tie cannot leak | `D(s, DECLINED, T)` and `D(y, DECLINED, T)` for another transaction `y`, recorded before and, in a variant, after `s` | neither counts, in both modes and both recording orders |
| F8 window edges | decisions at `T − 3,600,000 ms`, `T − 3,599,999 ms` and `T` | only the one at `T − 3,599,999 ms` counts |
| F9 account mismatch | transaction `m` held for account A; a decision for `m` naming account B | refused and not recorded; the gateway answers 409 `AUTHORIZATION_ACCOUNT_MISMATCH` |
| F10 unverified decision | a decision for a transaction never observed | recorded, counted as unverified, and counted by later scores |
| F11 the request field is never read | earlier transactions carry `DECLINED` in `tx.raw.v1`'s field; no decisions recorded | `INSUFFICIENT_HISTORY` |
| F12 absence and currencies | no decision in the window; decisions for transactions in two currencies | `INSUFFICIENT_HISTORY`; and a ratio over both currencies |

**Mutations that must be caught:**
- counting the scored transaction's own decision;
- an inclusive upper edge;
- reading the transaction field;
- a conflicting delivery overwriting the first;
- tie order decided by arrival.

### 9. What changes when this is implemented

| Area | Change |
|---|---|
| Contracts | `tx.authorization.v1.json`, generated models, `RELEASED.json`, `topics.yaml`, EVENT_CONTRACTS §3, OpenAPI (new route; the request field deprecated) |
| Features | `Stream.AUTHORIZATION_DECISION`, the namespace, `CurrentObservation.PRIOR_KNOWN`, registry validation, `declined_ratio_1h`, `FEATURE_SET_VERSION = "3.0.0"`, stream-aware recorded form |
| Implementations | reference windows; Redis record and read scripts; the completeness guard for the new stream |
| Gateway | the §4 route, its request model, conflict and mismatch problems, counters |
| Generator | eval-v2 only: `UNKNOWN` on transactions, the decision stream, DM-1 keyed by `transaction_id` |
| Replay | the fourth stream; derivation for historical sources; the order at ties; the report |
| Evaluation | LPC-5 §5.2 and §8 rules 3–5 read the decision stream |
| Tests | §8 fixtures and mutations; contract tests; the budget test; replay order tests |

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| **B — declare TRACE-X post-authorization** (ADR-0046 §7) | Contradicts ADR-0002 and the synchronous topology of `docs/ARCHITECTURE.md` §1. |
| **C — keep request-supplied outcomes, counted after a minimum lag** (ADR-0046 §7) | A heuristic lag nothing measures. The pre-score contract would still carry post-decision data. |
| **Window on transaction time, filtered by knowledge time** ("transactions of the last hour whose decision was known before `as_of`") | Keeps the old wording, but needs two times per record and a filter no `WindowedAggregate` expresses. The warm path would need a stream-stream join with an inequality on a second time column. The two definitions differ only by the decision latency, which is far below the window. |
| **`tx.raw.v2` without the field** | The breaking-change procedure and a dual-write window, for a field whose released value `UNKNOWN` already says "not known". |
| **Reuse the four-valued `AuthorizationOutcome` in the new event** | `UNKNOWN` is not an observation and `REVERSED` is a different, later event. Both would need consumer rules nothing exercises. |
| **Token-scoped identity, like identity events** | The business key is `transaction_id`. Token scoping would let two callers record two decisions for one transaction. |
| **Carry the decision on `tx.scored.v1`** | `tx.scored.v1` is TRACE-X's decision, not the payment system's, and it is PLANNED, with no producer. |

## Consequences

**Positive.**
- A transaction's own decision cannot reach its own features, by construction, in every mode and at
  every tie.
- Earlier decisions reach features only once they exist, as they would in production.
- eval-v2's declines, legitimate and planted, reach features through one path. The request field that
  was a label proxy in eval-v1 is read by nothing.
- Replay order is declared and reproducible from the seed and the transaction id.

**Negative.**
- One more topic, route, stream, store structure and set of fixtures to maintain. The local Kafka disk
  headroom shrinks.
- The feature now measures decisions known in the last hour, not transactions of the last hour. At the
  start of a burst it sees fewer decisions than before, so R002 fires later in a card-testing burst, by
  design.
- Replay values under feature set 3.0.0 are not comparable with Phase 2's or Step 1's without a
  manifest-diff note, including on eval-v1, whose decisions become DM-1-derived.
- A decision delivered while the online store is down is a hole, and completeness is withdrawn until
  the epoch moves past it.
- DM-1 is chosen, not measured. Its spread matters only at window edges.

**Risks.**
- **A producer that re-stamps the decision time on retry** shows up as conflicts. The conflict counter
  is the signal.
- **A source whose decisions arrive late** makes `AS_SERVED` and `EVENT_TIME_COMPLETE` diverge. The
  parity job measures this; it is not hidden.
- **A consumer needs reversals.** A reversal contract is then a new decision, not an enum value slipped
  into this one.

## Status

Proposed
