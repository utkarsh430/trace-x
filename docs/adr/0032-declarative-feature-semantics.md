# ADR-0032: Features declare their semantics, not just their implementation

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 2
- **Supersedes / Superseded by:** —

## Context

ADR-0002 splits the system into a hot path (Redis, ~100 ms) and a warm path (Spark, event-time
correct) and accepts a stated cost: **the same feature is computed twice, by two different
mechanisms, and the two can drift.** `docs/DATA_ENGINEERING.md` §4 mitigates that with "a single
feature definition module" plus an automated parity test.

Phase 2 had to make that concrete, and doing so exposed a gap in the mitigation. A shared Python
module only helps if what it shares is the feature's **meaning**. Phase 1's `FeatureSpec` shared a
`compute` callable — but a Python closure over a Redis client cannot be executed by Spark. Phase 3
would therefore have read the prose description and re-derived the intent, which is precisely how two
implementations of "distinct merchants in the last hour" come to disagree about whether the window is
closed at both ends, whether the scored transaction is inside its own window, and whether an
observation exactly `W` seconds old counts.

A second gap appeared while writing the features. ADR-0022 gives a feature two outcomes: a value, or
`UNAVAILABLE` because the source does not supply its inputs. Phase 2 needs a third. An account opened
four hours ago has no 24-hour baseline — the input exists, the source supplies it, there is simply not
enough of it yet. Folding that into `UNAVAILABLE` is wrong in both directions: `UNAVAILABLE`
permanently disqualifies a feature from a cross-dataset transfer metric (ADR-0021), whereas
insufficient history resolves by itself as data arrives; and short history is common in healthy
traffic, so counting it as a coverage gap would make the coverage report wrong.

## Decision

**Every feature declares a `semantics` object alongside its `compute` function**, drawn from a closed
taxonomy of four shapes, each with exactly one known event-time translation:

| Shape | Online (Redis, Phase 2) | Offline (Spark, Phase 3) |
|---|---|---|
| `WindowedAggregate(entity, window, aggregation, stream, dimension?)` | sorted set / bucketed hash | `withWatermark` + event-time `window()` + `groupBy(entity)` |
| `ProfileAttribute(entity, metric)` | hash | join against the Gold entity profile |
| `PairwiseWithPrevious(entity, metric, stream)` | hash holding the last observation | `lag()` over `occurred_at` partitioned by entity |
| `RowLocal()` | nothing read | a column expression |

Adding a fifth shape requires an ADR, because it obliges Phase 3 to learn a fifth translation.

Windows are **half-open `(t − seconds, t]`** — the scored transaction is inside its own window, an
observation exactly `seconds` old is outside it — and the boundary is stated in the type rather than
left to each implementation.

**A feature has three outcomes, and only one of them is a number.** `AVAILABLE` carries a value;
`UNAVAILABLE` means the source does not supply the inputs (ADR-0022); `INSUFFICIENT_HISTORY` means the
entity has too little history yet. Reading `.value` on either absent state raises, and `FeatureValue`
exposes no `__float__` and no arithmetic, so neither can silently become `0.0`.

**A `FeatureValue` carries its own provenance**: `source` (`ONLINE_ONLY` / `RECONCILED`) and
`approximate`. These are properties of the value, not of the HTTP response, so they survive being
logged, stored and compared.

**The feature set is versioned** (`FEATURE_SET_VERSION`) and recorded on every `RiskDecision` and in
every run manifest: a number produced by a different feature set is not comparable to one produced by
this one.

**Parity is verified by a shared conformance suite** stated as observations-in/values-out, with no
reference to any store. Phase 2 runs it against a naive reference implementation and the Redis store;
Phase 3 subclasses the same file **unmodified** for Spark. If proving parity required editing the
suite, the suite would be describing what the implementations happen to do rather than what the
features mean.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Share only the `compute` closure, as Phase 1 did | A closure over a Redis client cannot run in Spark. Phase 3 would re-derive the semantics from prose, which is the drift this is meant to prevent |
| Write the feature once in Spark and call it from the hot path | Micro-batch latency cannot serve a 100 ms decision. ADR-0002 rejected this and nothing has changed |
| Express features in SQL and run the same SQL against both stores | Redis is not a SQL engine; it would mean an embedded query layer over Redis, which is a database, not a feature store |
| A full DSL with its own parser and optimiser | A general language has to be validated, versioned and secured, and every feature here fits four shapes. The cost lands immediately and the benefit is hypothetical |
| Fold `INSUFFICIENT_HISTORY` into `UNAVAILABLE` | They have different causes, different remedies and different consequences for a transfer metric. Collapsing them would misreport coverage on every young account |
| Represent absence as `None` and let callers handle it | Invites `value or 0`, which is the exact failure ADR-0022 exists to prevent |

## Consequences

**Positive.** Phase 3 can compile the offline implementation from the declaration rather than from
prose, and the parity test compares feature logic rather than two retrieval paths. Approximation is
declared per feature, so a parity tolerance above zero is a stated design property rather than a
number someone tuned until a test passed. The strongest test in the suite became cheap to write: on an
**empty** store every feature must be absent, which catches any `.get(key, 0)` that would otherwise
turn a cold store into a confident "no risk detected" on every transaction.

**Negative.** Every feature now carries a declaration that must be kept true — a third thing to get
right beside the code and the description, and a declaration that disagrees with its `compute` is a
new class of bug. The taxonomy constrains what a feature may be: a genuinely novel shape needs an ADR
before it can be written, which is friction by design but still friction. `INSUFFICIENT_HISTORY` adds a
third branch every consumer must handle, and rules in particular must abstain on it rather than
treating it as false.

**Risks.** The declaration drifts from the implementation — a `WindowedAggregate` that says 1h while
`compute` reads the 5m state. Signal: the conformance suite disagrees between implementations while
both look correct in isolation. Mitigation: the suite asserts values, not declarations, so a lying
declaration surfaces as a parity failure in Phase 3 rather than being discovered by inspection. Second
risk: the four shapes prove insufficient for a Phase 4 ML feature, and the taxonomy is widened
reflexively rather than deliberately. Signal: a proposed fifth shape with no offline translation
written down.

## Status

Accepted
