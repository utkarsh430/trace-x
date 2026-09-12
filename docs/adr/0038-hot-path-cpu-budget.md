# ADR-0038: The hot path is CPU-bound, and the online snapshot reads only what the features declare

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 2
- **Supersedes / Superseded by:** —

## Context

ROADMAP Phase 2 states the target: **p99 < 100 ms, p50 < 20 ms at 500 TPS sustained, zero 5xx over a
ten-minute run.** The first canonical run missed it by an order of magnitude.

**That run has no `run_id`, and its figures are therefore not reproduced here.** The harness refused
to record it: a run that does not sustain the offered rate has not measured the test it claims to,
and writing its latencies under a "500 TPS" heading would be a false claim however honestly they were
collected (CLAUDE.md §13 rule 2). The raw k6 summary was retained as working evidence. What can be
said about it without publishing a number is the part that mattered anyway, and it is two
observations that disagree with the obvious reading:

* **The fastest request in the run was three orders of magnitude faster than the median.** That is
  the signature of a queue, not of a slow path. The work was quick; the wait was long.
* **The gateway container sat near a full core for the whole run**, while Redis and Postgres were
  close to idle. A process that spends its life blocked on sockets does not do that.

Three measurements settled it.

**1. The handler body is not slow.** A full-path decomposition against the real Redis and the real
Postgres, in process:

| stage | mean | p99 |
|---|---|---|
| auth (HMAC verify) | 0.0004 ms | 0.0005 ms |
| rate limit (Redis) | 0.169 ms | 0.322 ms |
| idempotency lookup (Redis) | 0.136 ms | 0.342 ms |
| score: features + rules | 1.910 ms | 4.363 ms |
| triage (Postgres transaction) | 1.434 ms | 2.596 ms |
| observe write (Redis) | 0.770 ms | 1.747 ms |
| serialise + replay store | 0.234 ms | 0.654 ms |
| **total** | **4.654 ms** | **8.653 ms** |

Comfortably inside a 100 ms budget. Also: **triage ran on 99.7% of profiled requests**, and the load
run opened **175,442 cases against ~183,000 requests**. The configuration comment claiming triage was
"a small share of traffic" was wrong, and the Postgres pool was sized from that wrong belief.

**2. Blocking the event loop was a real defect, and not the binding constraint.** The routes were
`async def` while every repository call inside them was a synchronous socket round trip, so Starlette
ran them *on* the event loop and one replica served one request at a time. Fixing it — the routes are
now `def`, which Starlette dispatches to its worker threadpool — is correct and is now guarded by a
test. On its own it moved throughput **not at all**: a 500 TPS run after the change achieved 223.7 TPS.

**3. The process is CPU-bound on one core.** `time.process_time()` around full ASGI requests measured
**3.153 ms of CPU per request — a ceiling of ~317 requests per second on one core**, against 305 TPS
observed. Those two numbers are the same number. No arrangement of waiting improves a CPU ceiling.

`cProfile` then said where the CPU went, and it was not FastAPI, Pydantic or the rules engine: it was
**redis-py's pure-Python RESP reader**, entered 1,368 times per request. One snapshot was parsing and
decoding several hundred returned values.

It was parsing them because it had asked for them. `snapshot` issued the full cross product — every
entity × every stream × every window — and discarded most of it during assembly:

| read | issued per request | read by any feature |
|---|---|---|
| velocity `ZCOUNT` | 60 | **10** |
| previous-observation `HGETALL` | 15 | **2** |
| amount-bucket `HGETALL` | 5 | **2** |
| distinct counts | 7 | 7 *(already derived from the declarations)* |

**163 Redis commands per request, of which 66 were fetched, transmitted, parsed and decoded for
nothing** — and the wasted ones were disproportionately `HGETALL`s, whose map replies cost far more
to parse than an integer.

## Decision

**1. The snapshot reads only what the declared feature set requires.** The read plan is derived once,
at import, from the `semantics` object ADR-0032 put on every `FeatureSpec` — the same derivation
`_plan_from_registry` already performed for distinct-count storage, which is exactly why distinct
counts were the one read kind that was not wasteful. This extends it to velocity counts,
previous-observation reads and amount buckets.

The plan keys velocity reads on **any** `WindowedAggregate`, not only `COUNT`. A window read solely
for `AMOUNT_SUM` still needs its `ZCOUNT`: `_merge_bucket_aggregates` keeps an existing exact count
and otherwise falls back to the minute-bucket sum, which is rounded to the minute and therefore a
different number. Pruning on `COUNT` alone would have silently swapped an exact count for an
approximate one — an optimisation that would not surface as a failure until a parity run months later.

**2. `hiredis` is a declared dependency.** It replaces redis-py's pure-Python RESP reader with a C
one: same protocol, same values, no application code change.

**3. The concurrency limit is stated, not inherited.** `REQUEST_THREADS = 64` sizes Starlette's
worker threadpool, with the Redis connection pool bounded to match and the Postgres pool re-sized from
the measured 96% triage rate rather than the assumed small one.

### What this is not

It does **not** change what is stored, how it is keyed, or how any feature is computed. ADR-0003 and
ADR-0034 are untouched; no storage strategy moves between EXACT and APPROXIMATE; no tolerance is
widened; no feature changes its value. The claim that values are unchanged is not an argument, it is
`tests/conformance/feature_semantics_suite.py` — the same suite, unmodified, run against the naive
reference implementation and against Redis, which is the oracle for exactly this question.

It is also **not** a throughput fix bought by reading less *than the features need*. Every value any
declared feature consumes is still read. What stopped is reading values nothing consumes.

## Consequences

Measured, one core, CPU per request through the full ASGI stack:

| | CPU / request | one-core ceiling |
|---|---|---|
| baseline | 3.153 ms | 317 req/s |
| `hiredis` only | 2.473 ms | 404 req/s |
| read plan only | 2.732 ms | 366 req/s |
| **both** | **2.127 ms** | **470 req/s** |

Redis commands per request fell from **163 to 97**; the feature read fell from **2.970 ms to
0.304 ms** and the whole score-plus-observe path from **4.638 ms to 0.883 ms**.

**Enabling a new feature may now require a warm-up or a reconciliation pass.** Previously the store
wrote and read state for combinations nothing consumed, so a newly declared feature would find some
history waiting. Now a newly declared read is genuinely new. This is the correct trade — Phase 3's
Gold→Redis reconciliation exists to backfill the online store, and paying for hypothetical future
features on every request is not a price a hot path should carry — but it is a real consequence and
belongs in the Phase 3 notes. The **write** path is deliberately left un-pruned for this reason: it
still records state for entities and streams no current feature reads, so history accumulates ahead of
a feature that needs it.

**A latent defect was fixed on the way.** `_assemble` recovered the account id from whichever window
came back non-zero, so an account whose every window count was zero — a new account, the case where a
profile matters most — silently lost the profile that had just been fetched for it. The account id is
now passed in.

**Two claims in the documentation were wrong and are corrected rather than quietly dropped:** that
triage touches "a small share of traffic" (it is 96% under the load profile), and the implicit claim
in the pool sizing that followed from it.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Add uvicorn workers | The straightforward answer to a single-core CPU ceiling, and the wrong one to reach for first: it would have met the number without explaining it, and shipped the 66 wasted commands per request to production multiplied by the worker count. Scaling out is still available — now on top of a hot path that is not wasteful, rather than instead of understanding why it was |
| Stop at the event-loop fix and call the rest acceptable debt | It was measured, and it was not enough: 223.7 TPS on a 500 TPS run afterwards. Recording a miss as debt when the cause is understood and the fix is a read plan would be choosing the write-up over the work |
| Compute the window counts in a Lua script server-side | Cuts round trips further, but moves feature computation into Redis, where ADR-0032's requirement that every feature have one known offline translation becomes materially harder to hold. Not worth it against a target the measured numbers already clear |
| Prune the write path the same way as the read path | Writes are what make a newly declared feature have history to read. Pruning them saves ~6 commands per request and buys a backfill obligation every time a feature is added. The asymmetry is deliberate |
| Cache the snapshot across requests | The window is `(t-W, t]` and every request has its own `t`. A cache would return a snapshot for a different instant, which is a wrong number rather than a stale one |
| Relax the 500 TPS target or the p99 budget to match the measurement | The target is the Phase 2 exit condition. Moving it to meet a result is the failure this project's constitution names first |

## Consequences

Measured, one core, CPU per request through the full ASGI stack:

| | CPU / request | one-core ceiling |
|---|---|---|
| baseline | 3.153 ms | 317 req/s |
| `hiredis` only | 2.473 ms | 404 req/s |
| read plan only | 2.732 ms | 366 req/s |
| **both** | **2.127 ms** | **470 req/s** |

**Positive.** Redis commands per request fell from **163 to 97**; the feature read fell from
**2.970 ms to 0.304 ms** and the whole score-plus-observe path from **4.638 ms to 0.883 ms**. The
per-request cost is now understood rather than observed, and the number that bounds it — CPU on one
core — is the one a capacity plan needs. A latent defect was fixed on the way: `_assemble` recovered
the account id from whichever window came back non-zero, so an account whose every window count was
zero, which is a new account, silently lost the profile that had just been fetched for it.

**Negative.** Enabling a new feature may now require a warm-up or a reconciliation pass. Previously
the store read state for combinations nothing consumed, so a newly declared feature could find some
history already waiting; now a newly declared read is genuinely new. Phase 3's Gold→Redis
reconciliation is what backfills it, and this belongs in the Phase 3 notes rather than in a surprise.
`hiredis` is a compiled dependency, so the environment now needs a wheel or a toolchain for every
platform the gateway is built for. And the read plan is derived at import from the feature registry,
which makes the store's behaviour depend on a module-level side effect — a declaration that changed
would change what is read, silently, without the store's own tests failing.

**Risks.** The plan is derived from `semantics`, so a feature whose declaration does not match what it
actually reads would now lose its input rather than get it by accident. That is the risk this change
creates, and the conformance suite is the control: `tests/conformance/feature_semantics_suite.py` runs
unmodified against the naive reference implementation and against Redis, so a declaration that
disagrees with the computation fails there. Second risk: `REQUEST_THREADS` is now a number that
decides whether the service meets its target, and nothing fails if it is set absurdly — the bound
protects against collapse, not against being wrong.

**Two documented claims were wrong and are corrected rather than quietly dropped:** that triage
touches "a small share of traffic" — it is 96% under the load profile, 175,442 cases against ~183,000
requests — and the Postgres pool sizing that followed from that belief.

## Status

Accepted
