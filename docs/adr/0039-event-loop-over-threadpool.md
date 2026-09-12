# ADR-0039: The hot path stays on the event loop; the workload, not the scheduler, is the bottleneck

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 2
- **Supersedes / Superseded by:** Supersedes the threadpool decision in ADR-0038 §3. ADR-0038's read-plan and `hiredis` decisions stand, confirmed by the same experiment.

## Context

ADR-0038 concluded that the scoring route blocked the asyncio event loop and moved it to Starlette's
worker threadpool, sized at 64. The reasoning was standard and the defect was real: every repository
call in the route is a synchronous socket round trip, and an `async def` handler runs on the loop.

It did not survive measurement. A canonical 500 TPS run after the change achieved **274.7 TPS** against
**305.34 TPS** before it. Two runs differing in code *and* in accumulated store state prove nothing, so
this ADR is written from a controlled experiment instead.

**Method.** Three gateway images, each built from an isolated git worktree, each run against stores
truncated to zero rows and flushed to zero keys, verified by fingerprint before every run. Same k6
script, same seed, same 500 TPS target, same 180 s duration, same host, same compose limits, same
single uvicorn worker. Only the gateway image differed.

* **A** = `8e07d34`, before any performance change.
* **B** = `ff9547e`, ADR-0038 as accepted: read plan + `hiredis` + 64-thread pool.
* **C** = `ff9547e` with the routes returned to the event loop; everything else retained.

## Decision

**Keep the routes on the event loop.** Revert the `def`/threadpool change and the AnyIO pool sizing.
**Keep the declarative read plan and `hiredis`** — the same experiment shows both are sound.

The measurements:

| | A (event loop) | B (64 threads) | C (event loop + read plan) |
|---|---|---|---|
| achieved TPS | 342.09 | **279.89 (−18.2%)** | **401.20 (+17.3%)** |
| dropped iterations | 27,645 | 39,142 (+41.6%) | 17,056 (−38.3%) |
| client p99 (ms) | 2,999.65 | 4,194.14 (+39.8%) | 3,000.58 (+0.0%) |
| scoring core p50 (ms) | 0.81 | **39.26 (+48×)** | **0.37 (−55%)** |
| gateway CPU mean | 76.7% | **122.7%** | 71.2% |
| gateway threads (max) | 49 | 73 | 44 |
| Redis commands / request | 175.1 | 109.1 (−37.7%) | 109.1 (−37.7%) |
| Postgres active conns (mean) | 1.11 | 1.11 | 1.14 |

**CPU rose 60% while throughput fell 18%.** That is the signature of contention, not of work: the
per-request cost is ~0.2 ms of Python holding the GIL against socket waits that are already
sub-millisecond against a local Redis, so 64 threads bought scheduling overhead and no overlap. The
scoring core inflating from 0.81 ms to 39.26 ms is the same fact measured from inside.

The premise of ADR-0038 §3 — that the loop was the constraint — is also directly contradicted: in A the
gateway used **76.7% of one core** and Postgres averaged **1.11 active connections**. Neither the CPU
nor the stores were saturated. Serialising was never the thing costing throughput.

**Blocking the event loop is acceptable here because the calls are bounded, not because they are fast.**
20 ms Redis socket timeouts with retries disabled, a 2 s Postgres timeout, and the ADR-0035 circuit
breaker are what make it survivable; `tests/contract/test_gateway_event_loop_policy.py` asserts those
bounds rather than asserting the handlers' colour.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Keep the 64-thread pool | Measured −18.2% throughput and +48× scoring latency against the event loop, on identical state |
| A small threadpool (4–8 threads) | Plausibly a middle ground and **not measured**. Recorded as an open question rather than adopted; ADR-0038 was written on unmeasured reasoning once already and this ADR exists because of it |
| Revert ADR-0038 wholesale | Would discard the read plan and `hiredis`, which the same experiment shows cut Redis commands 37.7% and scoring-core p50 55%. The regression was one of four changes; reverting all four would have hidden that |
| Move to an async Redis/Postgres driver | A genuine architectural option and a large one: it changes every repository signature and the ports they implement. Not justified by evidence that says the scheduler is not the bottleneck |
| Add uvicorn workers | Out of scope by instruction, and it would have met the number without explaining it. The finding below is what actually needs answering first |

## Consequences

**Positive.** The hot path is 17.3% faster than the pre-ADR-0038 baseline, with 37.7% fewer Redis
commands and a scoring core at 0.37 ms p50. The concurrency model is now the one the code actually
has, and the constant that bounds the Redis pool says what it bounds rather than implying a
threadpool that no longer exists.

**Negative.** One replica now serves requests strictly one at a time, so its throughput ceiling is the
reciprocal of total per-request work — **2.49 ms, or ~401 requests per second** — and no amount of
in-process concurrency raises it. Horizontal scaling is the only lever this design leaves, which is a
real constraint on a 500 TPS single-replica target. A slow dependency also degrades *every* concurrent
request rather than one, which is why the bounds above are load-bearing rather than defensive.

**Risks.** The untested middle ground (a small threadpool) may be better than either measured point,
and this ADR does not know. Second: the result is specific to a co-located Redis and Postgres with
sub-millisecond round trips. Against a networked store with millisecond-scale latency the balance
could invert, and this decision would deserve re-measuring rather than re-reading.

**The finding that outranks all of this.** Controlling the experiment exposed that the benchmark
workload, not the implementation, dominates the result. The k6 acceptance profile concentrates 80% of
offered load onto 5% of accounts, which at 500 TPS is **1,440 transactions per account per hour** —
4.8× to 36× over every velocity threshold in the rule pack. Noisy-OR over the three rules that fire on
rate alone is exactly 0.9400, which is CRITICAL. The measured consequence is that **91.7% of the load
test triages**, so nearly every request performs a three-insert Postgres transaction: **1.31 ms of the
2.49 ms per-request budget, 52.8%.**

Replaying 60,000 events of the project's own frozen `eval-v1` through the same gateway triages
**0.222%** — a **414×** difference. The benchmark is measuring the cost of opening investigations, not
the cost of scoring. That is an acceptance-specification question and is escalated rather than
resolved here; this ADR records the evidence.

## Status

Accepted
