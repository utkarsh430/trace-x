# ADR-0041: Online-store memory classes — a best-effort cache must not evict correctness-relevant state

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 2
- **Supersedes / Superseded by:** —

## Context

Redis runs with `--maxmemory 512mb --maxmemory-policy allkeys-lru`. `allkeys-lru` evicts whichever key
was least recently used, across the whole keyspace, with no notion of what any key is for. The
online store holds three kinds of state in that one pool, and they are not interchangeable:

| class | keys | what its loss costs | authoritative elsewhere? |
|---|---|---|---|
| **Correctness-relevant feature state** | `f:v` velocity, `f:b` amount buckets, `f:dx` exact distinct, `f:da` HLL, `f:p` profile, `f:pa` amounts, `f:l` previous observation | **A wrong decision.** The feature reads back empty, resolves to `INSUFFICIENT_HISTORY`, the rules over it abstain (ADR-0033), the score falls, and a transaction that should have been CRITICAL is scored LOW with `degraded=false` | Delta, via the Phase 3 Gold→Redis reconciliation — but not until Phase 3 exists |
| **Idempotency replay cache** | `idem` | A retry is re-scored instead of replayed. No duplicate case: `cases.trigger_transaction_id` is the authoritative tier (ADR-0007). The response may differ in score | **Yes** — Postgres |
| **Rate-limit windows** | `rl` | A token briefly gets more budget than it should. Fails open by design (CLAUDE.md §3.7) | No, and it does not need to be |

The first class is the only one whose loss changes an answer. The third is nearly free. The second is
a latency optimisation over a guarantee Postgres already keeps.

**Measured, on a representative 500 TPS ten-minute run** (293,944 requests, sampled with
`MEMORY USAGE`):

| class | avg bytes/key | keys | total |
|---|---|---|---|
| `idem` replay cache | **1,596** | 103,346 | **157 MB (37%)** |
| `f:b` buckets | 193 | 378,703 | 70 MB |
| `f:l` previous | 158 | 378,680 | 57 MB |
| `f:v` velocity | 150 | 379,143 | 54 MB |
| `f:dx` exact distinct | 106 | 436,825 | 44 MB |
| `f:da`, `f:p`, `f:pa` | — | — | 47 MB |
| | | **total** | **429 MB** |

The run ended pinned at **512.02 MB of 512.00 MB with 3,283,782 keys evicted**, and the consequences
were visible all the way out to the client:

* **36,178 `redis_unavailable` degradations** — 12.3% of requests hit the 20 ms socket timeout while
  Redis was under eviction pressure.
* 36,178 × 20 ms is **724 seconds of blocked event loop inside a 600-second run**. The routes are
  synchronous and run on the loop (ADR-0039), so one stalled call delays every request behind it.
* The run missed both the rate floor and the p99 budget, and **the harness refused to record it**, so
  its latencies have no `run_id` and are not reproduced here (CLAUDE.md §13 rule 2). The shape is what
  matters and can be stated without them: the **scoring core's own p99 stayed roughly three orders of
  magnitude below the client's**, so none of the tail was scoring. It was the queue behind a blocked
  loop.

So the acceptance gate failed for a reason that had nothing to do with the hot path's logic, and the
largest single consumer of the memory that broke it was the one class of state whose loss costs
nothing.

**Two things follow that are worth separating.** One is a capacity fact. The other is a design fault.

The capacity fact: features alone are **970 bytes per request**, so a ten-minute 500 TPS run needs
**277 MB** and fits comfortably. With the replay cache it is 1,531 bytes per request and **430 MB**
plus fragmentation, which does not.

The design fault is worse than the arithmetic. Caching every response for 24 hours at 500 TPS implies
**64 GB/day** for the replay cache alone. It does not scale with the target rate, and under
`allkeys-lru` it spends the budget by evicting the feature state that decides whether fraud is caught.

## Decision

**1. The three classes are named, documented and given different guarantees.** The table above is the
contract. `docs/OPERATIONS.md` carries it, because the person who needs it is holding a pager.

**2. Eviction is observable.** `online_store_evicted_keys_total` and `online_store_memory_bytes` are
published as gauges read from Redis's own counters. This is the only signal that distinguishes "this
account is new" from "we lost what we knew about this account" — the decision itself cannot tell them
apart, because an evicted key and an unseen key both read back empty. The gauges report **nothing**
rather than zero when the store cannot be reached: a gauge asserting "0 evictions" when it cannot see
is a false negative of exactly the kind ADR-0032 refuses for features.

**3. The replay cache must not share an eviction pool with feature state.** A best-effort latency
optimisation evicting correctness-relevant state is a defect regardless of the memory budget, and
`allkeys-lru` cannot express the difference. **The mechanism is not decided here** — a separate Redis
instance for cache-class state is the obvious candidate and it appears budget-neutral within
`docs/ARCHITECTURE.md` §14 — because it adds a process boundary, and §3.1 requires those to be argued
rather than assumed. It is recorded as the blocking finding it is, with the measurement behind it,
for a decision that is architectural.

**4. Until then, the Phase 2 acceptance gate runs against the existing 512 MB.** No number is
published from a run whose Redis was reconfigured to make it pass.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Raise `maxmemory` until the gate passes | Changes the measurement to fit the target, and the 64 GB/day trajectory means it only moves the failure. Used once as a *diagnostic*, outside the recording harness, and no record was written from it |
| Switch to `noeviction` | Turns memory exhaustion into failing writes, which fail open and are counted — honest, and permanently degrading once full, since the working set never shrinks below the cap. Trades a silent wrong answer for a guaranteed degraded one |
| Switch to `volatile-lru` | No effect: every key in this store already carries a TTL, so the volatile set is the whole keyspace |
| Shorten `REPLAY_TTL_S` from 24 h | Does nothing inside a ten-minute window, where every entry is younger than any plausible TTL. It would help steady state, and it weakens the response-identity guarantee that the cache exists to provide |
| Stop caching responses entirely | Idempotency stays correct — Postgres is authoritative — but a retry would be re-scored against newer features and return a different score under the same `case_id`. That breaks "a retry returns an identical response", which is a contract promise, not an optimisation |
| Prune the write path the way ADR-0038 pruned reads | Saves ~6 of 18 keys per request, roughly 33%. Real, and not enough: 430 MB becomes ~370 MB and still exceeds the cap once fragmentation is counted. It also creates a backfill obligation for every newly declared feature |

## Consequences

**Positive.** Eviction is no longer silent, and it was caught on the gauge's first real run. The three
state classes now have stated guarantees, so an operator can reason about what a full store costs
rather than discovering it from a fraud miss. The capacity requirement is a measured number —
970 bytes per request for feature state — instead of an assumption.

**Negative.** The classes still share one pool, so the defect is documented rather than fixed, and
until it is fixed a full store can still evict feature state and quietly lower a score. The
Phase 2 acceptance gate does not pass at 500 TPS on the local 512 MB budget for this reason, and that
is recorded as a miss rather than tuned away.

**Risks.** The measured 970 bytes per request is specific to the representative entity model; a
workload with different cardinality has a different figure, and nothing recomputes it automatically.
Second: eviction pressure manifests as *timeouts*, which the breaker correctly reads as an unavailable
dependency — so a memory problem presents as an availability problem, and without the new gauges an
operator would chase the wrong one. That is precisely what happened here before they existed.

## Status

Accepted
