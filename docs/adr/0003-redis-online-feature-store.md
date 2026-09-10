# ADR-0003: Redis as the online feature store

- **Status:** Accepted
- **Date:** 2026-09-10
- **Phase:** 0

## Context
The hot path must compute ~20 behavioural features within a ~50 ms slice of a 100 ms budget: sliding
window velocity (1m/5m/1h/24h), distinct-entity cardinality, per-account profiles, failed-attempt
counters. A relational query per feature per transaction cannot meet that.

## Decision
Redis 7 as the online feature store, with the data structure chosen per feature shape:

| Feature shape | Structure | Complexity |
|---|---|---|
| Sliding-window velocity | Sorted set, score = event-time epoch, trimmed by `ZREMRANGEBYSCORE` | O(log n) |
| Distinct devices/IPs/merchants | HyperLogLog | O(1), ~0.81% error |
| Account/device profiles | Hash | O(1) |
| Idempotency keys, rate limits | String with TTL | O(1) |

Redis holds **derived state only**. It is authoritative for nothing: Spark Gold reconciles it, and it is
fully rebuildable from Delta.

## Alternatives Considered
| Alternative | Why rejected |
|---|---|
| Postgres with window functions | Correct but far too slow per transaction at the hot-path budget |
| In-process cache | Not shared across gateway replicas; lost on restart; incoherent under scaling |
| Feast / a managed feature store | Heavy dependency, another service in an 8 GB budget, and it hides the online/offline consistency problem that is the interesting part of this design |
| Redis Streams for the work queue too | Rejected separately — see ADR-0007 |

## Consequences
**Positive.** Sub-millisecond reads. Structures are O(log n) or better by construction. HLL bounds
memory for high-cardinality counts.
**Negative.** HLL is approximate (~0.81%), so features built on it are approximate — this must be stated
wherever those features are used, and the parity tolerance must account for it. Redis is an additional
failure mode.
**Risks.** Treating Redis as authoritative. Signal: any code path that cannot be rebuilt from Delta.
Mitigation: Redis loss degrades to rules-only and is visible in `X-Trace-Degraded`.

## Status
Accepted
