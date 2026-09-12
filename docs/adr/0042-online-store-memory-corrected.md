# ADR-0042: Online-store memory, corrected — AOF was the latency cause; eviction is the correctness cause

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 2
- **Supersedes / Superseded by:** **Supersedes ADR-0041's measurements and its conclusion.** ADR-0041's classification of the three state classes stands and is not restated here.

## Context

ADR-0041 reported that feature state costs **970 bytes per request**, that a ten-minute 500 TPS run
therefore needs **277 MB**, and that separating the idempotency replay cache would make it fit inside
the 512 MB cap.

**Every one of those numbers is wrong, and the error has a single cause.** They were sampled from a
keyspace that was *already at the cap and evicting*. `MEMORY USAGE` over the surviving keys measures
what Redis kept, not what the workload wrote — and Redis had discarded 3,283,782 keys before the
sample was taken. The figure was not an estimate that came out low; it was an accounting of the
survivors of the very problem being measured.

Re-measured on a store with headroom, where nothing was evicted, using the `idem` key count (exactly
one per scored request) to recover the true denominator:

| class | bytes/key | keys/request | bytes/request |
|---|---|---|---|
| `idem` replay cache | 1,579 | 1.00 | **1,579** |
| `f:l` previous observation | 158 | 4.01 | 634 |
| `f:b` amount buckets | 165 | 3.99 | 658 |
| `f:v` velocity | 121 | 3.98 | 482 |
| `f:dx` exact distinct | 103 | 4.50 | 464 |
| `f:da` HLL buckets | 130 | 1.54 | 200 |
| `f:p` profile | 168 | 0.91 | 153 |
| `f:pa` amount sample | 133 | 0.91 | 121 |
| | | **total** | **4,335** |

**4,335 bytes per request, of which 2,711 is feature state** — 2.8× ADR-0041's figure. A ten-minute
500 TPS run needs **1.32 GB**, measured directly at the end of a run with headroom, and features alone
need **760 MB**. Separating the replay cache does not make it fit. ADR-0041's recommendation rested on
the understated number.

**The second correction is larger, and it inverts the diagnosis.** ADR-0041 attributed the gate's
latency failure to eviction. Three runs on the representative profile, identical but for one variable
each, say otherwise:

| configuration | dropped | degraded | p99 |
|---|---|---|---|
| 1.6 GB, AOF on, **with mid-run `SCAN` sampling** | 611 | 2,921 | 610.85 ms |
| 1.6 GB, AOF **on**, no sampling | 51 | 5,151 | 32.66 ms |
| 1.6 GB, AOF **off**, no sampling | 0 | 0 | 12.43 ms |
| **512 MB, AOF off, no sampling** | **4** | **0** | **17.65 ms** |

Two things had been changed at once and neither was what ADR-0041 named.

**The largest distortion was the measurement itself.** Sampling Redis with `SCAN` during a run cost
14–16 ms per command against a single-threaded server and a 20 ms client timeout. The instrument was
producing most of the effect it was there to observe.

**The second was AOF.** `appendfsync everysec` produced 5,151 timeouts and 51 dropped iterations. Not
catastrophic on p99, but dropped iterations fail the harness's integrity gate outright, so the run
could not be recorded however good the latency looked.

**Eviction, on its own, costs almost nothing in latency.** The last row evicted 346,067 keys and
still returned **zero** degraded responses, with a p99 comfortably inside budget. ADR-0041 had the
causality backwards.

## Decision

**1. The recorded per-request footprint is 4,335 bytes (2,711 for feature state).** ADR-0041's 970 is
withdrawn. Any capacity statement cites this ADR.

**2. Memory pressure is recorded as a correctness concern, not a latency one.** This is the part of
ADR-0041 that survives, and it is the part that matters: an evicted feature key reads back empty,
which is indistinguishable from an account with no history, so the rules abstain and a transaction
that should have been CRITICAL is approved with `degraded=false`. The 512 MB run evicted 346,067 keys
and *looked perfectly healthy from the outside* — every latency target met, nothing degraded. That is
the failure mode at its most dangerous: silent, and wearing the appearance of success.

**3. A sampling protocol, because the instrument is part of the system.** No `SCAN`, `MEMORY USAGE`
or `KEYS` against Redis while a run is in flight. Counters from `INFO` are cheap and are what the
`online_store_*` gauges already publish; anything that walks the keyspace waits until the run ends.

**4. The remaining gap is 4 dropped iterations in 300,001** — every other exit condition passes,
including the rate floor, both latency budgets and zero 5xx. That run's figures have no `run_id`
because the harness refused it on the dropped iterations (CLAUDE.md §13 rule 2), so they are not
reproduced here. The drops are attributable to eviction-driven spikes: the same profile with headroom
drops none and its worst observed request is less than half as slow. **The sizing
decision is escalated rather than taken here**, because closing it means either raising the
`docs/ARCHITECTURE.md` §14 memory budget — which `api`, `worker` and `ui` have not yet drawn against —
or reversing ADR-0038's deliberate choice to leave the write path un-pruned. Both are product-level
calls about the local-first requirement in CLAUDE.md §12.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Leave ADR-0041 standing and note the correction elsewhere | The wrong number is load-bearing: it says the store fits when it needs 2.8× more. ADRs are immutable once accepted (CLAUDE.md §14), so the remedy is to supersede, and the error is recorded rather than quietly replaced |
| Raise `maxmemory` until the gate passes and record that | Changes the measurement to fit the target. Used only as a *diagnostic*, outside the recording harness, and no record was written from any such run |
| Increase `preAllocatedVUs` so the 4 drops disappear | The drops are real and the generator is not what produced them; raising pre-allocation after seeing a failure is tuning the instrument to the answer (CLAUDE.md §6) |
| Compress the replay cache payload | Measured: 1,201 → 623 bytes, 13% of total, at 0.008 ms per request. Real but it does not close a 2.6× gap, and it makes stored values opaque to inspection. Recorded as an available lever |
| Prune the write path to the read plan | Measured: ~18.5%. Also does not close the gap, reverses an ADR-0038 decision, and creates a backfill obligation for every newly declared feature. Available lever, not a fix |
| Keep AOF for restart durability | It persisted more than the container could load: after a 1.32 GB run, Redis crash-looped on start reading its own AOF. Durability that cannot be restored is not durability |

## Consequences

**Positive.** The hot path met every latency and throughput target on a representative workload at
the committed configuration, which no previous run had done — the figures stay unpublished until a run
clears every integrity condition and earns a `run_id`. The capacity figure is now measured on a store
that was not destroying its own evidence. The
sampling protocol prevents a repeat of an error that cost three ten-minute runs.

**Negative.** The gate still does not pass: 4 dropped iterations against a threshold of zero, and the
fix is a budget decision rather than a code change. Feature state is still silently evicted at the
committed configuration, so decisions made near the memory ceiling are of lower quality than they
appear and nothing in them says so. ADR-0041 is barely hours old and already superseded, which is the
honest cost of having published a number measured through the problem it described.

**Risks.** The 4,335 bytes is specific to the representative entity model and the cold-store condition
that model produces; a warmer store would shorten `insufficient_history_features` in the cached
response and lower the `idem` figure, so this number is an upper bound for that class and nothing
recomputes it automatically. Second: with AOF off, a Redis restart loses the entire keyspace, and
until Phase 3's reconciliation exists there is nothing to rebuild it from — the store warms by serving
traffic, and the traffic served while it warms is scored on less history than it should be.

## Status

Accepted
