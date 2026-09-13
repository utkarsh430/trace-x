# ADR-0043: The small-threadpool middle ground is measured, and it is worse

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 2
- **Supersedes / Superseded by:** Closes the open question ADR-0039 recorded under **Risks**. ADR-0039's decision is unchanged and reaffirmed.

## Context

ADR-0039 moved the hot-path routes back onto the event loop after a controlled A/B showed a
64-thread worker pool cost 18% of throughput. It recorded one thing it did not know:

> *"A small threadpool (4–8 threads) — plausibly a middle ground and **not measured**. Recorded as an
> open question rather than adopted; ADR-0038 was written on unmeasured reasoning once already and
> this ADR exists because of it."*

Leaving that open has a cost: the next person to look at the hot path finds a plausible untested idea
and runs the same experiment. It is now run.

The hypothesis had a real mechanism behind it, which is why it was worth testing rather than
dismissing. On the **saturation** profile 97% of requests open an investigation, and a Postgres
transaction is ~1.4 ms of socket wait that genuinely releases the GIL. Unlike the Redis calls — which
are sub-millisecond against a co-located instance — that is exactly the shape of work threads are
supposed to overlap.

## Decision

**Keep the event loop. Do not adopt a threadpool at any size.**

Measured on the saturation profile, 500 TPS offered for 600 s, identical store state, same host, same
image but for the scheduling change. The event-loop column is the recorded run
`run_id: load-20260912-gateway-05186248`; the threadpool column was a **diagnostic**, run outside the
recording harness, so it has no `run_id` and its absolute figures are not published (CLAUDE.md §13
rule 2). It is reported as change against that baseline, which is what the question asked anyway:

| | event loop (recorded) | 4 threads, relative |
|---|---|---|
| sustained TPS | 405.6 TPS | **−7.1%** |
| backlog (dropped iterations) | 55,753 | **+31.9%** |
| client p50 | *(see the recorded report)* | +10.3% |
| client p99 | *(see the recorded report)* | −8.3% |
| **scoring core p99** | *(see the recorded report)* | **4.2× worse** |
| 5xx | 0 | 0 |
| triage rate | 97.0% | 96.8% |

Four threads regress on the workload chosen to favour them, and on the one metric that isolates the
scheduler from the queue — the scoring core's own p99 — they regress by a factor of four. The p99
improvement is not a gain: throughput fell and the backlog grew by a third, so the surviving requests
are a smaller, luckier sample of the offered load.

Both measured points now agree, at 4 threads and at 64. The mechanism is the same one ADR-0039
identified: the per-request Python work holds the GIL, the socket waits are short, and threads buy
contention rather than overlap. A Postgres transaction on 97% of requests was the most favourable case
available and it was not enough.

**This closes the question.** A future proposal to thread the hot path should carry evidence that the
work has become materially more I/O-bound — a networked store with millisecond round trips, or async
drivers replacing the synchronous ones — rather than re-running this sweep.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Sweep 1 / 2 / 8 threads as well | 4 and 64 both regress, on the workload most favourable to threading, and the mechanism explains both. Four more ten-minute runs to interpolate between two points that agree would be spending the budget on a foregone conclusion |
| Adopt 4 threads for its lower p99 | The p99 fell because throughput fell and the backlog grew 31.9%; the requests that completed are a luckier subset. Reading that as an improvement is selecting on the outcome |
| Leave the question open in ADR-0039 | An untested plausible idea in an accepted ADR is an invitation to re-run the experiment. It cost one run to close, and the answer is now written down with its numbers |
| Adopt threads for the representative profile instead | That profile already meets every target with roughly 7× headroom on p99. There is nothing to buy, and the change would be complexity against no measured need |

## Consequences

**Positive.** The concurrency model is settled on evidence at two points rather than on one point and
an argument. The implementation stays the simpler one — no threadpool sizing constant, no
thread-safety surface beyond what the stores already guarantee — and ADR-0039's one acknowledged gap
is closed.

**Negative.** One replica remains bounded by the reciprocal of its per-request work, so horizontal
scaling is the only lever this design offers. That was already true; it is now true with the
alternative eliminated rather than merely unattempted.

**Risks.** The result is specific to co-located stores with sub-millisecond round trips. Against a
networked Redis or Postgres the socket waits lengthen, the GIL is released for longer, and the balance
could genuinely invert — so this ADR closes the question for *this* deployment shape and not for every
one. The signal to revisit is a store that is no longer local, and the cited numbers are the baseline
to beat.

## Status

Accepted
