# ADR-0035: Where "scoring fails open" ends — gateway degradation, per dependency

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 2
- **Supersedes / Superseded by:** —

## Context

CLAUDE.md §3.7 and `docs/ARCHITECTURE.md` §18 state the project's fail-safe rule:
**scoring fails open** (approve and flag, counted), **actions fail closed** (never execute under
uncertainty). Phase 2 is the first code that has to act on it, and doing so exposed that the rule as
written does not say *where scoring ends*.

Two rows of §18's failure table point in opposite directions and both are correct:

> | Redis down | health probe / 20 ms timeout | Hot path → rules-only, `degraded=true` on the response, alert. Never fail-open silently |
> | Postgres down | connection error | Gateway 503; worker stops consuming — no work lost, the queue is in PG |

For a LOW-band transaction those disagree. Nothing about a LOW decision needs Postgres, so §3.7's
fail-open would suggest answering; §18 says 503. The document never reconciles them because the
question only becomes concrete once something has to be written.

A third dependency appeared that neither row covers: the **rate limiter**, which also lives in Redis.
An unavailable limiter is not a scoring-quality problem at all.

And a fourth issue was not a policy question but a measurement. With a real Redis paused, a single
scored request took **21.8 seconds** against a configured 20 ms timeout. Two independent causes:
`redis-py` applies a default retry policy with exponential backoff, so `socket_timeout` bounds one
*attempt* rather than one call (a single `GET` measured **4.10 s** under a 50 ms timeout, and
**0.053 s** with retries disabled); and one request makes several Redis calls, so even bounded, an
outage costs their sum. At 500 TPS that is not degradation, it is collapse — callers time out, retry,
and the gateway is serving double the load it already cannot answer.

## Decision

**The fail-safe direction is decided per dependency, by what its loss actually costs.**

| Dependency | Loss costs | Behaviour | Readiness |
|---|---|---|---|
| **Redis** (features) | scoring *quality* — fewer inputs | Rules-only. Features report absent, rules over them **abstain**, the score falls honestly, `degraded=true` and the reason travel in the body and in `X-Trace-Degraded`. Never a 5xx. | **Unaffected** |
| **Redis** (rate limiter) | a protection for *TRACE-X*, not for the customer | **Fails open**, counted as `degraded_mode_total{reason="rate_limit_unavailable"}` | Unaffected |
| **Redis** (replay cache) | byte-identical replay only | Request is re-scored; the effect stays safe because Postgres enforces it | Unaffected |
| **Postgres** | a CRITICAL transaction's investigation cannot be **durably recorded** | `503` with `Retry-After`, as a problem document | **Fails**, so a load balancer drains the instance |

**Postgres is where scoring ends, and that is the substance of this ADR.** Losing Redis makes a
decision *worse*; losing Postgres makes a decision *unrecordable*. Approving a CRITICAL transaction
and silently losing its investigation is data loss wearing the costume of a successful response — and
the alternative, buffering case creation to a local WAL, would break ADR-0007's requirement that the
case row and the queue row move in one transaction. So §18 stands as written, and the reason it does
not contradict §3.7 is now stated: §3.7 governs scoring, and recording an investigation is not
scoring.

**Degradation is expressed as missing inputs, never as a separate code path.** An unreachable store
yields an empty feature context; every feature reports `INSUFFICIENT_HISTORY`; every rule over one
abstains (ADR-0033). There is no degraded scorer to drift from the healthy one, and a decision made
with no features is distinguishable from one made with all of them because it says which were absent.

**Redis is fronted by a circuit breaker, and retries are disabled.** Three consecutive failures open
the circuit for a five-second cooldown, during which calls are skipped without touching a socket;
recovery is automatic on the first success after cooldown. One breaker for the whole Redis dependency
— shared by the feature store, the limiter and the cache — because three would each pay a timeout to
learn the same outage. Three failures rather than one because a single timeout is as likely to be a
GC pause as an outage; five seconds rather than thirty because a gateway that degrades correctly and
stays degraded has converted a transient outage into a permanent one.

**Every degraded decision is tagged and counted.** `degraded_mode_total{reason}` and the response's
`degraded_reasons` carry the same list. An untagged fail-open is indistinguishable from a healthy
decision, which makes the whole permission to fail open unauditable.

**Liveness and readiness answer different questions.** `/healthz` touches no dependency — a liveness
probe that fails on a database blip restarts a healthy process. `/readyz` reports Postgres as
unhealthy and Redis as degraded, because draining for a planned degradation would turn the design
into the outage it exists to prevent.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| 503 on every dependency loss, including Redis | Contradicts §18 and §3.7 directly, and throws away the entire rules tier — which is the part of the system that works without any store at all |
| Approve LOW/MEDIUM with Postgres down, 503 only when triage is needed | Better availability, and the fail-open/fail-closed boundary then depends on the score. A boundary that moves with the data is one nobody can reason about during an incident, and the band is exactly what is least trustworthy when a dependency is missing |
| Buffer case creation to a bounded local WAL when Postgres is down | Breaks ADR-0007's single-transaction guarantee between the case row and the queue row, which is the property that makes the queue correct at all |
| Fail the rate limiter closed | Refuses traffic the gateway could have scored, to protect the gateway from load it is not actually under. The limiter protects us, not the customer's money |
| Treat a missing feature as `0.0` and score normally | The exact fabrication ADR-0022 exists to prevent, one level up: the score would read as a confident "no risk found" rather than "not assessed" |
| Timeout alone, no circuit breaker | Measured insufficient. A bounded call still costs a timeout, and several calls per request at 500 TPS is a collapse rather than a degradation |
| Retries on the hot path | On a 100 ms budget a retry is not resilience — the caller has already given up, and the retry is load the gateway *adds* to an outage |

## Consequences

**Positive.** Each dependency's failure has one documented behaviour and a test that produces it
(`pytest -m chaos tests/chaos/test_redis_down.py` pauses the real container). Degradation costs one
probe per cooldown instead of one timeout per call. Recovery is automatic and asserted. A caller can
always tell a degraded decision from a healthy one, in the body and in a header, without asking us.

**Negative.** Postgres availability now bounds gateway availability, and that is a real reduction in
uptime accepted deliberately rather than engineered around. The circuit breaker adds a state machine
to the hot path — a component that can itself be wrong, and whose wrongness looks like unexplained
degradation. Shared breaker state means one failing collaborator (say the limiter) opens the circuit
for the feature store too; that is the intended trade, but it means a Redis that is healthy for reads
and failing for writes degrades more than it strictly must.

**Risks.** The breaker's thresholds are judgement, not measurement: three failures and five seconds
were chosen from the shape of the failure, not from production data. Signal: `degraded_mode_total`
rising with no corresponding Redis incident (too sensitive), or degraded decisions continuing well
past a recovery (too slow). Second risk: the "Postgres bounds availability" decision is revisited
under an outage rather than in advance, and a WAL is added in a hurry — which would silently drop
ADR-0007's transactional guarantee at exactly the moment correctness matters most.

## Status

Accepted
