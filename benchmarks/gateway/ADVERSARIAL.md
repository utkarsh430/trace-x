# trace-gateway — hot-path load test

> Written by `make load-gateway`, never by hand. Every figure below comes from the
> run recorded as `run_id: load-20260912-gateway-05186248`, which `make check-claims` resolves.
>
> This is a `LOADTEST` record: it exercised the service, not a model. It can
> substantiate latency, throughput and availability, and the claim linter refuses
> to let it back a quality claim (`docs/EVALUATION.md` §8 rule 2).

> **Workload: `triage-saturation` — this is NOT the acceptance gate.** It concentrates 80% of load onto 5% of a 20,000-account pool, which drives nearly every request over the velocity thresholds and into triage. It characterises the system under saturation; the gate is `benchmarks/gateway/REPORT.md`.

## Run — `run_id: load-20260912-gateway-05186248`

| field | value |
|---|---|
| service | `trace-gateway` 0.1.0 |
| instrument | `k6` v0.49.0 |
| image | `grafana/k6:0.49.0@sha256:8cd78f9d0de5f50bc8821cceecf356d5d9e839e6611c226a3fcf13c591080fbd` |
| workload profile | **triage-saturation** |
| offered rate | 500 TPS |
| window | 600 s |
| traffic seed | 20260912 |
| rule pack | `core` `sha256:88cfb527c75c54743be406c945fcdd2bd005123bebf66017585bd808008c74a4` |
| thresholds | `sha256:644fc085fbe0134660fa1f73636db29e9bc25458865d94c527ca28624953a953` |
| feature set | 1.0.0 |
| degraded mode observed | false |
| host | macOS-15.7.4-arm64-arm-64bit |
| commit | `05186248ef350ed03de1bf7c59e5b804107010df` |
| dirty worktree | false |
| started / finished | 2026-09-12T18:02:28.030204Z / 2026-09-12T18:12:31.419225Z |

The rule-pack and threshold digests were read from the running gateway's own
decision, not from this checkout, and the two were required to agree before the
run started.

## Latency — `run_id: load-20260912-gateway-05186248`

`client` is `http_req_duration`: what a caller experiences, which is what the
ROADMAP budget is about. `server` is the gateway's own `RiskDecision.latency_ms`,
which separates the scoring path from the network and the load generator.

| statistic | client | server |
|---|---|---|
| p50 | 1913.22 ms | 0.44 ms |
| p90 | 2084.30 ms | — |
| p95 | 2181.56 ms | — |
| p99 | 2893.55 ms | 0.76 ms |
| avg | 1879.05 ms | — |
| min / max | 1.33 / 17385.86 ms | — |

## Load actually offered — `run_id: load-20260912-gateway-05186248`

| quantity | value |
|---|---|
| requests | 244,248 |
| iterations started | 244,248 |
| iterations dropped | 55,753 |
| run duration | 602.20 s |
| achieved rate | 405.60 req/s |

## Responses — `run_id: load-20260912-gateway-05186248`

| class | count |
|---|---|
| 2xx | 244,248 |
| 4xx | 0 |
| 429 | 0 |
| 5xx | 0 |
| degraded decisions | 0 |

## Decision mix — `run_id: load-20260912-gateway-05186248`

Reported because a run in which every transaction banded the same way exercised
one branch of the rule engine at the offered rate, and its p99 would not describe
production. The traffic is seeded and skewed (80% of it over 5% of the entities);
the workload profile's k6 script says why.

| band | count |
|---|---|
| LOW | 5,374 |
| MEDIUM | 1,910 |
| HIGH | 10,116 |
| CRITICAL | 226,848 |

## Exit conditions — `run_id: load-20260912-gateway-05186248`

Asserted by `scripts/load_gateway.py`, which exits non-zero on any failure.
Recorded as found, whichever way they went (CLAUDE.md §17).

| check | kind | result | detail |
|---|---|---|---|
| `requests_were_made` | INTEGRITY | PASS | 244248 iterations completed; a run with no data substantiates nothing |
| `target_rate_sustained` | INTEGRITY | PASS | SATURATION RESULT, not a gate: sustained 405.6 TPS of 500 TPS offered. The latency below describes the rate ACHIEVED, and must never be quoted as latency at 500 TPS |
| `no_dropped_iterations` | INTEGRITY | PASS | SATURATION RESULT, not a gate: 55753 iterations were never started. This is the backlog the offered rate could not clear |
| `not_rate_limited` | INTEGRITY | PASS | 0 responses were 429. A rate-limited run measures the limiter, not the scoring path: raise TRACE_RATE_LIMIT_PER_MINUTE above target_tps * 60 for the load window, or spread the load over more tokens |
| `no_client_errors` | INTEGRITY | PASS | 0 responses were 4xx; a rejected request exercises the validation layer, and its latency is not the scoring path's |
| `responses_were_readable` | INTEGRITY | PASS | 0 successful responses could not be parsed, so the band and degraded counts below them are incomplete |
| `zero_5xx` | TARGET | PASS | 0 server errors over 602s (ROADMAP Phase 2 exit condition: zero) |
| `p99_under_budget` | TARGET | FAIL | p99 2893.55 ms against a 100 ms budget |
| `p50_under_budget` | TARGET | FAIL | p50 1913.22 ms against a 20 ms budget |

Displayed figures are rounded for reading; the unrounded values are in
`eval/manifest/load-20260912-gateway-05186248.json`.
