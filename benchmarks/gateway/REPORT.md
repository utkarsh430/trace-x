# trace-gateway — hot-path load test

> Written by `make load-gateway`, never by hand. Every figure below comes from the
> run recorded as `run_id: load-20260912-gateway-550789bb`, which `make check-claims` resolves.
>
> This is a `LOADTEST` record: it exercised the service, not a model. It can
> substantiate latency, throughput and availability, and the claim linter refuses
> to let it back a quality claim (`docs/EVALUATION.md` §8 rule 2).

> **Workload: `representative` — this IS the Phase 2 acceptance gate.** Its entity model is derived from `eval/track_a/eval-v1.manifest.json`, and its population is derived from the offered rate so per-account velocity stays realistic.

## Run — `run_id: load-20260912-gateway-550789bb`

| field | value |
|---|---|
| service | `trace-gateway` 0.1.0 |
| instrument | `k6` v0.49.0 |
| image | `grafana/k6:0.49.0@sha256:8cd78f9d0de5f50bc8821cceecf356d5d9e839e6611c226a3fcf13c591080fbd` |
| workload profile | **representative** |
| offered rate | 500 TPS |
| window | 600 s |
| traffic seed | 20260912 |
| rule pack | `core` `sha256:88cfb527c75c54743be406c945fcdd2bd005123bebf66017585bd808008c74a4` |
| thresholds | `sha256:644fc085fbe0134660fa1f73636db29e9bc25458865d94c527ca28624953a953` |
| feature set | 1.0.0 |
| degraded mode observed | false |
| host | macOS-15.7.4-arm64-arm-64bit |
| commit | `550789bb42da7ff6408cdbbc2d3fffb3d7372d7a` |
| dirty worktree | false |
| started / finished | 2026-09-12T17:49:29.333514Z / 2026-09-12T17:59:30.498186Z |

The rule-pack and threshold digests were read from the running gateway's own
decision, not from this checkout, and the two were required to agree before the
run started.

## Latency — `run_id: load-20260912-gateway-550789bb`

`client` is `http_req_duration`: what a caller experiences, which is what the
ROADMAP budget is about. `server` is the gateway's own `RiskDecision.latency_ms`,
which separates the scoring path from the network and the load generator.

| statistic | client | server |
|---|---|---|
| p50 | 1.70 ms | 0.31 ms |
| p90 | 3.08 ms | — |
| p95 | 3.34 ms | — |
| p99 | 14.25 ms | 0.43 ms |
| avg | 2.25 ms | — |
| min / max | 1.35 / 90.68 ms | — |

## Load actually offered — `run_id: load-20260912-gateway-550789bb`

| quantity | value |
|---|---|
| requests | 300,001 |
| iterations started | 300,001 |
| iterations dropped | 0 |
| run duration | 600.04 s |
| achieved rate | 499.97 req/s |

## Responses — `run_id: load-20260912-gateway-550789bb`

| class | count |
|---|---|
| 2xx | 300,001 |
| 4xx | 0 |
| 429 | 0 |
| 5xx | 0 |
| degraded decisions | 0 |

## Decision mix — `run_id: load-20260912-gateway-550789bb`

Reported because a run in which every transaction banded the same way exercised
one branch of the rule engine at the offered rate, and its p99 would not describe
production. The traffic is seeded and skewed (80% of it over 5% of the entities);
the workload profile's k6 script says why.

| band | count |
|---|---|
| LOW | 299,994 |
| MEDIUM | 2 |
| HIGH | 2 |
| CRITICAL | 3 |

## Exit conditions — `run_id: load-20260912-gateway-550789bb`

Asserted by `scripts/load_gateway.py`, which exits non-zero on any failure.
Recorded as found, whichever way they went (CLAUDE.md §17).

| check | kind | result | detail |
|---|---|---|---|
| `requests_were_made` | INTEGRITY | PASS | 300001 iterations completed; a run with no data substantiates nothing |
| `target_rate_sustained` | INTEGRITY | PASS | achieved 500.0 TPS against a 500 TPS target (floor 495.0); below it, the latency describes a smaller test than the one claimed |
| `no_dropped_iterations` | INTEGRITY | PASS | 0 iterations were never started: offered load that never left the generator |
| `not_rate_limited` | INTEGRITY | PASS | 0 responses were 429. A rate-limited run measures the limiter, not the scoring path: raise TRACE_RATE_LIMIT_PER_MINUTE above target_tps * 60 for the load window, or spread the load over more tokens |
| `no_client_errors` | INTEGRITY | PASS | 0 responses were 4xx; a rejected request exercises the validation layer, and its latency is not the scoring path's |
| `responses_were_readable` | INTEGRITY | PASS | 0 successful responses could not be parsed, so the band and degraded counts below them are incomplete |
| `zero_5xx` | TARGET | PASS | 0 server errors over 600s (ROADMAP Phase 2 exit condition: zero) |
| `p99_under_budget` | TARGET | PASS | p99 14.25 ms against a 100 ms budget |
| `p50_under_budget` | TARGET | PASS | p50 1.70 ms against a 20 ms budget |

Displayed figures are rounded for reading; the unrounded values are in
`eval/manifest/load-20260912-gateway-550789bb.json`.
