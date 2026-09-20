# trace-gateway — observation log hot-path A/B

> Written by `scripts/observation_log_ab.py report`, never by hand. The arms, the
> workload, the state reset and the rule were pre-registered in ADR-0051 §7 before the
> first run.
>
> These are `LOADTEST` records: they exercised the service, not a model. They can
> substantiate latency and throughput, never a quality claim (`docs/EVALUATION.md` §8
> rule 2).

## Method

- One gateway image; the arms differ only in configuration, read back from each gateway's
  own `/readyz` checks into its record.
- `log-off`: no broker configured. `log-on`: the observation log publishing.
  `log-relay`: the observation log publishing and the outbox relay in the gateway.
- Workload `representative`, its seed, the same offered rate and window for every run,
  from flushed stores, truncated tables and recreated topics, verified empty before each
  run.
- Order: `log-off`, `log-on`, `log-relay`, `log-relay`, `log-on`, `log-off`.
- Commit `8c9a85a498cc30cd28c2c142c2f8420c7a6c7655`, clean worktree for every run.
- Gateway image `sha256:a4e1314511a9f83ed2c5244476a1ec419426af46612784d8324233035bfa7c96`, built once from that commit and checked before every run.

## Runs

| # | arm | run | scoring-core p99 (ms) | achieved rate (req/s) | client p99 (ms) | dropped | 5xx |
|---|---|---|---|---|---|---|---|
| 1 | `log-off` | `run_id: load-20260915-042542-gateway-8c9a85a4` | 0.666 | 499.91 | 10.50 | 0 | 0 |
| 2 | `log-on` | `run_id: load-20260915-042857-gateway-8c9a85a4` | 0.714 | 499.91 | 22.22 | 0 | 0 |
| 3 | `log-relay` | `run_id: load-20260915-043205-gateway-8c9a85a4` | 0.671 | 499.91 | 21.71 | 0 | 0 |
| 4 | `log-relay` | `run_id: load-20260915-043512-gateway-8c9a85a4` | 0.677 | 499.91 | 20.02 | 0 | 0 |
| 5 | `log-on` | `run_id: load-20260915-044858-gateway-8c9a85a4` | 0.861 | 499.91 | 96.82 | 0 | 0 |
| 6 | `log-off` | `run_id: load-20260915-045205-gateway-8c9a85a4` | 0.690 | 499.91 | 14.21 | 0 | 0 |

## The relay's process (the pre-registered rule)

| metric | arms | means | difference | noise | within noise |
|---|---|---|---|---|---|
| scoring-core p99 (ms) | `log-on` vs `log-relay` | 0.787 vs 0.674 | 0.114 | 0.147 | yes | `run_id: load-20260915-042857-gateway-8c9a85a4`, `run_id: load-20260915-044858-gateway-8c9a85a4`, `run_id: load-20260915-043205-gateway-8c9a85a4`, `run_id: load-20260915-043512-gateway-8c9a85a4` |
| achieved rate (req/s) | `log-on` vs `log-relay` | 499.908 vs 499.915 | 0.007 | 0.005 | no | `run_id: load-20260915-042857-gateway-8c9a85a4`, `run_id: load-20260915-044858-gateway-8c9a85a4`, `run_id: load-20260915-043205-gateway-8c9a85a4`, `run_id: load-20260915-043512-gateway-8c9a85a4` |

## The publisher's cost (reported, no threshold)

| metric | arms | means | difference | noise | within noise |
|---|---|---|---|---|---|
| scoring-core p99 (ms) | `log-off` vs `log-on` | 0.678 vs 0.787 | 0.109 | 0.147 | yes | `run_id: load-20260915-042542-gateway-8c9a85a4`, `run_id: load-20260915-045205-gateway-8c9a85a4`, `run_id: load-20260915-042857-gateway-8c9a85a4`, `run_id: load-20260915-044858-gateway-8c9a85a4` |
| achieved rate (req/s) | `log-off` vs `log-on` | 499.908 vs 499.908 | 0.000 | 0.005 | yes | `run_id: load-20260915-042542-gateway-8c9a85a4`, `run_id: load-20260915-045205-gateway-8c9a85a4`, `run_id: load-20260915-042857-gateway-8c9a85a4`, `run_id: load-20260915-044858-gateway-8c9a85a4` |

## Decision

**Option A is rejected:** the relay moves to option B, because `log-relay` left the no-relay control's run-to-run noise on at least one metric. ADR-0049 is updated to match.
