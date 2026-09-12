# Gateway triage bands on a replayed Track A stream

> ROADMAP Phase 2 MANUAL VALIDATION. Produced by `eval/replay/gateway_replay.py`,
> which connects as **`trace_eval`** -- the only role permitted to read ground truth
> (ADR-0004). The gateway scored every transaction over HTTP before any label was
> joined, and the replayed events carry the released envelope and payload only.

**This is a distribution check, not a quality metric.** Phase 2 has no model and its
thresholds are declared rather than fitted, so precision and recall here would be
numbers with no meaning attached (CLAUDE.md §13). The question it answers is whether
the hot path puts known fraud into the bands that open an investigation more often
than it does legitimate traffic. A 'no' would mean the rules are blind.

Dataset `eval-v1`, 60,000 transactions scored.

## What was replayed

| stream | events |
|---|---|
| `tx.raw.v1` | 60,000 |
| `identity.events.v1` | 41 |
| `device.events.v1` | 5 |

All three released ingress streams, merged on `occurred_at` and replayed in that
order. Transactions alone would hold `failed_logins_1h` and
`hours_since_identity_change` permanently absent, and every rule over them would
abstain rather than fire -- so R008, R012 and R013 could not have fired whatever the
data said.

**Event-time projection:** every event shifted forward by one constant of
`21,703,869s` (251 days), applied identically to all
three streams. Relative gaps and cross-stream ordering are preserved exactly; the
frozen dataset is unmodified. Without it the request contract rejects the stream as
backdated, which is correct behaviour for a real feed and an obstacle only here.

| label | LOW | MEDIUM | HIGH | CRITICAL | triaged |
|---|---|---|---|---|---|
| fraudulent (322) | 192 | 10 | 6 | 114 | 37.3% |
| legitimate (59,678) | 59,640 | 19 | 1 | 18 | 0.0% |

## Rules that fired, by label

| rule | on fraud | on legitimate |
|---|---|---|
| `R001_card_testing_burst` | 50 | 0 |
| `R002_declined_ratio_high` | 15 | 0 |
| `R003_card_probing_velocity` | 41 | 0 |
| `R004_velocity_spike_1m` | 2 | 0 |
| `R005_velocity_spike_5m` | 15 | 0 |
| `R007_impossible_travel` | 16 | 12 |
| `R008_identity_change_then_new_device` | 6 | 0 |
| `R009_new_device_high_value` | 0 | 1 |
| `R010_device_shared_across_accounts` | 40 | 5 |
| `R015_high_value_anomaly` | 5 | 2 |
| `R016_unhabitual_merchant_and_category` | 1 | 0 |
| `R017_far_from_home_new_device` | 4 | 2 |
| `R018_high_risk_category_unfamiliar` | 17 | 17 |

## Degradation

| reason | decisions |
|---|---|
| `rate_limit_unavailable` | 1 |

**0** decisions were made without the feature store. Only reasons marked
above mean that; a decision can be flagged degraded for reasons that leave feature
retrieval entirely intact, and reporting those as a blind gateway sends an operator
to look for an outage that did not happen.

## Reading this honestly

- Known fraud is triaged at **37.3%** against **0.0%** for legitimate traffic. The hot path separates them, which is what this check asks.
- A replayed prefix is not a fraud rate: which scenarios fall inside it is a property of where the prefix ends, not of the dataset.
- Every transaction was scored against a cold store that warmed as the replay proceeded, so early transactions had less history than later ones -- the same condition a real deployment starts in.
