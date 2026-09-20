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
| `tx.authorization.v1` | 59,999 |

All four released ingress streams, merged on `occurred_at` and replayed in that
order, an outcome after transactions at one instant. Transactions alone would hold
`failed_logins_1h`, `hours_since_identity_change` and `declined_ratio_1h`
permanently absent, and every rule over them would abstain rather than fire.

**Event-time projection:** every event shifted forward by one constant of
`21,900,013s` (253 days), applied identically to all
four streams. Relative gaps and cross-stream ordering are preserved exactly; the
frozen dataset is unmodified. Without it every decision in the run carries the
`occurred_at_backdated` flag -- correct, since the events really are old, but a
uniform flag on all of them carries no information and hides a genuine degradation
among it.

## Authorization outcomes

Feature set `3.0.0` reads verified authorization outcomes, never a
transaction's own request field (ADR-0049 §5).

The dataset has no `tx.authorization.v1` stream, so 59,999
outcomes were derived from its transactions' outcome column by the generator's own
builder (DM-1: 40 ms + |N(300 ms, 150 ms)|, keyed by the seed and the transaction id; seed `42`, the source
manifest's). Loaded transactions whose column derives nothing:

| value | transactions |
|---|---|
| _none_ | 0 |

## Triage bands

| label | LOW | MEDIUM | HIGH | CRITICAL | triaged |
|---|---|---|---|---|---|
| fraudulent (322) | 193 | 4 | 8 | 117 | 38.8% |
| legitimate (59,678) | 59,650 | 0 | 2 | 26 | 0.0% |

## Rules that fired, by label

| rule | on fraud | on legitimate |
|---|---|---|
| `R001_card_testing_burst` | 62 | 0 |
| `R003_card_probing_velocity` | 53 | 0 |
| `R004_velocity_spike_1m` | 10 | 0 |
| `R005_velocity_spike_5m` | 29 | 0 |
| `R007_impossible_travel` | 16 | 12 |
| `R010_device_shared_across_accounts` | 43 | 14 |
| `R015_high_value_anomaly` | 5 | 2 |

## Degradation

| reason | decisions |
|---|---|
| `history_incomplete` | 60,000 |

**0** decisions were made without the feature store. Only reasons marked
above mean that; a decision can be flagged degraded for reasons that leave feature
retrieval entirely intact, and reporting those as a blind gateway sends an operator
to look for an outage that did not happen.

## Reading this honestly

- Known fraud is triaged at **38.8%** against **0.0%** for legitimate traffic. The hot path separates them, which is what this check asks.
- A replayed prefix is not a fraud rate: which scenarios fall inside it is a property of where the prefix ends, not of the dataset.
- Every transaction was scored against a cold store that warmed as the replay proceeded, so early transactions had less history than later ones -- the same condition a real deployment starts in.

**Feature store:** vouched from the shifted window start, 2026-09-11T11:20:13.428763Z.
