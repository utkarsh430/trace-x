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

Dataset `eval-v2`, 60,000 transactions scored.

## What was replayed

| stream | events |
|---|---|
| `tx.raw.v1` | 60,000 |
| `identity.events.v1` | 63,168 |
| `device.events.v1` | 2,123 |
| `tx.authorization.v1` | 59,999 |

All four released ingress streams, merged on `occurred_at` and replayed in that
order, an outcome after transactions at one instant. Transactions alone would hold
`failed_logins_1h`, `hours_since_identity_change` and `declined_ratio_1h`
permanently absent, and every rule over them would abstain rather than fire.

**Event-time projection:** every event shifted forward by one constant of
`21,900,270s` (253 days), applied identically to all
four streams. Relative gaps and cross-stream ordering are preserved exactly; the
frozen dataset is unmodified. Without it every decision in the run carries the
`occurred_at_backdated` flag -- correct, since the events really are old, but a
uniform flag on all of them carries no information and hides a genuine degradation
among it.

## Authorization outcomes

Feature set `3.0.0` reads verified authorization outcomes, never a
transaction's own request field (ADR-0049 §5).

Replayed from the dataset's `tx.authorization.v1` stream: 59,999 outcomes.

## Triage bands

| label | LOW | MEDIUM | HIGH | CRITICAL | triaged |
|---|---|---|---|---|---|
| fraudulent (143) | 60 | 0 | 0 | 83 | 58.0% |
| legitimate (59,857) | 59,841 | 0 | 1 | 15 | 0.0% |

## Rules that fired, by label

| rule | on fraud | on legitimate |
|---|---|---|
| `R001_card_testing_burst` | 33 | 0 |
| `R003_card_probing_velocity` | 15 | 0 |
| `R007_impossible_travel` | 3 | 0 |
| `R010_device_shared_across_accounts` | 47 | 15 |
| `R015_high_value_anomaly` | 0 | 1 |

## Degradation

| reason | decisions |
|---|---|
| `history_incomplete` | 60,000 |

**0** decisions were made without the feature store. Only reasons marked
above mean that; a decision can be flagged degraded for reasons that leave feature
retrieval entirely intact, and reporting those as a blind gateway sends an operator
to look for an outage that did not happen.

## Reading this honestly

- Known fraud is triaged at **58.0%** against **0.0%** for legitimate traffic. The hot path separates them, which is what this check asks.
- A replayed prefix is not a fraud rate: which scenarios fall inside it is a property of where the prefix ends, not of the dataset.
- Every transaction was scored against a cold store that warmed as the replay proceeded, so early transactions had less history than later ones -- the same condition a real deployment starts in.

**Feature store:** vouched from the shifted window start, 2026-09-11T11:24:30.615261Z.

**eval-v2 status.** `LPC-5` strict acceptance: FAIL; Category A findings: 0 (`eval/track_a/eval-v2.manifest.json`). eval-v2 is not `LPC-5` compliant, its scenario mix is a coverage floor, and no natural fraud prevalence may be claimed from it.
