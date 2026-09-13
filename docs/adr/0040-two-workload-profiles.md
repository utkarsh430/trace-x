# ADR-0040: Two workload profiles — a representative acceptance gate and an adversarial saturation benchmark

- **Status:** Accepted
- **Date:** 2026-09-12
- **Phase:** 2
- **Supersedes / Superseded by:** —

## Context

Phase 2's target is *"p99 < 100 ms, p50 < 20 ms at 500 TPS sustained"*. The target says what the
latency must be. It did not say what the traffic is, and the traffic turned out to decide the answer.

The k6 profile written to measure it concentrates **80% of offered load onto 5% of a
20,000-account pool**. At 500 TPS that is **1,440 transactions per account per hour**, against a rule
pack whose velocity thresholds are 5 per minute, 12 per five minutes and 40 per hour:

| rule | feature | implied value at 500 TPS | threshold | over by |
|---|---|---|---|---|
| R004 `velocity_spike_1m` | `account_tx_count_1m` | 24 | ≥ 5 | 4.8× |
| R005 `velocity_spike_5m` | `account_tx_count_5m` | 120 | ≥ 12 | 10× |
| R006 `velocity_spike_1h` | `account_tx_count_1h` | 1,440 | ≥ 40 | 36× |
| R003 `card_probing_velocity` | `card_tx_count_5m` | 120 | ≥ 10 | 12× |

Noisy-OR across the three that fire on rate alone is exactly **0.9400** — CRITICAL — which matches
the score and the rule set observed on a primed account. Device and IP are drawn independently per
transaction, so R010 and R011 fire too, taking it to 0.9987; and because merchant and MCC are also
independent draws, `merchant_is_habitual` and `mcc_is_habitual_for_account` are structurally zero, so
R016 and R018 can never fire.

The measured consequence: **91.7% of the load test opened an investigation**, each one a three-insert
Postgres transaction, costing **1.31 ms of a 2.49 ms per-request budget — 52.8%**.

Replaying 60,000 events of the project's own frozen `eval-v1` through the same gateway triages
**0.222%**. The two differ by **414×**. `eval-v1`'s busiest account does 33 transactions in three and
a half days; a hot account in the load profile does 24 per minute.

The load test was therefore measuring the cost of opening investigations, not the cost of scoring.
Both are real costs, and only one of them is what a hot-path latency budget is about.

## Decision

**Two named profiles, run by the same harness, reported to different files.**

**1. `representative` — the canonical Phase 2 acceptance gate.** Its entity model is taken from
`eval/track_a/eval-v1.manifest.json` and `data/generator/population.py` rather than invented a second
time: 1 card per account, 1–3 home devices, 1–2 home IPs, 4–12 habitual merchants used 75% of the
time, Zipf merchant popularity for the rest. Per-account affinity is derived from the account's own
index, so it is stable across a run exactly as it is in the dataset.

**Its population is derived from the offered rate, and that is the step the first profile skipped.**
`eval-v1` runs at ~0.43 transactions per account per day; holding that rate at 500 TPS requires a
population on the order of 100 million, and using 20,000 instead is precisely what manufactured the
velocity. So the account count is computed from `TARGET_TPS`, and the entity ratios are held at
eval-v1's. Change the rate and the population follows — the only way "500 TPS" and "represents normal
traffic" can both remain true.

**2. `triage-saturation` — the original profile, preserved unweakened.** It is not deleted and not
made easier. It characterises real and worth-knowing behaviour: sustainable TPS, triage rate, queue
growth, Postgres and Redis pressure, latency, resource use and correctness when nearly every request
opens a case. It is simply not the acceptance gate.

**The targets are unchanged.** p99 < 100 ms, p50 < 20 ms, 500 TPS, zero 5xx, zero dropped iterations,
every existing integrity condition. Only the distribution they are measured against is now stated.

The profile is recorded in the run provenance and printed at the top of every report, because a
latency figure without its workload is as unattributable as one without its rule-pack digest.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Keep one profile and lower the target to what it achieves | Weakening a target after seeing the result, which CLAUDE.md §6 names first among the ways not to game one. The target was never the thing that was wrong |
| Keep one profile and treat 91.7% triage as the design point | It would mean accepting that the system's normal state is opening an investigation on nine of every ten transactions — which contradicts the project's own dataset by 414× and would make the investigation queue the product |
| Fix the old profile in place | Destroys the adversarial signal. "Nearly every request triages" is a genuine operating condition worth characterising — a fraud wave, a misconfigured threshold, a scenario injection — and there would then be no benchmark for it |
| Derive the acceptance profile from production traffic | There is no production traffic. `eval-v1` is the project's only evidenced statement of what normal looks like, and it is frozen and digest-pinned |
| Reduce the offered rate until the current profile behaves | Silently changing the workload to meet a target, and it would leave the 500 TPS claim unmeasured |

## Consequences

**Positive.** The acceptance gate now measures the hot path on the distribution the project itself
says is normal, so a p99 from it means what a reader assumes. The adversarial profile keeps its
value and stops being mistaken for the gate. Both reports name their profile in the first lines and
in the run record, so neither can be quoted as the other.

**Negative.** There are now two benchmarks to maintain and two sets of numbers to keep straight, and
a reader who quotes the wrong one will be badly wrong in either direction. Deriving the population
from the offered rate also means the representative profile has **very low entity reuse** at 500 TPS —
close to one transaction per account within a ten-minute window — so it exercises a nearly cold
feature store and cannot say anything about a warmed one. That is a real limitation of measuring a
ten-minute window against a realistic population, not an artefact to tune away, and the replay
validation rather than the load test is what exercises accumulated history.

**Risks.** The derived population is large, so distinct-entity cardinality per run is high and the
online store's working set grows accordingly; whether it fits the local-first memory budget is a
measurement, not an assumption, and is taken in ADR-0041. Second risk: `EVAL_V1_TX_PER_ACCOUNT_PER_DAY`
is a constant in a JavaScript file derived from a frozen manifest, and nothing mechanically ties the
two — if the dataset is ever refrozen with a different shape, this profile silently keeps the old one.

**Historical evidence is not rewritten.** The earlier runs measured what they measured, on the
profile that existed. They are described as saturation measurements, which is what they were, and no
document claims that profile ever represented normal traffic.

## Status

Accepted
