# R010 threshold study on `eval-v2` (decision U10)

> `eval-v2` did not pass `LPC-5` (revision 4 strict acceptance: FAIL, with zero Category A findings; `eval/track_a/audits/stage-2-evidence/lpc5-eval-v2-acceptance-rev4.txt`). Its scenario mix is a coverage floor, not natural prevalence. Counts here compare operating points on synthetic data; they are not rates and not a quality metric.

Produced by `eval/replay/r010_threshold_study.py` from a replayed prefix's decisions.

| input | value |
|---|---|
| transactions decided | 60,000 |
| decisions file | `replay_eval_v2_fs3_decisions.jsonl` `sha256:803b9807db6960974de5c5fb89a7944c99b83a1b4afcd5c0f047239c2d65d230` |
| rule pack | `packages/trace_core/rules/packs/core.v1.yaml` `sha256:1631d9f496ff417071290b4fec84020c0951be15aa5adb0e31d94c14da41b5c1` |
| thresholds | `packages/trace_core/scoring/config/core.v1.yaml` `sha256:b4d6fc4aab6ad347b0b055e7ff537f488b2572444a94a07b23abd9f8b09ed9b5` |
| declared R010 threshold | 5 |

## Self-checks

- Recomputed bands differing from the gateway's: **0**.
- Offline R010 at 5 against the R010 the gateway fired (offline, served: count):
  - offline False, served False: 59,938
  - offline True, served True: 62

## Operating points

| threshold | device matches (fraud / legit) | HIGH or CRITICAL (fraud / legit) | raised by R010 alone (fraud / legit) |
|---|---|---|---|
| 2 | 92 / 14,266 | 112 / 14,266 | 76 / 14,265 |
| 3 | 57 / 2,043 | 93 / 2,044 | 57 / 2,043 |
| 4 | 53 / 208 | 89 / 209 | 53 / 208 |
| 5 (declared) | 47 / 15 | 83 / 16 | 47 / 15 |
| 6 | 37 / 0 | 73 / 1 | 37 / 0 |
| 7 | 30 / 0 | 66 / 1 | 30 / 0 |
| 8 | 25 / 0 | 61 / 1 | 25 / 0 |
| 9 | 22 / 0 | 58 / 1 | 22 / 0 |
| 10 | 20 / 0 | 56 / 1 | 20 / 0 |

## Decision rule

R010's threshold changes only on clear evidence of a better operating point: more fraud and fewer legitimate high-risk decisions together, or a large legitimate reduction for a negligible fraud loss, with both self-checks clean. Otherwise it stays at 5 (U10). Any change is a separate, versioned ruleset decision.
