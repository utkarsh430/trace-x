# Rule re-validation: feature set 3.0.0 against 2.0.0 on the replayed eval-v1 prefix

> Phase 3 Stage 2 step 13. A controlled comparison, not a quality metric: counts of rule firings and
> bands on a replayed prefix, split by label only after counting, as `trace_eval` (ADR-0004).

## Method

Both gateways replayed the same 60,000-transaction `eval-v1` prefix through the same client
(`eval/replay/gateway_replay.py` at `a4b3d4c` plus the replay flags committed with this report). Each
ran on an emptied feature store, vouched from the dataset's shifted window start
(`--vouch-from-manifest`), and an emptied set of replay tables.

| run | gateway | feature set | authorization outcomes | decisions (sha256) |
|---|---|---|---|---|
| before | `tracex-gateway:fs2` built from `b43a75c` (`sha256:30ee59e1c9971d2c68b21ea06a9a7032993f50910904181a04da515a009f190c`) | 2.0.0 | withheld: the gateway predates ADR-0049 | `56fec39fc8ddaea0857faf3751a7ef02a2ad8476cc86a77e3a8514080df82cac` |
| after | `tracex-gateway:dev` built from `a4b3d4c` | 3.0.0 | derived by DM-1 from eval-v1's run record, posted | `56fec39fc8ddaea0857faf3751a7ef02a2ad8476cc86a77e3a8514080df82cac` |

The outcomes were derived in both runs and withheld from the 2.0.0 one just before posting, so the
events and the event-time projection match; only the outcome stream differs.

## Result

**No rule and no band changed.** The two decision files are byte-identical (the same sha256 above,
from two separate runs and files): every decision's rules, score, band and degradation match. Every
rule fired on the same labels in both runs, and no transaction changed band.

| rule | fraud (2.0.0 / 3.0.0) | legitimate (2.0.0 / 3.0.0) |
|---|---|---|
| `R001_card_testing_burst` | 62 / 62 | 0 / 0 |
| `R003_card_probing_velocity` | 53 / 53 | 0 / 0 |
| `R004_velocity_spike_1m` | 10 / 10 | 0 / 0 |
| `R005_velocity_spike_5m` | 29 / 29 | 0 / 0 |
| `R007_impossible_travel` | 16 / 16 | 12 / 12 |
| `R010_device_shared_across_accounts` | 43 / 43 | 14 / 14 |
| `R015_high_value_anomaly` | 5 / 5 | 2 / 2 |

| label | LOW | MEDIUM | HIGH | CRITICAL |
|---|---|---|---|---|
| fraudulent (322), both runs | 193 | 4 | 8 | 117 |
| legitimate (59,678), both runs | 59,650 | 0 | 2 | 26 |

## What this does not show

- **R002 is not exercised.** Feature set 3.0.0 changes only `declined_ratio_1h`, which R002 alone
  reads, together with `account_tx_count_1h >= 10`. R002 fired in neither run, so this prefix cannot
  distinguish the two semantics for it. Its served behaviour is covered by the feature-semantics
  fixtures and conformance tests of feature set 3.0.0.
- **Profile rules are not exercised.** The store is vouched from the window start, so every 30-day
  feature is incomplete within a prefix of a few days, every decision carries `history_incomplete`, and
  known-device, habitual and tenure rules abstain in both runs. Phase 3 Step 1's vouched comparison
  used a store claiming older history and is not comparable with this one.
- A replayed prefix is not a fraud rate, and eval-v1 carries the label proxies recorded in
  `docs/PROGRESS.md`.
