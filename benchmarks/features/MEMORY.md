# Online feature store — memory model

> Written by `benchmarks/features/memory_model.py`, never by hand. Every figure comes
> from the run recorded as `run_id: bench-20260915-095206-memory-model-77d9afa4`, which `make check-claims` resolves.

Measured through `RedisOnlineFeatureStore` itself, the Step 1b layout (ADR-0046 §5), on a
throwaway `redis:7-alpine` (Redis 7.4.11), with Redis's exact
`MEMORY USAGE` summed by
key family. Population and rate are the representative profile's (ADR-0040), unchanged
from Phase 2: 669,767 accounts, 50,233 merchants, 803,720 devices,
334,884 IPs, 500 TPS, uniform arrivals. Retentions are the store's `LAYOUT`.
It supersedes the Phase 2 model (`run_id: bench-20260913-memory-model-5c770259`), which
measured the Phase 2 key shapes.

## Measured curves — `run_id: bench-20260915-095206-memory-model-77d9afa4`

| family | size driver | measured points (size, bytes per key) |
|---|---|---|
| `obs` | one key | (1, 416) |
| `tx` | members | (1, 128), (16, 960), (64, 3,648), (128, 7,232), (129, 18,272), (256, 32,688), (1,024, 130,088) |
| `card` | members | (1, 128), (16, 960), (64, 3,648), (128, 7,232), (129, 17,992), (256, 33,040), (1,024, 129,824) |
| `dev` | members | (1, 144), (16, 1,344), (64, 5,184), (128, 10,304), (129, 20,168), (256, 37,264), (1,024, 146,528) |
| `epoch` | one key | (1, 56) |
| `position` | one key | (1, 56) |
| `hw` | one key | (1, 48) |
| `pf` | folded observations | (1, 192), (20, 192), (128, 192), (512, 192) |
| `pfh` | folded observations | (1, 120), (20, 288), (128, 288), (512, 320) |
| `pfa` | folded observations | (1, 88), (20, 152), (128, 520), (512, 520) |
| `pfl` | folded observations | (1, 144), (20, 1,856), (128, 1,856), (512, 1,856) |
| `pfc` | folded observations | (1, 80), (20, 80), (128, 80), (512, 80) |
| `hll` | accounts in the bucket | (1, 124), (2, 124), (4, 124), (8, 124), (16, 172), (32, 268), (64, 460), (128, 460), (256, 844), (512, 1,612), (1,024, 2,636) |
| `mcv` | minutes held | (1, 120), (16, 960), (128, 7,232), (512, 94,328), (1,440, 317,304) |
| `obs_identity` | one key | (1, 304) |
| `ie` | members | (1, 120), (16, 712), (64, 2,632), (128, 5,192), (129, 15,928), (256, 29,024), (1,024, 113,448) |
| `obs_outcome` | one outcome | (1, 360) |
| `aov` | one key | (1, 104) |
| `ao` | members | (1, 112), (16, 704), (128, 5,184), (129, 15,800) |
| `aod` | members | (1, 112), (16, 704), (128, 5,184), (129, 16,160) |

## The ten-minute acceptance run — `run_id: bench-20260915-095206-memory-model-77d9afa4`

| family | scope | retention | keys | size | bytes/key | total | basis |
|---|---|---|---|---|---|---|---|
| `obs+aov` | authorization outcome | 7,200s | 300,000 | 1.0 | 464 | **132.8 MiB** | ASSUMED one outcome per transaction, the upper bound |
| `obs` | transaction | 90,000s | 300,000 | 1.0 | 416 | **119.0 MiB** | one string key per observation, for the raw window |
| `ao+aod` | account | 7,200s | 241,814 | 1.2 | 243 | **56.0 MiB** | ASSUMED one outcome per transaction; measured curve at the accounts' depth |
| `dev` | dev | 90,000s | 250,372 | 1.2 | 160 | **38.2 MiB** | measured curve at the active entities' depth |
| `hll` | IP | 7,500s | 300,000 | 1.0 | 124 | **35.5 MiB** | measured sketch at its bucket's cardinality x active buckets |
| `tx` | account | 90,000s | 241,814 | 1.2 | 141 | **32.6 MiB** | measured curve at the active accounts' raw depth |
| `card` | card | 3,900s | 241,814 | 1.2 | 141 | **32.6 MiB** | measured curve at the active entities' depth |
| `mcv` | merchant/GBP | 90,060s | 50,105 | 6.0 | 399 | **19.1 MiB** | measured curve at the minutes each merchant holds |
| `hll` | MERCHANT | 7,500s | 100,210 | 3.0 | 124 | **11.9 MiB** | measured sketch at its bucket's cardinality x active buckets |
| `obs` | identity | 90,000s | 260 | 1.0 | 304 | **0.1 MiB** | one string key per identity event, for the raw window |
| `ie` | account | 90,000s | 259 | 1.0 | 120 | **0.0 MiB** | measured curve at the active accounts' depth |
| `epoch` | store | 0s | 1 | 1.0 | 56 | **0.0 MiB** | one key |
| `position` | store | 0s | 1 | 1.0 | 56 | **0.0 MiB** | one key |
| `hw` | store | 0s | 1 | 1.0 | 48 | **0.0 MiB** | one key |
| | | | | | | **477.7 MiB** | |

**Configured feature-store limit: 640mb**: the projection above
with 1.25x headroom for fragmentation and for the model's
own error,
rounded up to 64 MiB. The store runs with `noeviction`, so exceeding it is a refused
write
and a failed run, not a silent loss.

## Steady state at 500 TPS with the declared retention — `run_id: bench-20260915-095206-memory-model-77d9afa4`

| family | scope | retention | keys | size | bytes/key | total | basis |
|---|---|---|---|---|---|---|---|
| `obs` | transaction | 90,000s | 45,000,000 | 1.0 | 416 | **17,852.8 MiB** | one string key per observation, for the raw window |
| `mcv` | merchant/GBP | 90,060s | 50,233 | 896.4 | 186,695 | **8,943.8 MiB** | measured curve at the minutes each merchant holds |
| `dev` | dev | 90,000s | 803,720 | 56.0 | 4,543 | **3,482.3 MiB** | measured curve at the active entities' depth |
| `tx` | account | 90,000s | 669,767 | 67.2 | 3,827 | **2,444.1 MiB** | measured curve at the active accounts' raw depth |
| `pf..pfc` | account | 2,595,600s | 669,767 | 1,870.5 | 3,081 | **1,968.1 MiB** | folded profile prefix, measured through the store's own fold |
| `obs+aov` | authorization outcome | 7,200s | 3,600,000 | 1.0 | 464 | **1,593.0 MiB** | ASSUMED one outcome per transaction, the upper bound |
| `hll` | IP | 7,500s | 3,750,000 | 1.0 | 124 | **443.5 MiB** | measured sketch at its bucket's cardinality x active buckets |
| `ao+aod` | account | 7,200s | 666,665 | 5.4 | 571 | **363.2 MiB** | ASSUMED one outcome per transaction; measured curve at the accounts' depth |
| `hll` | MERCHANT | 7,500s | 1,255,825 | 3.0 | 124 | **148.5 MiB** | measured sketch at its bucket's cardinality x active buckets |
| `card` | card | 3,900s | 633,334 | 3.1 | 243 | **147.0 MiB** | measured curve at the active entities' depth |
| `obs` | identity | 90,000s | 38,925 | 1.0 | 304 | **11.3 MiB** | one string key per identity event, for the raw window |
| `ie` | account | 90,000s | 37,815 | 1.0 | 121 | **4.4 MiB** | measured curve at the active accounts' depth |
| `epoch` | store | 0s | 1 | 1.0 | 56 | **0.0 MiB** | one key |
| `position` | store | 0s | 1 | 1.0 | 56 | **0.0 MiB** | one key |
| `hw` | store | 0s | 1 | 1.0 | 48 | **0.0 MiB** | one key |
| | | | | | | **37,401.9 MiB** | |

This is the capacity requirement for serving the target rate indefinitely, and it is not
expected to fit a laptop. Lines marked ASSUMED rest on a stated upper bound, not on the
profile.

