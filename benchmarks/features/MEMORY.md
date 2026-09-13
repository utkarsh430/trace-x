# Online feature store — memory model

> Written by `benchmarks/features/memory_model.py`, never by hand. Every figure comes
> from the run recorded as `run_id: bench-20260913-memory-model-5c770259`, which `make check-claims` resolves.

Derived from the released `StatePlan` (what is stored, for how long) and from Redis's
own `MEMORY USAGE` on synthetic keys of each shape. Population and rate are the
representative profile's (ADR-0040): 669,767 accounts, 50,233 merchants,
803,720 devices, 334,884 IPs, 500 TPS, uniform arrivals.

## Measured bytes per primitive — `run_id: bench-20260913-memory-model-5c770259`

| primitive | base bytes | bytes per member | measured points (members, bytes) |
|---|---|---|---|
| `velocity` | 64 | 56.1 | (1, 120), (64, 3,656) |
| `buckets` | 0 | 318.7 | (1, 160), (120, 38,080) |
| `exact_distinct` | 76 | 27.9 | (1, 104), (32, 968) |
| `hll` | 112 | 0.0 | (1, 112), (2, 112), (4, 112), (8, 112), (16, 160), (32, 256), (64, 448), (128, 448), (256, 832), (512, 1,344), (1024, 2,624), (5000, 14,400) |
| `previous` | 160 | 0.0 | (1, 160) |
| `profile` | 145 | 46.5 | (1, 192), (12, 704) |
| `amounts` | 72 | 48.0 | (1, 120), (128, 6,216) |

## The ten-minute acceptance run — `run_id: bench-20260913-memory-model-5c770259`

| primitive | scope | retention | keys | members/key | bytes/key | total | basis |
|---|---|---|---|---|---|---|---|
| `buckets` | MERCHANT | 90,000s | 50,105 | 6.0 | 1,908 | **91.2 MiB** | measured fit x plan retention; trimmed per write |
| `buckets` | ACCOUNT | 7,200s | 241,814 | 1.2 | 395 | **91.2 MiB** | measured fit x plan retention; trimmed per write |
| `profile` | ACCOUNT | 2,592,000s | 241,814 | 1.2 | 203 | **46.8 MiB** | measured fit x plan horizon |
| `previous` | ACCOUNT/TRANSACTION | 90,000s | 241,814 | 1.0 | 160 | **36.9 MiB** | measured x plan retention |
| `hll` | IP.ACCOUNT | 7,200s | 300,000 | 1.0 | 112 | **32.0 MiB** | measured sketch size x active 5-min buckets |
| `velocity` | ACCOUNT/TRANSACTION | 90,000s | 241,814 | 1.2 | 134 | **30.8 MiB** | measured fit x plan retention |
| `velocity` | CARD/TRANSACTION | 3,900s | 241,814 | 1.2 | 134 | **30.8 MiB** | measured fit x plan retention |
| `amounts` | ACCOUNT | 2,592,000s | 241,814 | 1.2 | 132 | **30.3 MiB** | measured fit; sample capped at 128 |
| `exact_distinct` | DEVICE.ACCOUNT | 90,000s | 250,372 | 1.2 | 109 | **26.1 MiB** | measured fit x plan retention; members capped by ADR-0034 bound |
| `exact_distinct` | ACCOUNT.MERCHANT | 7,200s | 241,814 | 1.2 | 111 | **25.5 MiB** | measured fit x plan retention; members capped by ADR-0034 bound |
| `exact_distinct` | ACCOUNT.MCC | 3,900s | 241,814 | 1.2 | 111 | **25.5 MiB** | measured fit x plan retention; members capped by ADR-0034 bound |
| `exact_distinct` | ACCOUNT.DEVICE | 90,000s | 241,814 | 1.2 | 111 | **25.5 MiB** | measured fit x plan retention; members capped by ADR-0034 bound |
| `exact_distinct` | ACCOUNT.COUNTRY | 90,000s | 241,814 | 1.2 | 111 | **25.5 MiB** | measured fit x plan retention; members capped by ADR-0034 bound |
| `velocity` | MERCHANT/TRANSACTION | 90,000s | 50,105 | 6.0 | 400 | **19.1 MiB** | measured fit x plan retention |
| `hll` | MERCHANT.ACCOUNT | 7,200s | 100,210 | 1.0 | 112 | **10.7 MiB** | measured sketch size x active 5-min buckets |
| `previous` | ACCOUNT/IDENTITY_CHANGE | 90,000s | 259 | 1.0 | 160 | **0.0 MiB** | measured x plan retention |
| `velocity` | ACCOUNT/IDENTITY_FAILED_LOGIN | 7,200s | 259 | 1.0 | 120 | **0.0 MiB** | measured fit x plan retention |
| `epoch` | store | 0s | 1 | 1.0 | 64 | **0.0 MiB** | one key |
| | | | | | | **548.0 MiB** | |

**Configured feature-store limit: 704mb** — the projection above
with 1.25x headroom for fragmentation and for the model's own
error, rounded up to 64 MiB. The gate runs with `noeviction`, so exceeding it would be a
refused write and a failed run, not a silent loss.

## Steady state at 500 TPS with the declared retention — `run_id: bench-20260913-memory-model-5c770259`

| primitive | scope | retention | keys | members/key | bytes/key | total | basis |
|---|---|---|---|---|---|---|---|
| `buckets` | MERCHANT | 90,000s | 50,233 | 895.8 | 285,460 | **13,675.2 MiB** | measured fit x plan retention; trimmed per write |
| `amounts` | ACCOUNT | 2,592,000s | 669,767 | 128.0 | 6,216 | **3,970.4 MiB** | measured fit; sample capped at 128 |
| `velocity` | ACCOUNT/TRANSACTION | 90,000s | 669,767 | 67.2 | 3,835 | **2,449.6 MiB** | measured fit x plan retention |
| `velocity` | MERCHANT/TRANSACTION | 90,000s | 50,233 | 895.8 | 50,344 | **2,411.8 MiB** | measured fit x plan retention |
| `exact_distinct` | DEVICE.ACCOUNT | 90,000s | 803,720 | 56.0 | 1,636 | **1,254.3 MiB** | measured fit x plan retention; members capped by ADR-0034 bound |
| `buckets` | ACCOUNT | 7,200s | 666,665 | 5.4 | 1,721 | **1,094.0 MiB** | measured fit x plan retention; trimmed per write |
| `profile` | ACCOUNT | 2,592,000s | 669,767 | 11.0 | 657 | **419.7 MiB** | measured fit x plan horizon |
| `hll` | IP.ACCOUNT | 7,200s | 3,600,000 | 1.0 | 112 | **384.5 MiB** | measured sketch size x active 5-min buckets |
| `exact_distinct` | ACCOUNT.MERCHANT | 7,200s | 666,665 | 5.4 | 227 | **144.0 MiB** | measured fit x plan retention; members capped by ADR-0034 bound |
| `velocity` | CARD/TRANSACTION | 3,900s | 633,334 | 3.1 | 237 | **143.0 MiB** | measured fit x plan retention |
| `hll` | MERCHANT.ACCOUNT | 7,200s | 1,205,592 | 1.0 | 112 | **128.8 MiB** | measured sketch size x active 5-min buckets |
| `previous` | ACCOUNT/TRANSACTION | 90,000s | 669,767 | 1.0 | 160 | **102.2 MiB** | measured x plan retention |
| `exact_distinct` | ACCOUNT.MCC | 3,900s | 633,334 | 3.1 | 162 | **97.7 MiB** | measured fit x plan retention; members capped by ADR-0034 bound |
| `exact_distinct` | ACCOUNT.DEVICE | 90,000s | 669,767 | 2.0 | 132 | **84.1 MiB** | measured fit x plan retention; members capped by ADR-0034 bound |
| `exact_distinct` | ACCOUNT.COUNTRY | 90,000s | 669,767 | 1.5 | 118 | **75.2 MiB** | measured fit x plan retention; members capped by ADR-0034 bound |
| `previous` | ACCOUNT/IDENTITY_CHANGE | 90,000s | 37,815 | 1.0 | 160 | **5.8 MiB** | measured x plan retention |
| `velocity` | ACCOUNT/IDENTITY_FAILED_LOGIN | 7,200s | 3,107 | 1.0 | 120 | **0.4 MiB** | measured fit x plan retention |
| `epoch` | store | 0s | 1 | 1.0 | 64 | **0.0 MiB** | one key |
| | | | | | | **26,440.8 MiB** | |

This is the honest capacity requirement for serving the target rate indefinitely. It
does not fit a laptop, and the local budget does not pretend to: at the configured limit
the local store holds **13 minutes** of
500 TPS before it refuses writes -- loudly, with `feature_write_failed` on
every
decision and the completeness epoch withdrawn (ADR-0044). The largest lines above are
where any reduction would have to come from, and each is a feature-semantics decision
rather than a tuning one.

