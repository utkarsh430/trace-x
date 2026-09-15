# Online feature store — score latency by account depth

> Written by `benchmarks/features/score_latency.py`, never by hand. Every figure
> comes
> from the run recorded as `run_id: bench-20260915-071647-score-latency-86fabdbf`, which `make check-claims`
> resolves.

One account, one throwaway `redis:7-alpine` (Redis 7.4.11), nothing else
writing.
Depth is the transactions the account holds in its raw window before the timed
scores.
Wall time covers the script and the client's decoding and reductions.

## Per-score cost — `run_id: bench-20260915-071647-score-latency-86fabdbf`

| depth | scores | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Redis script (µs/call) |
|---|---|---|---|---|---|---|
| 1 | 200 | 3.584 | 6.584 | 6.946 | 7.267 | 381 |
| 64 | 200 | 5.060 | 8.193 | 10.147 | 14.413 | 814 |
| 512 | 200 | 14.388 | 17.405 | 20.154 | 25.265 | 2,626 |
| 2,048 | 200 | 42.969 | 50.322 | 54.767 | 70.555 | 6,883 |
| 8,192 | 200 | 171.462 | 183.323 | 190.322 | 195.141 | 32,754 |
