# Online feature store — score latency by account depth

> Written by `benchmarks/features/score_latency.py`, never by hand. Every figure
> comes
> from the run recorded as `run_id: bench-20260915-094759-score-latency-870f7e94`, which `make check-claims`
> resolves.

One account, one throwaway `redis:7-alpine` (Redis 7.4.11), nothing else
writing.
Depth is the transactions the account holds in its raw window before the timed
scores.
Wall time covers the script and the client's decoding and reductions.

## Per-score cost — `run_id: bench-20260915-094759-score-latency-870f7e94`

| depth | scores | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) | Redis script (µs/call) |
|---|---|---|---|---|---|---|
| 1 | 200 | 3.734 | 6.562 | 9.184 | 11.742 | 505 |
| 64 | 200 | 5.402 | 7.977 | 8.921 | 12.929 | 979 |
| 512 | 200 | 9.249 | 11.371 | 14.419 | 14.913 | 1,350 |
| 2,048 | 200 | 10.104 | 14.610 | 20.421 | 29.499 | 1,945 |
| 8,192 | 200 | 12.249 | 13.425 | 16.683 | 19.144 | 2,916 |
