# Stream throughput and outage recovery

> Written by `make load-stream`, never by hand. Every figure below comes from the run
> recorded as `run_id: bench-20260918-141621-stream-throughput-50aad177`, which `make check-claims` resolves.
>
> An operational benchmark on synthetic traffic, on one laptop. It measures the Kafka ->
> Bronze -> Silver stream and Gold's freshness; it says nothing about fraud-detection
> quality and predicts nothing about a cloud deployment.

## Verdict — `run_id: bench-20260918-141621-stream-throughput-50aad177`

**FAIL** (`P3.stream-throughput`). Targets (ROADMAP Phase 3, PHASE3_PLAN §4.3):
the 5000 events/s rate offered in both phases and never lowered; consumer lag below 10000 ms throughout the measured window and recovering below it after a 120 s consumer outage; Gold freshness.

| verdict | kind | result | detail |
|---|---|---|---|
| `consumer_survived` | target | FAIL | the consumer (generation 1) exited with code 1 while it should have been running, so it did not sustain the offered rate; first error in its log: Caused by: java.lang.OutOfMemoryError: Java heap space; log: /private/var/folders/j9/wgfy5qqj1sd6b6xwy6brqdkc0000gn/T/trace-x-load-stream/bench-20260918-141621-stream-throughput-50aad177-logs/consumer-1.log |
| `clock_offset_bounded` | integrity | pass | every record's broker-to-host clock offset lies in [-134.1689453125, 157.83203125] ms over 790935 delivery reports (drift between records 2.161865234375 ms); must lie within +-500.0 ms |
| `producers_healthy` | integrity | pass | 0 producer worker error(s): [] |
| `deliveries_confirmed` | integrity | pass | every record delivered |
| `log_append_time` | integrity | pass | 0 record(s) stamped with something other than LogAppendTime |
| `partitions_offered` | integrity | pass | partitions that received no benchmark record: [] |
| `throughput_sustained` | target | FAIL | the measured window never completed: the consumer (generation 1) exited with code 1 while it should have been running, so it did not sustain the offered rate; first error in its log: Caused by: java.lang.OutOfMemoryError: Java heap space; log: /private/var/folders/j9/wgfy5qqj1sd6b6xwy6brqdkc0000gn/T/trace-x-load-stream/bench-20260918-141621-stream-throughput-50aad177-logs/consumer-1.log |
| `lag_recovers_after_outage` | target | FAIL | the consumer (generation 1) exited with code 1 while it should have been running, so it did not sustain the offered rate; first error in its log: Caused by: java.lang.OutOfMemoryError: Java heap space; log: /private/var/folders/j9/wgfy5qqj1sd6b6xwy6brqdkc0000gn/T/trace-x-load-stream/bench-20260918-141621-stream-throughput-50aad177-logs/consumer-1.log |
| `samples_authentic` | integrity | pass | every sample is anchored to broker records |

## Run — `run_id: bench-20260918-141621-stream-throughput-50aad177`

| field | value |
|---|---|
| commit | `50aad177d388a3d5a3d2eda4001a0dc66e040724` (dirty: False) |
| host | macOS-26.6.2-arm64-arm-64bit, 12 CPUs, 18 GiB |
| toolchain pins | Spark 4.0.1, Delta 4.0.1, Java 17, Hadoop 3.4.x, Scala 2.13 |
| broker | `apache/kafka:3.9.1@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed` |
| consumer | `local[4]`, driver 2g, 2 shuffle partitions, triggers 2.0 s / 2.0 s, maxOffsetsPerTrigger None |
| Gold | on, driver 2g |
| mix | {'tx.scored.v1': 14, 'tx.authorization.v1': 5, 'identity.events.v1': 1} (4 producer workers) |
| windows | warm-up 120 to 600 s, measured 600 s, pre-outage 60 s, recovery bound 900 s + hold 60 s |
| clock offset (broker minus host) | [-1.051025390625, -3.212890625] ms from 790935 delivery reports |

## Throughput window — `run_id: bench-20260918-141621-stream-throughput-50aad177`

| measure | value |
|---|---|
| offered (events/s) | — |
| Kafka → Silver committed (events/s) | — |
| Silver commits sampled | None |
| consumer lag p50 / p95 / p99 / max | — / — / — / — |
| widest interval without a Silver commit | — |

## Outage — `run_id: bench-20260918-141621-stream-throughput-50aad177`

| measure | value |
|---|---|
| offered during the outage phase (events/s) | — |
| recovery after the restart | — |
| peak consumer lag after the restart | — |

## Gold builds — `run_id: bench-20260918-141621-stream-throughput-50aad177`

| build | started (ms) | finished (ms) | exit | lag |
|---|---|---|---|---|
| 0 | 1789741013453 | 1789741092099 | 0 | 64,599 ms |
| None | 1789741092100 | None | None | — |

## Resources — `run_id: bench-20260918-141621-stream-throughput-50aad177`

| role | peak RSS (MiB) | mean CPU % | max CPU % |
|---|---|---|---|
| all | 4769.1 | 801.6 | 957.4 |
| consumer | 3084.9 | 454.8 | 586.1 |
| gold | 1739.9 | 177.6 | 301.5 |
| producers | 335.1 | 220.4 | 306.3 |

The full lag series, every verdict and the configuration are in `eval/manifest/bench-20260918-141621-stream-throughput-50aad177.json`.
