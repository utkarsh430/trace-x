# Stream throughput and outage recovery

> Written by `make load-stream`, never by hand. Every figure below comes from the run
> recorded as `run_id: bench-20260920-061851-stream-throughput-a9cee6ce`, which `make check-claims` resolves.
>
> An operational benchmark on synthetic traffic, on one laptop. It measures the Kafka ->
> Bronze -> Silver stream and Gold's freshness; it says nothing about fraud-detection
> quality and predicts nothing about a cloud deployment.

## Verdict — `run_id: bench-20260920-061851-stream-throughput-a9cee6ce`

**FAIL** (`P3.stream-throughput`). Targets (ROADMAP Phase 3, PHASE3_PLAN §4.3):
the 5000 events/s rate offered in both phases and never lowered; consumer lag below 10000 ms throughout the measured window and recovering below it after a 120 s consumer outage; Gold freshness.

| verdict | kind | result | detail |
|---|---|---|---|
| `consumer_survived` | target | FAIL | the consumer (generation 1) exited with code 3 while it should have been running, so it did not sustain the offered rate; first error in its log: 26/09/20 06:19:06 ERROR Utils: Aborting task; log: /private/var/folders/j9/wgfy5qqj1sd6b6xwy6brqdkc0000gn/T/trace-x-load-stream/bench-20260920-061851-stream-throughput-a9cee6ce-logs/consumer-1.log |
| `clock_offset_bounded` | integrity | pass | every record's broker-to-host clock offset lies in [-127.159912109375, 216.550048828125] ms over 160866 delivery reports (drift between records 0.023193359375 ms); must lie within +-500.0 ms |
| `producers_healthy` | integrity | pass | 0 producer worker error(s): [] |
| `deliveries_confirmed` | integrity | pass | every record delivered |
| `log_append_time` | integrity | pass | 0 record(s) stamped with something other than LogAppendTime |
| `partitions_offered` | integrity | pass | partitions that received no benchmark record: [] |
| `throughput_sustained` | target | FAIL | the measured window never completed: the consumer (generation 1) exited with code 3 while it should have been running, so it did not sustain the offered rate; first error in its log: 26/09/20 06:19:06 ERROR Utils: Aborting task; log: /private/var/folders/j9/wgfy5qqj1sd6b6xwy6brqdkc0000gn/T/trace-x-load-stream/bench-20260920-061851-stream-throughput-a9cee6ce-logs/consumer-1.log |
| `lag_recovers_after_outage` | target | FAIL | the consumer (generation 1) exited with code 3 while it should have been running, so it did not sustain the offered rate; first error in its log: 26/09/20 06:19:06 ERROR Utils: Aborting task; log: /private/var/folders/j9/wgfy5qqj1sd6b6xwy6brqdkc0000gn/T/trace-x-load-stream/bench-20260920-061851-stream-throughput-a9cee6ce-logs/consumer-1.log |
| `samples_authentic` | integrity | pass | every sample is anchored to broker records; not held against the run because the consumer failed: partitions whose newest committed record predates the run: []; never committed: ['identity.events.v1[0]', 'identity.events.v1[1]', 'identity.events.v1[2]', 'tx.authorization.v1[0]', 'tx.authorization.v1[1]', 'tx.authorization.v1[2]', 'tx.authorization.v1[3]', 'tx.authorization.v1[4]', 'tx.authorization.v1[5]', 'tx.scored.v1[0]', 'tx.scored.v1[1]', 'tx.scored.v1[2]', 'tx.scored.v1[3]', 'tx.scored.v1[4]', 'tx.scored.v1[5]'] |

## Run — `run_id: bench-20260920-061851-stream-throughput-a9cee6ce`

| field | value |
|---|---|
| commit | `a9cee6ceccdd8ffa3d9cabeec4afedbd06fe677e` (dirty: False) |
| host | macOS-26.6.2-arm64-arm-64bit, 12 CPUs, 18 GiB |
| toolchain pins | Spark 4.0.1, Delta 4.0.1, Java 17, Hadoop 3.4.x, Scala 2.13 |
| broker | `apache/kafka:3.9.1@sha256:4ceccc577f03f51f6af8dbfda55194d0d892f4fa7913ffbded567ce3895622ed` |
| consumer | `local[8]`, driver 3g, 2 shuffle partitions, triggers 2.0 s / 4.0 s, maxOffsetsPerTrigger 30000, maxFilesPerTrigger None, maxBytesPerTrigger 67108864 |
| Gold | on, driver 2g |
| mix | {'tx.scored.v1': 14, 'tx.authorization.v1': 5, 'identity.events.v1': 1} (4 producer workers) |
| windows | warm-up 120 to 600 s, measured 600 s, pre-outage 60 s, recovery bound 900 s + hold 60 s |
| clock offset (broker minus host) | [-0.660888671875, -0.68408203125] ms from 160866 delivery reports |

## Throughput window — `run_id: bench-20260920-061851-stream-throughput-a9cee6ce`

| measure | value |
|---|---|
| offered (events/s) | — |
| Kafka → Silver committed (events/s) | — |
| Silver commits sampled | None |
| consumer lag p50 / p95 / p99 / max | — / — / — / — |
| widest interval without a Silver commit | — |

## Outage — `run_id: bench-20260920-061851-stream-throughput-a9cee6ce`

| measure | value |
|---|---|
| offered during the outage phase (events/s) | — |
| recovery after the restart | — |
| peak consumer lag after the restart | — |

## Gold builds — `run_id: bench-20260920-061851-stream-throughput-a9cee6ce`

| build | started (ms) | finished (ms) | exit | lag |
|---|---|---|---|---|
| None | 1789885149426 | None | None | — |

## Resources — `run_id: bench-20260920-061851-stream-throughput-a9cee6ce`

| role | peak RSS (MiB) | mean CPU % | max CPU % |
|---|---|---|---|
| all | 4256.6 | 653.7 | 890.2 |
| consumer | 3698.1 | 480.5 | 558.5 |
| gold | 391.1 | 105.8 | 121.1 |
| producers | 269.6 | 190.5 | 277.0 |

The full lag series, every verdict and the configuration are in `eval/manifest/bench-20260920-061851-stream-throughput-a9cee6ce.json`.
