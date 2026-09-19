# ADR-0047: Event transport — declared topics, one producer factory, and a scheduled fault overlay

- **Status:** Proposed (Phase 3 Step 2; revised after critic review, integrated by the lead)
- **Date:** 2026-09-13
- **Phase:** 3
- **Supersedes / Superseded by:** None. Implements Step 2 of `docs/PHASE3_PLAN.md` (§2 B6; §3 Q2, Q3,
  Q8; §4.3). ADR-0006 (Kafka KRaft) and ADR-0026 (envelope and schema evolution) are unchanged.

## Context

Phase 3 puts Kafka on the warm path, and several things that the plan assumed were already true
turned out not to be:

- **There was no topic declaration.** `docs/EVENT_CONTRACTS.md` names `deploy/kafka/topics.yaml` as
  the topic configuration, but the file did not exist. The compose broker ran with automatic topic
  creation on, so a mistyped topic name would have been created silently with broker defaults: one
  partition, `CreateTime` timestamps and no byte cap.
- **The only producer was unsafe in several independent ways.**
  - The generator's Kafka sink used librdkafka's default partitioner, `consistent_random` (CRC32),
    while the Java client, and therefore Spark's Kafka sink, uses murmur2. The same account landed
    on different partitions depending on which client wrote it.
  - It was not idempotent, it published whatever bytes it was given, and it sent no `trace_id`
    header, although EVENT_CONTRACTS §6.6 requires one.
  - Its close flushed for thirty seconds and returned, so anything still queued was lost without
    an error.
- **Starting the broker image revealed defects that reading the compose file did not.**
  - Once any `KAFKA_*` variable is set, the `apache/kafka` image stops using its bundled
    `server.properties`, and `log.dirs` falls back to `/tmp/kafka-logs` in the container's writable
    layer. The volume stayed empty.
  - The only advertised listener was `localhost`, which a container on the compose network cannot
    use.
  - The healthcheck could never fail on reachability: `kafka-broker-api-versions.sh` exits 0 even
    when it prints `-> ERROR` for a broker it cannot reach. This was observed on this image, with a
    wrong advertised port.
  - The image gives every container the same default cluster id.
- **`retention.bytes` is not a disk bound.** Kafka deletes whole segments, only on a retention pass
  (the default interval is 300 s), and deleted segments stay on disk for the delete delay (60 s by
  default). Internal topics consume disk too, and by default their bound is large: 1 GiB metadata
  segments, and 50 consumer-offsets partitions of 100 MiB segments.
- **The frozen dataset exercises none of the difficult paths.** `eval-v1` contains no duplicates, no
  retries, no late or out-of-order events and no malformed records (PHASE3_PLAN §1). It spans days
  of event time, so a replay that is not truly paced makes almost every record late by accident.

## Decision

**1. The broker is pinned and configured so that the declaration is the only source of topics.**

- **Image:** `apache/kafka:3.9.1`, pinned by tag and by the digest of its multi-arch OCI index,
  which contains `linux/amd64` and `linux/arm64` images. 3.9.1 is the `kafka-clients` version Spark
  4.0.1 builds against.
- **Compose settings:**
  - `auto.create.topics.enable=false`, and `log.dirs` on the volume;
  - a `PLAINTEXT` listener advertised to the host, and an `INTERNAL` listener advertised as
    `kafka:19092` for containers;
  - the broker settings the disk bound depends on: retention check interval, delete delay,
    metadata segment size and retention, consumer-offsets partitions and segment size.
- **Healthcheck:** goes through the internal listener, requires the broker line, and fails on any
  `-> ERROR`. The host-advertised address cannot be checked from inside the container.
- The heap and the container memory limit are unchanged.

**2. `deploy/kafka/topics.yaml` declares only released topics** (`tx.raw.v1`, `identity.events.v1`,
`device.events.v1`, `investigation.requested.v1`). Each topic states:

- its key field, which a test checks against `trace_core.contracts.topics` and the release ledger;
- its dedup identity (Q2): `payload.transaction_id` for transactions, `envelope.event_id` for
  identity and device events, and `envelope.idempotency_key` for investigation requests;
- the **contract**: `cleanup.policy=delete`, `message.timestamp.type=LogAppendTime` (Q3), and the
  conceptual `retention.ms` from EVENT_CONTRACTS §3;
- separately, under `local`, the **local-development overrides** (Q8): partition count and
  replication, `retention.bytes` and `segment.bytes`, and the design ingress the transient term
  assumes. An override may never restate a contract key, and the loader refuses one that does;
- the cloud partition count, as documentation until Phase 12.

The declared local disk cap is the plan's budget of about 5.5 GiB. The bound it must cover is derived,
and its assumptions are stated:

    peak(partition) <= retention.bytes + 2 x segment.bytes + ingress x (2 x check interval + delete delay)

- After a retention pass a partition holds less than R + S.
- Between passes it grows by one check interval of ingress.
- Segments deleted within the last delete delay still hold at most S plus the ingress of I + D.

The R + 2S term is exact for every declared and reserved partition. The transient term is a design
assumption, the Q7 target rate times an *assumed mean* stored record size with no compression credit:

- it is not a ceiling, because identifiers have no maximum length and non-ASCII text is escaped;
- a test requires the largest generator-shaped ASCII transaction to fit the assumption.

The metadata log contributes its retention plus two segments. Consumer offsets contribute
partitions × 2 segments, assuming committed offsets stay far below one segment. The PLANNED
observation-log topic has a **reservation**, including its transient term, without being declared.
No `.dlq` topics are declared.

**3. `scripts/kafka_topics.py` applies and verifies the declaration with the AdminClient.**

- **Environment guard:** `--environment local` is required. `apply` and `verify` refuse a non-loopback
  bootstrap and a cluster with more than one broker, so local byte caps cannot reach another cluster.
- **`apply`:** creates only missing topics, then verifies. It refuses, and creates nothing, when an
  existing declared topic has a different partition count or cleanup policy. It never changes a
  partition count or an existing configuration.
- **`verify`:** compares every declared topic setting and every declared broker setting. It also
  reports any setting made on a declared topic that the declaration does not contain, and any
  undeclared topic.
- **Output:** every run prints the cluster id, and the JSON summary carries the environment and each
  topic id, which, unlike the shared default cluster id, changes when a topic is recreated.
- **Exit codes:** 0 ok, 1 drift, 2 refused, 3 unreachable, 4 invalid declaration, 5 unexpected error.

**4. `trace_core.contracts.publish` is the one producer factory.**

- **Contract settings:** every producer carries `enable.idempotence=true`, `acks=all`,
  `compression.type=zstd`, `partitioner=murmur2_random` and `queue.buffering.max.kbytes=65536`, and a
  finite `message.timeout.ms` (default 120000). None of the contract settings is a parameter.
- **Schema validation:** every value is validated against its released contract model before it is
  produced (EVENT_CONTRACTS §6.1), whatever the caller already checked.
  - **Recorded exception to §6.1:** `allow_invalid_events=True`, for fault injection, is refused unless
    every bootstrap server is a loopback address.
- **Keys:** always from `topics.partition_key()`. A `key_event`, needed only for a value that is not an
  event, must name the same key as the value whenever the value can be read.
- **Trace header:** `trace_id` is copied from `envelope.trace_id`, and a caller cannot supply it.
- **Missing topics:** a topic that does not exist fails at the first publish, from a metadata check,
  instead of after the full message timeout.
- **Accounting:** a `DeliveryLedger` bound into the producer's configuration counts, per topic, every
  message the producer accepted and every delivery report.
- **Closing:** `EventPublisher.close` refuses further publishes (a refused publish is itself counted),
  flushes, and raises `EventPublishError` unless all of these hold:
  - nothing is queued, failed, shed or refused;
  - no fatal error occurred;
  - delivered + failed equals accepted for every topic.
- **Concurrency:** publishing and closing share one gate, so a publish cannot land between a flush and
  its verdict.
- **Java partitioning:** a port of Kafka's `Utils.murmur2` and `toPositive`, pinned to Kafka's own test
  vectors and to values recorded from the verified jar, predicts every record's partition.
- **Observability:** counters `event_publish_outcomes_total{topic,outcome}` and
  `event_publisher_errors_total{error,fatal}`.
- The generator's `KafkaSink` is rebuilt on `EventPublisher`.

**5. `eval/replay/faults.py` overlays seeded, deterministic faults on released events, and schedules
them.**

- **Classes:**
  - per-key reordering;
  - exact duplicates, transaction retries with a new `event_id`, and conflicting duplicates under
    each topic's dedup identity;
  - **late exact duplicates** and **late retries**;
  - late events, and future-dated events inside and beyond the 24 h skew bound;
  - malformed JSON, the wrong schema version, and unknown enum values.
- **Exact counts:** a request the base cannot meet raises. Each base event carries at most one fault;
  the combinations that matter are classes of their own.
- **Determinism:** selection comes from SHA-256 over the overlay version, the seed and the base
  position. Identities derive from unshifted content.
- **Wrong schema version:** these records *validate* against the released models, because the envelope
  only requires `schema_version >= 1`. A consumer catches them only by comparing the version with the
  topic suffix.
- **Ground truth:** strict schema validation of every base record keeps label *fields* out. It does not
  remove label *proxies* a base already carries.

Lateness is arrival delay (`LogAppendTime - occurred_at`, the quantity §4.3 defines `is_late` on), and a
paced replay keeps a schedule:

- **Fault-free and other on-time records** are published at their base `ingested_at` moved to the
  replay anchor. Both clocks move by one recorded shift, so each arrives after its event time by its
  recorded ingestion lag. The build refuses a base or a band that would plan an on-time record outside
  a declared ceiling kept well below the late threshold.
- **Late copies** are held back: same content, published at the source's event time plus the late
  delay. That costs real wall-clock time.
- **Late and future-dated originals are stamped:** `occurred_at` is the scheduled time minus the delay,
  or plus the lead, and `ingested_at` is the scheduled time. Their event time moves, because holding
  them back would need waits longer than the late threshold.
- **Publishing:** `publish_overlay` sleeps to each scheduled time, stops with `ReplayScheduleError` when
  it slips beyond a declared bound, and confirms at hand-over that every record lands in its intended
  arrival class.
- **Publish manifest:** records the anchor, the shift, and every scheduled and actual hand-over time,
  rendered timestamp and value digest. Every paced byte can be reproduced from the overlay and the
  anchor.
- **Backfill:** does not pace or shift, marks every record `trace-replay-mode: backfill`, refuses every
  late class, and stamps future-dated records at the actual hand-over time, which the manifest records.
- **Future skew** for overlay records follows the other-producer rule (LogAppendTime plus the bound plus
  its margin). Gateway-observed events are judged against the gateway receipt time carried in the
  event, which no released topic carries.

**6. The integration tests run the compose broker.**

- **Broker:** the same image and settings, a named volume at the log directory, a user network with the
  `kafka` alias, and the compose healthcheck executed inside the container, with a negative control on
  an unreachable advertised address.
- **CI:** under `CI` a missing Docker, `stream` extra or broker fails instead of skipping. The shared
  `tests/conftest.py` also fails every Docker-backed test at setup in CI when Docker is missing, rather
  than skipping it before any Kafka fixture runs.

## Alternatives Considered

| Alternative | Why rejected |
|---|---|
| Keep librdkafka's default partitioner | CRC32 and murmur2 put most keys on different partitions (asserted against a real broker), so a second kind of producer, or Spark, would silently reorder per-account history |
| Pass the key at each call site | That is the Phase 1 defect: a call site that forgot the key published unkeyed. The key belongs to the topic and is read from the event |
| Leave schema validation to encoders | A publisher that trusts its caller publishes whatever it is given; the factory is the one place every event passes through, and the invalid path is an explicit, loopback-only opt-in |
| Let the broker, or the application at start-up, create topics | Broker defaults are wrong for every topic here, and creation spread across processes cannot be verified against one declaration |
| Let `apply` alter partition counts or configuration to match | Changing a partition count moves keys between partitions, and lowering a byte cap in place can delete data a consumer has not read |
| Run `apply` against any bootstrap | Local byte caps and partition counts applied to a shared or cloud cluster would delete data there; the tool refuses anything but a single loopback broker |
| Use partitions × `retention.bytes` as the disk bound, or budget only declared topics | Understates the peak: Kafka deletes whole segments on a timer and keeps them for the delete delay, and internal topics with broker defaults would be the largest line in the budget |
| Call the per-record size a ceiling | Identifiers have no maximum length and non-ASCII text is escaped; the figure is an assumption, and the documents say so |
| Trust the tool's exit status in the healthcheck | `kafka-broker-api-versions.sh` exits 0 while printing `-> ERROR`; only its output distinguishes a reachable broker |
| Shift the newest base event to the publish start and publish at once | On any base longer than the late threshold almost every fault-free record arrives late, and every lateness test built on it passes vacuously |
| Hold every late record back by wall-clock time | Needs waits longer than the late threshold for every late record; stamping gives the same arrival pattern for originals, and only duplicates -- whose content must not change -- are held back |
| Draw overlay faults from a seeded `random.Random` | Its algorithms are not a cross-version contract; SHA-256 selection is stable and needs no suppressed lint rule |
| Declare `<topic>.dlq` topics now | No non-Spark consumer exists; Spark rejects go to Delta quarantine. An empty contract nobody exercises is what ADR-0028 refuses |
| Use the testcontainers library for integration tests | The repository's pattern is throwaway containers through the docker CLI, which adds no dependency |

## Consequences

**Positive.**
- **Topics:** every topic on a local broker comes from one reviewed file, and drift, including
  settings nobody declared, is detected.
- **Publishing:** a publisher cannot write an unkeyed, CRC32-partitioned, untraced or schema-invalid
  record, cannot publish into a missing topic for minutes before failing, and cannot lose accepted
  records silently at close. The verdict accounts for exactly what was accepted.
- **Partitioning:** placement is predicted and asserted against the Java client.
- **Disk:** the local cap covers a derived bound with its assumptions written down.
- **Faults:** Silver and parity tests get a reproducible stream that contains the faults they must
  handle, arriving in the classes it claims, with a manifest of what was published.
- **Compose broker:** keeps its log on its volume, is reachable from containers, and its healthcheck can
  fail.

**Negative.**
- **Workflow:** topic creation is a separate step that must run before anything publishes.
- **Cost:** every published event is validated even when the caller already validated it, and
  publishes are serialised through one gate per publisher.
- **Retention and internal topics:** the retention pass runs every 5 s and deleted segments are removed
  after 10 s. Consumer offsets have 4 partitions instead of 50.
- **Transient bound:** holds only up to the declared ingress and assumed mean record size.
- **Replay duration:** a paced replay takes as long as its base's event-time span, and held-back late
  copies extend it by their delay. Late and future-dated originals have their event time moved, so
  parity tests must compare against the published records, never the base.
- **Cluster id:** every broker started from this image shares the image's default cluster id.
- **Mirrored constants:** the overlay mirrors two §4.3 constants until Step 6 lands them.
- **CI services:** the CI guard covers a missing Docker. Tests that find a service for themselves,
  such as the Redis and PostgreSQL suites, still skip loudly in CI when that service is absent.

**Risks.**
- **murmur2 port:** could diverge from a future Java client. It is pinned to recorded vectors and to
  live placement by librdkafka, so a divergence fails tests.
- **Design ingress:** could be exceeded in practice, for example by an unthrottled generator. The
  `budget` command shows the derivation, so the consequence can be computed.
- **Refusal counting:** `EventPublisher.publish` counts every exception, including a caller bug, as a
  refusal, which leaves a session unconfirmed. That errs in the safe direction.

## Status

Proposed
