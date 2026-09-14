#!/usr/bin/env python3
"""Declared Kafka topics: create what is missing, verify what exists, never repartition.

`deploy/kafka/topics.yaml` is the only source of topic configuration. The broker
runs with `auto.create.topics.enable=false`, so a topic this tool did not create
does not exist, and a publisher that names one fails instead of writing into a
topic with broker-default partitions and timestamps.

Commands:

  apply    create every declared topic that is missing, then verify. If an
           EXISTING declared topic has a different partition count or cleanup
           policy it refuses loudly and creates nothing: changing a partition count
           moves keys to different partitions and silently reorders history, and
           a changed cleanup policy can erase it (docs/EVENT_CONTRACTS.md §3). It
           never alters an existing topic's configuration either; that drift is
           reported by the verify step it ends with.
  verify   compare every declared setting with the live broker, report any setting
           made on a declared topic that the declaration does not contain, and any
           topic on the broker that is not declared. Exit 1 on drift.
  budget   print the local disk bound derived from the declaration. No broker.

`--environment local` is required. The declaration's `local` section describes a
single laptop broker, so `apply` and `verify` refuse a bootstrap that is not a
loopback address and a cluster with more than one broker: applying local byte caps
to any other cluster would silently delete data there.

Every broker command prints the cluster id, and its JSON summary (the last line of
output) carries the environment and each declared topic's id. The topic id matters
more locally: this broker image gives every container the same default cluster id,
while a topic id changes whenever a topic is deleted and recreated.

Exit codes: 0 ok, 1 drift, 2 refused, 3 broker unreachable, 4 invalid declaration,
5 unexpected error.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import yaml

ROOT: Final = Path(__file__).resolve().parents[1]
DECLARATION: Final = ROOT / "deploy" / "kafka" / "topics.yaml"
# This repository's packages, not whichever checkout an editable install points at:
# run from a worktree, the script must use that worktree's trace_core.
if str(ROOT / "packages") not in sys.path:
    sys.path.insert(0, str(ROOT / "packages"))

from trace_core.contracts.publish import is_loopback_bootstrap  # noqa: E402

EXIT_OK: Final = 0
EXIT_DRIFT: Final = 1
EXIT_REFUSED: Final = 2
EXIT_UNREACHABLE: Final = 3
EXIT_INVALID: Final = 4
EXIT_ERROR: Final = 5

ENVIRONMENTS: Final = ("local",)

CONTRACT_KEYS: Final = frozenset({"cleanup.policy", "message.timestamp.type", "retention.ms"})
"""Every declared topic states exactly these, identically in every environment."""
OVERRIDE_KEYS: Final = frozenset({"retention.bytes", "segment.bytes"})
"""Local-development overrides (PHASE3_PLAN Q8). Disjoint from the contract by rule."""
INGRESS_KEYS: Final = frozenset({"events_per_s", "assumed_mean_record_bytes"})
BUDGET_BROKER_KEYS: Final = (
    "log.retention.check.interval.ms",
    "log.segment.delete.delay.ms",
    "metadata.log.segment.bytes",
    "metadata.max.retention.bytes",
    "offsets.topic.num.partitions",
    "offsets.topic.segment.bytes",
)
REQUIRED_BROKER: Final[Mapping[str, str]] = {"auto.create.topics.enable": "false"}

TIMESTAMP_TYPES: Final = frozenset({"CreateTime", "LogAppendTime"})
CLEANUP_POLICIES: Final = frozenset({"delete", "compact", "compact,delete", "delete,compact"})
DEDUP_ROOTS: Final = frozenset({"envelope", "payload"})
INTERNAL_PREFIX: Final = "_"


class DeclarationError(ValueError):
    """topics.yaml does not have the shape this tool enforces."""


# ------------------------------------------------------------ declaration ----


@dataclass(frozen=True)
class TopicDeclaration:
    name: str
    key_field: str
    dedup_identity: str
    config: Mapping[str, str]
    partitions: int
    replication_factor: int
    overrides: Mapping[str, str]
    design_events_per_s: int
    assumed_record_bytes: int
    cloud_partitions: int

    @property
    def applied_config(self) -> dict[str, str]:
        """What `apply` creates the topic with locally: contract plus local overrides."""
        return {**self.config, **self.overrides}

    @property
    def retention_bytes(self) -> int:
        return int(self.overrides["retention.bytes"])

    @property
    def segment_bytes(self) -> int:
        return int(self.overrides["segment.bytes"])


@dataclass(frozen=True)
class Reservation:
    reserved_for: str
    partitions: int
    retention_bytes: int
    segment_bytes: int
    design_events_per_s: int
    assumed_record_bytes: int


@dataclass(frozen=True)
class Declaration:
    broker: Mapping[str, str]
    topics: Mapping[str, TopicDeclaration]
    reservations: tuple[Reservation, ...]
    cap_bytes: int


def _exact_keys(node: Any, expected: set[str] | frozenset[str], where: str) -> dict[str, Any]:
    if not isinstance(node, dict):
        raise DeclarationError(f"{where}: expected a mapping, found {type(node).__name__}")
    keys = set(node)
    if keys != set(expected):
        missing = sorted(set(expected) - keys)
        extra = sorted(keys - set(expected))
        raise DeclarationError(f"{where}: missing keys {missing}, unexpected keys {extra}")
    return node


def _positive_int(value: Any, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise DeclarationError(f"{where}: expected a positive integer, found {value!r}")
    return value


def _config_value(value: Any, where: str) -> str:
    """Kafka configuration values are strings; YAML would silently coerce anything else."""
    if not isinstance(value, str) or not value:
        raise DeclarationError(
            f"{where}: expected a quoted string, found {value!r}. Kafka compares configuration "
            f"as strings, and an unquoted YAML value (true, 1e9, 010) may not survive intact."
        )
    return value


def _numeric_string(value: str, where: str) -> int:
    if not value.lstrip("-").isdigit():
        raise DeclarationError(f"{where}: expected an integer string, found {value!r}")
    return int(value)


def _ingress(node: Any, where: str) -> tuple[int, int]:
    ingress = _exact_keys(node, INGRESS_KEYS, where)
    return (
        _positive_int(ingress["events_per_s"], f"{where}.events_per_s"),
        _positive_int(ingress["assumed_mean_record_bytes"], f"{where}.assumed_mean_record_bytes"),
    )


def _topic(name: str, node: Any) -> TopicDeclaration:
    where = f"topics.{name}"
    body = _exact_keys(node, {"key_field", "dedup_identity", "config", "local", "cloud"}, where)
    config_node = _exact_keys(body["config"], CONTRACT_KEYS, f"{where}.config")
    config = {k: _config_value(v, f"{where}.config.{k}") for k, v in config_node.items()}
    if config["cleanup.policy"] not in CLEANUP_POLICIES:
        raise DeclarationError(f"{where}.config.cleanup.policy: {config['cleanup.policy']!r}")
    if config["message.timestamp.type"] not in TIMESTAMP_TYPES:
        raise DeclarationError(
            f"{where}.config.message.timestamp.type: {config['message.timestamp.type']!r}"
        )
    _numeric_string(config["retention.ms"], f"{where}.config.retention.ms")

    local = _exact_keys(
        body["local"],
        {"partitions", "replication_factor", "overrides", "design_ingress"},
        f"{where}.local",
    )
    overrides_node = local["overrides"]
    if isinstance(overrides_node, dict) and set(overrides_node) & CONTRACT_KEYS:
        raise DeclarationError(
            f"{where}.local.overrides redefines contract settings "
            f"{sorted(set(overrides_node) & CONTRACT_KEYS)}. Local overrides sit beside the "
            f"contract; they never replace it (PHASE3_PLAN Q8)."
        )
    overrides_node = _exact_keys(overrides_node, OVERRIDE_KEYS, f"{where}.local.overrides")
    overrides = {
        k: _config_value(v, f"{where}.local.overrides.{k}") for k, v in overrides_node.items()
    }
    retention_bytes = _numeric_string(overrides["retention.bytes"], f"{where} retention.bytes")
    segment_bytes = _numeric_string(overrides["segment.bytes"], f"{where} segment.bytes")
    if segment_bytes <= 0 or retention_bytes < segment_bytes:
        raise DeclarationError(
            f"{where}.local.overrides: retention.bytes ({retention_bytes}) must be at least one "
            f"segment ({segment_bytes}); Kafka deletes whole segments, so a smaller cap is "
            f"not a cap"
        )
    events_per_s, record_bytes = _ingress(local["design_ingress"], f"{where}.design_ingress")
    cloud = _exact_keys(body["cloud"], {"partitions"}, f"{where}.cloud")

    dedup = body["dedup_identity"]
    root, _, field = str(dedup).partition(".")
    if root not in DEDUP_ROOTS or not field or "." in field:
        raise DeclarationError(
            f"{where}.dedup_identity: {dedup!r} must name one field as envelope.<f> or payload.<f>"
        )
    key_field = body["key_field"]
    if not isinstance(key_field, str) or not key_field:
        raise DeclarationError(f"{where}.key_field: expected a field name, found {key_field!r}")

    return TopicDeclaration(
        name=name,
        key_field=key_field,
        dedup_identity=str(dedup),
        config=config,
        partitions=_positive_int(local["partitions"], f"{where}.local.partitions"),
        replication_factor=_positive_int(
            local["replication_factor"], f"{where}.local.replication_factor"
        ),
        overrides=overrides,
        design_events_per_s=events_per_s,
        assumed_record_bytes=record_bytes,
        cloud_partitions=_positive_int(cloud["partitions"], f"{where}.cloud.partitions"),
    )


def load_declaration(path: Path = DECLARATION) -> Declaration:
    """Parse and structurally validate topics.yaml. Raises `DeclarationError`."""
    document = yaml.safe_load(path.read_text())
    top = _exact_keys(
        document, {"declaration_version", "broker", "topics", "local_disk_budget"}, str(path)
    )
    if top["declaration_version"] != 1:
        raise DeclarationError(f"unsupported declaration_version {top['declaration_version']!r}")

    broker_node = top["broker"]
    if not isinstance(broker_node, dict):
        raise DeclarationError("broker: expected a mapping")
    broker = {k: _config_value(v, f"broker.{k}") for k, v in broker_node.items()}
    for key, value in REQUIRED_BROKER.items():
        if broker.get(key) != value:
            raise DeclarationError(f"broker.{key} must be declared as {value!r}")
    for key in BUDGET_BROKER_KEYS:
        if key not in broker:
            raise DeclarationError(f"broker.{key} must be declared: the disk bound depends on it")
        _numeric_string(broker[key], f"broker.{key}")

    topics_node = top["topics"]
    if not isinstance(topics_node, dict) or not topics_node:
        raise DeclarationError("topics: expected a non-empty mapping")
    topics = {str(name): _topic(str(name), node) for name, node in topics_node.items()}

    budget = _exact_keys(top["local_disk_budget"], {"cap_bytes", "reservations"}, "budget")
    reservations: list[Reservation] = []
    for index, node in enumerate(budget["reservations"] or []):
        where = f"local_disk_budget.reservations[{index}]"
        item = _exact_keys(
            node,
            {"reserved_for", "partitions", "retention_bytes", "segment_bytes", "design_ingress"},
            where,
        )
        if item["reserved_for"] in topics:
            raise DeclarationError(
                f"{where}: {item['reserved_for']} is declared AND reserved; remove the reservation"
            )
        events_per_s, record_bytes = _ingress(item["design_ingress"], f"{where}.design_ingress")
        reservations.append(
            Reservation(
                reserved_for=str(item["reserved_for"]),
                partitions=_positive_int(item["partitions"], f"{where}.partitions"),
                retention_bytes=_positive_int(item["retention_bytes"], f"{where}.retention"),
                segment_bytes=_positive_int(item["segment_bytes"], f"{where}.segment_bytes"),
                design_events_per_s=events_per_s,
                assumed_record_bytes=record_bytes,
            )
        )
    return Declaration(
        broker=broker,
        topics=topics,
        reservations=tuple(reservations),
        cap_bytes=_positive_int(budget["cap_bytes"], "local_disk_budget.cap_bytes"),
    )


# ----------------------------------------------------------------- budget ----


def partition_peak_bytes(retention_bytes: int, segment_bytes: int) -> int:
    """The steady part of one partition's on-disk peak: R + 2S (see topics.yaml)."""
    return retention_bytes + 2 * segment_bytes


def transient_window_s(broker: Mapping[str, str]) -> float:
    """2I + D, in seconds: how long ingress can accumulate beyond the steady bound."""
    check = int(broker["log.retention.check.interval.ms"])
    delete_delay = int(broker["log.segment.delete.delay.ms"])
    return (2 * check + delete_delay) / 1000


@dataclass(frozen=True)
class BudgetLine:
    name: str
    kind: str
    """topic | reservation | internal"""
    steady_bytes: int
    transient_bytes: int

    @property
    def total_bytes(self) -> int:
        return self.steady_bytes + self.transient_bytes


@dataclass(frozen=True)
class Budget:
    lines: tuple[BudgetLine, ...]
    window_s: float
    cap_bytes: int

    @property
    def total_bytes(self) -> int:
        return sum(line.total_bytes for line in self.lines)

    @property
    def headroom_bytes(self) -> int:
        return self.cap_bytes - self.total_bytes


def compute_budget(declaration: Declaration) -> Budget:
    broker = declaration.broker
    window = transient_window_s(broker)
    lines: list[BudgetLine] = []
    for topic in declaration.topics.values():
        lines.append(
            BudgetLine(
                name=topic.name,
                kind="topic",
                steady_bytes=topic.partitions
                * partition_peak_bytes(topic.retention_bytes, topic.segment_bytes),
                transient_bytes=int(
                    topic.design_events_per_s * topic.assumed_record_bytes * window
                ),
            )
        )
    for reservation in declaration.reservations:
        lines.append(
            BudgetLine(
                name=reservation.reserved_for,
                kind="reservation",
                steady_bytes=reservation.partitions
                * partition_peak_bytes(reservation.retention_bytes, reservation.segment_bytes),
                transient_bytes=int(
                    reservation.design_events_per_s * reservation.assumed_record_bytes * window
                ),
            )
        )
    # KRaft's metadata log: log and snapshots are trimmed together to
    # metadata.max.retention.bytes, and like any log it can hold up to two segments
    # beyond that between trims.
    lines.append(
        BudgetLine(
            name="__cluster_metadata",
            kind="internal",
            steady_bytes=partition_peak_bytes(
                int(broker["metadata.max.retention.bytes"]),
                int(broker["metadata.log.segment.bytes"]),
            ),
            transient_bytes=0,
        )
    )
    # Compacted consumer offsets: each partition holds its active segment and at most
    # one cleaned segment, provided the committed offsets themselves stay far below
    # one segment -- true for the handful of consumer groups a laptop runs.
    lines.append(
        BudgetLine(
            name="__consumer_offsets",
            kind="internal",
            steady_bytes=int(broker["offsets.topic.num.partitions"])
            * 2
            * int(broker["offsets.topic.segment.bytes"]),
            transient_bytes=0,
        )
    )
    return Budget(lines=tuple(lines), window_s=window, cap_bytes=declaration.cap_bytes)


# ------------------------------------------------------------ comparisons ----


@dataclass(frozen=True)
class LiveTopic:
    name: str
    partitions: int
    replication_factor: int
    config: Mapping[str, str]
    topic_id: str | None = None
    """Kafka's topic UUID. Unlike the cluster id -- which this image gives every broker --
    it changes when a topic is deleted and recreated, so it names one topic's history."""
    overridden: frozenset[str] = frozenset()
    """Settings made on the topic itself, as opposed to inherited broker defaults."""


@dataclass(frozen=True)
class LiveCluster:
    cluster_id: str | None
    topics: Mapping[str, LiveTopic]
    broker_configs: Mapping[int, Mapping[str, str]]


def bootstrap_refusals(environment: str, bootstrap: str) -> list[str]:
    """Reasons a command must not touch this bootstrap in this environment."""
    if environment not in ENVIRONMENTS:
        return [f"unknown environment {environment!r}; this declaration describes {ENVIRONMENTS}"]
    if not is_loopback_bootstrap(bootstrap):
        return [
            f"--environment {environment} describes a single laptop broker, but bootstrap "
            f"{bootstrap!r} is not a loopback address. Local byte caps applied to another "
            f"cluster would delete its data; refusing."
        ]
    return []


def cluster_refusals(environment: str, cluster: LiveCluster) -> list[str]:
    if len(cluster.broker_configs) != 1:
        return [
            f"--environment {environment} describes a single broker, but cluster "
            f"{cluster.cluster_id} reports {len(cluster.broker_configs)} broker(s); refusing."
        ]
    return []


def structural_conflicts(declaration: Declaration, cluster: LiveCluster) -> list[str]:
    """Existing declared topics whose partition count or cleanup policy differ."""
    conflicts: list[str] = []
    for name, topic in sorted(declaration.topics.items()):
        live = cluster.topics.get(name)
        if live is None:
            continue
        if live.partitions != topic.partitions:
            conflicts.append(
                f"{name}: has {live.partitions} partition(s), declared {topic.partitions}. A "
                f"partition count is never changed in place: that moves keys to different "
                f"partitions and silently reorders history (docs/EVENT_CONTRACTS.md §3). "
                f"Release a new topic version; on a disposable local broker only, delete the "
                f"topic and run apply again."
            )
        live_cleanup = live.config.get("cleanup.policy")
        if live_cleanup != topic.config["cleanup.policy"]:
            conflicts.append(
                f"{name}: cleanup.policy is {live_cleanup!r}, declared "
                f"{topic.config['cleanup.policy']!r}. Compaction keeps one record per key and "
                f"would erase history; this is never corrected in place."
            )
    return conflicts


def drift(declaration: Declaration, cluster: LiveCluster) -> list[str]:
    """Every difference between the declaration and the live cluster."""
    found = structural_conflicts(declaration, cluster)
    for name, topic in sorted(declaration.topics.items()):
        live = cluster.topics.get(name)
        if live is None:
            found.append(f"{name}: declared but missing from the broker")
            continue
        if live.replication_factor != topic.replication_factor:
            found.append(
                f"{name}: replication factor {live.replication_factor}, "
                f"declared {topic.replication_factor}"
            )
        for key, expected in sorted(topic.applied_config.items()):
            if key == "cleanup.policy":
                continue  # reported above, as structural
            actual = live.config.get(key)
            if actual != expected:
                found.append(f"{name}: {key}={actual!r}, declared {expected!r}")
        for key in sorted(live.overridden - set(topic.applied_config)):
            found.append(
                f"{name}: {key}={live.config.get(key)!r} is set on the topic but not declared in "
                f"topics.yaml; every topic-level setting must come from the declaration"
            )
    if not cluster.broker_configs:
        found.append("broker: no broker configuration could be read")
    for broker_id, config in sorted(cluster.broker_configs.items()):
        for key, expected in sorted(declaration.broker.items()):
            actual = config.get(key)
            if actual != expected:
                found.append(f"broker {broker_id}: {key}={actual!r}, declared {expected!r}")
    for name in sorted(cluster.topics):
        if name.startswith(INTERNAL_PREFIX) or name in declaration.topics:
            continue
        found.append(
            f"{name}: exists on the broker but is not declared in topics.yaml. With automatic "
            f"creation disabled, something created it deliberately and outside the declaration."
        )
    return found


# -------------------------------------------------------------------- I/O ----


def _admin(bootstrap: str) -> Any:
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 10_000})


def _entries(entries: Mapping[str, Any]) -> dict[str, str]:
    return {name: str(entry.value) for name, entry in entries.items() if entry.value is not None}


def _overridden(entries: Mapping[str, Any]) -> frozenset[str]:
    """Names whose value was set on the topic itself (source DYNAMIC_TOPIC_CONFIG)."""
    from confluent_kafka.admin import ConfigSource

    topic_level = ConfigSource.DYNAMIC_TOPIC_CONFIG.value
    return frozenset(
        name
        for name, entry in entries.items()
        if getattr(entry.source, "value", entry.source) == topic_level
    )


def fetch_cluster(admin: Any, *, timeout_s: float) -> LiveCluster:
    """Topics, their partition counts, replication, configuration and ids, and broker config."""
    from confluent_kafka import TopicCollection
    from confluent_kafka.admin import ConfigResource

    metadata = admin.list_topics(timeout=timeout_s)
    topic_meta = dict(metadata.topics)
    for name, meta in topic_meta.items():
        if meta.error is not None:
            raise RuntimeError(f"metadata for {name} carries an error: {meta.error}")

    configs: dict[str, dict[str, str]] = {}
    overridden: dict[str, frozenset[str]] = {}
    topic_ids: dict[str, str] = {}
    external = [name for name in topic_meta if not name.startswith(INTERNAL_PREFIX)]
    if external:
        resources = [ConfigResource(ConfigResource.Type.TOPIC, name) for name in external]
        futures = admin.describe_configs(resources, request_timeout=timeout_s)
        for resource, future in futures.items():
            entries = future.result(timeout=timeout_s)
            configs[resource.name] = _entries(entries)
            overridden[resource.name] = _overridden(entries)
        described = admin.describe_topics(TopicCollection(external), request_timeout=timeout_s)
        for name, future in described.items():
            topic_ids[name] = str(future.result(timeout=timeout_s).topic_id)

    topics = {
        name: LiveTopic(
            name=name,
            partitions=len(meta.partitions),
            replication_factor=min((len(p.replicas) for p in meta.partitions.values()), default=0),
            config=configs.get(name, {}),
            topic_id=topic_ids.get(name),
            overridden=overridden.get(name, frozenset()),
        )
        for name, meta in topic_meta.items()
    }

    broker_configs: dict[int, dict[str, str]] = {}
    for broker_id in sorted(metadata.brokers):
        resource = ConfigResource(ConfigResource.Type.BROKER, str(broker_id))
        future = admin.describe_configs([resource], request_timeout=timeout_s)[resource]
        broker_configs[broker_id] = _entries(future.result(timeout=timeout_s))

    return LiveCluster(cluster_id=metadata.cluster_id, topics=topics, broker_configs=broker_configs)


def create_missing(
    admin: Any, declaration: Declaration, cluster: LiveCluster, *, timeout_s: float
) -> list[str]:
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import NewTopic

    missing = [t for name, t in declaration.topics.items() if name not in cluster.topics]
    if not missing:
        return []
    futures = admin.create_topics(
        [
            NewTopic(
                t.name,
                num_partitions=t.partitions,
                replication_factor=t.replication_factor,
                config=t.applied_config,
            )
            for t in missing
        ],
        request_timeout=timeout_s,
        operation_timeout=timeout_s,
    )
    created: list[str] = []
    for name, future in futures.items():
        try:
            future.result(timeout=timeout_s)
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            if isinstance(error, KafkaError) and error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                continue  # created concurrently; the structure is re-checked afterwards
            raise
        created.append(name)
    return sorted(created)


def _wait_until_visible(admin: Any, declaration: Declaration, *, timeout_s: float) -> LiveCluster:
    """Topic creation is acknowledged before every partition has a leader in metadata."""
    deadline = time.monotonic() + timeout_s
    while True:
        cluster = fetch_cluster(admin, timeout_s=timeout_s)
        ready = all(
            name in cluster.topics and cluster.topics[name].partitions == topic.partitions
            for name, topic in declaration.topics.items()
        )
        if ready or time.monotonic() >= deadline:
            return cluster
        time.sleep(0.5)


Printer = Callable[[str], None]


def _summary(out: Printer, **fields: Any) -> None:
    out(json.dumps(fields, sort_keys=True))


def _declared_topic_ids(declaration: Declaration, cluster: LiveCluster) -> dict[str, str | None]:
    return {
        name: cluster.topics[name].topic_id
        for name in sorted(declaration.topics)
        if name in cluster.topics
    }


def _connect(
    command: str, environment: str, bootstrap: str, *, timeout_s: float, out: Printer
) -> tuple[Any, LiveCluster] | int:
    """Refuse or reach the broker. Returns (admin, cluster), or an exit code already reported."""
    from confluent_kafka import KafkaException

    refusals = bootstrap_refusals(environment, bootstrap)
    if refusals:
        for refusal in refusals:
            out(f"  REFUSED {refusal}")
        _summary(
            out,
            command=command,
            environment=environment,
            bootstrap=bootstrap,
            cluster_id=None,
            refused=refusals,
            exit=EXIT_REFUSED,
        )
        return EXIT_REFUSED
    admin = _admin(bootstrap)
    try:
        cluster = fetch_cluster(admin, timeout_s=timeout_s)
    except KafkaException as exc:
        out(f"UNREACHABLE {bootstrap}: {exc}")
        _summary(
            out,
            command=command,
            environment=environment,
            bootstrap=bootstrap,
            cluster_id=None,
            exit=EXIT_UNREACHABLE,
        )
        return EXIT_UNREACHABLE
    out(f"kafka-topics {command}: cluster_id={cluster.cluster_id} bootstrap={bootstrap}")
    refusals = cluster_refusals(environment, cluster)
    if refusals:
        for refusal in refusals:
            out(f"  REFUSED {refusal}")
        _summary(
            out,
            command=command,
            environment=environment,
            bootstrap=bootstrap,
            cluster_id=cluster.cluster_id,
            refused=refusals,
            exit=EXIT_REFUSED,
        )
        return EXIT_REFUSED
    return admin, cluster


def run_verify(
    bootstrap: str,
    declaration: Declaration,
    *,
    environment: str,
    timeout_s: float,
    out: Printer,
) -> int:
    connected = _connect("verify", environment, bootstrap, timeout_s=timeout_s, out=out)
    if isinstance(connected, int):
        return connected
    _, cluster = connected
    problems = drift(declaration, cluster)
    for problem in problems:
        out(f"  DRIFT {problem}")
    if not problems:
        out(f"  OK {len(declaration.topics)} declared topic(s) match the broker")
    code = EXIT_DRIFT if problems else EXIT_OK
    _summary(
        out,
        command="verify",
        environment=environment,
        bootstrap=bootstrap,
        cluster_id=cluster.cluster_id,
        topic_ids=_declared_topic_ids(declaration, cluster),
        drift=problems,
        exit=code,
    )
    return code


def run_apply(
    bootstrap: str,
    declaration: Declaration,
    *,
    environment: str,
    timeout_s: float,
    out: Printer,
) -> int:
    connected = _connect("apply", environment, bootstrap, timeout_s=timeout_s, out=out)
    if isinstance(connected, int):
        return connected
    admin, cluster = connected

    conflicts = structural_conflicts(declaration, cluster)
    if conflicts:
        for conflict in conflicts:
            out(f"  REFUSED {conflict}")
        out("  nothing was created: apply is all-or-nothing when the declaration conflicts")
        _summary(
            out,
            command="apply",
            environment=environment,
            bootstrap=bootstrap,
            cluster_id=cluster.cluster_id,
            created=[],
            refused=conflicts,
            exit=EXIT_REFUSED,
        )
        return EXIT_REFUSED

    created = create_missing(admin, declaration, cluster, timeout_s=timeout_s)
    for name in created:
        topic = declaration.topics[name]
        out(f"  CREATED {name} partitions={topic.partitions} config={topic.applied_config}")
    for name in sorted(set(declaration.topics) - set(created)):
        out(f"  EXISTS {name}")

    cluster = _wait_until_visible(admin, declaration, timeout_s=timeout_s)
    conflicts = structural_conflicts(declaration, cluster)
    problems = drift(declaration, cluster)
    for problem in problems:
        out(f"  DRIFT {problem}")
    code = EXIT_REFUSED if conflicts else (EXIT_DRIFT if problems else EXIT_OK)
    if code == EXIT_OK:
        out(f"  OK {len(declaration.topics)} declared topic(s) match the broker")
    _summary(
        out,
        command="apply",
        environment=environment,
        bootstrap=bootstrap,
        cluster_id=cluster.cluster_id,
        topic_ids=_declared_topic_ids(declaration, cluster),
        created=created,
        drift=problems,
        exit=code,
    )
    return code


def run_budget(declaration: Declaration, *, environment: str, out: Printer) -> int:
    budget = compute_budget(declaration)
    mib = 1024 * 1024
    out(f"local disk budget (window 2I+D = {budget.window_s:g} s)")
    for line in budget.lines:
        out(
            f"  {line.kind:<11} {line.name:<28} steady {line.steady_bytes / mib:9.1f} MiB   "
            f"transient {line.transient_bytes / mib:9.1f} MiB"
        )
    out(
        f"  total {budget.total_bytes / mib:.1f} MiB of cap {budget.cap_bytes / mib:.1f} MiB, "
        f"headroom {budget.headroom_bytes / mib:.1f} MiB"
    )
    code = EXIT_OK if budget.headroom_bytes >= 0 else EXIT_INVALID
    _summary(
        out,
        command="budget",
        environment=environment,
        cap_bytes=budget.cap_bytes,
        total_bytes=budget.total_bytes,
        headroom_bytes=budget.headroom_bytes,
        window_s=budget.window_s,
        exit=code,
    )
    return code


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0] if __doc__ else None)
    parser.add_argument("command", choices=("apply", "verify", "budget"))
    parser.add_argument("--environment", required=True, choices=ENVIRONMENTS)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--file", type=Path, default=DECLARATION)
    parser.add_argument("--timeout", type=float, default=20.0, help="seconds per admin request")
    args = parser.parse_args(argv)

    def out(line: str) -> None:
        print(line, flush=True)

    try:
        try:
            declaration = load_declaration(args.file)
        except (DeclarationError, OSError, yaml.YAMLError) as exc:
            out(f"INVALID {args.file}: {exc}")
            _summary(out, command=args.command, environment=args.environment, exit=EXIT_INVALID)
            return EXIT_INVALID

        if args.command == "budget":
            return run_budget(declaration, environment=args.environment, out=out)
        runner = run_verify if args.command == "verify" else run_apply
        return runner(
            args.bootstrap,
            declaration,
            environment=args.environment,
            timeout_s=args.timeout,
            out=out,
        )
    except Exception as exc:
        # A crash must not look like drift (exit 1): report it as its own outcome,
        # with the summary line every caller parses.
        out(f"ERROR {type(exc).__name__}: {exc}")
        _summary(
            out,
            command=args.command,
            environment=args.environment,
            error=f"{type(exc).__name__}: {exc}",
            exit=EXIT_ERROR,
        )
        return EXIT_ERROR


if __name__ == "__main__":
    sys.exit(main())
