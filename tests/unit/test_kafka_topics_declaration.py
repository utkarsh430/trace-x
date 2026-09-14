"""`deploy/kafka/topics.yaml` agrees with every other statement of the same facts.

The topic declaration restates things that are already written down elsewhere --
which topics are released and how they are keyed (RELEASED.json and
`trace_core.contracts.topics`), their partitions, retention and cleanup policy
(docs/EVENT_CONTRACTS.md §3), the dedup identities (PHASE3_PLAN §3 Q2), and the
broker settings its disk bound depends on (deploy/compose.yml). Two copies of a
fact drift; these tests diff them in both directions, and each carries a
negative control showing the comparison can fail.

The broker-level half -- that Kafka actually accepts every key, and that apply
and verify behave -- is `tests/integration/test_kafka_platform.py`.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import re
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

from trace_core.contracts.canonical_json import canonical_bytes
from trace_core.contracts.topics import PARTITION_KEY_FIELD

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
DECLARATION = ROOT / "deploy" / "kafka" / "topics.yaml"
LEDGER = ROOT / "docs" / "contracts" / "RELEASED.json"
EVENT_CONTRACTS = ROOT / "docs" / "EVENT_CONTRACTS.md"
SCHEMAS = ROOT / "docs" / "contracts" / "events"
COMPOSE = ROOT / "deploy" / "compose.yml"
JARS_LOCK = ROOT / "packages" / "trace_core" / "stream" / "jars.lock"

MIB = 1024 * 1024
GIB = 1024 * MIB

KAFKA_3_9_TOPIC_CONFIGS = frozenset(
    {
        # Every topic-level configuration name a live apache/kafka:3.9.1 broker
        # returned from describe_configs on a topic (2026-09-13). The integration
        # test re-derives the accepted set from a running broker, so this literal
        # cannot quietly become the only evidence.
        "cleanup.policy",
        "compression.gzip.level",
        "compression.lz4.level",
        "compression.type",
        "compression.zstd.level",
        "delete.retention.ms",
        "file.delete.delay.ms",
        "flush.messages",
        "flush.ms",
        "follower.replication.throttled.replicas",
        "index.interval.bytes",
        "leader.replication.throttled.replicas",
        "local.retention.bytes",
        "local.retention.ms",
        "max.compaction.lag.ms",
        "max.message.bytes",
        "message.downconversion.enable",
        "message.format.version",
        "message.timestamp.after.max.ms",
        "message.timestamp.before.max.ms",
        "message.timestamp.difference.max.ms",
        "message.timestamp.type",
        "min.cleanable.dirty.ratio",
        "min.compaction.lag.ms",
        "min.insync.replicas",
        "preallocate",
        "remote.log.copy.disable",
        "remote.log.delete.on.disable",
        "remote.storage.enable",
        "retention.bytes",
        "retention.ms",
        "segment.bytes",
        "segment.index.bytes",
        "segment.jitter.ms",
        "segment.ms",
        "unclean.leader.election.enable",
    }
)

APPROVED_DEDUP_IDENTITY = {
    # docs/PHASE3_PLAN.md §3 Q2 as approved, written out by hand: the independent
    # oracle. topics.yaml and eval/replay/faults.py both restate it, and a wrong
    # identity copied into both would otherwise agree with itself.
    "tx.raw.v1": "payload.transaction_id",
    "identity.events.v1": "envelope.event_id",
    "device.events.v1": "envelope.event_id",
    "investigation.requested.v1": "envelope.idempotency_key",
}

CONTRACTS_ROW = re.compile(
    r"^\| `(?P<topic>[a-z0-9.]+)` \| `(?P<key>[a-z_]+)` \| (?P<local>\d+) / (?P<cloud>\d+) "
    r"\| (?P<days>\d+) d \| (?P<cleanup>[a-z,]+) \|"
)


def _load_tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "kafka_topics", ROOT / "scripts" / "kafka_topics.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Registered before execution: dataclasses resolve their module by name.
    sys.modules["kafka_topics"] = module
    spec.loader.exec_module(module)
    return module


kt = _load_tool()


@pytest.fixture(scope="module")
def declaration() -> Any:
    return kt.load_declaration(DECLARATION)


@pytest.fixture(scope="module")
def ledger() -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(LEDGER.read_text())
    return loaded


@pytest.fixture(scope="module")
def raw() -> dict[str, Any]:
    loaded: dict[str, Any] = yaml.safe_load(DECLARATION.read_text())
    return loaded


def _contracts_rows() -> dict[str, dict[str, str]]:
    rows = {}
    for line in EVENT_CONTRACTS.read_text().splitlines():
        match = CONTRACTS_ROW.match(line)
        if match:
            rows[match["topic"]] = match.groupdict()
    return rows


def _write(tmp_path: Path, document: dict[str, Any]) -> Path:
    path = tmp_path / "topics.yaml"
    path.write_text(yaml.safe_dump(document, sort_keys=False))
    return path


# ------------------------------------------------------------ what is declared --


def test_exactly_the_released_topics_are_declared(declaration: Any, ledger: Any) -> None:
    released = {entry["topic"] for entry in ledger["released"]}
    planned = {entry["topic"] for entry in ledger["planned"]}
    assert released, "the ledger releases nothing; the comparison would pass vacuously"
    assert set(declaration.topics) == released, (
        f"topics.yaml declares {sorted(declaration.topics)} but RELEASED.json releases "
        f"{sorted(released)}. A topic is declared in the step that releases its schema."
    )
    assert not set(declaration.topics) & planned, "a PLANNED topic is declared for creation"


def test_no_dead_letter_topic_is_declared(declaration: Any) -> None:
    """Spark rejects go to a Delta quarantine table; DLQ topics wait for a non-Spark consumer."""
    assert not [name for name in declaration.topics if name.endswith(".dlq")]


def test_every_key_matches_the_ledger_and_the_code(declaration: Any, ledger: Any) -> None:
    ledger_keys = {entry["topic"]: entry["key"] for entry in ledger["released"]}
    for name, topic in declaration.topics.items():
        assert topic.key_field == PARTITION_KEY_FIELD[name] == ledger_keys[name], (
            f"{name}: topics.yaml keys on {topic.key_field!r}, trace_core.contracts.topics on "
            f"{PARTITION_KEY_FIELD[name]!r}, the ledger on {ledger_keys[name]!r}"
        )


def test_the_dedup_identities_are_the_approved_ones(declaration: Any) -> None:
    declared = {name: topic.dedup_identity for name, topic in declaration.topics.items()}
    assert declared == APPROVED_DEDUP_IDENTITY


def test_every_dedup_identity_is_a_required_field_of_its_schema(declaration: Any) -> None:
    """An identity that may be absent cannot deduplicate the events that lack it."""
    envelope = json.loads((SCHEMAS / "envelope.v1.json").read_text())
    for name, topic in declaration.topics.items():
        section, field = topic.dedup_identity.split(".")
        if section == "envelope":
            required = envelope["required"]
        else:
            schema = json.loads((SCHEMAS / f"{name}.json").read_text())
            required = schema["properties"]["payload"]["required"]
        assert field in required, f"{name}: {topic.dedup_identity} is not a required field"


def test_partitions_retention_and_cleanup_match_the_event_contracts(
    declaration: Any, ledger: Any
) -> None:
    rows = _contracts_rows()
    ledger_entries = {entry["topic"]: entry for entry in ledger["released"]}
    assert set(declaration.topics) <= set(rows), "the §3 table parse found no row for a topic"
    for name, topic in declaration.topics.items():
        row = rows[name]
        assert topic.partitions == int(row["local"]), f"{name}: local partitions"
        assert topic.cloud_partitions == int(row["cloud"]), f"{name}: cloud partitions"
        assert int(topic.config["retention.ms"]) == int(row["days"]) * 86_400_000, (
            f"{name}: retention.ms {topic.config['retention.ms']} is not the {row['days']} d "
            f"conceptual retention of EVENT_CONTRACTS §3 -- local caps belong in overrides"
        )
        assert topic.config["cleanup.policy"] == row["cleanup"] == ledger_entries[name]["cleanup"]
        assert ledger_entries[name]["retention"] == f"{row['days']}d"


def test_the_contracts_table_parse_is_not_vacuous() -> None:
    rows = _contracts_rows()
    assert "tx.raw.v1" in rows and rows["tx.raw.v1"]["local"] == "6"
    assert CONTRACTS_ROW.match("| `x.v1` | `k` | 6 / 24 | 7 d | compact | PLANNED |")
    assert not CONTRACTS_ROW.match("| `x.v1` | `k` | 6 | 7 d | delete |")


def test_every_topic_is_delete_and_log_append_time(declaration: Any) -> None:
    for name, topic in declaration.topics.items():
        assert topic.config["cleanup.policy"] == "delete", name
        assert topic.config["message.timestamp.type"] == "LogAppendTime", (
            f"{name}: Q3 requires the broker to stamp arrival time; lateness is measured on it"
        )


def test_every_declared_config_key_is_one_kafka_accepts(declaration: Any) -> None:
    for name, topic in declaration.topics.items():
        unknown = set(topic.applied_config) - KAFKA_3_9_TOPIC_CONFIGS
        assert not unknown, f"{name}: {sorted(unknown)} are not Kafka 3.9 topic configurations"
    # Negative control: the set really does exclude near-misses.
    assert "retention.byte" not in KAFKA_3_9_TOPIC_CONFIGS
    assert "log.retention.bytes" not in KAFKA_3_9_TOPIC_CONFIGS  # a broker name, not a topic one


# ----------------------------------------------------------- local overrides --


def test_local_overrides_never_redefine_the_contract(declaration: Any) -> None:
    for name, topic in declaration.topics.items():
        assert not set(topic.overrides) & set(topic.config), name
        assert set(topic.overrides) == {"retention.bytes", "segment.bytes"}, name


def test_the_loader_refuses_an_override_of_contract_retention(
    raw: dict[str, Any], tmp_path: Path
) -> None:
    document = copy.deepcopy(raw)
    document["topics"]["tx.raw.v1"]["local"]["overrides"]["retention.ms"] = "3600000"
    with pytest.raises(kt.DeclarationError, match="redefines contract settings"):
        kt.load_declaration(_write(tmp_path, document))


def test_the_loader_refuses_unknown_and_unquoted_settings(
    raw: dict[str, Any], tmp_path: Path
) -> None:
    unknown = copy.deepcopy(raw)
    unknown["topics"]["tx.raw.v1"]["config"]["retention.byte"] = "1"
    with pytest.raises(kt.DeclarationError, match="unexpected keys"):
        kt.load_declaration(_write(tmp_path, unknown))

    unquoted = copy.deepcopy(raw)
    unquoted["topics"]["tx.raw.v1"]["config"]["retention.ms"] = 604_800_000
    with pytest.raises(kt.DeclarationError, match="quoted string"):
        kt.load_declaration(_write(tmp_path, unquoted))

    auto_create = copy.deepcopy(raw)
    auto_create["broker"]["auto.create.topics.enable"] = "true"
    with pytest.raises(kt.DeclarationError, match=r"auto\.create\.topics\.enable"):
        kt.load_declaration(_write(tmp_path, auto_create))


# -------------------------------------------------------------------- budget --


def _independent_total(document: dict[str, Any]) -> int:
    """The bound recomputed from the raw YAML, without the tool's code."""
    broker = document["broker"]
    window_s = (
        2 * int(broker["log.retention.check.interval.ms"])
        + int(broker["log.segment.delete.delay.ms"])
    ) / 1000
    total = 0
    sized = [
        (
            topic["local"]["partitions"],
            int(topic["local"]["overrides"]["retention.bytes"]),
            int(topic["local"]["overrides"]["segment.bytes"]),
            topic["local"]["design_ingress"],
        )
        for topic in document["topics"].values()
    ] + [
        (r["partitions"], r["retention_bytes"], r["segment_bytes"], r["design_ingress"])
        for r in document["local_disk_budget"]["reservations"]
    ]
    for partitions, retention, segment, ingress in sized:
        total += partitions * (retention + 2 * segment)
        total += int(ingress["events_per_s"] * ingress["assumed_mean_record_bytes"] * window_s)
    total += int(broker["metadata.max.retention.bytes"]) + 2 * int(
        broker["metadata.log.segment.bytes"]
    )
    total += (
        int(broker["offsets.topic.num.partitions"]) * 2 * int(broker["offsets.topic.segment.bytes"])
    )
    return total


def test_the_local_disk_bound_stays_within_the_declared_cap(
    declaration: Any, raw: dict[str, Any]
) -> None:
    budget = kt.compute_budget(declaration)
    assert budget.total_bytes == _independent_total(raw), (
        "the tool's budget and an independent recomputation from the YAML disagree"
    )
    assert budget.total_bytes <= declaration.cap_bytes, (
        f"the declared topics, reservations and internal topics need "
        f"{budget.total_bytes / MIB:.0f} MiB at peak, over the {declaration.cap_bytes / MIB:.0f} "
        f"MiB cap"
    )
    assert declaration.cap_bytes <= 5.5 * GIB, "PHASE3_PLAN's local budget is about 5.5 GiB"


def test_the_cap_check_can_fail(raw: dict[str, Any], tmp_path: Path) -> None:
    """Negative control: a cap that no longer covers the bound is detected."""
    document = copy.deepcopy(raw)
    document["topics"]["tx.raw.v1"]["local"]["overrides"]["retention.bytes"] = str(1024 * MIB)
    budget = kt.compute_budget(kt.load_declaration(_write(tmp_path, document)))
    assert budget.total_bytes > budget.cap_bytes


def test_every_consumer_of_disk_is_budgeted(declaration: Any) -> None:
    lines = {line.name: line for line in kt.compute_budget(declaration).lines}
    assert set(declaration.topics) <= set(lines)
    assert {"__cluster_metadata", "__consumer_offsets"} <= set(lines)
    for reservation in declaration.reservations:
        assert lines[reservation.reserved_for].transient_bytes > 0, (
            f"the {reservation.reserved_for} reservation has no transient term"
        )
    for name in declaration.topics:
        assert lines[name].transient_bytes > 0, name


def test_the_bound_is_not_just_the_byte_cap(declaration: Any) -> None:
    """Kafka deletes whole segments on a timer: the bound must exceed partitions x retention."""
    budget = kt.compute_budget(declaration)
    naive = sum(t.partitions * t.retention_bytes for t in declaration.topics.values()) + sum(
        r.partitions * r.retention_bytes for r in declaration.reservations
    )
    assert budget.total_bytes > naive
    broker = declaration.broker
    assert budget.window_s == pytest.approx(
        (
            2 * int(broker["log.retention.check.interval.ms"])
            + int(broker["log.segment.delete.delay.ms"])
        )
        / 1000
    )


def test_the_assumed_record_size_covers_a_generator_shaped_record_at_its_ascii_maximum(
    declaration: Any,
) -> None:
    """The size is an assumption, not a ceiling -- but it must cover the obvious case.

    A transaction with generator-shaped identifiers and every optional
    attacker-controlled string at its maximum length in ASCII. Non-ASCII text is
    escaped to several bytes per character and identifiers have no maximum length,
    which is why topics.yaml calls the figure an assumption.
    """
    envelope = {
        "event_id": "01900000-0000-7000-8000-000000000000",
        "event_type": "tx.raw",
        "schema_version": 1,
        "occurred_at": "2026-09-13T12:00:00.123456Z",
        "ingested_at": "2026-09-13T12:00:00.223456Z",
        "producer": "data-generator@1.0.0",
        "trace_id": "0" * 32,
        "correlation_id": "corr_" + "0" * 59,
        "idempotency_key": "sha256:" + "0" * 64,
    }
    payload = {
        "transaction_id": "tx_" + "0" * 12,
        "account_id": "acct_" + "0" * 9,
        "card_id": "card_" + "0" * 9,
        "device_id": "dev_" + "0" * 9,
        "merchant_id": "mrch_" + "0" * 6,
        "ip_id": "ip_" + "0" * 7,
        "amount_minor": -100_000_000_000,
        "currency": "GBP",
        "channel": "CARD_NOT_PRESENT",
        "entry_mode": "CONTACTLESS",
        "merchant_mcc": "5411",
        "merchant_country": "GB",
        "merchant_name": "m" * 128,
        "latitude": -89.123456789012,
        "longitude": -179.123456789012,
        "user_agent": "u" * 256,
        "memo": "n" * 256,
        "authorization_outcome": "APPROVED",
    }
    size = len(canonical_bytes({"envelope": envelope, "payload": payload}))
    assert size <= declaration.topics["tx.raw.v1"].assumed_record_bytes, size


def test_tx_topics_respect_the_planned_per_partition_cap(declaration: Any) -> None:
    assert declaration.topics["tx.raw.v1"].retention_bytes <= 256 * MIB
    for reservation in declaration.reservations:
        assert reservation.retention_bytes <= 256 * MIB, reservation.reserved_for


def test_reservations_are_for_planned_topics_only(declaration: Any, ledger: Any) -> None:
    """A tripwire for the step that releases a reserved topic: the reservation must go."""
    planned = {entry["topic"] for entry in ledger["planned"]}
    released = {entry["topic"] for entry in ledger["released"]}
    for reservation in declaration.reservations:
        assert reservation.reserved_for in planned, reservation.reserved_for
        assert reservation.reserved_for not in released, (
            f"{reservation.reserved_for} is RELEASED: replace its reservation with a declaration"
        )


def test_the_budget_command_reports_within_cap(capsys: pytest.CaptureFixture[str]) -> None:
    assert kt.main(["budget", "--environment", "local"]) == kt.EXIT_OK
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["environment"] == "local" and summary["headroom_bytes"] >= 0


# ------------------------------------------------------------------- broker --


def _kafka_env() -> dict[str, str]:
    service = yaml.safe_load(COMPOSE.read_text())["services"]["kafka"]
    return {key: str(value) for key, value in service["environment"].items()}


def _property_name(variable: str) -> str:
    return variable.removeprefix("KAFKA_").lower().replace("_", ".")


def test_compose_sets_every_broker_setting_the_declaration_depends_on(declaration: Any) -> None:
    env = {_property_name(k): v for k, v in _kafka_env().items() if k.startswith("KAFKA_")}
    for key, expected in declaration.broker.items():
        assert env.get(key) == expected, (
            f"topics.yaml depends on broker {key}={expected!r}; compose sets {env.get(key)!r}"
        )


def test_the_broker_log_lives_on_the_volume() -> None:
    """Regression: with KAFKA_* set, the image put the log in /tmp, off the volume."""
    service = yaml.safe_load(COMPOSE.read_text())["services"]["kafka"]
    mounts = [str(volume).split(":", 1)[1] for volume in service["volumes"]]
    assert _kafka_env()["KAFKA_LOG_DIRS"] in mounts


def test_the_healthcheck_cannot_pass_on_an_unreachable_advertised_broker() -> None:
    """kafka-broker-api-versions.sh exits 0 while printing '-> ERROR'; the check must not."""
    test = yaml.safe_load(COMPOSE.read_text())["services"]["kafka"]["healthcheck"]["test"]
    command = " ".join(str(part) for part in test)
    assert "kafka:19092" in command
    assert "-> ERROR" in command and "exit 1" in command


def test_the_broker_image_is_pinned_to_the_client_spark_builds_against() -> None:
    image = yaml.safe_load(COMPOSE.read_text())["services"]["kafka"]["image"]
    match = re.fullmatch(r"apache/kafka:(\d+\.\d+\.\d+)@sha256:[0-9a-f]{64}", image)
    assert match, f"kafka image {image!r} is not pinned by tag and digest"
    lock = JARS_LOCK.read_text()
    client = re.search(r"org\.apache\.kafka:kafka-clients:(\d+\.\d+\.\d+)", lock)
    assert client, "jars.lock names no kafka-clients coordinate"
    assert match.group(1) == client.group(1), (
        f"broker {match.group(1)} but Spark's connector ships kafka-clients {client.group(1)}"
    )
