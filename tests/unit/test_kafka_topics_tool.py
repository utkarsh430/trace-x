"""`scripts/kafka_topics.py`: the comparison logic, the environment guard, and its exits.

The comparisons are pure functions over a `LiveCluster`, so every refusal and every
kind of drift is checked here without a broker, each beside a clean control that
must report nothing. The same verdicts against a live broker -- and the unreachable
exit -- are in `tests/integration/test_kafka_platform.py`.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
DECLARATION = ROOT / "deploy" / "kafka" / "topics.yaml"


def _load_tool() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "kafka_topics_tool", ROOT / "scripts" / "kafka_topics.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["kafka_topics_tool"] = module
    spec.loader.exec_module(module)
    return module


kt = _load_tool()


@pytest.fixture
def declaration() -> Any:
    return kt.load_declaration(DECLARATION)


def _matching_cluster(declaration: Any) -> Any:
    """A live cluster exactly as the declaration describes it."""
    topics = {
        name: kt.LiveTopic(
            name=name,
            partitions=topic.partitions,
            replication_factor=topic.replication_factor,
            config=dict(topic.applied_config),
            topic_id=f"id-{name}",
            overridden=frozenset(topic.applied_config),
        )
        for name, topic in declaration.topics.items()
    }
    topics["__consumer_offsets"] = kt.LiveTopic(
        name="__consumer_offsets", partitions=4, replication_factor=1, config={}
    )
    return kt.LiveCluster(
        cluster_id="cluster-under-test",
        topics=topics,
        broker_configs={1: dict(declaration.broker)},
    )


def _with_topic(cluster: Any, name: str, **changes: Any) -> Any:
    topics = dict(cluster.topics)
    current = topics[name]
    fields = {
        "name": current.name,
        "partitions": current.partitions,
        "replication_factor": current.replication_factor,
        "config": dict(current.config),
        "topic_id": current.topic_id,
        "overridden": current.overridden,
        **changes,
    }
    topics[name] = kt.LiveTopic(**fields)
    return kt.LiveCluster(
        cluster_id=cluster.cluster_id, topics=topics, broker_configs=cluster.broker_configs
    )


def _last_summary(capsys: pytest.CaptureFixture[str]) -> dict[str, Any]:
    summary: dict[str, Any] = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    return summary


# ------------------------------------------------------------------- drift --


def test_a_matching_cluster_reports_nothing(declaration: Any) -> None:
    """The control every other test in this file is measured against."""
    cluster = _matching_cluster(declaration)
    assert kt.structural_conflicts(declaration, cluster) == []
    assert kt.drift(declaration, cluster) == []
    assert kt.cluster_refusals("local", cluster) == []


def test_a_different_partition_count_is_a_structural_conflict(declaration: Any) -> None:
    cluster = _with_topic(_matching_cluster(declaration), "tx.raw.v1", partitions=12)
    conflicts = kt.structural_conflicts(declaration, cluster)
    assert len(conflicts) == 1 and "tx.raw.v1" in conflicts[0] and "partition" in conflicts[0]
    assert conflicts[0] in kt.drift(declaration, cluster)


def test_a_different_cleanup_policy_is_a_structural_conflict(declaration: Any) -> None:
    base = _matching_cluster(declaration)
    config = {**base.topics["investigation.requested.v1"].config, "cleanup.policy": "compact"}
    cluster = _with_topic(base, "investigation.requested.v1", config=config)
    conflicts = kt.structural_conflicts(declaration, cluster)
    assert len(conflicts) == 1 and "cleanup.policy" in conflicts[0]


def test_a_missing_topic_is_drift_but_not_a_conflict(declaration: Any) -> None:
    cluster = _matching_cluster(declaration)
    topics = {n: t for n, t in cluster.topics.items() if n != "device.events.v1"}
    missing = kt.LiveCluster(cluster_id="c", topics=topics, broker_configs=cluster.broker_configs)
    assert kt.structural_conflicts(declaration, missing) == []
    assert any("device.events.v1" in d and "missing" in d for d in kt.drift(declaration, missing))


def test_a_changed_declared_setting_is_drift(declaration: Any) -> None:
    base = _matching_cluster(declaration)
    config = {**base.topics["tx.raw.v1"].config, "retention.bytes": "1048576"}
    problems = kt.drift(declaration, _with_topic(base, "tx.raw.v1", config=config))
    assert problems == ["tx.raw.v1: retention.bytes='1048576', declared '268435456'"]


def test_a_setting_made_on_the_broker_but_not_declared_is_drift(declaration: Any) -> None:
    """`verify` compares every live override, not only the declared keys.

    A topic-level `compression.type=uncompressed`, for one, would quietly change
    what a byte on disk means for the declared bound.
    """
    base = _matching_cluster(declaration)
    live = base.topics["tx.raw.v1"]
    cluster = _with_topic(
        base,
        "tx.raw.v1",
        config={**live.config, "compression.type": "uncompressed"},
        overridden=live.overridden | {"compression.type"},
    )
    problems = kt.drift(declaration, cluster)
    assert len(problems) == 1
    assert "compression.type" in problems[0] and "not declared" in problems[0]


def test_an_undeclared_topic_is_drift_and_internal_topics_are_not(declaration: Any) -> None:
    base = _matching_cluster(declaration)
    topics = {
        **base.topics,
        "tx.raw.v1.typo": kt.LiveTopic(
            name="tx.raw.v1.typo", partitions=1, replication_factor=1, config={}
        ),
    }
    cluster = kt.LiveCluster(cluster_id="c", topics=topics, broker_configs=base.broker_configs)
    problems = kt.drift(declaration, cluster)
    assert len(problems) == 1 and "tx.raw.v1.typo" in problems[0]


def test_a_broker_setting_the_bound_depends_on_is_checked(declaration: Any) -> None:
    base = _matching_cluster(declaration)
    broker = {**declaration.broker, "offsets.topic.num.partitions": "50"}
    cluster = kt.LiveCluster(cluster_id="c", topics=base.topics, broker_configs={1: broker})
    problems = kt.drift(declaration, cluster)
    assert len(problems) == 1 and "offsets.topic.num.partitions" in problems[0]


# ---------------------------------------------------------------- environment --


@pytest.mark.parametrize("bootstrap", ["kafka:19092", "10.0.0.7:9092", "localhost:9092,b1:9092"])
def test_a_non_loopback_bootstrap_is_refused(bootstrap: str) -> None:
    assert kt.bootstrap_refusals("local", bootstrap)
    assert kt.bootstrap_refusals("local", "localhost:9092") == []  # control


def test_a_refused_bootstrap_exits_two_without_connecting(
    capsys: pytest.CaptureFixture[str],
) -> None:
    for command in ("apply", "verify"):
        code = kt.main([command, "--environment", "local", "--bootstrap", "broker.example:9092"])
        summary = _last_summary(capsys)
        assert code == kt.EXIT_REFUSED
        assert summary["exit"] == kt.EXIT_REFUSED and summary["environment"] == "local"
        assert summary["cluster_id"] is None


def test_a_cluster_with_more_than_one_broker_is_refused(declaration: Any) -> None:
    base = _matching_cluster(declaration)
    two = kt.LiveCluster(
        cluster_id="c",
        topics=base.topics,
        broker_configs={1: dict(declaration.broker), 2: dict(declaration.broker)},
    )
    assert kt.cluster_refusals("local", two)


def test_the_environment_must_be_named() -> None:
    with pytest.raises(SystemExit):
        kt.main(["budget"])


# ---------------------------------------------------------------------- exits --


def test_an_invalid_declaration_exits_four(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    document = yaml.safe_load(DECLARATION.read_text())
    document["topics"]["tx.raw.v1"]["local"]["overrides"]["retention.ms"] = "1"
    path = tmp_path / "topics.yaml"
    path.write_text(yaml.safe_dump(document))
    assert kt.main(["verify", "--environment", "local", "--file", str(path)]) == kt.EXIT_INVALID
    out = capsys.readouterr().out.strip().splitlines()
    assert out[0].startswith("INVALID")
    assert json.loads(out[-1])["exit"] == kt.EXIT_INVALID


def test_a_budget_over_its_cap_exits_non_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert kt.main(["budget", "--environment", "local"]) == kt.EXIT_OK  # control
    capsys.readouterr()
    document = copy.deepcopy(yaml.safe_load(DECLARATION.read_text()))
    document["local_disk_budget"]["cap_bytes"] = 1024 * 1024
    path = tmp_path / "topics.yaml"
    path.write_text(yaml.safe_dump(document))
    assert kt.main(["budget", "--environment", "local", "--file", str(path)]) == kt.EXIT_INVALID
    assert _last_summary(capsys)["headroom_bytes"] < 0


def test_a_crash_exits_five_and_still_prints_its_summary(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A crash must never look like drift (exit 1)."""

    def explode(*args: Any, **kwargs: Any) -> int:
        raise RuntimeError("metadata for tx.raw.v1 carries an error")

    monkeypatch.setattr(kt, "run_verify", explode)
    assert kt.main(["verify", "--environment", "local"]) == kt.EXIT_ERROR == 5
    summary = _last_summary(capsys)
    assert summary["exit"] == 5 and "RuntimeError" in summary["error"]
