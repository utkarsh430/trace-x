"""Bronze without a JVM: the registry, the row, the start position and the topic identity (Step 5).

Everything here is decided before a query starts, so it is tested as pure logic. What Spark and the
broker actually do with these decisions is `tests/stream/test_bronze_tables.py` and
`tests/integration/test_bronze_kafka.py`.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from trace_core.domain.errors import (
    CheckpointRefusedError,
    LakeContractError,
    StreamingSourceRetentionError,
    UnreleasedTopicError,
)
from trace_core.stream import bronze
from trace_core.stream.bronze import (
    BRONZE_COLUMNS,
    BRONZE_JOB,
    BRONZE_TOPICS,
    TOPIC_IDENTITY_FILENAME,
    TopicIdentity,
    Trigger,
    batch_progress,
    bronze_declaration,
    bronze_schema,
    bronze_sink,
    bronze_table_name,
    bronze_topic,
    kafka_reader_options,
    read_topic_identity,
    released_topics,
    require_never_latest,
    require_topic_identity,
)
from trace_core.stream.checkpoints import KafkaSourceStart, SparkProgress
from trace_core.stream.lake import IDENTIFIER, Tier
from trace_core.stream.tables import BASELINE_FEATURES, protocol_ceiling

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
RELEASED = {
    entry["topic"]
    for entry in json.loads((ROOT / "docs" / "contracts" / "RELEASED.json").read_text())["released"]
}
TOPIC = "tx.raw.v1"
RECORDED = {"startingOffsets": "earliest", "failOnDataLoss": "true"}


# ------------------------------------------------------------------ registry ---


def test_every_released_topic_has_exactly_one_bronze_table_and_one_query() -> None:
    assert set(BRONZE_TOPICS) == RELEASED, "Bronze ingests every released topic and nothing else"
    tables = [spec.table for spec in BRONZE_TOPICS.values()]
    queries = [spec.query for spec in BRONZE_TOPICS.values()]
    assert len(set(tables)) == len(tables) and len(set(queries)) == len(queries)
    for topic, spec in BRONZE_TOPICS.items():
        assert spec.topic == topic
        assert spec.table.tier is Tier.BRONZE
        assert spec.table.name == topic.replace(".", "_") == bronze_table_name(topic)
        assert spec.query == f"{BRONZE_JOB}_{spec.table.name}"
        assert IDENTIFIER.fullmatch(spec.table.name) and IDENTIFIER.fullmatch(spec.query)
    assert BRONZE_TOPICS[TOPIC].query == "bronze_ingest_tx_raw_v1"


def test_a_topic_that_is_not_released_has_no_bronze_table() -> None:
    with pytest.raises(UnreleasedTopicError):
        bronze_topic("not.released.v1")
    with pytest.raises(UnreleasedTopicError):
        released_topics(["tx.raw.v1", "not.released.v1"])
    assert released_topics(None) == tuple(BRONZE_TOPICS)
    assert released_topics(["tx.scored.v1"]) == ("tx.scored.v1",)


# ----------------------------------------------------------------------- row ---


def test_the_bronze_row_keeps_raw_bytes_every_header_and_the_transport_metadata() -> None:
    schema = bronze_schema()
    assert tuple(schema.fieldNames()) == BRONZE_COLUMNS
    fields = {field.name: field for field in schema.fields}
    assert fields["kafka_key"].dataType.simpleString() == "binary"
    assert fields["kafka_value"].dataType.simpleString() == "binary"
    # An array, not a map: Kafka allows a header name twice, and order is part of what arrived.
    assert fields["kafka_headers"].dataType.jsonValue() == {
        "type": "array",
        "containsNull": True,
        "elementType": {
            "type": "struct",
            "fields": [
                {"name": "key", "type": "string", "nullable": True, "metadata": {}},
                {"name": "value", "type": "binary", "nullable": True, "metadata": {}},
            ],
        },
    }
    nullable = {field.name for field in schema.fields if field.nullable}
    assert nullable == {"kafka_key", "kafka_value", "kafka_headers"}, (
        "a tombstone has no value, a record may have no key or headers; nothing else may be absent"
    )


def test_the_declaration_is_append_only_at_the_minimum_protocol() -> None:
    declaration = bronze_declaration(TOPIC)
    assert declaration.ref == BRONZE_TOPICS[TOPIC].table
    assert dict(declaration.properties) == {"delta.appendOnly": "true"}
    assert declaration.check_constraints == ()
    assert declaration.layout.partition_columns == () == declaration.layout.clustering_columns
    assert declaration.required_features() == {"invariants"}
    assert declaration.allowed_features() == BASELINE_FEATURES
    assert protocol_ceiling(declaration.allowed_features()) == (1, 2)


# ------------------------------------------------------------ start position ---


@pytest.mark.parametrize(
    "starting",
    [
        None,
        "latest",
        " LATEST ",
        '{"tx.raw.v1": {"0": -1}}',
        '{"tx.raw.v1": {"0": 5, "1": -1}}',
        '{"tx.raw.v1": {"0": -3}}',
        '{"tx.raw.v1": {"0": 1.5}}',
        '{"tx.raw.v1": {"0": true}}',
        '{"tx.raw.v1": {"zero": 0}}',
        '{"tx.raw.v1": {}}',
        '{"tx.scored.v1": {"0": 0}}',
        '{"tx.raw.v1": {"0": 0}, "tx.scored.v1": {"0": 0}}',
        "not json",
    ],
)
def test_a_start_position_that_is_or_may_be_latest_is_refused(starting: str | None) -> None:
    with pytest.raises(CheckpointRefusedError):
        require_never_latest(TOPIC, starting)


@pytest.mark.parametrize("starting", ["earliest", '{"tx.raw.v1": {"0": -2, "1": 17, "2": 0}}'])
def test_earliest_and_explicit_non_latest_offsets_are_accepted(starting: str) -> None:
    assert require_never_latest(TOPIC, starting) == starting


@pytest.mark.parametrize(
    "offsets",
    ['{"tx.raw.v1": {"0": -1}}', '{"tx.raw.v1": {"0": 5, "1": -1}}', '{"tx.raw.v1": {"0": -3}}'],
)
def test_the_checkpoint_convention_itself_refuses_a_latest_or_invalid_explicit_offset(
    offsets: str,
) -> None:
    """Spark spells latest -1 inside explicit offsets. The shared convention refuses it at
    construction, so no Kafka checkpoint can start there; Bronze's own refusal stays as defence."""
    with pytest.raises(CheckpointRefusedError):
        KafkaSourceStart(TOPIC, offsets)


def test_the_checkpoint_convention_accepts_earliest_and_real_offsets() -> None:
    start = KafkaSourceStart(TOPIC, '{"tx.raw.v1": {"0": -2, "1": 17, "2": 0}}')
    assert require_never_latest(TOPIC, start.start) == start.start


def test_the_reader_options_are_fixed_and_always_fail_on_data_loss() -> None:
    options = kafka_reader_options(TOPIC, RECORDED, bootstrap_servers=" 127.0.0.1:9092 ")
    assert options == {
        "kafka.bootstrap.servers": "127.0.0.1:9092",
        "subscribe": TOPIC,
        "startingOffsets": "earliest",
        "failOnDataLoss": "true",
        "includeHeaders": "true",
    }
    bounded = kafka_reader_options(
        TOPIC, RECORDED, bootstrap_servers="127.0.0.1:9092", max_offsets_per_trigger=500
    )
    assert bounded["maxOffsetsPerTrigger"] == "500"


@pytest.mark.parametrize(
    ("recorded", "bootstrap", "max_offsets", "error"),
    [
        ({"startingOffsets": "earliest"}, "b:1", None, StreamingSourceRetentionError),
        ({"startingOffsets": "earliest", "failOnDataLoss": "false"}, "b:1", None,
         StreamingSourceRetentionError),
        ({"failOnDataLoss": "true"}, "b:1", None, CheckpointRefusedError),
        ({"startingOffsets": "latest", "failOnDataLoss": "true"}, "b:1", None,
         CheckpointRefusedError),
        (RECORDED, "  ", None, LakeContractError),
        (RECORDED, "b:1", 0, ValueError),
    ],
)  # fmt: skip
def test_reader_options_are_refused_when_they_could_skip_data(
    recorded: dict[str, str], bootstrap: str, max_offsets: int | None, error: type[Exception]
) -> None:
    with pytest.raises(error):
        kafka_reader_options(
            TOPIC, recorded, bootstrap_servers=bootstrap, max_offsets_per_trigger=max_offsets
        )


# ------------------------------------------------------------ topic identity ---


def test_a_new_checkpoint_records_its_topic_id_and_a_resume_must_match_it(tmp_path: Path) -> None:
    current = TopicIdentity(TOPIC, "q1Z8-topic-id-one")
    assert require_topic_identity(tmp_path, current, SparkProgress()) is True
    assert json.loads((tmp_path / TOPIC_IDENTITY_FILENAME).read_text()) == {
        "format": 1,
        "topic": TOPIC,
        "topic_id": "q1Z8-topic-id-one",
    }
    planned = SparkProgress(planned=frozenset({0, 1}), committed=frozenset({0}))
    assert require_topic_identity(tmp_path, current, planned) is False
    with pytest.raises(CheckpointRefusedError, match="deleted and recreated"):
        require_topic_identity(tmp_path, TopicIdentity(TOPIC, "another-id"), planned)
    with pytest.raises(CheckpointRefusedError, match="deleted and recreated"):
        require_topic_identity(
            tmp_path, TopicIdentity("tx.scored.v1", "q1Z8-topic-id-one"), planned
        )
    assert [p.name for p in tmp_path.iterdir()] == [TOPIC_IDENTITY_FILENAME], "no staging left"


def test_a_checkpoint_that_planned_batches_without_a_topic_id_is_refused(tmp_path: Path) -> None:
    with pytest.raises(CheckpointRefusedError, match="nothing proves"):
        require_topic_identity(
            tmp_path, TopicIdentity(TOPIC, "id"), SparkProgress(planned=frozenset({0}))
        )
    assert not (tmp_path / TOPIC_IDENTITY_FILENAME).exists()


@pytest.mark.parametrize(
    "content",
    [
        "not json",
        '{"format": 2, "topic": "tx.raw.v1", "topic_id": "id"}',
        '{"format": 1, "topic": "tx.raw.v1"}',
        '{"format": 1, "topic": "tx.raw.v1", "topic_id": 7}',
        '{"format": 1, "topic": "tx.raw.v1", "topic_id": "id", "extra": true}',
    ],
)
def test_an_unreadable_topic_identity_is_refused(tmp_path: Path, content: str) -> None:
    (tmp_path / TOPIC_IDENTITY_FILENAME).write_text(content)
    with pytest.raises(CheckpointRefusedError, match="unreadable topic identity"):
        require_topic_identity(tmp_path, TopicIdentity(TOPIC, "id"), SparkProgress())


def test_the_bronze_row_records_the_topic_id_it_was_read_under() -> None:
    """Critic finding B4: after a recreation, offsets 0..k exist under two topics; the id tells them
    apart, and duplicates are counted per id."""
    fields = {field.name: field for field in bronze_schema().fields}
    assert BRONZE_COLUMNS[:2] == ("kafka_topic", "kafka_topic_id")
    assert fields["kafka_topic_id"].dataType.simpleString() == "string"
    assert not fields["kafka_topic_id"].nullable


def test_a_checkpoint_that_recorded_where_it_began_without_a_topic_id_is_refused(
    tmp_path: Path,
) -> None:
    """Critic finding B3: Spark's initial offsets exist, so this version began reading some topic
    before any batch was planned, and nothing records which one."""
    (tmp_path / "sources" / "0").mkdir(parents=True)
    (tmp_path / "sources" / "0" / "0").write_bytes(b'\x00v1\n{"tx.raw.v1":{"0":0}}')
    with pytest.raises(CheckpointRefusedError, match="nothing proves"):
        require_topic_identity(tmp_path, TopicIdentity(TOPIC, "id"), SparkProgress())
    assert not (tmp_path / TOPIC_IDENTITY_FILENAME).exists()


def test_the_recorded_topic_id_is_read_back(tmp_path: Path) -> None:
    assert read_topic_identity(tmp_path) is None
    current = TopicIdentity(TOPIC, "q1Z8-topic-id-one")
    assert require_topic_identity(tmp_path, current, SparkProgress())
    assert read_topic_identity(tmp_path) == current


class _Opened:
    """The two things a Bronze sink uses of an `OpenedCheckpoint`."""

    def __init__(self) -> None:
        self.identity = SimpleNamespace(app_id="trace-x:bronze_ingest_tx_raw_v1:v1:id")
        self.directory = Path("checkpoint/v1")
        self.appended: list[tuple[Any, int, Any]] = []

    def append(self, frame: Any, *, batch_id: int, target: Any) -> None:
        self.appended.append((frame, batch_id, target))


def test_every_batch_checks_the_broker_s_topic_id_before_and_after_it_is_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Critic finding B3: a topic deleted, recreated and refilled past the checkpoint's offset while
    the query runs. The start-time guard never sees it; each batch does."""
    framed: list[dict[str, Any]] = []

    def frame(batch: Any, **kwargs: Any) -> Any:
        framed.append(kwargs)
        return ("framed", batch)

    monkeypatch.setattr(bronze, "bronze_frame", frame)
    recorded = TopicIdentity(TOPIC, "q1Z8-topic-id-one")
    recreated = TopicIdentity(TOPIC, "r2Y9-topic-id-two")
    spec = BRONZE_TOPICS[TOPIC]

    def sink_answering(*answers: TopicIdentity) -> tuple[Any, _Opened, list[TopicIdentity]]:
        pending, opened = list(answers), _Opened()
        asked: list[TopicIdentity] = []

        def current() -> TopicIdentity:
            asked.append(pending[0])
            return pending.pop(0)

        sink = bronze_sink(spec, cast(Any, opened), recorded=recorded, current_topic=current)
        return sink, opened, asked

    sink, opened, asked = sink_answering(recorded, recorded)
    sink("batch", 4)
    assert opened.appended == [(("framed", "batch"), 4, spec.table)] and len(asked) == 2
    assert framed == [
        {"batch_id": 4, "checkpoint_id": opened.identity.app_id, "topic_id": recorded.topic_id}
    ]

    sink, opened, _ = sink_answering(recreated)
    with pytest.raises(CheckpointRefusedError, match="deleted and recreated"):
        sink("batch", 5)
    assert opened.appended == [], "refused before anything was written"

    sink, opened, _ = sink_answering(recorded, recreated)
    with pytest.raises(CheckpointRefusedError, match="after it was written"):
        sink("batch", 6)
    assert len(opened.appended) == 1, "written, then stopped before Spark can record the batch"

    unguarded = _Opened()
    unchecked: Any = bronze_sink(spec, cast(Any, unguarded), recorded=recorded, current_topic=None)
    unchecked("batch", 7)
    assert len(unguarded.appended) == 1


# ------------------------------------------------------------ trigger, progress ---


def test_a_trigger_is_exactly_one_of_available_now_or_a_positive_interval() -> None:
    assert Trigger(available_now=True).interval_s is None
    assert Trigger(interval_s=5.0).available_now is False
    for bad in ({}, {"available_now": True, "interval_s": 1.0}, {"interval_s": 0.0}):
        with pytest.raises(ValueError, match=r"trigger|interval"):
            Trigger(**bad)  # type: ignore[arg-type]


def test_a_progress_event_yields_the_batch_its_rows_and_how_far_behind_it_was() -> None:
    """Shaped as Spark's `StreamingQueryProgress.json` for a one-source Kafka query."""
    event: dict[str, Any] = {
        "id": "8a5b",
        "runId": "2c1d",
        "name": "bronze_ingest_tx_raw_v1",
        "batchId": 3,
        "numInputRows": 12,
        "sources": [
            {
                "description": "KafkaV2[Subscribe[tx.raw.v1]]",
                "startOffset": {"tx.raw.v1": {"0": 10, "1": 4}},
                "endOffset": {"tx.raw.v1": {"0": 18, "1": 8}},
                "latestOffset": {"tx.raw.v1": {"0": 25, "1": 8}},
                "numInputRows": 12,
            }
        ],
    }
    progress = batch_progress(TOPIC, event)
    assert (progress.batch_id, progress.input_rows) == (3, 12)
    assert dict(progress.end_offsets) == {0: 18, 1: 8}
    assert progress.offsets_behind == 7  # (25 - 18) + (8 - 8)
    first = batch_progress(
        TOPIC,
        {**event, "sources": [{**event["sources"][0], "startOffset": None, "latestOffset": None}]},
    )
    assert first.offsets_behind is None
    with pytest.raises(LakeContractError, match="one source"):
        batch_progress(TOPIC, {**event, "sources": event["sources"] * 2})
    with pytest.raises(LakeContractError, match="no end offset"):
        batch_progress(TOPIC, {**event, "sources": [{"endOffset": None}]})
