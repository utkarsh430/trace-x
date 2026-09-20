"""A Bronze topic table and checkpoint as Bronze's query leaves them, without a broker.

Rows are appended through the checkpoint convention's own `OpenedCheckpoint.append` (one idempotent,
stamped commit per batch). Spark's files are written beside it the way Spark writes them: the Kafka
source's initial offsets, `offsets/<n>` before the batch is written, and `commits/<n>` after. The
topic id sidecar is `trace_core.stream.bronze.TopicIdentity`. So Bronze conservation, Silver and the
maintenance tooling read exactly what the real query leaves; tests/integration/
test_resource_bounds_kafka.py proves the same path with a real broker.

Values are valid `identity.events.v1` events, so Silver admits every row. Each record also carries
the observation log's session headers, so the same rows feed Bronze coverage (ADR-0051 §5).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Mapping
from typing import Any, Final

from trace_core.contracts.canonical_json import canonical_bytes
from trace_core.contracts.envelope import build_event
from trace_core.contracts.topics import IDENTITY_EVENTS_V1
from trace_core.domain.time import event_time
from trace_core.observation.log import SEQ_HEADER, SESSION_HEADER
from trace_core.stream import checkpoints
from trace_core.stream.bronze import (
    TOPIC_IDENTITY_FILENAME,
    TopicIdentity,
    Trigger,
    bronze_declaration,
    bronze_schema,
    bronze_topic,
)
from trace_core.stream.checkpoints import KafkaSourceStart
from trace_core.stream.lake import LakeConfig
from trace_core.stream.silver import start_silver_query
from trace_core.stream.tables import CommitProvenance, create_table, snapshot_facts

TOPIC: Final = IDENTITY_EVENTS_V1
# Spelled as the broker's id reaches Bronze: confluent-kafka's str(Uuid), standard base64.
TOPIC_ID: Final = "hB5M5d1ZQpW+lA3/T0Hn1w"
SHA: Final = hashlib.sha1(b"trace-x resource bounds", usedforsecurity=False).hexdigest()
_METADATA: Final = '{"batchWatermarkMs":0,"batchTimestampMs":1757923200000,"conf":{}}'


class BronzeWriter:
    """Bronze's query for one topic, one micro-batch per `append`."""

    def __init__(
        self,
        spark: Any,
        lake: LakeConfig,
        *,
        partitions: int = 3,
        session_id: str = "resource-bounds-session",
        topic: str = TOPIC,
    ) -> None:
        self.spark, self.lake, self.session_id = spark, lake, session_id
        self.topic = topic
        self.spec = bronze_topic(topic)
        provenance = CommitProvenance(SHA, False, self.spec.query)
        create_table(spark, bronze_declaration(topic), lake, provenance)
        self.opened = checkpoints.open_checkpoint(
            spark,
            lake,
            self.spec.query,
            targets=[self.spec.table],
            sources=[KafkaSourceStart(topic, "earliest")],
            git_sha=SHA,
            dirty_worktree=False,
            now=dt.datetime.now(dt.UTC),
        )
        directory = self.opened.directory
        (directory / TOPIC_IDENTITY_FILENAME).write_text(TopicIdentity(topic, TOPIC_ID).to_json())
        self.ends = dict.fromkeys(range(partitions), 0)
        initial = directory.joinpath(*checkpoints.INITIAL_OFFSETS_PARTS)
        initial.parent.mkdir(parents=True, exist_ok=True)
        initial.write_bytes(b"\x00v1\n" + self._offsets().encode())
        (directory / "offsets").mkdir(exist_ok=True)
        (directory / "commits").mkdir(exist_ok=True)
        self.batch = 0
        self.events = 0
        self.versions: list[int] = []

    def _offsets(self) -> str:
        return json.dumps({self.topic: {str(p): end for p, end in sorted(self.ends.items())}})

    def _value(self, partition: int, now: dt.datetime) -> bytes:
        """A valid identity event, or -- for any other covered topic -- an envelope carrying just
        what the coverage rule reads from a value: its producer and the writer's stamp."""
        if self.topic == TOPIC:
            return canonical_bytes(
                build_event(
                    event_type="identity.events",
                    occurred_at=event_time(now),
                    payload={
                        "account_id": f"acct_{partition:06d}",
                        "identity_event_type": "PASSWORD_CHANGE",
                    },
                    producer="trace-gateway@0.1.0",
                    trace_id="0" * 32,
                    correlation_id=f"idev_{self.events:032d}",
                )
            )
        stamp = event_time(now).isoformat().replace("+00:00", "Z")
        return canonical_bytes(
            {"envelope": {"producer": "trace-gateway@0.1.0", "ingested_at": stamp}, "payload": {}}
        )

    @property
    def path(self) -> Any:
        return self.spec.table.local_path(self.lake)

    def append(self, counts: Mapping[int, int], *, files: int = 1) -> int:
        """One batch of `counts[partition]` records per partition; the Bronze version it made."""
        now = dt.datetime.now(dt.UTC)
        rows = []
        for partition, count in sorted(counts.items()):
            for offset in range(self.ends[partition], self.ends[partition] + count):
                self.events += 1
                value = self._value(partition, now)
                headers = [
                    (SESSION_HEADER, self.session_id.encode()),
                    (SEQ_HEADER, str(self.events).encode()),
                ]
                rows.append(
                    (self.topic, TOPIC_ID, partition, offset, now, 1, b"k", value, headers,
                     "UNTRUSTED", now, self.batch, self.opened.identity.app_id)
                )  # fmt: skip
            self.ends[partition] += count
        directory = self.opened.directory
        (directory / "offsets" / str(self.batch)).write_text(f"v1\n{_METADATA}\n{self._offsets()}")
        frame = self.spark.createDataFrame(rows, bronze_schema())
        frame = (
            frame.coalesce(1) if files == 1 else frame.repartitionByRange(files, "kafka_partition")
        )
        self.opened.append(frame, batch_id=self.batch, target=self.spec.table)
        (directory / "commits" / str(self.batch)).write_text('v1\n{"nextBatchWatermarkMs":0}')
        self.batch += 1
        facts = snapshot_facts(self.spark, self.path)
        assert facts is not None
        self.versions.append(facts.version)
        return facts.version


def run_silver(spark: Any, lake: LakeConfig) -> None:
    handle = start_silver_query(
        spark,
        lake,
        TOPIC,
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
        trigger=Trigger(available_now=True),
    )
    handle.query.awaitTermination()
    assert handle.query.exception() is None, handle.query.exception()
