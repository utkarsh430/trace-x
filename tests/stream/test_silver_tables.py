"""Silver on real Delta, from hand-built Bronze rows (ADR-0053).

Bronze rows are written directly, as fixtures. Everything after that is the code under test: the
Silver query, its sink, the uniqueness assertions and conservation. The dispositions Spark writes
are compared with the pure `silver_rules` functions applied to the same records, so the Spark SQL
classification is held equal to the specification it mirrors.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import threading
import uuid
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from typing import Any

import pytest
from services.gateway.pipeline import ScoringOutcome, ScoringPipeline
from services.stream import silver as service

from trace_core.contracts import authorization
from trace_core.contracts.api.transaction import TransactionRequest
from trace_core.contracts.canonical_json import canonical_bytes
from trace_core.contracts.envelope import build_event
from trace_core.contracts.topics import (
    DEVICE_EVENTS_V1,
    IDENTITY_EVENTS_V1,
    INVESTIGATION_REQUESTED_V1,
    TX_AUTHORIZATION_V1,
    TX_RAW_V1,
    TX_SCORED_V1,
)
from trace_core.domain.time import event_time
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.reference import ReferenceFeatureStore
from trace_core.observation.scored_event import build_scored_event
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.stream import checkpoints
from trace_core.stream import silver_rules as rules
from trace_core.stream.bronze import Trigger, bronze_declaration, bronze_schema, bronze_topic
from trace_core.stream.lake import LakeConfig
from trace_core.stream.silver import (
    REWRITTEN_TABLES,
    create_silver_tables,
    shared_declarations,
    silver_declaration,
    silver_sink,
    silver_sources,
    silver_targets,
    start_silver_query,
)
from trace_core.stream.silver_conservation import check_silver_conservation
from trace_core.stream.tables import (
    CommitProvenance,
    create_table,
    describe_live_table,
    require_no_drift,
)
from trace_core.stream.timing import BACKFILL_MODE, REPLAY_MODE_HEADER

pytestmark = pytest.mark.stream

SHA = hashlib.sha1(b"trace-x silver stream tests", usedforsecurity=False).hexdigest()
TOPIC_ID = "silver-topic-id-one"
T0 = dt.datetime(2026, 9, 15, 9, 0, tzinfo=dt.UTC)
NOW = dt.datetime(2026, 9, 15, 10, 0, tzinfo=dt.UTC)
GENERATOR = "trace-generator@1.0.0"
Header = tuple[str, bytes | None]


@pytest.fixture(scope="module")
def spark() -> Iterator[Any]:
    from trace_core.stream.session import build_session

    session = build_session("trace-x-silver-tables")
    try:
        yield session
    finally:
        session.stop()


# ------------------------------------------------------------------ fixtures ---


def _authorization(transaction: int, outcome: str = "DECLINED") -> bytes:
    """A gateway outcome. Each call mints a new envelope (a new event_id): a producer retry."""
    occurred_ms = int(T0.timestamp() * 1000) + transaction * 1_000
    return canonical_bytes(
        authorization.build_event(
            transaction_id=f"tx_{transaction:012d}",
            account_id="acct_000001",
            authorization_outcome=outcome,
            decided_ms=occurred_ms + 250,
            transaction_occurred_ms=occurred_ms,
            transaction_occurred_at=authorization.iso_millis(occurred_ms),
            producer="trace-gateway@0.1.0",
            trace_id=uuid.uuid4().hex,
            correlation_id=f"tx_{transaction:012d}",
            ingested_ms=occurred_ms + 260,
        )
    )


def _generated(event_type: str, occurred: dt.datetime, payload: dict[str, Any]) -> bytes:
    return canonical_bytes(
        build_event(
            event_type=event_type,
            occurred_at=event_time(occurred),
            payload=payload,
            producer=GENERATOR,
            trace_id=uuid.uuid4().hex,
            correlation_id="corr_" + uuid.uuid4().hex,
        )
    )


def _identity(occurred: dt.datetime, kind: str = "PASSWORD_CHANGE") -> bytes:
    return _generated(
        "identity.events", occurred, {"account_id": "acct_000001", "identity_event_type": kind}
    )


def _raw(occurred: dt.datetime) -> bytes:
    return _generated(
        "tx.raw",
        occurred,
        {
            "transaction_id": "tx_it_0000000001",
            "account_id": "acct_000000001",
            "card_id": "card_000000001",
            "device_id": "dev_000000001",
            "merchant_id": "mrch_000001",
            "ip_id": "ip_0000001",
            "amount_minor": 501,
            "currency": "EUR",
            "channel": "CARD_PRESENT",
            "entry_mode": "CHIP",
            "merchant_mcc": "5812",
            "merchant_country": "DE",
            "latitude": 52.52,
            "longitude": 13.4,
            "authorization_outcome": "APPROVED",
        },
    )


def _device(occurred: dt.datetime) -> bytes:
    return _generated(
        "device.event",
        occurred,
        {
            "device_id": "dev_000000001",
            "account_id": "acct_000000001",
            "device_event_type": "FIRST_SEEN",
            "platform": "android",
        },
    )


def _investigation(occurred: dt.datetime) -> bytes:
    return _generated(
        "investigation.requested",
        occurred,
        {
            "case_id": f"case_{1:032x}",
            "transaction_id": "tx_it_0000000001",
            "account_id": "acct_000000001",
            "risk_band": "CRITICAL",
            "score": 0.91,
            "rule_pack_id": "core",
            "rule_pack_digest": "sha256:" + "c" * 64,
            "threshold_config_digest": "sha256:" + "d" * 64,
            "feature_set_version": "1.0.0",
            "feature_source": "ONLINE_ONLY",
            "degraded": False,
            "fired_rule_ids": ["R002_amount"],
        },
    )


def _pipeline() -> ScoringPipeline:
    return ScoringPipeline(
        pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
        thresholds=load_thresholds(),
        feature_store=ReferenceFeatureStore(),
    )


def _score(pipeline: ScoringPipeline, *, transaction: str = "tx_000000000901") -> ScoringOutcome:
    request = TransactionRequest.model_validate(
        {
            "transaction_id": transaction,
            "account_id": "acct_000901",
            "amount_minor": 7_500,
            "currency": "GBP",
            "occurred_at": T0.isoformat().replace("+00:00", "Z"),
        }
    )
    return pipeline.score(request, now=T0)


def _scored(
    outcome: ScoringOutcome,
    *,
    observe_outcome: str | None = None,
    epoch_ms: int | None = None,
    position: int | None = None,
) -> bytes:
    return canonical_bytes(
        build_scored_event(
            canonical=outcome.canonical,
            decision=outcome.decision,
            features=outcome.features,
            context=outcome.context,
            observe_outcome=observe_outcome or outcome.observe_outcome.value,
            store_position=outcome.observe_position if position is None else position,
            store_epoch_ms=outcome.store_epoch_ms if epoch_ms is None else epoch_ms,
            producer="trace-gateway@0.1.0",
            trace_id=uuid.uuid4().hex,
        )
    )


Row = tuple[int, int, dt.datetime, bytes | None, Sequence[Header] | None, int]
"""(partition, offset, LogAppendTime, value, headers, timestamp type)."""


def _row(
    partition: int,
    offset: int,
    logged_at: dt.datetime,
    value: bytes | None,
    *,
    headers: Sequence[Header] | None = None,
    timestamp_type: int = 1,
) -> Row:
    return (partition, offset, logged_at, value, headers, timestamp_type)


def _create_bronze(spark: Any, lake: LakeConfig, topic: str) -> None:
    query = bronze_topic(topic).query
    create_table(spark, bronze_declaration(topic), lake, CommitProvenance(SHA, False, query))


def _append_bronze(
    spark: Any, lake: LakeConfig, topic: str, rows: Sequence[Row], batch: int
) -> None:
    """One Bronze commit, as one Bronze micro-batch would make it."""
    tuples = [
        (topic, TOPIC_ID, p, o, at, ts_type, b"k", value, list(headers) if headers else None,
         "UNTRUSTED", NOW, batch, "bronze-writer")
        for p, o, at, value, headers, ts_type in rows
    ]  # fmt: skip
    path = bronze_topic(topic).table.local_path(lake)
    spark.createDataFrame(tuples, bronze_schema()).write.format("delta").mode("append").save(
        str(path)
    )


def _run_silver(spark: Any, lake: LakeConfig, topic: str) -> None:
    handle = start_silver_query(
        spark,
        lake,
        topic,
        git_sha=SHA,
        dirty_worktree=False,
        now=dt.datetime.now(dt.UTC),
        trigger=Trigger(available_now=True),
    )
    handle.query.awaitTermination()
    assert handle.query.exception() is None, handle.query.exception()


def _rows(spark: Any, lake: LakeConfig, ref: Any, topic: str | None = None) -> list[Any]:
    frame = spark.read.format("delta").load(str(ref.local_path(lake)))
    if topic is not None:
        frame = frame.filter(frame.silver_topic == topic)
    return frame.collect()


def _coords(row: Any) -> tuple[int, int]:
    return (int(row["kafka_partition"]), int(row["kafka_offset"]))


def _expected(
    topic: str, rows: Sequence[Row], committed: dict[str, rules.Canonical]
) -> dict[tuple[int, int], str]:
    """What the pure rules say each row becomes: `canonical`, `duplicate`, `superseded`,
    `conflict`, or the reason a reject was quarantined. Updates `committed` as a commit would."""
    result: dict[tuple[int, int], str] = {}
    candidates = []
    for p, o, at, value, headers, ts_type in rows:
        record = rules.BronzeRecord(
            topic, rules.Coordinates(TOPIC_ID, p, o), at, ts_type, value, tuple(headers or ())
        )
        admission = rules.admit(record)
        if admission.outcome is rules.Outcome.QUARANTINED:
            assert admission.reason is not None
            result[(p, o)] = admission.reason.value
            continue
        candidates.append(rules.candidate(admission, record))
    supersedable = topic == rules.SUPERSEDABLE_TOPIC
    for classified in rules.classify(candidates, committed, supersedable=supersedable):
        c = classified.candidate.coordinates
        disposition = classified.disposition
        if disposition in (rules.Disposition.ADMIT, rules.Disposition.SUPERSEDE):
            committed[classified.candidate.identity] = classified.canonical
        if classified.replaces is not None:
            replaced = classified.replaces.coordinates
            result[(replaced.partition, replaced.offset)] = "superseded"
        result[(c.partition, c.offset)] = {
            rules.Disposition.ADMIT: "canonical",
            rules.Disposition.SUPERSEDE: "canonical",
            rules.Disposition.REPLAYED: "canonical",
            rules.Disposition.DUPLICATE: "duplicate",
            rules.Disposition.CONFLICT: "conflict",
        }[disposition]
    return result


def _actual(spark: Any, lake: LakeConfig, topic: str) -> dict[tuple[int, int], str]:
    spec = rules.silver_topic(topic)
    found: dict[tuple[int, int], str] = {}
    for row in _rows(spark, lake, spec.table):
        found[_coords(row)] = "canonical"
    for row in _rows(spark, lake, rules.DUPLICATES, topic):
        assert _coords(row) not in found
        found[_coords(row)] = str(row["disposition"])
    for row in _rows(spark, lake, rules.QUARANTINE, topic):
        assert _coords(row) not in found
        reason = str(row["reason"])
        found[_coords(row)] = "conflict" if reason == "identity_conflict" else reason
    return found


def _crash_before_spark_commit(lake: LakeConfig, topic: str) -> None:
    """A crash after the sink's commits, before Spark's: the commit file never existed, and neither
    did the checksum Hadoop's local file system writes beside it."""
    spec = rules.silver_topic(topic)
    state = checkpoints.read_state(lake, spec.query)
    assert state.identity is not None and state.progress.last_committed is not None
    directory = checkpoints.query_directory(lake, spec.query) / f"v{state.identity.version}"
    last = state.progress.last_committed
    (directory / "commits" / str(last)).unlink()
    (directory / "commits" / f".{last}.crc").unlink(missing_ok=True)


def _snapshot(spark: Any, lake: LakeConfig, topic: str) -> dict[str, list[Any]]:
    spec = rules.silver_topic(topic)
    return {
        "canonical": sorted(_coords(r) for r in _rows(spark, lake, spec.table)),
        "duplicates": sorted(_coords(r) for r in _rows(spark, lake, rules.DUPLICATES, topic)),
        "quarantine": sorted(_coords(r) for r in _rows(spark, lake, rules.QUARANTINE, topic)),
        "late": sorted(tuple(r) for r in _rows(spark, lake, rules.LATE_EVENTS, topic)),
    }


# --------------------------------------------------------------------- tests ---


def test_silver_tables_are_created_from_their_declarations(spark: Any, tmp_path: Path) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    for topic in (TX_AUTHORIZATION_V1, TX_SCORED_V1):
        create_silver_tables(spark, lake, topic, git_sha=SHA, dirty_worktree=False)
    declarations = (
        silver_declaration(TX_AUTHORIZATION_V1),
        silver_declaration(TX_SCORED_V1),
        *shared_declarations(),
    )
    for declaration in declarations:
        live = describe_live_table(spark, declaration.ref.path_identifier(lake))
        require_no_drift(declaration, live)
        append_only = live.properties.get("delta.appendOnly") == "true"
        assert append_only is (declaration.ref not in REWRITTEN_TABLES), declaration.ref
    assert {str(ref) for ref in REWRITTEN_TABLES} == {"silver.tx_scored_v1", "silver.late_events"}
    identity = silver_declaration(TX_AUTHORIZATION_V1).schema["transaction_id"]
    assert identity.nullable is False
    late = describe_live_table(spark, rules.LATE_EVENTS.path_identifier(lake))
    assert late.partition_columns == ("silver_topic",), "topic-disjoint MERGEs must not conflict"
    for ref in (rules.DUPLICATES, rules.QUARANTINE):
        assert describe_live_table(spark, ref.path_identifier(lake)).partition_columns == ()


def test_outcomes_are_admitted_once_and_every_other_record_is_accounted_for(
    spark: Any, tmp_path: Path
) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    topic = TX_AUTHORIZATION_V1
    _create_bronze(spark, lake, topic)
    first = _authorization(1)
    batch_one = [
        _row(0, 0, T0 + dt.timedelta(seconds=10), first),
        _row(0, 1, T0 + dt.timedelta(seconds=11), first),  # a redelivery of the same bytes
        _row(1, 0, T0 + dt.timedelta(seconds=9), _authorization(1)),  # a retry that arrived first
        _row(1, 1, T0 + dt.timedelta(seconds=12), _authorization(1, "APPROVED")),  # a conflict
        _row(0, 2, T0 + dt.timedelta(seconds=13), b"not json"),
        _row(0, 3, T0 + dt.timedelta(seconds=14), None),
        _row(1, 2, T0 + dt.timedelta(seconds=15), _authorization(2)),
    ]
    _append_bronze(spark, lake, topic, batch_one, batch=0)
    committed: dict[str, rules.Canonical] = {}
    expected = _expected(topic, batch_one, committed)
    _run_silver(spark, lake, topic)
    assert _actual(spark, lake, topic) == expected
    assert expected[(1, 0)] == "canonical" and expected[(0, 0)] == "duplicate"
    assert expected[(1, 1)] == "conflict"

    batch_two = [
        _row(0, 4, T0 + dt.timedelta(seconds=30), _authorization(2)),  # a later retry of a row
        _row(1, 3, T0 + dt.timedelta(seconds=31), _authorization(2, "APPROVED")),
        _row(0, 5, T0 + dt.timedelta(seconds=32), _authorization(3)),
    ]
    _append_bronze(spark, lake, topic, batch_two, batch=1)
    expected.update(_expected(topic, batch_two, committed))
    _run_silver(spark, lake, topic)
    assert _actual(spark, lake, topic) == expected

    spec = rules.silver_topic(topic)
    canonical = _rows(spark, lake, spec.table)
    assert sorted(str(r["silver_identity"]) for r in canonical) == [
        "tx_000000000001",
        "tx_000000000002",
        "tx_000000000003",
    ]
    conflicts = [
        r for r in _rows(spark, lake, rules.QUARANTINE, topic) if r["reason"] == "identity_conflict"
    ]
    pairs = {
        (_coords(r), (r["canonical_kafka_partition"], r["canonical_kafka_offset"]))
        for r in conflicts
    }
    assert pairs == {((1, 1), (1, 0)), ((1, 3), (1, 2))}
    assert all(r["kafka_value"] is not None for r in conflicts), "a conflict keeps its raw bytes"

    report = check_silver_conservation(spark, lake, topic)
    assert report.conserved, report.summary()
    assert (report.bronze_rows, report.canonical, report.duplicates, report.quarantined) == (
        10,
        3,
        3,
        4,
    )
    assert report.through_version == 2

    rogue = spark.read.format("delta").load(str(rules.DUPLICATES.local_path(lake))).limit(1)
    rogue.write.format("delta").mode("append").save(str(rules.DUPLICATES.local_path(lake)))
    tampered = check_silver_conservation(spark, lake, topic)
    assert not tampered.conserved and tampered.double_counted == 1


def test_lateness_backfill_future_skew_and_arrival_time_are_decided_as_declared(
    spark: Any, tmp_path: Path
) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    topic = IDENTITY_EVENTS_V1
    _create_bronze(spark, lake, topic)
    rows = [
        _row(0, 0, T0 + dt.timedelta(seconds=5), _identity(T0)),
        _row(0, 1, T0 + dt.timedelta(seconds=700), _identity(T0 - dt.timedelta(seconds=1))),
        _row(0, 2, T0 + dt.timedelta(days=30), _identity(T0 - dt.timedelta(days=1)),
             headers=[(REPLAY_MODE_HEADER, BACKFILL_MODE)]),
        _row(0, 3, T0 + dt.timedelta(seconds=6), _identity(T0 + dt.timedelta(days=2))),
        _row(0, 4, T0 + dt.timedelta(seconds=7), _identity(T0), timestamp_type=0),
        _row(0, 5, T0 + dt.timedelta(seconds=8), _identity(T0 - dt.timedelta(seconds=3))),
        _row(0, 6, T0 + dt.timedelta(seconds=9), _identity(T0, kind="NOT_A_RELEASED_TYPE")),
    ]  # fmt: skip
    _append_bronze(spark, lake, topic, rows, batch=0)
    _run_silver(spark, lake, topic)

    spec = rules.silver_topic(topic)
    canonical = {_coords(r): r for r in _rows(spark, lake, spec.table)}
    assert set(canonical) == {(0, 0), (0, 1), (0, 2), (0, 5)}
    assert canonical[(0, 0)]["is_late"] is False
    assert canonical[(0, 1)]["is_late"] is True and canonical[(0, 1)]["arrival_delay_ms"] == 701_000
    assert canonical[(0, 2)]["is_late"] is None and canonical[(0, 2)]["is_backfill"] is True
    assert canonical[(0, 5)]["is_late"] is False, "out of order is not late"
    late = _rows(spark, lake, rules.LATE_EVENTS, topic)
    assert [(_coords(r), r["silver_identity"]) for r in late] == [
        ((0, 1), canonical[(0, 1)]["silver_identity"])
    ]
    reasons = {_coords(r): r["reason"] for r in _rows(spark, lake, rules.QUARANTINE, topic)}
    assert reasons == {
        (0, 3): "future_skew",
        (0, 4): "not_log_append_time",
        (0, 6): "invalid_event",
    }
    report = check_silver_conservation(spark, lake, topic)
    assert report.conserved, report.summary()
    assert (report.bronze_rows, report.canonical, report.quarantined) == (7, 4, 3)


def test_a_batch_replayed_after_its_silver_commits_landed_changes_nothing(
    spark: Any, tmp_path: Path
) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    topic = TX_AUTHORIZATION_V1
    _create_bronze(spark, lake, topic)
    rows = [
        _row(0, 0, T0 + dt.timedelta(seconds=1), _authorization(7)),
        _row(0, 1, T0 + dt.timedelta(seconds=2), _authorization(7)),
        _row(0, 2, T0 + dt.timedelta(seconds=3), b"{}"),
        _row(0, 3, T0 + dt.timedelta(seconds=700), _authorization(8)),
    ]
    _append_bronze(spark, lake, topic, rows, batch=0)
    _run_silver(spark, lake, topic)
    before = _snapshot(spark, lake, topic)
    _crash_before_spark_commit(lake, topic)
    _run_silver(spark, lake, topic)
    assert _snapshot(spark, lake, topic) == before
    assert check_silver_conservation(spark, lake, topic).conserved


def test_a_replay_of_a_batch_without_duplicates_adds_no_duplicate(
    spark: Any, tmp_path: Path
) -> None:
    """Critic finding C4: before `replayed`, a replay counted the rows it had admitted as duplicates
    of themselves, and conservation then counted every row twice."""
    lake = LakeConfig.at(tmp_path / "lake")
    topic = TX_AUTHORIZATION_V1
    _create_bronze(spark, lake, topic)
    rows = [
        _row(0, 0, T0 + dt.timedelta(seconds=1), _authorization(11)),
        _row(1, 0, T0 + dt.timedelta(seconds=2), _authorization(12)),
        _row(0, 1, T0 + dt.timedelta(seconds=900), _authorization(13)),
    ]
    _append_bronze(spark, lake, topic, rows, batch=0)
    _run_silver(spark, lake, topic)
    before = _snapshot(spark, lake, topic)
    assert before["duplicates"] == [] and len(before["canonical"]) == 3 and len(before["late"]) == 1
    _crash_before_spark_commit(lake, topic)
    _run_silver(spark, lake, topic)
    assert _snapshot(spark, lake, topic) == before
    assert _rows(spark, lake, rules.DUPLICATES, topic) == []
    report = check_silver_conservation(spark, lake, topic)
    assert report.conserved, report.summary()


def test_a_repeated_bronze_coordinate_is_classified_as_the_pure_rules_say(
    spark: Any, tmp_path: Path
) -> None:
    """Critic finding C3: Python and Spark agree on a coordinate Bronze holds twice, and
    conservation reports it."""
    lake = LakeConfig.at(tmp_path / "lake")
    topic = TX_AUTHORIZATION_V1
    _create_bronze(spark, lake, topic)
    twice = _row(0, 0, T0 + dt.timedelta(seconds=1), _authorization(21))
    records = [
        rules.BronzeRecord(topic, rules.Coordinates(TOPIC_ID, p, o), at, ts, value, ())
        for p, o, at, value, _headers, ts in (twice, twice)
    ]
    candidates = [rules.candidate(rules.admit(r), r) for r in records]
    dispositions = [c.disposition for c in rules.classify(candidates, {}, supersedable=False)]
    assert dispositions == [rules.Disposition.ADMIT, rules.Disposition.DUPLICATE]

    _append_bronze(spark, lake, topic, [twice, twice], batch=0)
    _run_silver(spark, lake, topic)
    spec = rules.silver_topic(topic)
    assert [_coords(r) for r in _rows(spark, lake, spec.table)] == [(0, 0)]
    duplicates = _rows(spark, lake, rules.DUPLICATES, topic)
    assert [(_coords(r), r["disposition"]) for r in duplicates] == [((0, 0), "duplicate")]
    report = check_silver_conservation(spark, lake, topic)
    assert not report.conserved
    assert (report.bronze_repeated, report.double_counted) == (1, 1)


def _redelivery_then_recorded() -> tuple[bytes, bytes]:
    pipeline = _pipeline()
    recorded = _score(pipeline)
    redelivered = _score(pipeline)
    assert (recorded.observe_outcome.value, redelivered.observe_outcome.value) == (
        "RECORDED",
        "REDELIVERY",
    )
    return _scored(redelivered), _scored(recorded)


def _later_epoch_then_earlier_epoch() -> tuple[bytes, bytes]:
    outcome = _score(_pipeline(), transaction="tx_000000000902")
    base_ms = int(T0.timestamp() * 1000)
    later = _scored(outcome, observe_outcome="RECORDED", epoch_ms=base_ms + 7_200_000, position=1)
    earlier = _scored(outcome, observe_outcome="RECORDED", epoch_ms=base_ms + 3_600_000, position=9)
    return later, earlier


@pytest.mark.parametrize(
    "values",
    [_redelivery_then_recorded, _later_epoch_then_earlier_epoch],
    ids=["redelivery-then-recorded", "later-epoch-then-earlier-epoch"],
)
def test_tx_scored_converges_to_the_same_canonical_row_however_the_batches_fall(
    spark: Any, tmp_path: Path, values: Callable[[], tuple[bytes, bytes]]
) -> None:
    """Critic finding A1: the canonical row is the first of one total order, in one batch or two."""
    topic = TX_SCORED_V1
    first_value, second_value = values()
    first = _row(0, 0, T0 + dt.timedelta(seconds=1), first_value)
    second = _row(0, 1, T0 + dt.timedelta(seconds=2), second_value)
    outcomes: dict[str, dict[str, Any]] = {}
    for name, batches in (("split", [[first], [second]]), ("single", [[first, second]])):
        lake = LakeConfig.at(tmp_path / name / "lake")
        _create_bronze(spark, lake, topic)
        committed: dict[str, rules.Canonical] = {}
        expected: dict[tuple[int, int], str] = {}
        for index, rows in enumerate(batches):
            _append_bronze(spark, lake, topic, rows, batch=index)
            expected.update(_expected(topic, rows, committed))
            _run_silver(spark, lake, topic)
        assert _actual(spark, lake, topic) == expected, name
        report = check_silver_conservation(spark, lake, topic)
        assert report.conserved, (name, report.summary())
        (canonical,) = _rows(spark, lake, rules.silver_topic(topic).table)
        (duplicate,) = _rows(spark, lake, rules.DUPLICATES, topic)
        outcomes[name] = {
            "canonical": _coords(canonical),
            "observe_outcome": canonical["observe_outcome"],
            "store_epoch": canonical["store_epoch"],
            "duplicate": (_coords(duplicate), duplicate["disposition"]),
            "points_to": (
                duplicate["canonical_kafka_partition"],
                duplicate["canonical_kafka_offset"],
            ),
            "superseded": report.superseded,
        }
    split, single = outcomes["split"], outcomes["single"]
    assert split["canonical"] == single["canonical"] == (0, 1)
    assert split["observe_outcome"] == single["observe_outcome"] == "RECORDED"
    assert split["store_epoch"] == single["store_epoch"]
    assert split["duplicate"] == ((0, 0), "superseded") and split["superseded"] == 1
    assert single["duplicate"] == ((0, 0), "duplicate") and single["superseded"] == 0
    assert split["points_to"] == single["points_to"] == (0, 1)


def test_a_checkpoint_reset_rereading_bronze_leaves_late_events_unchanged(
    spark: Any, tmp_path: Path
) -> None:
    """Critic finding B1: a reset re-derives `late_events` instead of appending it again, so the
    uniqueness assertion never stops the query. Duplicates are per checkpoint version."""
    lake = LakeConfig.at(tmp_path / "lake")
    topic = TX_AUTHORIZATION_V1
    _create_bronze(spark, lake, topic)
    rows = [
        _row(0, 0, T0 + dt.timedelta(seconds=1), _authorization(31)),
        _row(0, 1, T0 + dt.timedelta(seconds=2), _authorization(31)),
        _row(1, 0, T0 + dt.timedelta(seconds=900), _authorization(32)),
    ]
    _append_bronze(spark, lake, topic, rows, batch=0)
    _run_silver(spark, lake, topic)
    spec = rules.silver_topic(topic)
    late_before = sorted(tuple(r) for r in _rows(spark, lake, rules.LATE_EVENTS, topic))
    canonical_before = sorted(_coords(r) for r in _rows(spark, lake, spec.table))
    assert len(late_before) == 1

    identity = checkpoints.reset_checkpoint(
        spark,
        lake,
        spec.query,
        targets=silver_targets(topic),
        sources=silver_sources(topic),
        reason="stream test: re-read Bronze from version 0",
        now=dt.datetime.now(dt.UTC),
    )
    _run_silver(spark, lake, topic)

    assert sorted(tuple(r) for r in _rows(spark, lake, rules.LATE_EVENTS, topic)) == late_before
    assert sorted(_coords(r) for r in _rows(spark, lake, spec.table)) == canonical_before
    duplicates = _rows(spark, lake, rules.DUPLICATES, topic)
    assert len(duplicates) == 2, "one duplicate row per checkpoint version"
    assert {r["silver_checkpoint_id"] for r in duplicates} >= {identity.app_id}
    report = check_silver_conservation(spark, lake, topic)
    assert report.conserved, report.summary()
    assert report.checkpoint_version == identity.version and report.duplicates == 1


def test_every_released_topic_runs_through_the_sink(spark: Any, tmp_path: Path) -> None:
    """Critic finding C5: all six topics, from table creation through MERGE, quarantine and
    conservation, including `tx.scored.v1`'s JSON columns and nullable timestamps and `tx.raw.v1`'s
    NOT NULL doubles."""
    lake = LakeConfig.at(tmp_path / "lake")
    valid = {
        TX_RAW_V1: _raw(T0),
        IDENTITY_EVENTS_V1: _identity(T0),
        DEVICE_EVENTS_V1: _device(T0),
        INVESTIGATION_REQUESTED_V1: _investigation(T0),
        TX_AUTHORIZATION_V1: _authorization(41),
        TX_SCORED_V1: _scored(_score(_pipeline(), transaction="tx_000000000941")),
    }
    assert set(valid) == set(rules.SILVER_TOPICS)
    for topic, value in valid.items():
        _create_bronze(spark, lake, topic)
        rows = [
            _row(0, 0, T0 + dt.timedelta(seconds=1), value),
            _row(0, 1, T0 + dt.timedelta(seconds=2), b"not json"),
        ]
        _append_bronze(spark, lake, topic, rows, batch=0)
        _run_silver(spark, lake, topic)
        spec = rules.silver_topic(topic)
        (canonical,) = _rows(spark, lake, spec.table)
        (quarantined,) = _rows(spark, lake, rules.QUARANTINE, topic)
        assert (_coords(canonical), quarantined["reason"]) == ((0, 0), "invalid_event"), topic
        report = check_silver_conservation(spark, lake, topic)
        assert report.conserved, (topic, report.summary())
        if topic == TX_SCORED_V1:
            assert isinstance(json.loads(canonical["decision_summary_json"]), dict)
            assert isinstance(json.loads(canonical["served_features_json"]), list)
            assert canonical["store_epoch"] is None, "a nullable timestamp stays null"
            assert canonical["observe_outcome"] == "RECORDED"
        if topic == TX_RAW_V1:
            assert (canonical["latitude"], canonical["longitude"]) == (52.52, 13.4)


CONCURRENT_ROUNDS = 8


def _concurrent_rows(topic: str, round_: int) -> list[Row]:
    """One round's Bronze rows: an on-time record, its duplicate, a late record and a poison value.
    Every round is new identities, so each sink batch inserts into all three shared tables."""
    base = round_ * 10
    if topic == TX_AUTHORIZATION_V1:
        on_time_tx, late_tx = 100 + 2 * round_, 101 + 2 * round_
        on_time, late = _authorization(on_time_tx), _authorization(late_tx)
        on_time_at = T0 + dt.timedelta(seconds=on_time_tx + 1)
        late_at = T0 + dt.timedelta(seconds=late_tx + 700)
    else:
        on_time, late = _identity(T0), _identity(T0)
        on_time_at, late_at = T0 + dt.timedelta(seconds=1), T0 + dt.timedelta(seconds=700)
    return [
        _row(0, base, on_time_at, on_time),
        _row(0, base + 1, on_time_at + dt.timedelta(seconds=1), on_time),
        _row(0, base + 2, late_at, late),
        _row(0, base + 3, on_time_at + dt.timedelta(seconds=2), b"not json"),
    ]


def _failure(exc: BaseException) -> str:
    """The exception's class and first line, and the JVM exception's class when there is one."""
    java = getattr(exc, "java_exception", None)
    java_class = "" if java is None else f" [{java.getClass().getName()}]"
    lines = str(exc).splitlines()
    return f"{type(exc).__name__}{java_class}: {lines[0][:400] if lines else ''}"


def test_two_topics_sink_batches_commit_into_the_shared_tables_concurrently(
    spark: Any, tmp_path: Path
) -> None:
    """Lead finding: every topic's query writes the shared Silver tables, and `silver run` runs them
    at once. Each round, two topics' sink batches are released together by a barrier, each on its
    own session as a streaming query's batches are, and must all commit."""
    from pyspark.sql import functions as F  # noqa: N812

    lake = LakeConfig.at(tmp_path / "lake")
    topics = (TX_AUTHORIZATION_V1, IDENTITY_EVENTS_V1)
    opened = {}
    for topic in topics:
        _create_bronze(spark, lake, topic)
        for round_ in range(CONCURRENT_ROUNDS):
            _append_bronze(spark, lake, topic, _concurrent_rows(topic, round_), batch=round_)
        create_silver_tables(spark, lake, topic, git_sha=SHA, dirty_worktree=False)
        opened[topic] = checkpoints.open_checkpoint(
            spark,
            lake,
            rules.silver_topic(topic).query,
            targets=silver_targets(topic),
            sources=silver_sources(topic),
            git_sha=SHA,
            dirty_worktree=False,
            now=dt.datetime.now(dt.UTC),
        )
    # A streaming query runs each batch on its own cloned session; so does each thread here.
    sessions = {topic: spark.newSession() for topic in topics}
    failures: list[tuple[str, int, str]] = []

    for round_ in range(CONCURRENT_ROUNDS):
        barrier = threading.Barrier(len(topics))

        def work(topic: str, round_: int = round_, barrier: threading.Barrier = barrier) -> None:
            path = str(bronze_topic(topic).table.local_path(lake))
            batch = (
                sessions[topic]
                .read.format("delta")
                .load(path)
                .filter(F.col("bronze_batch_id") == round_)
            )
            sink = silver_sink(rules.silver_topic(topic), opened[topic], git_sha=SHA)
            barrier.wait(timeout=300)
            try:
                sink(batch, round_)
            except Exception as exc:
                failures.append((topic, round_, _failure(exc)))

        threads = [threading.Thread(target=work, args=(topic,)) for topic in topics]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=900)
    assert failures == [], failures
    for topic in topics:
        assert len(_rows(spark, lake, rules.LATE_EVENTS, topic)) == CONCURRENT_ROUNDS, topic
        assert len(_rows(spark, lake, rules.DUPLICATES, topic)) == CONCURRENT_ROUNDS, topic
        assert len(_rows(spark, lake, rules.QUARANTINE, topic)) == CONCURRENT_ROUNDS, topic


def test_every_topic_s_silver_query_runs_concurrently_in_one_session_as_silver_run_does(
    spark: Any, tmp_path: Path
) -> None:
    """Lead finding: all six queries started in one session, as `silver run` starts them, over
    Bronze data with late rows in three topics."""
    from pyspark.errors import StreamingQueryException

    lake = LakeConfig.at(tmp_path / "lake")
    arrived, late = T0 + dt.timedelta(seconds=1), T0 + dt.timedelta(seconds=700)
    rows = {
        TX_RAW_V1: [_row(0, 0, arrived, _raw(T0))],
        IDENTITY_EVENTS_V1: [
            _row(0, 0, arrived, _identity(T0)),
            _row(0, 1, late, _identity(T0)),
        ],
        DEVICE_EVENTS_V1: [_row(0, 0, arrived, _device(T0)), _row(0, 1, late, _device(T0))],
        INVESTIGATION_REQUESTED_V1: [_row(0, 0, arrived, _investigation(T0))],
        TX_AUTHORIZATION_V1: [
            _row(0, 0, T0 + dt.timedelta(seconds=52), _authorization(51)),
            _row(0, 1, T0 + dt.timedelta(seconds=752), _authorization(52)),
        ],
        TX_SCORED_V1: [
            _row(0, 0, arrived, _scored(_score(_pipeline(), transaction="tx_000000000951")))
        ],
    }
    assert set(rows) == set(rules.SILVER_TOPICS)
    for topic, topic_rows in rows.items():
        _create_bronze(spark, lake, topic)
        poison = _row(0, len(topic_rows), arrived, b"not json")
        _append_bronze(spark, lake, topic, [*topic_rows, poison], batch=0)
    handles = [
        start_silver_query(
            spark,
            lake,
            topic,
            git_sha=SHA,
            dirty_worktree=False,
            now=dt.datetime.now(dt.UTC),
            trigger=Trigger(available_now=True),
        )
        for topic in rules.SILVER_TOPICS
    ]
    code = service.supervise(
        spark.streams,
        handles,
        stop_requested=lambda: False,
        query_failure=StreamingQueryException,
    )
    failures = [
        (handle.spec.topic, _failure(error))
        for handle in handles
        if (error := handle.query.exception()) is not None
    ]
    assert (code, failures) == (0, [])
    for topic in rows:
        report = check_silver_conservation(spark, lake, topic)
        assert report.conserved, (topic, report.summary())
        expected_late = (
            1 if topic in (IDENTITY_EVENTS_V1, DEVICE_EVENTS_V1, TX_AUTHORIZATION_V1) else 0
        )
        assert len(_rows(spark, lake, rules.LATE_EVENTS, topic)) == expected_late, topic
        assert len(_rows(spark, lake, rules.QUARANTINE, topic)) == 1, topic
