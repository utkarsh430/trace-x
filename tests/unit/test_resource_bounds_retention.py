"""`P3.resource-bounds`, without a JVM: Bronze's retention floor, the Bronze-only `ignoreDeletes`
allowance, retirement in conservation, the consumer-position refusals, and the VACUUM and OPTIMIZE
guards (ADR-0052 amendment 1).

What these hold to their definitions runs on real Delta in
tests/stream/test_resource_bounds_maintenance.py and against a real broker in
tests/integration/test_resource_bounds_kafka.py.
"""

from __future__ import annotations

import datetime as dt
import json
import random
import socket
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

import pytest
from pyspark.sql.types import LongType, StructField, StructType
from services.stream.maintenance import parse_floors, parse_table

from trace_core.contracts.topics import TX_SCORED_V1
from trace_core.domain.errors import (
    ContractError,
    LakeContractError,
    StreamingSourceRetentionError,
    TableDeclarationError,
    TableDriftError,
)
from trace_core.domain.time import to_millis
from trace_core.observation.coverage import SessionRow
from trace_core.stream import tables
from trace_core.stream.bronze import BRONZE_TOPICS, bronze_declaration
from trace_core.stream.bronze_conservation import (
    APPEND_ONLY_LIFTED,
    ConservationReport,
    ConsumedRanges,
    OffsetRange,
    Record,
    append_only_lifted,
    judge_conservation,
    partition_rows_from_records,
)
from trace_core.stream.bronze_coverage import BronzeRecord, coverage_from_bronze, is_retired
from trace_core.stream.checkpoints import is_bronze_topic_table
from trace_core.stream.hydration import FUTURE_SKEW, Claim, ClaimInputs, after_ms, compute_claim
from trace_core.stream.lake import LakeConfig, Tier
from trace_core.stream.maintenance import (
    LOCK_DIRNAME,
    RETENTION_AUDIT,
    RETENTION_CHECK_CONF,
    BronzeBatch,
    ConsumerPin,
    RetentionEvidence,
    RetentionRefusedError,
    SilverPosition,
    declared_tables,
    delete_predicate,
    maintenance_lock,
    optimize_table,
    plan_retention,
    plan_vacuum,
)
from trace_core.stream.silver_conservation import START_WITHOUT_FLOORS, start_problem
from trace_core.stream.tables import (
    DECLARED_RETENTION,
    DELETED_FILE_RETENTION_PROPERTY,
    LOG_RETENTION_PROPERTY,
    FloorKey,
    LiveTable,
    TableDeclaration,
    TableRef,
)

pytestmark = pytest.mark.unit

TOPIC = "tx.raw.v1"
# Spelled as the broker's id reaches Bronze: confluent-kafka's str(Uuid), standard base64.
TID = "hB5M5d1ZQpW+lA3/T0Hn1w"
OLD_TID = "AAAAAAAAAAAAAAAAAAAAAB"
CURRENT = "trace-x:bronze_ingest_tx_raw_v1:v1:00000000000040008000000000000001"
KEY0: FloorKey = (TID, 0)
KEY1: FloorKey = (TID, 1)


# ----------------------------------------------------------------- the floor ---


def test_resource_bounds_a_floor_key_names_one_topic_id_and_partition() -> None:
    key = tables.retention_floor_key(TID, 3)
    assert key == f"trace_x.retention_floor.{TID}.3"
    assert tables.retention_floors({key: "42", "delta.appendOnly": "true"}) == {(TID, 3): 42}
    for topic_id in ("a.b", "x y", "", "x" * 65, "a'b", "a\\b", 'a"b', "a=b"):
        with pytest.raises(TableDeclarationError):
            tables.retention_floor_key(topic_id, 0)
    with pytest.raises(TableDeclarationError):
        tables.retention_floor_key(TID, -1)


def test_resource_bounds_a_floor_key_accepts_every_topic_id_the_kafka_client_spells() -> None:
    """Bronze records `str(TopicDescription.topic_id)`, which confluent-kafka renders in standard
    base64: about half of all topic ids carry `+` or `/`. Each must name a floor and read back."""
    from confluent_kafka import Uuid

    rng = random.Random(20260918)
    spelled = set()
    for _ in range(200):
        topic_id = str(Uuid(rng.getrandbits(64) - 2**63, rng.getrandbits(64) - 2**63))
        key = tables.retention_floor_key(topic_id, 7)
        assert tables.retention_floors({key: "9"}) == {(topic_id, 7): 9}
        spelled.update(set(topic_id) & {"+", "/"})
    assert spelled == {"+", "/"}
    url_safe = "hB5M5d1ZQpW-lA3_T0Hn1w"  # Kafka's own spelling of an id is accepted too
    assert tables.retention_floors({tables.retention_floor_key(url_safe, 0): "1"}) == {
        (url_safe, 0): 1
    }


@pytest.mark.parametrize(
    ("key", "value"),
    [
        (f"trace_x.retention_floor.{TID}", "1"),
        (f"trace_x.retention_floor.{TID}.01", "1"),
        (f"trace_x.retention_floor.{TID}.0", "-1"),
        (f"trace_x.retention_floor.{TID}.0", "ten"),
        (f"trace_x.retention_floor.{TID}.0", ""),
    ],
)
def test_resource_bounds_an_unreadable_floor_never_reads_as_no_floor(key: str, value: str) -> None:
    with pytest.raises(TableDriftError, match="unreadable retention floors"):
        tables.retention_floors({key: value})


def _bronze_live(**properties: str) -> tuple[TableDeclaration, LiveTable]:
    declaration = bronze_declaration(TOPIC)
    live = LiveTable(
        table_id="table-id",
        format="delta",
        min_reader_version=1,
        min_writer_version=2,
        table_features=tables.BASELINE_FEATURES,
        properties={"delta.appendOnly": "true", **properties},
        partition_columns=(),
        clustering_columns=(),
        schema_json=declaration.schema.jsonValue(),
    )
    return declaration, live


def test_resource_bounds_bronze_accepts_well_formed_floors_and_no_other_table_does() -> None:
    declaration, live = _bronze_live()
    assert declaration.retention_floors
    assert tables.check_drift(declaration, live) == ()
    floored = replace(
        live, properties={**live.properties, tables.retention_floor_key(TID, 0): "10"}
    )
    assert tables.check_drift(declaration, floored) == ()
    malformed = replace(
        live, properties={**live.properties, f"trace_x.retention_floor.{TID}.0": "x"}
    )
    assert [d.aspect for d in tables.check_drift(declaration, malformed)] == ["property"]
    floorless = TableDeclaration(
        ref=TableRef(Tier.SILVER, "floorless"),
        schema=declaration.schema,
        properties={"delta.appendOnly": "true"},
    )
    assert [d.aspect for d in tables.check_drift(floorless, floored)] == ["property"]


def test_resource_bounds_only_the_append_only_maintenance_lifts_reads_as_interrupted() -> None:
    declaration, live = _bronze_live()
    lifted = replace(live, properties={"delta.appendOnly": "false"})
    assert tables.check_drift(declaration, lifted) == (APPEND_ONLY_LIFTED,)
    assert append_only_lifted(declaration, lifted)
    assert not append_only_lifted(declaration, live)
    with pytest.raises(TableDriftError):
        append_only_lifted(
            declaration,
            replace(
                lifted, properties={"delta.appendOnly": "false", "delta.checkpointInterval": "3"}
            ),
        )


# ----------------------------------------------------------------- retention ---


def test_resource_bounds_retention_is_declared_per_table_and_absent_means_delta_s_default() -> None:
    declaration, live = _bronze_live()
    assert declaration.retention_properties() == dict(DECLARED_RETENTION)
    assert (declaration.log_retention_hours(), declaration.deleted_file_retention_hours()) == (
        720,
        168,
    )
    assert tables.check_drift(declaration, replace(live, properties={
        **live.properties, **DECLARED_RETENTION
    })) == ()  # fmt: skip
    shortened = replace(
        live,
        properties={**live.properties, DELETED_FILE_RETENTION_PROPERTY: "interval 1 hours"},
    )
    assert [str(d) for d in tables.check_drift(declaration, shortened)] == [
        "property: 'delta.deletedFileRetentionDuration' is 'interval 1 hours', declared "
        "'interval 7 days'"
    ]
    topic_tables = {spec.table for spec in BRONZE_TOPICS.values()}
    declared = declared_tables()
    assert RETENTION_AUDIT in declared
    for ref, table in declared.items():
        creation = table.creation_properties()
        assert {k: creation[k] for k in DECLARED_RETENTION} == table.retention_properties(), ref
        assert table.deleted_file_retention_hours() <= table.log_retention_hours(), ref
        assert table.retention_floors == (ref in topic_tables), ref


@pytest.mark.parametrize(
    ("properties", "message"),
    [
        ({DELETED_FILE_RETENTION_PROPERTY: "interval 40 days"}, "exceeds"),
        ({LOG_RETENTION_PROPERTY: "30 days"}, "is not 'interval"),
        ({tables.retention_floor_key(TID, 0): "1"}, "never a declared property"),
    ],
)
def test_resource_bounds_a_declaration_with_unusable_retention_is_refused(
    properties: dict[str, str], message: str
) -> None:
    with pytest.raises(TableDeclarationError, match=message):
        TableDeclaration(
            ref=TableRef(Tier.SILVER, "retention_declared"),
            schema=StructType([StructField("id", LongType())]),
            properties=properties,
        )


# ------------------------------------------------------------ source safety ---


def test_resource_bounds_ignore_deletes_is_the_only_option_a_bronze_source_is_granted() -> None:
    assert set(tables.RETENTION_SOURCE_OPTIONS) == {"ignoreDeletes"}
    for spelling in ({"ignoreDeletes": "true"}, {"IGNOREDELETES": "TRUE"}):
        tables.require_no_loss_tolerance(
            session_conf={}, source_options=spelling, retention_source=True
        )
    with pytest.raises(StreamingSourceRetentionError, match="ignoreDeletes"):
        tables.require_no_loss_tolerance(session_conf={}, source_options={"ignoreDeletes": "true"})
    for refused in (
        {"skipChangeCommits": "true"},
        {"ignoreChanges": "true"},
        {"failOnDataLoss": "false"},
        {"ignoreMissingFiles": "true"},
        {"ignoreCorruptFiles": "true"},
    ):
        with pytest.raises(StreamingSourceRetentionError, match="tolerate data loss"):
            tables.require_no_loss_tolerance(
                session_conf={},
                source_options={**refused, "ignoreDeletes": "true"},
                retention_source=True,
            )
    with pytest.raises(StreamingSourceRetentionError, match="ignoreMissingFiles"):
        tables.require_no_loss_tolerance(
            session_conf={"spark.sql.files.ignoreMissingFiles": "true"},
            source_options={},
            retention_source=True,
        )


def test_resource_bounds_only_released_topic_tables_are_bronze_retention_sources() -> None:
    for spec in BRONZE_TOPICS.values():
        assert is_bronze_topic_table(spec.table)
    assert not is_bronze_topic_table(RETENTION_AUDIT)
    assert not is_bronze_topic_table(TableRef(Tier.BRONZE, "tx_raw_v1_copy"))
    assert not is_bronze_topic_table(TableRef(Tier.SILVER, "tx_raw_v1"))


# --------------------------------------------------------------- conservation ---


def _judge(
    records: list[Record],
    ranges: dict[int, OffsetRange],
    floors: dict[FloorKey, int],
    *,
    lifted: bool = False,
) -> ConservationReport:
    consumed = ConsumedRanges(TOPIC, CURRENT, 1, 1, 1, ranges)
    return judge_conservation(
        consumed,
        partition_rows_from_records(
            records, checkpoint_id=CURRENT, ranges=ranges, topic_id=TID, floors=floors
        ),
        table="bronze.tx_raw_v1",
        table_version=5,
        foreign_topic_rows=0,
        version=1,
        topic_id=TID,
        floors=floors,
        append_only_lifted=lifted,
    )


def _rows(partition: int, start: int, end: int, topic_id: str = TID) -> list[Record]:
    return [(partition, offset, CURRENT, topic_id) for offset in range(start, end)]


def test_resource_bounds_offsets_below_the_floor_are_retired_whether_present_or_not() -> None:
    ranges = {0: OffsetRange(0, 10), 1: OffsetRange(0, 4)}
    report = _judge(_rows(0, 6, 10) + _rows(1, 0, 4), ranges, {KEY0: 6, KEY1: 2})
    assert report.conserved, report.summary()
    verdicts = {v.partition: v for v in report.partitions}
    assert (verdicts[0].missing, verdicts[0].floor, verdicts[0].retired_present) == (0, 6, 0)
    assert (verdicts[1].missing, verdicts[1].floor, verdicts[1].retired_present) == (0, 2, 2)
    assert report.summary()["floors"] == {f"{TID}.0": 6, f"{TID}.1": 2}


def test_resource_bounds_retirement_never_hides_a_loss_a_duplicate_or_another_topic_s_rows() -> (
    None
):
    ranges = {0: OffsetRange(0, 10)}
    floors = {KEY0: 6}
    above = [r for r in _rows(0, 6, 10) if r[1] != 8]
    assert _judge(above, ranges, floors).partitions[0].missing == 1
    unfloored = _judge(_rows(0, 6, 10), ranges, {})
    assert not unfloored.conserved and unfloored.partitions[0].missing == 6
    duplicate = _judge(_rows(0, 6, 10) + _rows(0, 7, 8), ranges, floors)
    assert duplicate.partitions[0].duplicates == 1 and not duplicate.conserved
    another_id = _judge(_rows(0, 6, 10), ranges, {(OLD_TID, 0): 6})
    assert not another_id.conserved, "a floor of another topic id retires nothing of this one"


def test_resource_bounds_interrupted_maintenance_is_judged_but_never_conserved() -> None:
    report = _judge(_rows(0, 0, 4), {0: OffsetRange(0, 4)}, {}, lifted=True)
    assert report.conserved_rows and not report.conserved
    assert report.summary()["append_only_lifted"] is True


def test_resource_bounds_a_silver_start_after_version_zero_is_judged_only_once_floors_exist() -> (
    None
):
    assert start_problem("version:0", floors_exist=False) is None
    assert start_problem("version:0", floors_exist=True) is None
    assert START_WITHOUT_FLOORS in (start_problem("version:7", floors_exist=False) or "")
    assert start_problem("version:7", floors_exist=True) is None
    assert "not at a Bronze version" in (start_problem("snapshot", floors_exist=True) or "")


# ------------------------------------------------------------- the plan ---


SILVER = SilverPosition(
    "silver_transform_tx_raw_v1", 1, 4, True, (), MappingProxyType({KEY0: 12, KEY1: 12})
)


def _batch(number: int, highest0: int, highest1: int, *, read: bool = True) -> BronzeBatch:
    return BronzeBatch(
        CURRENT, number, 8, MappingProxyType({KEY0: highest0, KEY1: highest1}), 8 if read else 0
    )


def _evidence(**overrides: Any) -> RetentionEvidence:
    values: dict[str, Any] = {
        "table": "bronze.tx_raw_v1",
        "table_id": "table-id",
        "bronze_version": 6,
        "topic_id": TID,
        "floors": MappingProxyType({}),
        "append_only_lifted": False,
        "bronze_conserved": True,
        "bronze_problems": (),
        "bronze_ends": MappingProxyType({KEY0: 16, KEY1: 16}),
        "silver": (SILVER,),
        "batches": (
            _batch(0, 3, 3),
            _batch(1, 7, 7),
            _batch(2, 11, 11),
            _batch(3, 15, 15, read=False),
        ),
    }
    values.update(overrides)
    return RetentionEvidence(**values)


def test_resource_bounds_floors_advance_only_as_far_as_every_consumer_has_read() -> None:
    automatic = plan_retention(_evidence(), None)
    assert not automatic.blockers
    assert dict(automatic.floors) == {KEY0: 12, KEY1: 12}
    assert [b.batch_id for b in automatic.deletable] == [0, 1, 2]
    explicit = plan_retention(_evidence(), {KEY0: 8, KEY1: 8})
    assert dict(explicit.advanced) == {KEY0: (None, 8), KEY1: (None, 8)}
    assert [b.batch_id for b in explicit.deletable] == [0, 1]
    uneven = plan_retention(_evidence(), {KEY0: 8, KEY1: 4})
    assert [b.batch_id for b in uneven.deletable] == [0], "every row of a batch must be retired"
    unread = plan_retention(_evidence(batches=(_batch(0, 3, 3, read=False),)), {KEY0: 8})
    assert unread.deletable == (), "a batch Silver has not read whole is kept"
    unchanged = plan_retention(_evidence(floors=MappingProxyType({KEY0: 8, KEY1: 8})), None)
    assert dict(unchanged.advanced) == {KEY0: (8, 12), KEY1: (8, 12)}


@pytest.mark.parametrize(
    ("overrides", "requested", "message"),
    [
        ({}, {KEY0: 13}, "Silver checkpoint silver_transform_tx_raw_v1 v1 has read whole"),
        (
            {"silver": (replace(SILVER, ends=MappingProxyType({KEY0: 20, KEY1: 20})),)},
            {KEY0: 17},
            "where Bronze's checkpoint has consumed to",
        ),
        ({"floors": MappingProxyType({KEY0: 8})}, {KEY0: 4}, "never lowered"),
        ({}, {(TID, 5): 1}, "never read this partition"),
        ({"silver": ()}, None, "no Silver checkpoint has read"),
        (
            {"silver": (replace(SILVER, conserved=False, problems=("missing 2",)),)},
            None,
            "missing 2",
        ),
        (
            {"silver": (replace(SILVER, through_version=None),)},
            None,
            "read no whole Bronze version",
        ),
        (
            {"bronze_conserved": False, "bronze_problems": ("skipped [20, 25)",)},
            None,
            "retirement never hides",
        ),
    ],
)
def test_resource_bounds_a_floor_past_any_required_position_is_refused_naming_it(
    overrides: dict[str, Any], requested: dict[FloorKey, int] | None, message: str
) -> None:
    plan = plan_retention(_evidence(**overrides), requested)
    assert plan.deletable == () and dict(plan.advanced) == {}
    assert any(message in blocker for blocker in plan.blockers), plan.blockers


def test_resource_bounds_the_delete_names_whole_batches_and_bounds_offsets_below_floors() -> None:
    plan = plan_retention(_evidence(), {KEY0: 8, KEY1: 8})
    assert delete_predicate(plan) == (
        f"((bronze_checkpoint_id = '{CURRENT}' AND bronze_batch_id IN (0, 1))) AND "
        f"((kafka_topic_id = '{TID}' AND kafka_partition = 0 AND kafka_offset < 8) OR "
        f"(kafka_topic_id = '{TID}' AND kafka_partition = 1 AND kafka_offset < 8))"
    )
    hostile = replace(plan, deletable=(replace(plan.deletable[0], checkpoint_id="x' OR '1'='1"),))
    with pytest.raises(RetentionRefusedError, match="not a TRACE-X checkpoint app id"):
        delete_predicate(hostile)


# -------------------------------------------------------- VACUUM and OPTIMIZE ---

VACUUM: dict[str, Any] = {
    "table": "bronze.tx_raw_v1",
    "declared_hours": 168,
    "retain_hours": None,
    "session_conf": {},
    "removal_version": 6,
    "consumers": (ConsumerPin("Silver checkpoint silver_transform_tx_raw_v1 v1", 6),),
}


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"retain_hours": 1}, "below bronze.tx_raw_v1's declared deletedFileRetentionDuration"),
        ({"session_conf": {RETENTION_CHECK_CONF: "false"}}, "Delta's own retention check"),
        ({"session_conf": {"spark.sql.files.ignoreMissingFiles": "true"}}, "tolerate data loss"),
        ({"consumers": (ConsumerPin("Silver checkpoint q v1", 4),)}, "only through version 4"),
        ({"consumers": (ConsumerPin("Gold build 3", None),)}, "Gold build 3 has read or pinned"),
        ({"supported": False}, "never VACUUMs it"),
    ],
)
def test_resource_bounds_vacuum_is_refused_below_retention_without_the_check_or_behind_a_reader(
    overrides: dict[str, Any], message: str
) -> None:
    _, blockers = plan_vacuum(**{**VACUUM, **overrides})
    assert any(message in blocker for blocker in blockers), blockers
    assert plan_vacuum(**VACUUM) == (168, ())
    assert (
        plan_vacuum(**{**VACUUM, "removal_version": None, "consumers": (ConsumerPin("x", None),)})[
            1
        ]
        == ()
    )


def test_resource_bounds_optimize_is_refused_on_bronze_before_touching_the_table(
    tmp_path: Path,
) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    for ref in (TableRef(Tier.BRONZE, "tx_raw_v1"), RETENTION_AUDIT):
        with pytest.raises(RetentionRefusedError, match="OPTIMIZE is refused"):
            optimize_table(cast(Any, None), lake, ref)


def test_resource_bounds_one_maintenance_run_per_table_and_a_dead_owner_s_lock_is_taken_over(
    tmp_path: Path,
) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    table = TableRef(Tier.BRONZE, "tx_raw_v1")
    path = lake.root / LOCK_DIRNAME / "bronze_tx_raw_v1.lock"
    with (
        maintenance_lock(lake, table),
        pytest.raises(RetentionRefusedError, match="already running"),
        maintenance_lock(lake, table),
    ):
        pass
    assert not path.exists()

    class CrashError(Exception):
        pass

    with pytest.raises(CrashError), maintenance_lock(lake, table):
        raise CrashError
    assert not path.exists(), "an interrupted run releases its lock"

    done = subprocess.run(
        [sys.executable, "-c", "import os; print(os.getpid())"],
        capture_output=True,
        text=True,
        check=True,
    )
    path.mkdir(parents=True)
    owner = {"host": socket.gethostname(), "pid": int(done.stdout), "table": str(table)}
    (path / "owner.json").write_text(json.dumps(owner))
    with maintenance_lock(lake, table):
        assert json.loads((path / "owner.json").read_text())["pid"] != owner["pid"]
    path.mkdir(parents=True)
    (path / "owner.json").write_text(json.dumps({**owner, "host": "another-host"}))
    with (
        pytest.raises(RetentionRefusedError, match="already running"),
        maintenance_lock(lake, table),
    ):
        pass


# ------------------------------------------------------- coverage and retirement ---

COVERAGE_AT = dt.datetime(2026, 9, 15, 8, 0, tzinfo=dt.UTC)
COVERAGE_TID = "cov5M5d1ZQpW-lA3_T0Hn1w"


def _covered(seq: int, offset: int, *, topic_id: str | None = COVERAGE_TID) -> BronzeRecord:
    """One gateway observation of session `s1`, numbered `seq`, at Bronze offset `offset`."""
    arrived = COVERAGE_AT + dt.timedelta(seconds=offset)
    stamp = arrived - dt.timedelta(milliseconds=50)
    return BronzeRecord(
        topic=TX_SCORED_V1,
        partition=0,
        offset=offset,
        logged_at_us=(arrived - dt.datetime(1970, 1, 1, tzinfo=dt.UTC))
        // dt.timedelta(microseconds=1),
        timestamp_type=1,
        session_ids=(b"s1",),
        seqs=(str(seq).encode(),),
        producer="trace-gateway@0.1.0",
        ingested_at=stamp.isoformat().replace("+00:00", "Z"),
        topic_id=topic_id,
    )


COVERAGE_SESSION = SessionRow(
    session_id="s1",
    started_at=COVERAGE_AT - dt.timedelta(seconds=1),
    heartbeat_at=COVERAGE_AT + dt.timedelta(seconds=4),
    closed_at=COVERAGE_AT + dt.timedelta(seconds=5),
    last_seq=4,
)
COVERAGE_READ_AT = COVERAGE_AT + dt.timedelta(seconds=20)


def _coverage(records: list[BronzeRecord], **overrides: Any) -> Any:
    arguments: dict[str, Any] = {
        "bronze_high_water": COVERAGE_AT + dt.timedelta(seconds=10),
        "ledger_read_at": COVERAGE_READ_AT,
        "clock_margin_s": 1.0,
    }
    arguments.update(overrides)
    return coverage_from_bronze(records, [COVERAGE_SESSION], **arguments)


RETIRED_START = {(TX_SCORED_V1, COVERAGE_TID, 0): 2}
"""A floor that retires offsets 0 and 1: the start of session `s1`, numbers 1 and 2."""


def test_resource_bounds_a_row_is_retired_only_below_its_own_floor() -> None:
    floors = {(TX_SCORED_V1, COVERAGE_TID, 0): 2}
    assert is_retired(_covered(1, 0), floors) and is_retired(_covered(2, 1), floors)
    assert not is_retired(_covered(3, 2), floors)
    assert not is_retired(_covered(1, 0, topic_id=None), floors), "no topic id, no floor applies"
    assert not is_retired(_covered(1, 0, topic_id="another-topic-id"), floors)
    assert not is_retired(_covered(1, 0), {})


@pytest.mark.parametrize("deleted", [False, True], ids=["still present", "deleted"])
def test_resource_bounds_retired_numbers_are_neither_present_nor_gaps(deleted: bool) -> None:
    """A floor retires the start of a session. Whether the rows are still there or already deleted,
    their numbers are not losses -- and the region is not evidence of coverage either."""
    live = [_covered(3, 2), _covered(4, 3)]
    records = live if deleted else [_covered(1, 0), _covered(2, 1), *live]
    first_unretired = COVERAGE_AT + dt.timedelta(seconds=2)
    result = _coverage(records, floors=RETIRED_START, retired_through=first_unretired)

    assert result.retired == (0 if deleted else 2)
    assert result.observations == 2, "only the unretired rows are observations"
    assert result.retired_through == first_unretired
    losses = [gap for gap in result.coverage.gaps if gap.missing]
    assert losses == [], "the retired numbers are not reported as lost writes"
    assert [gap.missing for gap in result.retired_gaps] == [(1, 2)], "reported, apart from losses"

    span = [gap for gap in result.coverage.gaps if gap.session_id is None]
    assert len(span) == 1 and span[0].start == dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
    assert span[0].end == first_unretired + dt.timedelta(seconds=1)


def test_resource_bounds_a_session_cannot_vouch_across_a_retired_span() -> None:
    first_unretired = COVERAGE_AT + dt.timedelta(seconds=2)
    records = [_covered(3, 2), _covered(4, 3)]
    retired = _coverage(records, floors=RETIRED_START, retired_through=first_unretired)
    inside = COVERAGE_AT + dt.timedelta(seconds=1)
    after = first_unretired + dt.timedelta(seconds=4)
    assert not retired.coverage.vouches(inside, inside), "its record may have been retired"
    assert not retired.coverage.vouches(inside, after), "a range across the span never vouches"
    assert retired.coverage.vouches(after, after), "after the span the log still vouches"

    without = _coverage([_covered(1, 0), _covered(2, 1), *records])
    assert without.retired_through is None and without.retired_gaps == ()
    assert without.coverage.vouches(inside, inside), "without retention the same writes vouch"


def test_resource_bounds_an_unbounded_retired_region_vouches_for_nothing() -> None:
    result = _coverage(
        [_covered(3, 2)],
        floors=RETIRED_START,
        retired_open=True,
        retired_reasons=("tx.scored.v1[0]: every row below its floor is retired",),
    )
    (span,) = [gap for gap in result.coverage.gaps if gap.session_id is None]
    assert span.end is None, "an open region has no end, so nothing is ever vouched for"
    assert not result.coverage.vouches(COVERAGE_AT, COVERAGE_AT)
    assert not result.coverage.vouches(
        COVERAGE_AT + dt.timedelta(days=365), COVERAGE_AT + dt.timedelta(days=365)
    )
    assert result.retired_open and result.retired_reasons


def test_resource_bounds_hydration_claims_nothing_before_the_floor_and_nothing_when_open() -> None:
    """Amendment 1 §2: a retired Bronze region is one gap, so Redis hydration (ADR-0057 §4) refuses
    to claim before the floor with no change of its own. Real coverage into the real claim rule."""

    def claim_from(result: Any) -> Claim:
        return compute_claim(
            ClaimInputs(
                coverage=result.coverage,
                sessions=(COVERAGE_SESSION,),
                ledger_read_at=COVERAGE_READ_AT,
                clock_margin_s=1.0,
                begin_epoch_ms=to_millis(COVERAGE_AT + dt.timedelta(days=3)),
            )
        )

    first_unretired = COVERAGE_AT + dt.timedelta(seconds=2)
    live = [_covered(3, 2), _covered(4, 3)]
    unretired = claim_from(_coverage([_covered(1, 0), _covered(2, 1), *live]))
    assert unretired.claimable and unretired.since_ms == unretired.components["ledger_origin"]

    retired = claim_from(_coverage(live, floors=RETIRED_START, retired_through=first_unretired))
    assert retired.claimable
    assert retired.since_ms == after_ms(first_unretired + dt.timedelta(seconds=1) + FUTURE_SKEW)
    assert retired.since_ms == retired.components["coverage_gaps"], "the retired span decides"
    assert retired.since_ms > unretired.since_ms > to_millis(first_unretired)

    unbounded = claim_from(
        _coverage(live, floors=RETIRED_START, retired_open=True, retired_reasons=("open",))
    )
    assert not unbounded.claimable and unbounded.since_ms is None
    assert any("open gap" in reason for reason in unbounded.reasons)


def test_resource_bounds_the_maintenance_command_parses_floors_and_tables_strictly() -> None:
    assert parse_floors(["0=120", "3=7"]) == {0: 120, 3: 7}
    assert parse_floors(None) is None
    for bad in (["0"], ["a=1"], ["0=-1"], ["0=1", "0=2"]):
        with pytest.raises(ContractError):
            parse_floors(bad)
    assert parse_table("bronze.tx_raw_v1") == TableRef(Tier.BRONZE, "tx_raw_v1")
    for value in ("tx_raw_v1", "platinum.tx_raw_v1", "bronze.Bad-Name"):
        with pytest.raises((ContractError, LakeContractError)):
            parse_table(value)
