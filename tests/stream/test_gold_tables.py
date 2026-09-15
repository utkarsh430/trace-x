"""Gold on real Delta: declarations, primitives, build records, replay, refusals (ADR-0055).

Silver canonical rows are written directly, as fixtures (`gold_silver_rows`). Everything after that
is the code under test. What a context means is held to the literal fixtures in
tests/stream/test_feature_semantics_gold.py; these tests hold the build around it.
"""

from __future__ import annotations

import datetime as dt
import shutil
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from tests.stream.gold_silver_rows import rows_for_log, silver_row, write_silver

from trace_core.contracts.topics import IDENTITY_EVENTS_V1
from trace_core.domain.enums import AuthorizationOutcome, TransactionChannel
from trace_core.domain.errors import CheckpointRefusedError
from trace_core.domain.geo import GeoPoint, haversine_km
from trace_core.domain.time import event_time
from trace_core.features.observation import Event, authorization_observation
from trace_core.features.profile_math import whole_metres
from trace_core.features.semantics import FLOAT_PARITY_RELATIVE_TOLERANCE, Stream
from trace_core.stream import checkpoints
from trace_core.stream import gold_features as features
from trace_core.stream import gold_plan as plan
from trace_core.stream.checkpoints import DeltaSourceStart, SparkProgress
from trace_core.stream.gold import (
    build_gold,
    check_gold,
    compute_frames,
    create_gold_tables,
    gold_declarations,
    pin_sources,
    replace_table,
)
from trace_core.stream.lake import LakeConfig
from trace_core.stream.silver import create_silver_tables
from trace_core.stream.silver_rules import silver_topic
from trace_core.stream.tables import describe_live_table, require_no_drift

pytestmark = pytest.mark.stream

SHA = "7" * 40
T0 = event_time(dt.datetime(2026, 3, 1, 12, 0, tzinfo=dt.UTC))
BIG = 100_000_000_000
"""The largest released amount: its square does not fit a 64-bit integer."""


@pytest.fixture(scope="module")
def spark() -> Iterator[Any]:
    from trace_core.stream.session import build_session

    session = build_session("trace-x-gold-tables", driver_memory="2g")
    try:
        yield session
    finally:
        session.stop()


def _now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def _tx(event_id: str, seconds: float, **fields: Any) -> Event:
    values: dict[str, Any] = {
        "account_id": "acct_000001",
        "currency": "GBP",
        "amount_minor": 5_000,
        "merchant_id": "mrch_00001",
        "merchant_mcc": "5411",
        "merchant_country": "GB",
        "device_id": "dev_000001",
        "card_id": "card_000001",
        "ip_id": "ip_00001",
        "latitude": 51.5,
        "longitude": -0.12,
        "channel": TransactionChannel.CARD_PRESENT,
        "authorization_outcome": AuthorizationOutcome.DECLINED,
    }
    values.update(fields)
    return Event(
        stream=Stream.TRANSACTION,
        occurred_at=event_time(T0 + dt.timedelta(seconds=seconds)),
        event_id=event_id,
        **values,
    )


def _identity(event_id: str, seconds: float, stream: Stream) -> Event:
    return Event(
        stream=stream,
        occurred_at=event_time(T0 + dt.timedelta(seconds=seconds)),
        account_id="acct_000001",
        event_id=event_id,
    )


def _outcome(transaction: str, seconds: float, account: str = "acct_000001") -> Event:
    return authorization_observation(
        transaction_id=transaction,
        account_id=account,
        authorization_outcome=AuthorizationOutcome.DECLINED,
        decided_at=event_time(T0 + dt.timedelta(seconds=seconds)),
    )


def _history() -> list[Event]:
    return [
        _tx("tx_a", -120, amount_minor=BIG),
        _tx("tx_b", -110, amount_minor=BIG),
        _tx("tx_c", -30, account_id="acct_000002", currency="EUR"),
        _identity("evt_failed", -60, Stream.IDENTITY_FAILED_LOGIN),
        _outcome("tx_a", -100),
        _outcome("tx_c", -20),  # its transaction has another account: rejected
        _outcome("tx_unknown", -10),  # no transaction: pending
    ]


def _silver(spark: Any, lake: LakeConfig, events: list[Event], **kwargs: Any) -> None:
    rows = rows_for_log(events, **kwargs)
    # An identity event whose type feeds no stream is Silver history, but not an observation.
    topic, succeeded = silver_row(
        _identity("evt_succeeded", -50, Stream.IDENTITY_CHANGE),
        arrival=len(events),
        identity_type="LOGIN_SUCCEEDED",
    )
    assert topic == IDENTITY_EVENTS_V1
    rows.setdefault(topic, []).append(succeeded)
    write_silver(spark, lake, rows)


def _collect(spark: Any, lake: LakeConfig, ref: Any, version: int) -> list[tuple[Any, ...]]:
    frame = spark.read.format("delta").option("versionAsOf", str(version))
    return sorted(tuple(r) for r in frame.load(str(ref.local_path(lake))).collect())


def test_gold_tables_are_created_from_their_declarations(spark: Any, tmp_path: Path) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    create_gold_tables(spark, lake, git_sha=SHA, dirty_worktree=False)
    create_gold_tables(spark, lake, git_sha=SHA, dirty_worktree=False)  # an existing table is kept
    for declaration in gold_declarations():
        live = describe_live_table(spark, declaration.ref.path_identifier(lake))
        require_no_drift(declaration, live)
        assert (live.min_reader_version, live.min_writer_version) == (1, 2), declaration.ref
        assert (live.properties.get("delta.appendOnly") == "true") is (
            declaration.ref == plan.BUILDS
        )


def test_a_build_holds_every_observation_and_its_primitives_and_is_consistent(
    spark: Any, tmp_path: Path
) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    _silver(spark, lake, _history())
    first = build_gold(spark, lake, git_sha=SHA, dirty_worktree=False, now=_now())
    record = first.record
    assert (record.build_id, first.replayed) == (0, False)
    for ref in plan.GOLD_SOURCES:
        facts = checkpoints.snapshot_facts(spark, ref.local_path(lake))
        assert facts is not None and record.sources[plan.GOLD_SOURCES.index(ref)].version == (
            facts.version
        )
    assert record.lag_ms >= 0

    observations = {
        row["observation_identity"]: row
        for row in spark.read.format("delta")
        .option("versionAsOf", str(record.target(plan.OBSERVATIONS).version))
        .load(str(plan.OBSERVATIONS.local_path(lake)))
        .collect()
    }
    assert "identity_event:evt_succeeded" not in observations, "a type feeding no stream is not one"
    assert observations["identity_event:evt_failed"]["stream"] == Stream.IDENTITY_FAILED_LOGIN.value
    assert observations["transaction:tx_a"]["authorization_outcome"] is None, "never its own"
    verdicts = {
        row["event_id"]: row["verification"]
        for row in observations.values()
        if row["stream"] == Stream.AUTHORIZATION_OUTCOME.value
    }
    assert verdicts == {"tx_a": "VERIFIED", "tx_c": "REJECTED", "tx_unknown": "PENDING"}

    buckets = {
        (row["entity"], row["entity_id"], row["currency"]): row
        for row in _merchant_minute(spark, lake, record.target(plan.MINUTE_BUCKETS).version)
    }
    merchant = buckets[("MERCHANT", "mrch_00001", "GBP")]
    assert merchant["observation_count"] == 2
    assert merchant["amount_sum_minor"] == Decimal(2 * BIG)
    assert merchant["amount_sum_squares"] == Decimal(2 * BIG * BIG), "exact beyond 64 bits"
    distinct = _collect(
        spark, lake, plan.DISTINCT_BUCKETS, record.target(plan.DISTINCT_BUCKETS).version
    )
    merchant_accounts = {r[4] for r in distinct if r[:3] == ("MERCHANT", "mrch_00001", "ACCOUNT")}
    assert merchant_accounts == {"acct_000001", "acct_000002"}

    report = check_gold(spark, lake)
    assert report.consistent, report.summary()

    # Silver advances: a later build covers it; the earlier build's check still reads its own pins.
    late = _tx("tx_d", 30, amount_minor=1)
    extra = rows_for_log([late])
    write_silver(spark, lake, extra)
    second = build_gold(spark, lake, git_sha=SHA, dirty_worktree=False, now=_now())
    assert (second.record.build_id, second.replayed) == (1, False)
    assert second.record.sources[0].version == record.sources[0].version + 1
    assert second.record.target(plan.OBSERVATIONS).rows == record.target(plan.OBSERVATIONS).rows + 1
    assert check_gold(spark, lake).consistent

    # A Gold table replaced behind the build's back is reported, never trusted.
    shutil.rmtree(plan.TX_PROFILES.local_path(lake))
    create_gold_tables(spark, lake, git_sha=SHA, dirty_worktree=False)
    broken = check_gold(spark, lake)
    assert not broken.consistent and any("gold.tx_profiles" in p for p in broken.problems)


def _merchant_minute(spark: Any, lake: LakeConfig, version: int) -> list[Any]:
    return (
        spark.read.format("delta")
        .option("versionAsOf", str(version))
        .load(str(plan.MINUTE_BUCKETS.local_path(lake)))
        .filter("entity = 'MERCHANT'")
        .collect()
    )


def test_a_planned_build_that_never_committed_is_replayed_with_its_own_pins(
    spark: Any, tmp_path: Path
) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    _silver(spark, lake, _history())
    # The first part of a build, then a crash: planned, one table written, nothing committed.
    create_gold_tables(spark, lake, git_sha=SHA, dirty_worktree=False)
    opened = checkpoints.open_checkpoint(
        spark,
        lake,
        plan.GOLD_JOB,
        targets=list(plan.GOLD_TARGETS),
        sources=[DeltaSourceStart(ref) for ref in plan.GOLD_SOURCES],
        git_sha=SHA,
        dirty_worktree=False,
        now=_now(),
    )
    assert plan.next_build(SparkProgress.read(opened.directory)) == (0, False)
    planned = pin_sources(spark, lake, 0, _now())
    plan.write_plan(opened.directory, planned)
    frames = compute_frames(spark, lake, planned, plan.compile_windows())
    replace_table(opened, frames[plan.OBSERVATIONS], plan.OBSERVATIONS, 0)
    for frame in frames.values():
        frame.unpersist()

    # Silver advances before the restart.
    write_silver(spark, lake, rows_for_log([_tx("tx_after_crash", 40)]))
    replayed = build_gold(spark, lake, git_sha=SHA, dirty_worktree=False, now=_now())
    assert (replayed.record.build_id, replayed.replayed) == (0, True)
    assert replayed.record.sources == planned.sources, "a replay reads what its plan pinned"
    held = _collect(
        spark, lake, plan.OBSERVATIONS, replayed.record.target(plan.OBSERVATIONS).version
    )
    assert not [r for r in held if r[2] == "tx_after_crash"]
    from delta.tables import DeltaTable

    history = DeltaTable.forPath(spark, str(plan.OBSERVATIONS.local_path(lake))).history()
    merges = history.filter("operation = 'MERGE'").count()
    assert merges == 1, "the replay's write to the table the crash had written was skipped"
    assert check_gold(spark, lake).consistent

    following = build_gold(spark, lake, git_sha=SHA, dirty_worktree=False, now=_now())
    assert (following.record.build_id, following.replayed) == (1, False)
    held = _collect(
        spark, lake, plan.OBSERVATIONS, following.record.target(plan.OBSERVATIONS).version
    )
    assert [r for r in held if r[2] == "tx_after_crash"]


def test_gold_never_reads_is_late(spark: Any, tmp_path: Path) -> None:
    events = _history()
    built = {}
    for name, is_late in (("late", True), ("on_time", None)):
        lake = LakeConfig.at(tmp_path / name)
        _silver(spark, lake, events, is_late=is_late)
        record = build_gold(spark, lake, git_sha=SHA, dirty_worktree=False, now=_now()).record
        built[name] = {
            str(ref): _collect(spark, lake, ref, record.target(ref).version)
            for ref in plan.REPLACED_TABLES
        }
    assert built["late"] == built["on_time"]
    assert built["late"][str(plan.TX_WINDOWS)], "the comparison is not vacuous"


def test_a_build_refuses_a_missing_or_replaced_silver_table(spark: Any, tmp_path: Path) -> None:
    lake = LakeConfig.at(tmp_path / "lake")
    with pytest.raises(plan.GoldRefusedError, match="run Silver first"):
        build_gold(spark, lake, git_sha=SHA, dirty_worktree=False, now=_now())
    assert not (lake.root / "gold").exists(), "nothing is written before the refusal"

    _silver(spark, lake, _history())
    build_gold(spark, lake, git_sha=SHA, dirty_worktree=False, now=_now())
    identity = silver_topic(IDENTITY_EVENTS_V1)
    shutil.rmtree(identity.table.local_path(lake))
    create_silver_tables(spark, lake, IDENTITY_EVENTS_V1, git_sha=SHA, dirty_worktree=False)
    with pytest.raises(CheckpointRefusedError, match="table id"):
        build_gold(spark, lake, git_sha=SHA, dirty_worktree=False, now=_now())


def test_spark_distances_equal_python_in_whole_metres_and_within_float_tolerance(
    spark: Any,
) -> None:
    """The JVM's trigonometry is not bit-equal to the platform libm (ADR-0055, Risks). The medoid
    compares whole metres, which must agree; a distance must agree within the parity tolerance."""
    from pyspark.sql import functions as F  # noqa: N812

    points = [(0.0, lon) for lon in (0.0, 0.1, 0.35, 90.0, 179.0, -179.0, -178.5, 1.0, 5.0)]
    points += [(51.5, -0.12), (40.7, -74.0), (-33.9, 151.2), (89.9, 10.0), (-89.9, -170.0)]
    pairs = [(a, b) for a in points for b in points]
    frame = spark.createDataFrame(
        [((a[0], a[1]), (b[0], b[1])) for a, b in pairs],
        "p struct<lat:double,lon:double>, q struct<lat:double,lon:double>",
    )
    metres = frame.select(F.expr(features._haversine_metres_sql("p", "q")).alias("m")).collect()
    for (a, b), row in zip(pairs, metres, strict=True):
        expected = haversine_km(GeoPoint(*a), GeoPoint(*b))
        assert row["m"] == whole_metres(expected), (a, b)
        assert (
            abs(row["m"] / 1000 - expected) <= 0.0005 + FLOAT_PARITY_RELATIVE_TOLERANCE * expected
        )
