"""Bronze rows to the coverage rule, without a JVM (Step 5; ADR-0051 §5).

The adapter decides which rows are observations, when each was written, and how far the log is known
to be read. `trace_core.observation.coverage.assess` decides what they cover. These tests hold the
adapter to its documented cases and check, through the real `assess`, that the two compose.
"""

from __future__ import annotations

import datetime as dt
import inspect
from typing import Any

import pytest

from trace_core.contracts.envelope import build_event
from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_RAW_V1, TX_SCORED_V1
from trace_core.domain.errors import ContractError, LakeContractError, NaiveDatetimeError
from trace_core.domain.time import event_time
from trace_core.observation.coverage import Observed, SessionRow
from trace_core.observation.log import SEQ_HEADER, SESSION_HEADER, Sequenced
from trace_core.observation.supervisor import LEASE_S, TAKEOVER_MARGIN_S
from trace_core.stream.bronze_coverage import (
    LEDGER_QUERY,
    BronzeRecord,
    assess_bronze_coverage,
    coverage_from_bronze,
    high_water_arrival,
    logged_at,
    observation_from_record,
    read_ledger,
    written_at,
)

pytestmark = pytest.mark.unit

AT = dt.datetime(2026, 9, 15, 7, 59, 58, 123_000, tzinfo=dt.UTC)
AT_US = (AT - dt.datetime(1970, 1, 1, tzinfo=dt.UTC)) // dt.timedelta(microseconds=1)
STAMP = AT - dt.timedelta(milliseconds=40)
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
GATEWAY = "trace-gateway@0.1.0"
MICROSECOND = dt.timedelta(microseconds=1)


def _text(moment: dt.datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def _record(
    topic: str = TX_SCORED_V1,
    *,
    sessions: tuple[bytes | None, ...] = (b"s1",),
    seqs: tuple[bytes | None, ...] = (b"1",),
    at_us: int = AT_US,
    timestamp_type: int = 1,
    producer: str | None = GATEWAY,
    ingested_at: str | None = _text(STAMP),
) -> BronzeRecord:
    return BronzeRecord(topic, 0, 0, at_us, timestamp_type, sessions, seqs, producer, ingested_at)


def test_a_record_with_one_session_and_one_number_is_that_observation_with_its_writer_s_stamp() -> (
    None
):
    assert observation_from_record(_record()) == Observed("s1", 1, AT, STAMP)
    assert logged_at(AT_US) == AT and logged_at(AT_US).tzinfo is dt.UTC


def test_the_adapter_reads_the_headers_exactly_as_the_observation_log_writes_them() -> None:
    headers = dict(Sequenced(session_id="0f" * 16, seq=123).headers)
    record = _record(sessions=(headers[SESSION_HEADER],), seqs=(headers[SEQ_HEADER],))
    assert observation_from_record(record) == Observed("0f" * 16, 123, AT, STAMP)


def test_the_stamp_read_is_the_one_every_envelope_carries() -> None:
    before = dt.datetime.now(dt.UTC)
    event = build_event(
        event_type="identity.events",
        occurred_at=event_time(AT),
        payload={"account_id": "acct_000001", "identity_event_type": "PASSWORD_CHANGE"},
        producer=GATEWAY,
        trace_id="0" * 32,
        correlation_id="idev_" + "0" * 32,
    )
    stamp = written_at(event["envelope"]["ingested_at"])
    assert stamp is not None and stamp.tzinfo is dt.UTC
    assert before <= stamp <= dt.datetime.now(dt.UTC)
    assert written_at("2026-09-15T09:59:58.083+02:00") == STAMP


@pytest.mark.parametrize(
    "seqs",
    [(b"0",), (b"-1",), (b"01",), (b"1.0",), (b"",), (b" 1",), (b"abc",), (b"9" * 20,), (None,),
     (b"1", b"2")],
)  # fmt: skip
def test_an_unusable_sequence_number_reaches_the_rule_as_no_number(
    seqs: tuple[bytes | None, ...],
) -> None:
    assert observation_from_record(_record(seqs=seqs)) == Observed("s1", None, AT, STAMP)


@pytest.mark.parametrize(
    "sessions", [(b"\xff\xfe",), (b"",), (b"   ",), (None,), (b"s1", b"s1"), (b"s1", b"s2")]
)
def test_an_unusable_session_id_reaches_the_rule_as_no_session(
    sessions: tuple[bytes | None, ...],
) -> None:
    assert observation_from_record(_record(sessions=sessions)) == Observed(None, 1, AT, STAMP)


@pytest.mark.parametrize(
    "stamp", [None, "", "yesterday", "2026-09-15T07:59:58.083", "2026-09-15", "1757923198"]
)
def test_a_record_whose_stamp_is_absent_unparseable_or_offsetless_is_a_gap_at_its_arrival(
    stamp: str | None,
) -> None:
    """Sound without a write time: its number is missing from its session, whose gap holds it."""
    assert observation_from_record(_record(ingested_at=stamp)) == Observed(None, None, AT, AT)


def test_a_scored_record_without_session_headers_is_a_gap_whoever_produced_it() -> None:
    for producer in (GATEWAY, "data.generator@1.0.0", None):
        record = _record(sessions=(), seqs=(), producer=producer)
        assert observation_from_record(record) == Observed(None, None, AT, STAMP)


@pytest.mark.parametrize(
    ("producer", "observed"),
    [
        (GATEWAY, True),
        ("data.generator@1.0.0", False),
        ("trace-gateway-shadow@0.1.0", False),
        ("", False),
        (None, False),
    ],
)
def test_an_identity_event_without_session_headers_is_a_gap_only_when_the_gateway_produced_it(
    producer: str | None, observed: bool
) -> None:
    record = _record(IDENTITY_EVENTS_V1, sessions=(), seqs=(), producer=producer)
    expected = Observed(None, None, AT, STAMP) if observed else None
    assert observation_from_record(record) == expected


def test_an_identity_event_with_session_headers_is_an_observation_whoever_produced_it() -> None:
    record = _record(IDENTITY_EVENTS_V1, sessions=(b"s9",), seqs=(b"4",), producer=None)
    assert observation_from_record(record) == Observed("s9", 4, AT, STAMP)


def test_a_record_not_on_log_append_time_or_not_on_a_covered_topic_is_refused() -> None:
    with pytest.raises(ContractError, match="LogAppendTime"):
        observation_from_record(_record(timestamp_type=0))
    with pytest.raises(ContractError, match="no observations"):
        observation_from_record(_record(TX_RAW_V1))


def test_the_high_water_is_the_slowest_consumed_partition_s_newest_arrival() -> None:
    newest = {
        (TX_SCORED_V1, 0): AT_US + 5_000_000,
        (TX_SCORED_V1, 1): AT_US,
        (IDENTITY_EVENTS_V1, 0): AT_US + 9,
    }
    consumed = [(TX_SCORED_V1, 0), (TX_SCORED_V1, 1), (IDENTITY_EVENTS_V1, 0)]
    assert high_water_arrival(newest, consumed) == AT
    assert high_water_arrival(newest, [*consumed, (IDENTITY_EVENTS_V1, 2)]) is None
    assert high_water_arrival(newest, []) is None


def _session(session_id: str, *, closed: bool, last_seq: int | None) -> SessionRow:
    return SessionRow(
        session_id=session_id,
        started_at=AT - dt.timedelta(minutes=5),
        heartbeat_at=AT,
        closed_at=AT + dt.timedelta(seconds=1) if closed else None,
        last_seq=last_seq,
    )


def test_bronze_rows_and_ledger_rows_compose_through_the_real_rule() -> None:
    records = [
        _record(sessions=(b"closed",), seqs=(b"1",)),
        _record(sessions=(b"closed",), seqs=(b"3",), ingested_at=_text(STAMP + MICROSECOND)),
        _record(IDENTITY_EVENTS_V1, sessions=(), seqs=(), producer="data.generator@1.0.0"),
        _record(IDENTITY_EVENTS_V1, sessions=(), seqs=(), producer=GATEWAY),
    ]
    mark = AT + dt.timedelta(seconds=1)
    result = coverage_from_bronze(
        records,
        [_session("closed", closed=True, last_seq=3)],
        bronze_high_water=mark,
        ledger_read_at=AT + dt.timedelta(seconds=2),
        clock_margin_s=1.0,
        table_versions={TX_SCORED_V1: 4, IDENTITY_EVENTS_V1: 2},
    )
    (gap,) = result.coverage.sessions["closed"].gaps
    assert gap.missing == (2,)
    assert len(result.coverage.unknown) == 1, "the gateway identity event without headers"
    assert (result.observations, result.not_observations, result.beyond_high_water) == (3, 1, 0)
    assert result.log_read_through == mark - MICROSECOND
    assert result.coverage.through == mark - MICROSECOND - dt.timedelta(seconds=1)
    assert result.bronze_high_water == mark and result.ledger_read_at == AT + dt.timedelta(
        seconds=2
    )
    assert dict(result.table_versions) == {TX_SCORED_V1: 4, IDENTITY_EVENTS_V1: 2}


def test_rows_at_or_after_the_high_water_are_left_out_and_their_numbers_are_missing() -> None:
    """Broker timestamps are milliseconds: a row AT the mark may have an unread neighbour."""
    records = [
        _record(sessions=(b"closed",), seqs=(b"1",), at_us=AT_US - 1_000),
        _record(sessions=(b"closed",), seqs=(b"2",), ingested_at=_text(STAMP + MICROSECOND)),
    ]
    result = coverage_from_bronze(
        records,
        [_session("closed", closed=True, last_seq=2)],
        bronze_high_water=AT,
        ledger_read_at=AT + dt.timedelta(seconds=2),
        clock_margin_s=1.0,
    )
    assert (result.observations, result.beyond_high_water) == (1, 1)
    (gap,) = result.coverage.sessions["closed"].gaps
    assert gap.missing == (2,), "not known to be read, so treated as lost"


def test_without_a_high_water_mark_gaps_are_still_reported_and_nothing_is_vouched_for() -> None:
    rows = [_session("closed", closed=True, last_seq=1)]
    records = [_record(sessions=(b"closed",), seqs=(b"1",))]
    read_at = AT + dt.timedelta(seconds=2)
    marked = coverage_from_bronze(
        records, rows, bronze_high_water=AT + dt.timedelta(seconds=1), ledger_read_at=read_at,
        clock_margin_s=1.0,
    )  # fmt: skip
    assert marked.coverage.vouches(STAMP, STAMP), "the same rows, with a mark, vouch for the write"
    reason = ("tx.scored.v1: batches up to 3 are done but the checkpoint holds no initial offsets",)
    unmarked = coverage_from_bronze(
        records, rows, bronze_high_water=None, ledger_read_at=read_at, clock_margin_s=1.0,
        high_water_withheld=reason,
    )  # fmt: skip
    assert (
        unmarked.coverage.gap_free and unmarked.coverage.sessions["closed"].certified_through == 1
    )
    assert unmarked.log_read_through is None and unmarked.coverage.through == EPOCH
    assert not unmarked.coverage.vouches(STAMP, STAMP)
    assert unmarked.high_water_withheld == reason
    with pytest.raises(LakeContractError, match="withheld"):
        coverage_from_bronze(
            [], [], bronze_high_water=AT, ledger_read_at=AT, clock_margin_s=1.0,
            high_water_withheld=reason,
        )  # fmt: skip


def test_the_writer_bounds_default_to_the_gateway_supervisor_s_own() -> None:
    for function in (coverage_from_bronze, assess_bronze_coverage):
        parameters = inspect.signature(function).parameters
        assert parameters["lease_s"].default == LEASE_S
        assert parameters["takeover_margin_s"].default == TAKEOVER_MARGIN_S
        assert parameters["clock_margin_s"].default is inspect.Parameter.empty, "it is measured"


def test_a_naive_high_water_or_read_time_is_refused() -> None:
    naive = AT.replace(tzinfo=None)
    for kwargs in ({"bronze_high_water": naive, "ledger_read_at": AT},
                   {"bronze_high_water": None, "ledger_read_at": naive}):  # fmt: skip
        with pytest.raises(NaiveDatetimeError):
            coverage_from_bronze([], [], clock_margin_s=1.0, **kwargs)


class _Ledger:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, params: tuple[object, ...]) -> _Ledger:
        self.calls.append((query, params))
        return self

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows


def test_the_ledger_is_read_in_one_statement_with_its_read_time() -> None:
    read_at = AT + dt.timedelta(seconds=3)
    later = dt.timezone(dt.timedelta(hours=2))
    ledger = _Ledger(
        [
            (read_at, "a", AT, AT, AT.astimezone(later), 4),
            (read_at, "b", AT, AT, None, None),
        ]
    )
    sessions, when = read_ledger(ledger)
    assert ledger.calls == [(LEDGER_QUERY, ("trace-gateway",))]
    assert when == read_at
    assert sessions == (
        SessionRow("a", AT, AT, AT, 4),
        SessionRow("b", AT, AT, None, None),
    )
    assert sessions[0].closed_at is not None and sessions[0].closed_at.tzinfo is dt.UTC
    assert read_ledger(_Ledger([(read_at, None, None, None, None, None)])) == ((), read_at)


def test_a_ledger_read_with_no_time_or_a_naive_time_is_refused() -> None:
    with pytest.raises(LakeContractError, match="no row"):
        read_ledger(_Ledger([]))
    with pytest.raises(NaiveDatetimeError):
        read_ledger(_Ledger([(AT.replace(tzinfo=None), None, None, None, None, None)]))
    with pytest.raises(NaiveDatetimeError):
        read_ledger(_Ledger([(AT, "a", AT.replace(tzinfo=None), AT, None, None)]))
