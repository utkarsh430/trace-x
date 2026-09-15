"""Conservation into Bronze on hand-built checkpoints and rows (Step 5; `P3.kafka-ingest`).

Every verdict below is derived by hand in its comment. Each failure kind has a conserved control
beside it, so a judgement that failed everything would fail here as surely as one that passed
everything.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from trace_core.domain.errors import CheckpointRefusedError
from trace_core.stream.bronze_conservation import (
    ConservationReport,
    ConsumedRanges,
    OffsetRange,
    PartitionVerdict,
    Record,
    VersionConsumption,
    consumed_ranges,
    judge_conservation,
    parse_batch_offsets,
    parse_initial_offsets,
    partition_rows_from_records,
    read_progress_around_snapshot,
    skipped_between_versions,
)
from trace_core.stream.checkpoints import SparkProgress
from trace_core.stream.lake import AppId

pytestmark = pytest.mark.unit

TOPIC = "tx.raw.v1"
CURRENT = "trace-x:bronze_ingest_tx_raw_v1:v2:00000000000040008000000000000002"
OLD = "trace-x:bronze_ingest_tx_raw_v1:v1:00000000000040008000000000000001"
METADATA = '{"batchWatermarkMs":0,"batchTimestampMs":1757923200000,"conf":{}}'
SOURCE = Path("checkpoint")
TOPIC_ID = "q1Z8-topic-id-one"
OTHER_TOPIC_ID = "r2Y9-topic-id-two"
"""A topic deleted and recreated gets a new id, and its offsets start again at 0."""


def _initial(directory: Path, offsets: str, *, nul: bool = True) -> None:
    path = directory / "sources" / "0" / "0"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((b"\x00" if nul else b"") + b"v1\n" + offsets.encode())


def _batch(directory: Path, batch: int, offsets: str) -> None:
    path = directory / "offsets" / str(batch)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"v1\n{METADATA}\n{offsets}")


# ------------------------------------------------------------ checkpoint files ---


@pytest.mark.parametrize("nul", [True, False])
def test_initial_offsets_are_read_with_or_without_the_compatibility_byte(nul: bool) -> None:
    raw = (b"\x00" if nul else b"") + b'v1\n{"tx.raw.v1":{"1":7,"0":0}}'
    assert parse_initial_offsets(raw, TOPIC, source=SOURCE) == {0: 0, 1: 7}


@pytest.mark.parametrize(
    "raw",
    [
        b'\x00v2\n{"tx.raw.v1":{"0":0}}',
        b"\x00v1\n",
        b"\x00v1\nnot json",
        b'\x00v1\n{"tx.scored.v1":{"0":0}}',
        b'\x00v1\n{"tx.raw.v1":{"0":-2}}',
        b'\x00v1\n{"tx.raw.v1":{"p0":0}}',
        b"\x00\xff\xfe",
    ],
)
def test_unreadable_initial_offsets_are_refused(raw: bytes) -> None:
    with pytest.raises(CheckpointRefusedError):
        parse_initial_offsets(raw, TOPIC, source=SOURCE)


def test_batch_offsets_are_the_third_line_of_a_one_source_offset_log() -> None:
    text = f'v1\n{METADATA}\n{{"tx.raw.v1":{{"0":12,"2":3,"1":0}}}}'
    assert parse_batch_offsets(text, TOPIC, source=SOURCE) == {0: 12, 1: 0, 2: 3}
    for bad in (
        f'v1\n{METADATA}\n{{"tx.raw.v1":{{"0":1}}}}\n{{"tx.raw.v1":{{"0":1}}}}',  # two sources
        f'v2\n{METADATA}\n{{"tx.raw.v1":{{"0":1}}}}',
        f"v1\n{METADATA}\nnot json",
    ):
        with pytest.raises(CheckpointRefusedError):
            parse_batch_offsets(bad, TOPIC, source=SOURCE)


# ------------------------------------------------------------ consumed ranges ---


def _ranges(directory: Path, progress: SparkProgress, written: int | None) -> ConsumedRanges:
    return consumed_ranges(
        topic=TOPIC,
        checkpoint_id=CURRENT,
        directory=directory,
        progress=progress,
        written_batch=written,
    )


def test_a_checkpoint_that_consumed_nothing_has_empty_ranges(tmp_path: Path) -> None:
    assert dict(_ranges(tmp_path, SparkProgress(), None).ranges) == {}
    _initial(tmp_path, '{"tx.raw.v1":{"0":0,"1":5}}')
    consumed = _ranges(tmp_path, SparkProgress(planned=frozenset({0})), None)
    assert dict(consumed.ranges) == {0: OffsetRange(0, 0), 1: OffsetRange(5, 5)}
    assert consumed.problems == () and consumed.through_batch is None


def test_the_range_ends_at_the_later_of_spark_s_commit_and_the_table_s_write(
    tmp_path: Path,
) -> None:
    _initial(tmp_path, '{"tx.raw.v1":{"0":0,"1":5}}')
    _batch(tmp_path, 0, '{"tx.raw.v1":{"0":4,"1":5}}')
    _batch(tmp_path, 1, '{"tx.raw.v1":{"0":9,"1":8}}')
    _batch(tmp_path, 2, '{"tx.raw.v1":{"0":9,"1":11}}')
    progress = SparkProgress(planned=frozenset({0, 1, 2}), committed=frozenset({0, 1}))
    committed = _ranges(tmp_path, progress, 1)
    assert committed.through_batch == 1
    assert dict(committed.ranges) == {0: OffsetRange(0, 9), 1: OffsetRange(5, 8)}
    # Batch 2 reached the table before Spark recorded it: its rows are in, all or none.
    written = _ranges(tmp_path, progress, 2)
    assert written.through_batch == 2 and written.problems == ()
    assert dict(written.ranges) == {0: OffsetRange(0, 9), 1: OffsetRange(5, 11)}
    # A batch Spark committed that never reached the table still counts: its rows must be there.
    unwritten = _ranges(tmp_path, progress, 0)
    assert unwritten.through_batch == 1


def test_a_consumed_range_that_cannot_be_trusted_is_a_problem(tmp_path: Path) -> None:
    progress = SparkProgress(planned=frozenset({0}), committed=frozenset({0}))
    _batch(tmp_path, 0, '{"tx.raw.v1":{"0":4}}')
    assert "no initial offsets" in _ranges(tmp_path, progress, 0).problems[0]

    _initial(tmp_path, '{"tx.raw.v1":{"0":6,"1":0}}')
    _batch(tmp_path, 0, '{"tx.raw.v1":{"0":4,"2":3}}')
    untrusted = _ranges(tmp_path, progress, 7)
    assert untrusted.through_batch == 0, "an unplanned batch never widens the range"
    problems = " ".join(untrusted.problems)
    assert "never planned" in problems  # the table recorded batch 7
    assert "went backwards" in problems  # partition 0: 4 < 6
    assert "absent from batch 0" in problems  # partition 1
    assert "appeared after the query started" in problems  # partition 2


# ------------------------------------------------------------------ judgement ---


def _judge(
    records: list[Record],
    ranges: dict[int, OffsetRange],
    *,
    foreign: int = 0,
    problems: tuple[str, ...] = (),
) -> tuple[bool, dict[int, PartitionVerdict]]:
    consumed = ConsumedRanges(TOPIC, CURRENT, 1, 1, 1, ranges, problems)
    report = judge_conservation(
        consumed,
        partition_rows_from_records(
            records, checkpoint_id=CURRENT, ranges=ranges, topic_id=TOPIC_ID
        ),
        table="bronze.tx_raw_v1",
        table_version=3,
        foreign_topic_rows=foreign,
    )
    return report.conserved, {v.partition: v for v in report.partitions}


def _complete(
    partition: int, start: int, end: int, writer: str = CURRENT, topic_id: str = TOPIC_ID
) -> list[Record]:
    return [(partition, offset, writer, topic_id) for offset in range(start, end)]


def test_every_consumed_offset_exactly_once_is_conserved() -> None:
    records = _complete(0, 0, 10) + _complete(1, 5, 8)
    conserved, verdicts = _judge(
        records, {0: OffsetRange(0, 10), 1: OffsetRange(5, 8), 2: OffsetRange(3, 3)}
    )
    assert conserved
    assert verdicts[2] == PartitionVerdict(2, OffsetRange(3, 3), 0, 0, 0)


def test_a_missing_offset_is_a_loss() -> None:
    records = [r for r in _complete(0, 0, 10) if r[1] != 4]
    conserved, verdicts = _judge(records, {0: OffsetRange(0, 10)})
    assert not conserved
    assert (verdicts[0].missing, verdicts[0].duplicates, verdicts[0].out_of_range) == (1, 0, 0)


def test_a_partition_with_no_rows_misses_its_whole_range() -> None:
    conserved, verdicts = _judge(_complete(0, 0, 3), {0: OffsetRange(0, 3), 1: OffsetRange(2, 6)})
    assert not conserved and verdicts[1].missing == 4 and verdicts[0].conserved


def test_an_offset_written_twice_is_a_duplicate_within_or_across_checkpoints() -> None:
    within = [*_complete(0, 0, 5), (0, 3, CURRENT, TOPIC_ID)]
    conserved, verdicts = _judge(within, {0: OffsetRange(0, 5)})
    assert not conserved and (verdicts[0].missing, verdicts[0].duplicates) == (0, 1)
    # A reset re-read offsets 3 and 4 that the superseded checkpoint had already written.
    across = _complete(0, 0, 5, OLD) + _complete(0, 3, 8)
    conserved, verdicts = _judge(across, {0: OffsetRange(3, 8)})
    assert not conserved and (verdicts[0].missing, verdicts[0].duplicates) == (0, 2)


def test_rows_an_earlier_checkpoint_wrote_below_the_current_start_are_not_a_duplicate() -> None:
    """A reset that continued at explicit offsets: v1 wrote [0, 3), v2 starts at 3."""
    records = _complete(0, 0, 3, OLD) + _complete(0, 3, 8)
    conserved, verdicts = _judge(records, {0: OffsetRange(3, 8)})
    assert conserved and verdicts[0] == PartitionVerdict(0, OffsetRange(3, 8), 0, 0, 0)


def test_rows_outside_the_consumed_range_or_on_an_unknown_partition_are_refused() -> None:
    beyond = [*_complete(0, 0, 5), (0, 5, CURRENT, TOPIC_ID)]
    conserved, verdicts = _judge(beyond, {0: OffsetRange(0, 5)})
    assert not conserved and (verdicts[0].missing, verdicts[0].out_of_range) == (0, 1)
    below = [(0, 1, CURRENT, TOPIC_ID), *_complete(0, 2, 5)]
    conserved, verdicts = _judge(below, {0: OffsetRange(2, 5)})
    assert not conserved and verdicts[0].out_of_range == 1
    unknown = [*_complete(0, 0, 5), (7, 0, CURRENT, TOPIC_ID)]
    conserved, verdicts = _judge(unknown, {0: OffsetRange(0, 5)})
    assert not conserved and verdicts[7] == PartitionVerdict(7, None, 0, 0, 1)


def test_rows_of_another_topic_or_a_checkpoint_problem_are_never_conserved() -> None:
    records = _complete(0, 0, 5)
    assert _judge(records, {0: OffsetRange(0, 5)})[0]
    assert not _judge(records, {0: OffsetRange(0, 5)}, foreign=1)[0]
    assert not _judge(records, {0: OffsetRange(0, 5)}, problems=("unverifiable start",))[0]


def test_a_start_above_zero_is_reported_as_started_after_trim_without_failing() -> None:
    consumed = ConsumedRanges(TOPIC, CURRENT, 0, 0, 0, {0: OffsetRange(40, 42)})
    report = judge_conservation(
        consumed,
        partition_rows_from_records(
            _complete(0, 40, 42), checkpoint_id=CURRENT, ranges=consumed.ranges, topic_id=TOPIC_ID
        ),
        table="bronze.tx_raw_v1",
        table_version=1,
        foreign_topic_rows=0,
        version=1,
        topic_id=TOPIC_ID,
    )
    assert report.conserved and report.started_after_trim
    assert report.summary()["partitions"] == {
        "0": {
            "range": [40, 42],
            "missing": 0,
            "duplicates": 0,
            "out_of_range": 0,
            "skipped": [],
            "other_topic_id": 0,
        }
    }


def test_an_offset_range_cannot_run_backwards() -> None:
    with pytest.raises(ValueError, match="invalid offset range"):
        OffsetRange(5, 4)
    with pytest.raises(ValueError, match="invalid offset range"):
        OffsetRange(-1, 4)


# ------------------------------------------------------- across checkpoint versions ---

V1, V2, V3 = (str(AppId.new("bronze_ingest_tx_raw_v1", n)) for n in (1, 2, 3))


def _history(
    records: list[Record], versions: list[tuple[str, str, dict[int, OffsetRange]]]
) -> ConservationReport:
    """Judge every version, in order, as `check_conservation` does: the last one is current and
    carries the earlier ones as superseded."""
    reports: list[ConservationReport] = []
    for number, (writer, topic_id, ranges) in enumerate(versions, start=1):
        reports.append(
            judge_conservation(
                ConsumedRanges(TOPIC, writer, 1, 1, 1, ranges),
                partition_rows_from_records(
                    records, checkpoint_id=writer, ranges=ranges, topic_id=topic_id
                ),
                table="bronze.tx_raw_v1",
                table_version=9,
                foreign_topic_rows=0,
                version=number,
                topic_id=topic_id,
                superseded=tuple(reports) if number == len(versions) else (),
            )
        )
    return reports[-1]


def test_a_reset_that_jumped_past_unread_offsets_is_never_conserved() -> None:
    """Critic finding A1, exactly as reproduced. v1 read offsets 0-19; 20-24 were trimmed before any
    checkpoint read them; the only remedy for the stopped query, a reset at earliest, began v2 at
    25. Judged alone, v2's range [25, 30) is complete, so this used to report conserved."""
    records = _complete(0, 0, 20, V1) + _complete(0, 25, 30, V2)
    report = _history(
        records, [(V1, TOPIC_ID, {0: OffsetRange(0, 20)}), (V2, TOPIC_ID, {0: OffsetRange(25, 30)})]
    )
    assert not report.conserved
    (verdict,) = report.partitions
    assert (verdict.missing, verdict.duplicates, verdict.out_of_range, verdict.skipped) == (
        0,
        0,
        0,
        5,
    )
    assert report.summary()["partitions"]["0"]["skipped"] == [[20, 25]]
    assert [s.conserved for s in report.superseded] == [True], "v1 itself lost nothing"
    assert not report.started_after_trim, "v1, the first to read the partition, began at 0"
    # Control: the same history, with v2 continuing exactly where v1 stopped.
    records = _complete(0, 0, 20, V1) + _complete(0, 20, 30, V2)
    control = _history(
        records, [(V1, TOPIC_ID, {0: OffsetRange(0, 20)}), (V2, TOPIC_ID, {0: OffsetRange(20, 30)})]
    )
    assert control.conserved and control.partitions[0].skipped == 0


def test_every_boundary_between_versions_is_judged_and_a_re_read_is_not_a_skip() -> None:
    """Critic finding C3: a version starting above an earlier version's end, one boundary later.
    v1 [0, 10); v2 re-read from 8 to 15 (a duplicate, never a skip); v3 began at 18, so 15-17 were
    skipped."""

    def version(number: int, writer: str, ranges: dict[int, OffsetRange]) -> VersionConsumption:
        return VersionConsumption(number, TOPIC_ID, ConsumedRanges(TOPIC, writer, 1, 1, 1, ranges))

    history = [
        version(1, V1, {0: OffsetRange(0, 10)}),
        version(2, V2, {0: OffsetRange(8, 15)}),
        version(3, V3, {0: OffsetRange(18, 20), 1: OffsetRange(4, 6)}),
    ]
    assert skipped_between_versions(history) == {0: (OffsetRange(15, 18),)}
    records = _complete(0, 0, 10, V1) + _complete(0, 8, 15, V2) + _complete(0, 18, 20, V3)
    records += _complete(1, 4, 6, V3)
    report = _history(
        records, [(e.consumed.checkpoint_id, TOPIC_ID, dict(e.consumed.ranges)) for e in history]
    )
    verdicts = {v.partition: v for v in report.partitions}
    assert not report.conserved
    assert (verdicts[0].skipped, verdicts[0].duplicates) == (3, 2)
    assert verdicts[1].conserved, "partition 1 was first read by v3: a later first start is no skip"
    assert report.started_after_trim, "partition 1's first reader began at 4"


def test_a_loss_inside_a_superseded_version_is_not_forgotten_by_a_reset() -> None:
    records = [r for r in _complete(0, 0, 10, V1) if r[1] != 4] + _complete(0, 10, 15, V2)
    report = _history(
        records, [(V1, TOPIC_ID, {0: OffsetRange(0, 10)}), (V2, TOPIC_ID, {0: OffsetRange(10, 15)})]
    )
    assert report.partitions[0].conserved, "v2's own range is complete"
    assert not report.superseded[0].conserved and report.superseded[0].partitions[0].missing == 1
    assert not report.conserved
    assert report.summary()["superseded"][0]["checkpoint_version"] == 1


def test_a_recreated_topic_is_its_own_offset_space() -> None:
    """Critic finding B4: after a topic is recreated, offsets 0..k exist under both ids. They are
    neither duplicates nor, compared with the old topic's end, a skip."""
    records = _complete(0, 0, 20, V1, TOPIC_ID) + _complete(0, 0, 5, V2, OTHER_TOPIC_ID)
    report = _history(
        records,
        [(V1, TOPIC_ID, {0: OffsetRange(0, 20)}), (V2, OTHER_TOPIC_ID, {0: OffsetRange(0, 5)})],
    )
    assert report.conserved, report.summary()
    assert not report.started_after_trim
    # The new topic's first reader began at 3: trimmed before Bronze read it, not a skip.
    records = _complete(0, 0, 20, V1, TOPIC_ID) + _complete(0, 3, 5, V2, OTHER_TOPIC_ID)
    later = _history(
        records,
        [(V1, TOPIC_ID, {0: OffsetRange(0, 20)}), (V2, OTHER_TOPIC_ID, {0: OffsetRange(3, 5)})],
    )
    assert later.conserved and later.started_after_trim


def test_duplicates_count_per_topic_id_and_a_row_under_another_id_is_refused() -> None:
    same = [*_complete(0, 0, 5), (0, 3, CURRENT, TOPIC_ID)]
    assert _judge(same, {0: OffsetRange(0, 5)})[1][0].duplicates == 1
    other = [*_complete(0, 0, 5), (0, 3, CURRENT, OTHER_TOPIC_ID)]
    conserved, verdicts = _judge(other, {0: OffsetRange(0, 5)})
    assert not conserved
    assert (verdicts[0].duplicates, verdicts[0].other_topic_id) == (0, 1)


# ----------------------------------------------------------------- read order ---


def test_progress_is_read_so_a_batch_planned_and_written_between_the_reads_is_no_problem(
    tmp_path: Path,
) -> None:
    """Critic finding B2. Spark plans batch 1 and the sink commits it after progress is read and
    before the table snapshot. Read in that order, batch 1 looked "never planned"."""
    _initial(tmp_path, '{"tx.raw.v1":{"0":0}}')
    _batch(tmp_path, 0, '{"tx.raw.v1":{"0":5}}')
    (tmp_path / "commits").mkdir()
    (tmp_path / "commits" / "0").write_text("v1\n{}")

    def snapshot() -> int:
        _batch(tmp_path, 1, '{"tx.raw.v1":{"0":9}}')  # Spark plans batch 1 ...
        (tmp_path / "commits" / "1").write_text("v1\n{}")  # ... and records it, all mid-read
        return 1  # the table's snapshot holds batch 1

    stale = SparkProgress.read(tmp_path)
    progresses, written = read_progress_around_snapshot({1: tmp_path}, snapshot)
    progress = progresses[1]
    assert "never planned" in " ".join(_ranges(tmp_path, stale, written).problems)
    assert progress.committed == frozenset({0}), "only commits the snapshot is sure to hold"
    assert progress.planned == frozenset({0, 1})
    consumed = _ranges(tmp_path, progress, written)
    assert consumed.problems == () and dict(consumed.ranges) == {0: OffsetRange(0, 9)}
