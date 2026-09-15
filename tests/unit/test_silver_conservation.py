"""Bronze-to-Silver conservation's pure parts, without a JVM (ADR-0053 §5).

How far a Silver checkpoint has read its Bronze table is read from a Delta source offset; getting it
one version wrong would judge rows Silver never read, or skip rows it lost. The Spark half, which
counts coordinates and cross-checks the tables, is proved against real Delta in
tests/stream/test_silver_tables.py.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from trace_core.domain.errors import LakeContractError
from trace_core.stream.silver_conservation import (
    SilverConservationReport,
    data_files_in_commit,
    through_version,
)
from trace_core.stream.tables import DeltaSourceOffset

pytestmark = pytest.mark.unit


def _offset(version: int, index: int, *, starting: bool = False) -> DeltaSourceOffset:
    return DeltaSourceOffset("table-id", version, index, starting)


def test_a_base_index_means_every_earlier_version_was_read() -> None:
    assert through_version(_offset(5, -1), lambda v: 99) == (4, None)
    assert through_version(_offset(1, -1), lambda v: 99) == (0, None)


def test_offset_zero_minus_one_reads_nothing_and_is_refused() -> None:
    """Critic finding C7: it used to yield through version -1, judged as valid."""
    through, problem = through_version(_offset(0, -1), lambda v: 99)
    assert through is None and problem is not None and "(0, -1)" in problem


def test_the_last_data_file_of_a_version_means_that_version_was_read_whole() -> None:
    files = {7: 3}
    assert through_version(_offset(7, 2), files.__getitem__) == (7, None)
    through, problem = through_version(_offset(7, 1), files.__getitem__)
    assert through == 6 and problem is not None and "in part" in problem


def test_a_snapshot_offset_is_never_judged() -> None:
    through, problem = through_version(_offset(3, 4, starting=True), lambda v: 5)
    assert through is None and problem is not None and "snapshot" in problem


def test_data_files_are_the_add_actions_of_the_commit(tmp_path: Path) -> None:
    log = tmp_path / "_delta_log"
    log.mkdir()
    actions = [
        {"commitInfo": {"operation": "WRITE"}},
        {"add": {"path": "a.parquet", "dataChange": True}},
        {"add": {"path": "b.parquet", "dataChange": True}},
        {"add": {"path": "c.parquet", "dataChange": False}},
        {"remove": {"path": "old.parquet"}},
    ]
    (log / f"{4:020d}.json").write_text("\n".join(json.dumps(a) for a in actions) + "\n")
    assert data_files_in_commit(tmp_path, 4) == 2
    with pytest.raises(LakeContractError, match="missing"):
        data_files_in_commit(tmp_path, 5)


def _report(**overrides: object) -> SilverConservationReport:
    values: dict[str, object] = {
        "topic": "tx.raw.v1",
        "query": "silver_transform_tx_raw_v1",
        "checkpoint_version": 1,
        "through_version": 3,
        "bronze_rows": 10,
        "canonical": 6,
        "duplicates": 2,
        "quarantined": 2,
        "missing": 0,
        "double_counted": 0,
        "unexpected": 0,
        "bronze_repeated": 0,
    }
    values.update(overrides)
    return SilverConservationReport(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "overrides",
    [
        {"missing": 1},
        {"double_counted": 1},
        {"unexpected": 1},
        {"bronze_repeated": 1},
        {"canonical": 5},
        {"dangling_duplicates": 1},
        {"late_mismatched": 1},
        {"late_missing": 1},
        {"problems": ("a version read in part",)},
    ],
)
def test_any_unaccounted_double_foreign_or_inconsistent_row_is_not_conserved(
    overrides: dict[str, object],
) -> None:
    assert _report().conserved
    assert _report(superseded=1).conserved, "a superseded row is a duplicate, and is accounted"
    assert not _report(**overrides).conserved
