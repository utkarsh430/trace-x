"""The frozen partitions and lateness model, the PARITY record and the report (ADR-0056 §4, §6)."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from eval.parity.comparator import Observed, Pairing, ParityTally
from eval.parity.partition import (
    ADVERSARIAL,
    ADVERSARIAL_LATENESS,
    APPROXIMATE_RMS_BOUND,
    ARRIVAL_SKEW_BOUND,
    MIN_COMPARISONS_OVERALL,
    MIN_COMPARISONS_PER_STRATUM,
    REPRESENTATIVE,
    REPRESENTATIVE_LATENESS,
    LatenessModel,
    stratum_of,
)
from eval.parity.record import missing_fields, publishable_of, run_id_for, write_record
from eval.parity.report import CAVEAT, render
from eval.parity.skew import SkewTally
from eval.replay.faults import OVERLAY_VERSION, FaultClass, Timing

from trace_core.features.definitions import ONLINE_FEATURES

pytestmark = pytest.mark.parity

ROOT = Path(__file__).resolve().parents[2]


def test_the_frozen_declarations_have_not_moved() -> None:
    """Changing any parameter changes a digest; a new declaration needs a new ADR (ADR-0056 §4)."""
    assert REPRESENTATIVE_LATENESS.digest == (
        "sha256:95b79e09d3900513c496e41ee858b8cda17257e90e54d40f82d856438aa57e4d"
    )
    assert REPRESENTATIVE.digest == (
        "sha256:7b1237e98ed911a54094b7a8e536bf5998547fe8dfa9a4707730521dbbc21f8f"
    )
    assert ADVERSARIAL_LATENESS.digest == (
        "sha256:30b159ea35eb4d630abaf3c2a8337a82b03615b518c5b4350e84deb927c37e25"
    )
    assert ADVERSARIAL.digest == (
        "sha256:815e72584417f7159ed7885d42b2de45a7fc8c5419ebbb64b53fb84127350973"
    )
    assert (APPROXIMATE_RMS_BOUND, MIN_COMPARISONS_PER_STRATUM, MIN_COMPARISONS_OVERALL) == (
        0.01,
        200,
        1_000,
    )
    assert ARRIVAL_SKEW_BOUND == 0.01
    assert REPRESENTATIVE.gated and not ADVERSARIAL.gated


def test_adversarial_v2_differs_from_v1_only_in_its_reorder_rate_and_name() -> None:
    """v1 (reordered at 500 bp) was unsatisfiable on its own slice and never ran; v2 lowers that
    one rate to 300 under a new name (user-approved 2026-09-18). Rebuilding v1 from v2 by undoing
    exactly those two changes must reproduce v1's pinned digests, so nothing else moved."""
    assert ADVERSARIAL_LATENESS.name == "adversarial-ingress-v2"
    assert ADVERSARIAL_LATENESS.basis_points[FaultClass.REORDERED] == 300
    v1 = replace(
        ADVERSARIAL_LATENESS,
        name="adversarial-ingress-v1",
        basis_points={**ADVERSARIAL_LATENESS.basis_points, FaultClass.REORDERED: 500},
    )
    assert v1.digest == "sha256:e44ce34698bd95a7c64efa1b4d09154a6b1f156afb3bc4160a8a36095596f72b"
    assert replace(ADVERSARIAL, lateness=v1).digest == (
        "sha256:f686f127bbe847e850780d692e4075a30e72b10e9e4342ac88c360e3d58604aa"
    )
    assert v1.digest != ADVERSARIAL_LATENESS.digest


def test_the_representative_declaration_is_untouched_by_the_adversarial_change() -> None:
    assert REPRESENTATIVE_LATENESS.name == "representative-v1"
    assert REPRESENTATIVE.lateness is REPRESENTATIVE_LATENESS
    assert REPRESENTATIVE_LATENESS.basis_points[FaultClass.REORDERED] == 50


def test_the_partitions_name_the_frozen_eval_v2_dataset() -> None:
    manifest = json.loads((ROOT / REPRESENTATIVE.dataset_manifest).read_text())
    for partition in (REPRESENTATIVE, ADVERSARIAL):
        assert partition.dataset_digest == manifest["dataset_digest"]
        assert partition.dataset_version == manifest["dataset_version"] == "eval-v2"
        assert partition.warmup_start < partition.start < partition.end


def test_counts_round_half_up_from_basis_points() -> None:
    model = LatenessModel("m", 1, Timing.PACED, {FaultClass.LATE: 50})
    assert [model.counts(n)[FaultClass.LATE] for n in (99, 100, 300, 10_000)] == [0, 1, 2, 50]


@pytest.mark.parametrize(
    "fault", [FaultClass.CONFLICTING_DUPLICATE, FaultClass.MALFORMED_JSON, FaultClass.UNKNOWN_ENUM]
)
def test_classes_the_gateway_path_cannot_carry_are_refused(fault: FaultClass) -> None:
    with pytest.raises(ValueError, match="cannot be part of a parity overlay"):
        LatenessModel("m", 1, Timing.PACED, {fault: 1})


def test_the_representative_model_holds_nothing_back_and_plans_the_declared_counts() -> None:
    assert not REPRESENTATIVE_LATENESS.holds_back
    plan = REPRESENTATIVE_LATENESS.plan(10_000)
    assert dict(plan.counts) == {
        FaultClass.REORDERED: 50,
        FaultClass.EXACT_DUPLICATE: 50,
        FaultClass.RETRY_NEW_EVENT_ID: 20,
        FaultClass.LATE: 20,
        FaultClass.FUTURE_WITHIN_24H: 5,
    }
    assert plan.timing is Timing.PACED


def test_strata_are_decades_with_zero_apart() -> None:
    cases = {0: "0", 1: "1-9", 9: "1-9", 10: "10-99", 999: "100-999", 1_000: "1000+"}
    assert {k: stratum_of(k) for k in cases} == cases
    with pytest.raises(ValueError):
        stratum_of(-1)


def _record(**overrides: Any) -> dict[str, Any]:
    record: dict[str, Any] = {
        "record_type": "PARITY",
        "run_id": run_id_for(
            "representative",
            __import__("datetime").datetime(2026, 9, 15, 12, tzinfo=__import__("datetime").UTC),
            "a" * 16,
        ),
        "mode": "diagnostic",
        "publishable": False,
        "track": "SYNTHETIC",
        "git_commit_sha": "a" * 40,
        "dirty_worktree": True,
        "env_lock_digest": "sha256:" + "0" * 64,
        "python_version": "3.12.9",
        "started_at": "2026-09-15T12:00:00.000Z",
        "finished_at": "2026-09-15T12:10:00.000Z",
        "parity_semantics_version": 1,
        "feature_set_version": "3.0.0",
        "partition": {"name": "representative", "gated": True, "digest": REPRESENTATIVE.digest},
        "dataset": {"eval_v2_manifest_digest": None},
        "overlay": {"version": OVERLAY_VERSION, "seed": 1},
        "lateness_model": REPRESENTATIVE_LATENESS.as_dict(),
        "lateness_model_digest": REPRESENTATIVE_LATENESS.digest,
        "toolchain": {"spark": "4.0.1"},
        "gateway": {"target": "in-process", "image_digest": None},
        "counts": {"posts": {}},
        "results": {
            "as_served": ParityTally(Pairing.AS_SERVED).as_dict(["ip_distinct_accounts_1h"]),
            "event_time_complete": ParityTally(Pairing.EVENT_TIME_COMPLETE).as_dict(),
            "arrival_skew": SkewTally().as_dict(gated=True, measured=False),
        },
        "guard": {"violations": [{"kind": "OVERALL_BELOW_MINIMUM", "detail": "small"}]},
        "verdict": "DIAGNOSTIC",
    }
    record.update(overrides)
    return record


def test_a_complete_diagnostic_record_validates_and_is_never_publishable() -> None:
    assert _record()["run_id"] == "parity-20260915-120000-representative-aaaaaaaa"
    assert missing_fields(_record()) == []
    assert "publishable requires a measured run on a clean worktree" in missing_fields(
        _record(publishable=True)
    )
    assert "gateway" in missing_fields({k: v for k, v in _record().items() if k != "gateway"})


MEASURED_CLEAN = {
    "mode": "measured",
    "dirty_worktree": False,
    "dataset": {"eval_v2_manifest_digest": "sha256:" + "1" * 64},
    "gateway": {"target": "http://localhost:8010", "image_digest": "sha256:" + "2" * 64},
}


def test_a_measured_clean_fail_is_publishable_so_it_can_be_cited() -> None:
    """Publishable means citable, not passing: making a clean, measured FAIL uncitable would conceal
    an unfavourable result (CLAUDE.md §13.7, §17). The verdict stays FAIL."""
    assert publishable_of(measured=True, dirty=False, sha="a" * 40)
    failed = _record(**MEASURED_CLEAN, verdict="FAIL", publishable=True)
    assert missing_fields(failed) == []
    assert "Verdict: **FAIL**; publishable." in render([failed])


@pytest.mark.parametrize(
    ("measured", "dirty", "sha"),
    [(False, False, "a" * 40), (True, True, "a" * 40), (True, False, "unknown")],
)
def test_a_diagnostic_dirty_or_unattributed_run_is_not_publishable(
    measured: bool, dirty: bool, sha: str
) -> None:
    assert not publishable_of(measured=measured, dirty=dirty, sha=sha)


def test_a_record_claiming_publishable_at_an_unknown_commit_is_refused() -> None:
    problems = missing_fields(_record(**MEASURED_CLEAN, git_commit_sha="unknown", publishable=True))
    assert problems == ["publishable requires a resolved git_commit_sha"]
    dirty = _record(**{**MEASURED_CLEAN, "dirty_worktree": True}, publishable=True)
    assert missing_fields(dirty) == ["publishable requires a measured run on a clean worktree"]


def test_a_measured_record_needs_the_manifest_and_image_digests() -> None:
    problems = missing_fields(_record(mode="measured", dirty_worktree=False))
    assert problems == ["dataset.eval_v2_manifest_digest", "gateway.image_digest"]


def test_a_record_is_never_written_incomplete_or_twice(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        write_record({"record_type": "PARITY"}, tmp_path)
    path = write_record(_record(), tmp_path)
    assert json.loads(path.read_text())["run_id"] == _record()["run_id"]
    with pytest.raises(FileExistsError):
        write_record(_record(), tmp_path)


def test_the_report_is_read_from_records_and_cites_each_run_id() -> None:
    text = render([_record()])
    assert CAVEAT in text
    assert "run_id: parity-20260915-120000-representative-aaaaaaaa" in text
    assert "**NOT publishable**" in text
    assert "OVERALL_BELOW_MINIMUM" in text
    assert render([])  # renders the absence rather than failing
    stopped = render([_record(results={"error": "OrderEvidenceError: x"})])
    assert "stopped before comparing" in stopped


def test_the_report_states_the_excluded_unvouched_absences() -> None:
    """ADR-0046 §5's exclusions are reported, per feature and window, never dropped silently."""
    tally = ParityTally(Pairing.AS_SERVED)
    spec = ONLINE_FEATURES.get("card_tx_count_5m")
    unvouched = Observed("INSUFFICIENT_HISTORY", None, "INCOMPLETE")
    vouched_number = Observed("AVAILABLE", 2.0, "COMPLETE")
    tally.add("tx_1", spec, unvouched, vouched_number)
    tally.add("tx_2", spec, vouched_number, vouched_number)
    text = render(
        [
            _record(
                results={
                    "as_served": tally.as_dict(["ip_distinct_accounts_1h"]),
                    "event_time_complete": ParityTally(Pairing.EVENT_TIME_COMPLETE).as_dict(),
                    "arrival_skew": SkewTally().as_dict(gated=True, measured=False),
                }
            )
        ]
    )
    assert "Excluded, as-served absences the read did not vouch for" in text
    assert "card_tx_count_5m 5m': 1" in text
    assert "0.500000 of every comparison offered" in text
