"""Stage 2 step 10: the eval-v2 candidate freeze records what its regeneration must reproduce."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import NoReturn

import pytest
from data.generator.config import CORRECTIONS, BaselineIdentityConfig, GeneratorConfig
from data.generator.lpc5 import declaration as d
from data.generator.lpc5 import run
from eval.track_a import freeze_candidate as freeze
from eval.track_a import lpc5_control

from trace_core.domain.enums import FraudPattern

pytestmark = pytest.mark.unit

START = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)


def _small() -> GeneratorConfig:
    return GeneratorConfig(
        seed=4242,
        row_count=4_000,
        account_count=240,
        merchant_count=60,
        device_count=290,
        ip_count=120,
        fraud_rate=0.02,
        start_at=START,
        end_at=START + dt.timedelta(days=59),
        baseline_identity=BaselineIdentityConfig(),
    )


@pytest.fixture
def target(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    path = tmp_path / "candidate.manifest.json"
    monkeypatch.setattr(freeze, "CANDIDATE", path)
    monkeypatch.setattr(freeze, "candidate_config", _small)
    monkeypatch.setattr(freeze, "is_dirty", lambda: False)
    return path


def test_the_candidate_is_eval_v1s_frozen_configuration_with_the_gate_at_its_defaults() -> None:
    frozen = json.loads(freeze.EVAL_V1.read_text(encoding="utf-8"))["config"]
    config = json.loads(freeze.candidate_config().canonical_json())
    gate = config.pop("baseline_identity")
    assert config == frozen
    assert gate == json.loads(BaselineIdentityConfig().model_dump_json())
    assert freeze.gate(freeze.candidate_config()) == {
        "corrections_applied": list(CORRECTIONS),
        "corrections_disabled": [],
    }


def test_a_frozen_candidate_is_reproduced_and_a_changed_stream_is_not(target: Path) -> None:
    assert freeze.main([]) == 0
    manifest = json.loads(target.read_text(encoding="utf-8"))
    assert set(manifest["streams"]) == {*d.TOPICS.values(), d.OUTCOME_STREAM}
    assert manifest["streams"][d.TOPICS[d.Population.TX]] == {
        "digest": manifest["dataset_digest"],
        "rows": manifest["row_count"],
    }
    assert all(stream["rows"] > 0 for stream in manifest["streams"].values())
    assert manifest["criterion"] == {
        "id": d.CRITERION_ID,
        "revision": d.REVISION,
        "sha256": d.CRITERION_SHA256,
    }
    assert set(manifest["scenarios"]) == {pattern.value for pattern in FraudPattern}
    added = sum(entry["added_by_g2"] for entry in manifest["scenarios"].values())
    assert manifest["coverage_floor"]["instances_added"] == added
    assert (manifest["coverage_floor"]["disclosure"] is None) == (added == 0)
    assert (
        sum(entry["fraudulent_transactions"] for entry in manifest["scenarios"].values())
        == manifest["fraudulent_transactions"]
    )
    assert manifest["dirty_worktree"] is False
    assert freeze.main(["--check"]) == 0

    manifest["streams"][d.TOPICS[d.Population.DEV]]["rows"] += 1
    target.write_text(json.dumps(manifest), encoding="utf-8")
    assert freeze.main(["--check"]) == 2


def test_a_criterion_revision_after_the_freeze_invalidates_the_candidate() -> None:
    config = _small()
    record = freeze.criterion()
    stale = {**record, "revision": d.REVISION - 1}
    manifest = {
        "fraud_scenario_config_digest": config.digest(),
        "gate": freeze.gate(config),
        "criterion": stale,
        "row_count": 1,
    }
    problems = freeze.mismatches(manifest, config, {"row_count": 1})
    assert len(problems) == 1
    assert problems[0].startswith("criterion:")


def test_a_dirty_tree_or_an_existing_candidate_is_refused(
    target: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(freeze, "is_dirty", lambda: True)
    assert freeze.main([]) == 2
    assert not target.exists()
    monkeypatch.setattr(freeze, "is_dirty", lambda: False)
    target.write_text("{}", encoding="utf-8")
    assert freeze.main([]) == 2
    assert target.read_text(encoding="utf-8") == "{}"


def test_the_acceptance_run_verifies_every_stream_before_any_check(
    target: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """§1.2: a moved stream stops the run before any check; a criterion other than the frozen one
    is refused; the reproduced candidate is judged."""
    assert freeze.main([]) == 0
    capsys.readouterr()
    monkeypatch.setattr(lpc5_control, "is_dirty", lambda: False)
    argv = ["--control", "eval-v2", "--candidate", str(target)]
    manifest = json.loads(target.read_text(encoding="utf-8"))

    def no_check(*args: object, **kwargs: object) -> NoReturn:
        raise AssertionError("a check ran on an unverified candidate")

    moved = json.loads(json.dumps(manifest))
    moved["streams"][d.OUTCOME_STREAM]["digest"] = "sha256:" + "0" * 64
    target.write_text(json.dumps(moved), encoding="utf-8")
    with monkeypatch.context() as patch:
        patch.setattr(run, "compute_tables", no_check)
        assert lpc5_control.main(argv) == 2
    assert "MISMATCH (invalid), before any check" in capsys.readouterr().out

    stale = json.loads(json.dumps(manifest))
    stale["criterion"]["revision"] -= 1
    target.write_text(json.dumps(stale), encoding="utf-8")
    assert lpc5_control.main(argv) == 2
    assert "refused (invalid)" in capsys.readouterr().out

    target.write_text(json.dumps(manifest), encoding="utf-8")
    assert lpc5_control.main(argv) in (0, 1)
    assert "candidate stream digests: verified (4 streams)" in capsys.readouterr().out


def test_finalize_records_files_provenance_and_the_lpc5_outcome(
    target: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Step 12: the frozen manifest adds the materialised files, the materialisation's provenance
    and LPC-5's recorded outcome, and refuses a materialisation that does not match or ran dirty."""
    from data.generator import cli
    from typer.testing import CliRunner

    assert freeze.main([]) == 0
    final = tmp_path / "eval-v2.manifest.json"
    monkeypatch.setattr(freeze, "FINAL", final)
    candidate = json.loads(target.read_text(encoding="utf-8"))
    out, records = tmp_path / "out", tmp_path / "records"
    result = CliRunner().invoke(
        cli.app,
        [
            "--manifest",
            str(target),
            "--sink",
            "parquet",
            "--no-groundtruth",
            "--out",
            str(out),
            "--record-dir",
            str(records),
        ],
    )
    assert result.exit_code == 0, result.output
    (record_path,) = records.glob("*.json")
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["dirty_worktree"] = False
    record_path.write_text(json.dumps(record), encoding="utf-8")
    dataset_dir = out / candidate["dataset_version"]
    argv = ["--finalize", "--run-record", str(record_path), "--dataset-dir", str(dataset_dir)]

    assert freeze.main(argv) == 0
    manifest = json.loads(final.read_text(encoding="utf-8"))
    assert set(manifest["files"]) == {f"{topic}.parquet" for topic in candidate["streams"]}
    assert manifest["run_id"] == record["run_id"]
    assert manifest["candidate_run_id"] == candidate["run_id"]
    assert manifest["streams"] == candidate["streams"]
    assert manifest["lpc5"]["strict_acceptance"] == "FAIL"
    assert manifest["lpc5"]["category_a_findings"] == 0
    assert manifest["lpc5"]["compliant"] is False
    assert any("not LPC-5 compliant" in line for line in manifest["disclosures"])
    assert any("natural fraud prevalence" in line for line in manifest["disclosures"])
    assert freeze.main(argv) == 2  # never re-written in place

    final.unlink()
    record["dataset_digest"] = "sha256:" + "0" * 64
    record_path.write_text(json.dumps(record), encoding="utf-8")
    assert freeze.main(argv) == 2
    record["dataset_digest"] = candidate["dataset_digest"]
    record["dirty_worktree"] = True
    record_path.write_text(json.dumps(record), encoding="utf-8")
    assert freeze.main(argv) == 2
    assert not final.exists()
