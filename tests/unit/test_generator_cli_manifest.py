"""`make seed --manifest`: a frozen manifest's configuration, generated exactly or refused.

eval-v2's configuration carries the eval-v2 gate, which the sizing options cannot express, so the
frozen dataset is materialised from its manifest. A manifest whose configuration digest disagrees,
or a generation that does not reproduce every recorded stream digest, is refused before any ground
truth is written."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

import pytest
from data.generator import cli
from data.generator.config import BaselineIdentityConfig, GeneratorConfig
from eval.track_a import freeze_candidate
from typer.testing import CliRunner, Result

pytestmark = pytest.mark.unit

START = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)


def _manifest(tmp_path: Path) -> tuple[Path, dict[str, Any]]:
    config = GeneratorConfig(
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
    frozen: dict[str, Any] = {
        "dataset_version": "dev-manifest-test",
        "config": json.loads(config.canonical_json()),
        "fraud_scenario_config_digest": config.digest(),
        **freeze_candidate.measure(config),
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(frozen), encoding="utf-8")
    return path, frozen


def _seed(manifest: Path, tmp_path: Path, *extra: str) -> Result:
    return CliRunner().invoke(
        cli.app,
        [
            "--manifest",
            str(manifest),
            "--sink",
            "none",
            "--no-groundtruth",
            "--record-dir",
            str(tmp_path / "records"),
            "--out",
            str(tmp_path / "out"),
            *extra,
        ],
    )


def test_a_frozen_manifest_is_generated_exactly(tmp_path: Path) -> None:
    manifest, frozen = _manifest(tmp_path)
    result = _seed(manifest, tmp_path)
    assert result.exit_code == 0, result.output
    assert "every recorded digest matches" in result.output
    (record,) = (tmp_path / "records").glob("*.json")
    written = json.loads(record.read_text(encoding="utf-8"))
    assert written["dataset_version"] == frozen["dataset_version"]
    assert written["dataset_digest"] == frozen["dataset_digest"]
    assert written["fraud_scenario_config_digest"] == frozen["fraud_scenario_config_digest"]


def test_a_stream_the_generation_does_not_reproduce_is_refused(tmp_path: Path) -> None:
    manifest, frozen = _manifest(tmp_path)
    streams = dict(frozen["streams"])
    topic = sorted(streams)[0]
    streams[topic] = {**streams[topic], "rows": streams[topic]["rows"] + 1}
    manifest.write_text(json.dumps({**frozen, "streams": streams}), encoding="utf-8")
    result = _seed(manifest, tmp_path)
    assert result.exit_code == 2
    assert "does not reproduce its manifest" in result.output


def test_a_manifest_that_misdescribes_its_configuration_or_version_is_refused(
    tmp_path: Path,
) -> None:
    manifest, frozen = _manifest(tmp_path)
    manifest.write_text(
        json.dumps({**frozen, "fraud_scenario_config_digest": "sha256:" + "0" * 64}),
        encoding="utf-8",
    )
    assert _seed(manifest, tmp_path).exit_code == 2
    manifest, _ = _manifest(tmp_path)
    assert _seed(manifest, tmp_path, "--dataset-version", "eval-v1").exit_code == 2
