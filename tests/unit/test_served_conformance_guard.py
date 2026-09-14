"""No run record may name a feature set its values were not computed with.

ADR-0046 declares feature set 2.0.0 before the online store serves it. The version stamped on
decisions and manifests is `FEATURE_SET_VERSION`, so until the served values conform, the tools
that produce run records must refuse -- mechanically, not because a sentence in PROGRESS asks them
to (critic finding on Phase 3 Step 1a).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

from trace_core.domain.errors import NonConformantFeatureSetError
from trace_core.features import spec

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


def _load_script(name: str, relative: str) -> Any:
    loaded = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert loaded is not None and loaded.loader is not None
    module = importlib.util.module_from_spec(loaded)
    sys.modules[name] = module
    loaded.loader.exec_module(module)
    return module


def _forbidden(*_args: object, **_kwargs: object) -> None:
    raise AssertionError("work began before the conformance refusal")


def test_the_guard_refuses_exactly_while_the_served_values_do_not_conform(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(spec, "SERVED_FEATURES_CONFORM", False)
    with pytest.raises(NonConformantFeatureSetError, match=spec.FEATURE_SET_VERSION):
        spec.require_served_conformance("a run")
    monkeypatch.setattr(spec, "SERVED_FEATURES_CONFORM", True)
    spec.require_served_conformance("a run")


def test_the_load_harness_refuses_before_doing_anything(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    harness = _load_script("load_gateway_conformance_guard", "scripts/load_gateway.py")
    monkeypatch.setattr(spec, "SERVED_FEATURES_CONFORM", False)
    monkeypatch.setattr(harness, "k6_image", _forbidden)
    assert harness.main([]) == 2
    assert "refused" in capsys.readouterr().err


def test_the_gateway_replay_refuses_before_reading_the_dataset(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    replay = _load_script("gateway_replay_conformance_guard", "eval/replay/gateway_replay.py")
    monkeypatch.setattr(spec, "SERVED_FEATURES_CONFORM", False)
    monkeypatch.setattr(replay, "load_streams", _forbidden)
    monkeypatch.setattr(sys, "argv", ["gateway_replay.py", "--dataset-version", "eval-v1"])
    assert replay.main() == 2
    assert "refused" in capsys.readouterr().err


def test_the_gateway_replay_refuses_to_derive_outcomes_without_a_seed(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    """Conforming served values are not enough: a dataset without an outcome stream derives its
    outcomes by DM-1, which needs the source manifest's seed (ADR-0049 §7). Without it every
    replayed declined ratio would be absent, in a report naming the feature set that reads them."""
    replay = _load_script("gateway_replay_outcome_guard", "eval/replay/gateway_replay.py")
    monkeypatch.setattr(spec, "SERVED_FEATURES_CONFORM", True)
    monkeypatch.setattr(replay, "load_streams", _forbidden)
    monkeypatch.setattr(
        sys,
        "argv",
        ["gateway_replay.py", "--dataset-version", "eval-v1", "--dataset-dir", str(tmp_path)],
    )
    assert replay.main() == 2
    assert "authorization outcomes" in capsys.readouterr().err
