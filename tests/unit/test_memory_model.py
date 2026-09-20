"""The memory model's arithmetic, without Redis (Phase 3 Step 12).

The measurement itself runs against a real Redis in the benchmark; these hold the pieces that turn
measured points into a projection, so a wrong interpolation or a wrong retention cap fails here.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


def _load() -> Any:
    spec = importlib.util.spec_from_file_location(
        "memory_model", ROOT / "benchmarks" / "features" / "memory_model.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["memory_model"] = module
    spec.loader.exec_module(module)
    return module


mm = _load()


def test_a_curve_interpolates_between_points_and_extends_the_last_segment() -> None:
    curve = mm.Curve("tx", "members", ((1, 100), (128, 1_370), (129, 2_000), (256, 17_240)))
    assert curve.bytes_for(0) == 0.0
    assert curve.bytes_for(1) == 100 and curve.bytes_for(128) == 1_370
    assert curve.bytes_for(64.5) == pytest.approx(100 + 1_270 * 63.5 / 127)
    assert curve.bytes_for(512) == pytest.approx(17_240 + (17_240 - 2_000) / 127 * 256)
    assert mm.Curve("obs", "one key", ((1, 150),)).bytes_for(10) == 150


def test_a_key_s_family_is_its_first_segment_inside_the_namespace() -> None:
    assert mm.family_of("mm:obs:transaction:tx_1") == "obs"
    assert mm.family_of("mm:hll:IP:ip_0000001:1790000000000") == "hll"
    with pytest.raises(ValueError):
        mm.family_of("f:obs:transaction:tx_1")


def _flat(bytes_per_key: float) -> dict[str, Any]:
    names = (
        "obs",
        "obs_identity",
        "obs_outcome",
        "aov",
        "tx",
        "card",
        "dev",
        "hll",
        "mcv",
        "ie",
        "ao",
        "aod",
        "pf",
        "pfh",
        "pfa",
        "pfl",
        "pfc",
        "epoch",
        "position",
        "hw",
    )
    return {name: mm.Curve(name, "x", ((1, bytes_per_key), (2, bytes_per_key))) for name in names}


RETENTION = {
    "raw_tx": 90_000.0,
    "raw_ie": 90_000.0,
    "profile": 2_595_600.0,
    "card": 3_900.0,
    "dev": 90_000.0,
    "hll": 7_500.0,
    "cv": 90_060.0,
    "ao": 7_200.0,
    "hll_bucket": 300.0,
    "minute": 60.0,
}


def test_observation_keys_are_every_observation_inside_the_raw_window() -> None:
    ten = {(x.family, x.scope): x for x in mm.project(_flat(100), RETENTION, horizon_s=600)}
    assert ten[("obs", "transaction")].keys == 500 * 600
    steady = {
        (x.family, x.scope): x
        for x in mm.project(_flat(100), RETENTION, horizon_s=RETENTION["profile"])
    }
    assert steady[("obs", "transaction")].keys == 500 * 90_000, "capped by the raw window"
    assert steady[("obs", "transaction")].total_bytes == 500 * 90_000 * 100


def test_a_folded_prefix_exists_only_for_history_beyond_the_raw_window() -> None:
    ten = {x.family for x in mm.project(_flat(100), RETENTION, horizon_s=600)}
    steady = {x.family for x in mm.project(_flat(100), RETENTION, horizon_s=RETENTION["profile"])}
    assert "pf..pfc" not in ten and "pf..pfc" in steady


def test_active_entities_never_exceed_their_pool() -> None:
    assert mm.active_entities(1_000, 500, 10_000_000) == pytest.approx(1_000)
    assert mm.active_entities(1_000, 0, 600) == 0.0
    for line in mm.project(_flat(100), RETENTION, horizon_s=RETENTION["profile"]):
        if line.family == "card":
            assert line.keys <= mm.CARDS


def test_the_limit_is_the_ten_minute_set_with_headroom_rounded_up_to_64_mib() -> None:
    assert mm.configured_limit_mib(548 * mm.MIB, 1.25) == 704
    assert mm.configured_limit_mib(64 * mm.MIB, 1.0) == 64
    assert mm.configured_limit_mib(64 * mm.MIB + 1, 1.0) == 128


def test_the_measured_image_is_the_compose_feature_store_s() -> None:
    assert mm.compose_store_image((ROOT / "deploy" / "compose.yml").read_text()) == mm.STORE_IMAGE
