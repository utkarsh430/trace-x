"""Version pin matrix assertions (ADR-0018)."""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

import pytest

from trace_core.config import PINS, PinMismatchError, assert_pin

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


def test_all_required_pins_are_declared() -> None:
    """Every component whose drift breaks the stack must be pinned."""
    for component in ("python", "java", "spark", "delta", "hadoop", "scala"):
        assert component in PINS, f"{component} must be pinned (ADR-0018)"


def test_java_pin_is_spark_compatible() -> None:
    """Spark 4.0 supports Java 17/21 ONLY. Java 25 fails opaquely."""
    assert PINS["java"] in {"17", "21"}, (
        f"Java pin is {PINS['java']}, but Spark {PINS['spark']} supports 17/21 only"
    )


def test_delta_and_spark_majors_align() -> None:
    """Delta 4.x pairs with Spark 4.x; a mismatch surfaces as NoSuchMethodError."""
    assert PINS["delta"].split(".")[0] == PINS["spark"].split(".")[0]


def test_assert_pin_accepts_patch_drift() -> None:
    assert_pin("hadoop", "3.4.1")  # pin "3.4" tolerates patch level


def test_assert_pin_rejects_minor_drift() -> None:
    with pytest.raises(PinMismatchError, match="does not match the declared pin"):
        assert_pin("spark", "3.5.1")


def test_assert_pin_rejects_missing_component() -> None:
    with pytest.raises(PinMismatchError, match="not installed or not detectable"):
        assert_pin("spark", None)


def test_assert_pin_rejects_unknown_component() -> None:
    with pytest.raises(PinMismatchError, match="not a declared pin"):
        assert_pin("cobol", "85")


# --- non-Python tooling (ADR-0036) -----------------------------------------


def _tool_images() -> dict[str, str]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return dict(data["tool"]["trace_x"].get("tools", {}))


def test_every_tool_image_is_pinned_by_digest() -> None:
    """A tag can be moved; a digest cannot.

    These images are a CI gate (oasdiff) and a measurement instrument (k6).
    An image that changed under a stable tag would silently alter either what
    the build rejects or what a published benchmark number means -- the same
    failure the hashed Python lockfile exists to prevent.
    """
    images = _tool_images()
    assert images, "no pinned tool images declared; the gate would pass vacuously"
    for name, ref in images.items():
        assert "@sha256:" in ref, (
            f"tool image '{name}' is pinned as {ref!r}, without a digest. Pin it as "
            f"repo:tag@sha256:<digest> so a moved tag cannot change a gate or a benchmark."
        )
        tag, _, digest = ref.partition("@sha256:")
        assert ":" in tag, f"tool image '{name}' has no explicit tag: {ref!r}"
        assert re.fullmatch(r"[0-9a-f]{64}", digest), (
            f"tool image '{name}' has a malformed digest: {digest!r}"
        )
