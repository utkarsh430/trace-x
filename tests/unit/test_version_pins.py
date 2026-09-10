"""Version pin matrix assertions (ADR-0018)."""

from __future__ import annotations

import pytest

from trace_core.config import PINS, PinMismatchError, assert_pin

pytestmark = pytest.mark.unit


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
