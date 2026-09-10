"""Version pin matrix (ADR-0018).

Spark 4.0 supports Java 17/21 ONLY. The reference machine defaults to Java 25,
which fails with an UnsupportedClassVersionError that explains nothing. Delta
4.0.1 with Spark 4.0.1 requires Hadoop 3.4.x; a mismatch surfaces as an opaque
NoSuchMethodError.

Pins are declared once in pyproject.toml and asserted before any Spark job runs.
They are also recorded in every evaluation run manifest, because a metric that
moved due to a silent runtime upgrade is a misleading result (ADR-0017).
"""

from __future__ import annotations

import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Final

_PYPROJECT: Final = Path(__file__).resolve().parents[3] / "pyproject.toml"


class PinMismatchError(RuntimeError):
    """A component's runtime version does not match its declared pin."""


@lru_cache(maxsize=1)
def load_pins(pyproject: Path | None = None) -> dict[str, str]:
    """Read the pin matrix from pyproject.toml — the single declaration point."""
    path = pyproject or _PYPROJECT
    data = tomllib.loads(path.read_text())
    try:
        pins = data["tool"]["trace_x"]["pins"]
    except KeyError as exc:  # pragma: no cover - configuration error
        raise PinMismatchError(f"no [tool.trace_x.pins] table in {path}") from exc
    return {str(k): str(v) for k, v in pins.items()}


PINS: Final[dict[str, str]] = load_pins()


def assert_pin(component: str, actual: str | None) -> None:
    """Raise PinMismatchError unless `actual` satisfies the declared pin.

    Prefix matching, so a pin of "3.4" accepts "3.4.1" — patch-level drift within
    a declared minor is tolerated; minor/major drift is not.
    """
    expected = PINS.get(component)
    if expected is None:
        raise PinMismatchError(f"'{component}' is not a declared pin; known: {sorted(PINS)}")
    if actual is None:
        raise PinMismatchError(
            f"{component} is not installed or not detectable (pin: {expected}). "
            f"Run `make doctor` for the fix."
        )
    if not str(actual).startswith(expected):
        raise PinMismatchError(
            f"{component} version {actual} does not match the declared pin {expected}. "
            f"Version drift invalidates benchmark comparability (ADR-0017/0018). "
            f"Run `make doctor`."
        )
