"""Pure reductions over an account's observations that every implementation shares.

A declared estimator is only a single definition if it is computed by a single function.
The reference store, the Redis store and the offline implementation all reduce their own
state to the same inputs -- a list of located points, a list of amounts -- and hand them
to these functions, so the arithmetic that turns points into a home or amounts into a
median cannot drift between them (ADR-0046 §3).
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Sequence
from typing import Final

from trace_core.domain.geo import GeoPoint, haversine_km
from trace_core.features.context import MIN_OBSERVATIONS_FOR_ROBUST_Z
from trace_core.features.semantics import HOME_MIN_OBSERVATIONS

_METRES_PER_KM: Final = 1_000.0


def whole_metres(km: float) -> int:
    """`floor(d_m + 0.5)`: the rounding ADR-0046 declares before any medoid comparison."""
    return math.floor(km * _METRES_PER_KM + 0.5)


def geodesic_medoid(points: Sequence[tuple[int, str, float, float]]) -> tuple[float, float] | None:
    """The medoid of `(occurred_ms, identity, latitude, longitude)` points, or None.

    The caller supplies the declared sample -- the last `HOME_SAMPLE_SIZE` located
    observations strictly before `as_of` within the lifetime. The medoid is the sample
    point minimising the sum of whole-metre great-circle distances to the others; ties go
    to the earliest `(occurred_ms, identity)`. A medoid is always a point the account
    actually visited, and is defined on the sphere, so nothing here can break across the
    antimeridian the way a component-wise median of latitude and longitude does.
    """
    if len(points) < HOME_MIN_OBSERVATIONS:
        return None
    located = [GeoPoint(lat, lon) for _, _, lat, lon in points]
    best: tuple[int, int, str] | None = None
    chosen = 0
    for i, (occurred_ms, identity, _, _) in enumerate(points):
        total = sum(
            whole_metres(haversine_km(located[i], located[j])) for j in range(len(points)) if j != i
        )
        key = (total, occurred_ms, identity)
        if best is None or key < best:
            best, chosen = key, i
    return located[chosen].latitude, located[chosen].longitude


def robust_centre(amounts: Sequence[int]) -> tuple[float, float] | None:
    """Median and MAD of a declared amount sample, or None below the minimum.

    `statistics.median` takes the mean of the middle two for an even count, which is the
    definition ADR-0046 declares; an offline implementation must do the same.
    """
    if len(amounts) < MIN_OBSERVATIONS_FOR_ROBUST_Z:
        return None
    median = float(statistics.median(amounts))
    mad = float(statistics.median([abs(a - median) for a in amounts]))
    return median, mad


__all__ = ["geodesic_medoid", "robust_centre", "whole_metres"]
