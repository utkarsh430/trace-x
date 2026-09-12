"""Geography: points and great-circle distance.

The impossible-travel scenario (docs/FRAUD_SCENARIOS.md) is defined by implied
speed — distance over elapsed time — so distance has to be a real spherical
calculation, not a Euclidean approximation on latitude/longitude. Near the poles
and across the antimeridian a flat approximation is wrong by enough to change
the verdict, and a fraud label that depends on a rounding artefact is worthless
as ground truth.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Final

EARTH_RADIUS_KM: Final = 6371.0088
"""IUGG mean radius. The choice matters only to the fourth significant figure,
but it is pinned so a distance never moves between runs."""


@dataclass(frozen=True, slots=True)
class GeoPoint:
    """A WGS-84 latitude/longitude in decimal degrees."""

    latitude: float
    longitude: float

    def __post_init__(self) -> None:
        if not -90.0 <= self.latitude <= 90.0:
            raise ValueError(f"latitude out of range: {self.latitude}")
        if not -180.0 <= self.longitude <= 180.0:
            raise ValueError(f"longitude out of range: {self.longitude}")


def haversine_km(a: GeoPoint, b: GeoPoint) -> float:
    """Great-circle distance in kilometres.

    Haversine rather than the spherical law of cosines: the latter loses
    precision catastrophically for small distances because `acos` is
    ill-conditioned near 1, and most consecutive transactions for one account
    are a few kilometres apart.
    """
    lat1, lon1 = math.radians(a.latitude), math.radians(a.longitude)
    lat2, lon2 = math.radians(b.latitude), math.radians(b.longitude)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(min(1.0, h)))


def implied_speed_kmh(a: GeoPoint, b: GeoPoint, seconds: float) -> float:
    """Speed required to travel between two points in `seconds`.

    Zero or negative elapsed time yields infinity: two transactions at the same
    instant in different cities are maximally impossible, not a division error.
    """
    if seconds <= 0:
        return math.inf
    return haversine_km(a, b) / (seconds / 3600.0)
