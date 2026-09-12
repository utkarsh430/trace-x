"""Great-circle distance and implied speed.

The impossible-travel label is defined by implied speed, so a distance error
becomes a wrong ground-truth label — and a wrong label is worse than a missing
one, because every metric computed against it looks fine.
"""

from __future__ import annotations

import math

import pytest
from hypothesis import given
from hypothesis import strategies as st

from trace_core.domain.geo import GeoPoint, haversine_km, implied_speed_kmh

pytestmark = pytest.mark.unit

LATS = st.floats(min_value=-89.9, max_value=89.9, allow_nan=False, allow_infinity=False)
LONS = st.floats(min_value=-179.9, max_value=179.9, allow_nan=False, allow_infinity=False)
POINTS = st.builds(GeoPoint, latitude=LATS, longitude=LONS)

LONDON = GeoPoint(51.5074, -0.1278)
NEW_YORK = GeoPoint(40.7128, -74.0060)
SYDNEY = GeoPoint(-33.8688, 151.2093)


@pytest.mark.parametrize(
    ("bad_lat", "bad_lon"),
    [(91.0, 0.0), (-91.0, 0.0), (0.0, 181.0), (0.0, -181.0)],
)
def test_out_of_range_coordinates_are_refused(bad_lat: float, bad_lon: float) -> None:
    with pytest.raises(ValueError, match="out of range"):
        GeoPoint(bad_lat, bad_lon)


@pytest.mark.parametrize(
    ("a", "b", "expected_km", "tolerance_km"),
    [
        # Published great-circle distances; tolerance covers the choice of
        # Earth radius, not an error in the formula.
        (LONDON, NEW_YORK, 5570, 20),
        (LONDON, SYDNEY, 16990, 40),
        (NEW_YORK, SYDNEY, 15990, 40),
    ],
    ids=["london-ny", "london-sydney", "ny-sydney"],
)
def test_known_distances(a: GeoPoint, b: GeoPoint, expected_km: float, tolerance_km: float) -> None:
    assert haversine_km(a, b) == pytest.approx(expected_km, abs=tolerance_km)


def test_antimeridian_is_a_short_hop_not_a_world_crossing() -> None:
    """A flat lat/lon approximation calls this ~40000 km. That would invent fraud."""
    west = GeoPoint(0.0, 179.9)
    east = GeoPoint(0.0, -179.9)
    assert haversine_km(west, east) < 30


def test_poles_are_not_degenerate() -> None:
    north, south = GeoPoint(90.0, 0.0), GeoPoint(-90.0, 0.0)
    assert haversine_km(north, south) == pytest.approx(20015, abs=20)


@given(a=POINTS, b=POINTS)
@pytest.mark.property
def test_distance_is_symmetric_and_non_negative(a: GeoPoint, b: GeoPoint) -> None:
    assert haversine_km(a, b) >= 0
    assert haversine_km(a, b) == pytest.approx(haversine_km(b, a), rel=1e-9)


@given(point=POINTS)
@pytest.mark.property
def test_distance_to_self_is_zero(point: GeoPoint) -> None:
    assert haversine_km(point, point) == pytest.approx(0.0, abs=1e-9)


@given(a=POINTS, b=POINTS, c=POINTS)
@pytest.mark.property
def test_triangle_inequality(a: GeoPoint, b: GeoPoint, c: GeoPoint) -> None:
    assert haversine_km(a, c) <= haversine_km(a, b) + haversine_km(b, c) + 1e-6


# ------------------------------------------------------------ speed -------


def test_same_instant_in_two_cities_is_infinitely_fast() -> None:
    """Zero elapsed time must be maximally impossible, not a ZeroDivisionError."""
    assert implied_speed_kmh(LONDON, NEW_YORK, 0) == math.inf
    assert implied_speed_kmh(LONDON, NEW_YORK, -10) == math.inf


def test_implied_speed_is_distance_over_time() -> None:
    # London -> New York in one hour implies roughly the full distance in km/h.
    one_hour = implied_speed_kmh(LONDON, NEW_YORK, 3600)
    assert one_hour == pytest.approx(haversine_km(LONDON, NEW_YORK), rel=1e-9)


def test_a_plausible_commute_is_not_impossible() -> None:
    """Guards the label against firing on ordinary movement."""
    home, office = GeoPoint(51.5074, -0.1278), GeoPoint(51.4545, -0.9781)
    assert implied_speed_kmh(home, office, 3600) < 120
