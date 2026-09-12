"""Legitimate behaviour: when people transact, where, and for how much.

Fraud is only detectable as a *departure*, so the baseline has to have shape. A
uniform-in-time, uniform-in-merchant, uniform-in-amount stream would make every
injected scenario trivially separable and every reported metric meaningless --
the model would be learning "this row is unusual for a uniform distribution",
which is not fraud detection.

Three shapes are therefore explicit:

* **Diurnal.** Card volume collapses overnight and peaks around midday and early
  evening. A velocity burst at 04:00 means something different from the same
  burst at 13:00, and that only holds if the baseline has a trough.
* **Weekly.** Weekends differ from weekdays.
* **Lognormal amounts.** Most transactions are small with a long right tail, so
  "anomalously high value" is a statement about a distribution rather than a
  threshold someone picked.
"""

from __future__ import annotations

import datetime as dt
import itertools
import math
import random
from typing import Final

from data.generator.population import KM_PER_DEGREE, AccountProfile
from trace_core.domain.geo import GeoPoint

# Relative volume by hour of day (UTC). Trough at ~04:00, lunch and evening peaks.
HOUR_WEIGHTS: Final[tuple[float, ...]] = (
    0.22,
    0.13,
    0.09,
    0.07,
    0.06,
    0.09,  # 00-05
    0.18,
    0.38,
    0.62,
    0.82,
    0.95,
    1.05,  # 06-11
    1.20,
    1.15,
    1.02,
    0.98,
    1.00,
    1.12,  # 12-17
    1.25,
    1.18,
    0.95,
    0.72,
    0.52,
    0.34,  # 18-23
)

# Monday .. Sunday.
WEEKDAY_WEIGHTS: Final[tuple[float, ...]] = (0.98, 0.96, 1.00, 1.05, 1.22, 1.15, 0.85)

_HOUR_CUM: Final[tuple[float, ...]] = tuple(itertools.accumulate(HOUR_WEIGHTS))
_WEEKDAY_CUM: Final[tuple[float, ...]] = tuple(itertools.accumulate(WEEKDAY_WEIGHTS))


def sample_occurred_at(rng: random.Random, start: dt.datetime, end: dt.datetime) -> dt.datetime:
    """A timestamp in `[start, end)` following the diurnal and weekly shape.

    Sampled directly rather than by rejection: rejection sampling would consume
    a variable number of draws per row, which makes the stream position depend
    on the values drawn and therefore on the config -- a determinism hazard for
    no benefit.
    """
    span_days = max(1, int((end - start).total_seconds() // 86400))

    # Pick a day, weighted by weekday.
    for _ in range(8):
        day_offset = rng.randrange(span_days)
        candidate = start + dt.timedelta(days=day_offset)
        weight = WEEKDAY_WEIGHTS[candidate.weekday()] / max(WEEKDAY_WEIGHTS)
        if rng.random() <= weight:
            break
    else:  # pragma: no cover - 8 rejections is ~1e-4 with these weights
        candidate = start + dt.timedelta(days=rng.randrange(span_days))

    target = rng.random() * _HOUR_CUM[-1]
    hour = next(h for h, cum in enumerate(_HOUR_CUM) if target <= cum)
    moment = candidate.replace(
        hour=hour,
        minute=rng.randrange(60),
        second=rng.randrange(60),
        microsecond=rng.randrange(0, 1_000_000, 1000),
    )
    return moment if moment < end else end - dt.timedelta(seconds=1)


def sample_amount_minor(rng: random.Random, profile: AccountProfile) -> int:
    """A lognormal amount in integer minor units.

    Clamped to at least one minor unit: a zero-value authorisation is a distinct
    thing (card verification) and generating it accidentally would pollute the
    amount distribution the anomaly scenarios are measured against.
    """
    raw = math.exp(rng.gauss(profile.amount_mu, profile.amount_sigma))
    return max(1, min(int(raw), 50_000_00))


def sample_location(rng: random.Random, home: GeoPoint, jitter_km: float) -> GeoPoint:
    """A point near `home`.

    Gaussian rather than uniform-in-a-box, so most spend is local and the
    occasional legitimate outlier exists -- which is what stops impossible-travel
    detection from being a trivial radius check.
    """
    lat_delta = rng.gauss(0.0, jitter_km / KM_PER_DEGREE)
    scale = max(0.2, math.cos(math.radians(home.latitude)))
    lon_delta = rng.gauss(0.0, jitter_km / (KM_PER_DEGREE * scale))
    return GeoPoint(
        latitude=round(max(-89.9, min(89.9, home.latitude + lat_delta)), 6),
        longitude=round(max(-179.9, min(179.9, home.longitude + lon_delta)), 6),
    )


def pick_merchant_index(
    rng: random.Random,
    profile: AccountProfile,
    cum_weights: tuple[float, ...],
    habitual_ratio: float,
) -> int | None:
    """A habitual merchant most of the time, otherwise one drawn by popularity.

    Returns None when the caller should use the popularity draw, so the habitual
    set stays the account's own and the tail stays global.
    """
    if profile.habitual_merchants and rng.random() < habitual_ratio:
        return None
    target = rng.random() * cum_weights[-1]
    low, high = 0, len(cum_weights) - 1
    while low < high:
        mid = (low + high) // 2
        if cum_weights[mid] < target:
            low = mid + 1
        else:
            high = mid
    return low
