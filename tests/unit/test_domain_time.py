"""Timezone-aware UTC everywhere, with event time distinct from processing time.

ADR-0026 calls conflating the two the single most damaging shortcut available
here, because it corrupts windowed aggregates silently. The type-level
separation is checked by mypy; these tests check the runtime guard.
"""

from __future__ import annotations

import datetime as dt

import pytest
from hypothesis import given
from hypothesis import strategies as st

from trace_core.domain.errors import NaiveDatetimeError
from trace_core.domain.time import (
    UTC,
    event_time,
    from_millis,
    processing_time,
    require_aware_utc,
    to_millis,
    utc_now,
)

pytestmark = pytest.mark.unit


def test_naive_datetime_is_refused() -> None:
    """Guessing 'probably UTC' is how the bug ships; refusing is the only safe option."""
    with pytest.raises(NaiveDatetimeError, match="timezone-naive"):
        require_aware_utc(dt.datetime(2026, 9, 12, 14, 0, 0))


@pytest.mark.parametrize("bad", ["2026-09-12", 1757683200, None, dt.date(2026, 9, 12)])
def test_non_datetime_is_refused(bad: object) -> None:
    with pytest.raises(NaiveDatetimeError):
        require_aware_utc(bad)  # type: ignore[arg-type]


def test_aware_non_utc_is_normalised_not_rejected() -> None:
    """A correct offset is unambiguous, so it converts rather than raising."""
    tokyo = dt.timezone(dt.timedelta(hours=9))
    aware = dt.datetime(2026, 9, 12, 23, 0, 0, tzinfo=tokyo)
    out = require_aware_utc(aware)
    assert out.tzinfo == UTC
    assert out == dt.datetime(2026, 9, 12, 14, 0, 0, tzinfo=UTC)


def test_event_and_processing_time_are_separately_tagged() -> None:
    """They compare equal at runtime; mypy is what keeps them apart.

    This test documents that the *runtime* offers no protection, so nobody
    later concludes the distinction is enforced dynamically and drops the
    annotations.
    """
    moment = dt.datetime(2026, 9, 12, 14, 0, 0, tzinfo=UTC)
    assert event_time(moment) == processing_time(moment)


def test_event_time_rejects_naive_input() -> None:
    with pytest.raises(NaiveDatetimeError, match="occurred_at"):
        event_time(dt.datetime(2026, 9, 12))


def test_processing_time_rejects_naive_input() -> None:
    with pytest.raises(NaiveDatetimeError, match="ingested_at"):
        processing_time(dt.datetime(2026, 9, 12))


def test_utc_now_is_aware_and_utc() -> None:
    now = utc_now()
    assert now.tzinfo is not None
    assert now.utcoffset() == dt.timedelta(0)


@given(
    moment=st.datetimes(
        min_value=dt.datetime(1970, 1, 1),
        max_value=dt.datetime(2200, 1, 1),
        timezones=st.just(UTC),
    )
)
@pytest.mark.property
def test_millis_round_trip_to_the_millisecond(moment: dt.datetime) -> None:
    truncated = moment.replace(microsecond=(moment.microsecond // 1000) * 1000)
    assert from_millis(to_millis(truncated)) == truncated
