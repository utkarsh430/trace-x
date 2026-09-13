"""Eviction must be visible, because an evicted feature is invisible by nature.

**The failure this guards.** Redis runs `allkeys-lru` under a 512 MB cap. When
it evicts a velocity key, the next read returns empty -- and an empty read is
exactly what an account with no history looks like. The feature resolves to
`INSUFFICIENT_HISTORY`, the rules over it abstain (ADR-0033), the score falls,
and a transaction that should have been CRITICAL is scored LOW with
`degraded=false`. The decision carries no indication that anything was lost.

ADR-0032 deliberately refused to fold `INSUFFICIENT_HISTORY` into `UNAVAILABLE`
because the two have different causes and remedies. Eviction breaks that
distinction from underneath: it produces the state that means "new account" for
a reason that means "provision more memory". Nothing in a single decision can
tell them apart, so the signal has to come from the store itself -- which is
what these gauges are for. A measured run reached 393,176 evicted keys with a
63% keyspace-miss rate and reported nothing at all.
"""

from __future__ import annotations

import pytest

from trace_core.observability.metrics import (
    HOT_PATH_METRICS,
    ONLINE_STORE_EVICTED_KEYS,
    ONLINE_STORE_MEMORY_BYTES,
    observe_reading,
    register_online_store_gauges,
)

pytestmark = pytest.mark.unit


def test_both_store_gauges_are_declared_hot_path_metrics() -> None:
    """Declared, so `make verify`'s metric-name gate keeps them honest."""
    assert ONLINE_STORE_EVICTED_KEYS in HOT_PATH_METRICS
    assert ONLINE_STORE_MEMORY_BYTES in HOT_PATH_METRICS


def test_registration_tolerates_a_store_that_cannot_answer() -> None:
    """A failing callback must not take the whole scrape down.

    `/metrics` serves every instrument the application writes. A callback that
    raised would fail the scrape, so an unreachable Redis would hide the latency
    histograms, the band counters and the degraded counters -- losing the metrics
    that describe the outage in order to report the outage.
    """
    calls: list[int] = []

    def _explodes() -> dict[str, int]:
        calls.append(1)
        raise RuntimeError("redis is unreachable")

    # Registration itself must not invoke the callback, and must not raise.
    register_online_store_gauges("trace_core.test.unreachable", {"features": _explodes})
    assert calls == [], (
        "the callback ran at registration time. It must only run when the meter "
        "collects, or a store that is down at start-up would prevent the gateway "
        "from starting rather than being reported as down."
    )


def test_an_empty_reading_publishes_nothing_rather_than_zero() -> None:
    """Absent is not zero, and reporting zero evictions would be a false negative.

    A gauge that published 0 when it could not read the store would say
    "nothing has been evicted" on exactly the occasions it does not know. That is
    the same class of error as imputing a missing feature to zero, which
    docs/DATA_ENGINEERING.md §5 prohibits for the same reason.
    """
    assert observe_reading({}, "evicted_keys") == [], (
        "an unreadable store published an observation. A gauge reading of 0 would be "
        "indistinguishable from a genuine 0 evictions, which is the false negative this "
        "whole mechanism exists to prevent."
    )
    assert observe_reading({"used_memory": 0}, "used_memory") != [], (
        "a genuine zero must still be published; only an ABSENT reading is withheld."
    )
    observations = observe_reading({"evicted_keys": 393_176}, "evicted_keys")
    assert len(observations) == 1
    assert observations[0].value == 393_176
