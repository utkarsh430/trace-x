"""Each latency instrument must measure what its name says, and no more.

**This exists because one of them did not.** `tx_score_latency_seconds` was
recorded from a clock started before scoring and read *after* triage and the
observe-write. On the load profile ~92% of requests open an investigation, so
the metric named "score latency" was reporting a three-insert Postgres
transaction as scoring time -- and the number looked entirely plausible, which
is why nobody noticed until the two were measured separately and disagreed by a
factor of 40.

The failure mode is the one CLAUDE.md §4 asks about directly: *can this produce
a believable but incorrect result?* A latency histogram can, and a wrong one is
worse than a missing one, because capacity decisions get made from it.

So the invariant is a RELATIONSHIP, not a magic number (docs/TESTING.md §2
rule 4): scoring is a strict part of the request, therefore scoring latency must
be strictly less than request latency whenever the request did more than score,
and `latency_ms` in the response must be the scoring figure rather than the
request figure.
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest
from fastapi.testclient import TestClient
from services.gateway.app import GatewayState, create_app
from services.gateway.config import GatewaySettings
from services.gateway.pipeline import ScoringPipeline

from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.reference import ReferenceFeatureStore
from trace_core.observability.metrics import (
    ARCHITECTURE_DECLARED,
    HOT_PATH_METRICS,
    REQUEST_LATENCY,
    TX_SCORE_LATENCY,
    HotPathMetrics,
)
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.security.service_tokens import MIN_SECRET_LENGTH, ServiceTokenVerifier

pytestmark = pytest.mark.unit

SECRET = "s" * MIN_SECRET_LENGTH
TOKEN = f"psp-one.{SECRET}"


class _SlowStore:
    """A feature store with a deliberate delay, so scoring is measurably slow.

    The point is to make the two instruments diverge by an amount no clock
    resolution can explain. A store that answered instantly would let a test
    pass on two figures that were both ~0.
    """

    def __init__(self) -> None:
        self.inner = ReferenceFeatureStore()

    def snapshot(self, **kwargs: Any) -> Any:
        import time

        time.sleep(0.01)
        return self.inner.snapshot(**kwargs)

    def observe(self, event: Any) -> None:
        self.inner.observe(event)


def _client() -> TestClient:
    settings = GatewaySettings(
        redis_url="redis://localhost:1/0",
        redis_cache_url="redis://localhost:1/1",
        postgres_dsn="postgresql://nobody@localhost:1/none",
        rate_limit=10_000_000,
        rate_limit_window_s=60,
        pool_min_size=0,
        pool_max_size=1,
        log_json=False,
    )
    state = GatewayState(
        settings=settings,
        verifier=ServiceTokenVerifier({"psp-one": SECRET}),
        loader=default_loader(frozenset(ONLINE_FEATURES.ids)),
        pipeline=ScoringPipeline(
            pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
            thresholds=load_thresholds(),
            feature_store=_SlowStore(),
        ),
        metrics=HotPathMetrics(),
    )
    return TestClient(create_app(state))


def _transaction() -> dict[str, Any]:
    return {
        "transaction_id": "tx_scope_0000000001",
        "account_id": "acct_000001",
        "amount_minor": 5_000,
        "currency": "GBP",
        "occurred_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "merchant_id": "mrch_00001",
        "merchant_mcc": "5411",
        "merchant_country": "GB",
        "channel": "CARD_NOT_PRESENT",
        "entry_mode": "ECOMMERCE",
    }


def test_both_latency_instruments_are_declared() -> None:
    """Two instruments, because one cannot answer both questions."""
    assert TX_SCORE_LATENCY in HOT_PATH_METRICS
    assert REQUEST_LATENCY in HOT_PATH_METRICS
    assert {TX_SCORE_LATENCY, REQUEST_LATENCY} <= ARCHITECTURE_DECLARED, (
        "both latency histograms are design-level metrics and must be named in "
        "docs/ARCHITECTURE.md §13, so neither can be dropped silently."
    )


def _histogram_sum(exposition: str, name: str) -> float:
    for line in exposition.splitlines():
        if line.startswith(f"{name}_sum"):
            return float(line.rsplit(" ", 1)[1])
    raise AssertionError(f"{name}_sum absent from /metrics; the instrument was never recorded")


def test_request_latency_strictly_exceeds_scoring_latency() -> None:
    """The two instruments must not be measuring the same span.

    Asserting `latency_ms < round_trip` would be too weak to catch the original
    defect: a scoring figure that had silently widened to cover the whole
    request would still be under the round trip, because the client's own work
    is outside both. Comparing the two SERVER-side instruments to each other is
    the assertion that actually discriminates -- the request does strictly more
    than score, so its histogram must sum strictly higher.
    """
    with _client() as client:
        response = client.post(
            "/v1/transactions",
            json=_transaction(),
            headers={"Authorization": f"Bearer {TOKEN}", "X-Idempotency-Key": "scope-1"},
        )
        assert response.status_code == 200, response.text[:300]
        exposition = client.get("/metrics").text

    scoring = _histogram_sum(exposition, TX_SCORE_LATENCY)
    request = _histogram_sum(exposition, REQUEST_LATENCY)
    reported = response.json()["latency_ms"]

    assert scoring >= 0.010, (
        f"{TX_SCORE_LATENCY} summed to {scoring:.6f}s against a feature store that sleeps "
        f"10 ms per snapshot. It is not measuring the feature read it claims to cover."
    )
    assert request > scoring, (
        f"{REQUEST_LATENCY} ({request:.6f}s) is not greater than {TX_SCORE_LATENCY} "
        f"({scoring:.6f}s). The request does strictly more than score -- authentication, "
        f"the rate limit, the replay lookup, the observe-write, serialisation -- so equal "
        f"values mean one instrument has silently taken on the other's scope."
    )
    assert abs(reported / 1000.0 - scoring) < 0.002, (
        f"latency_ms ({reported / 1000.0:.6f}s) does not agree with {TX_SCORE_LATENCY} "
        f"({scoring:.6f}s). What the caller is told and what is graphed must be the same "
        f"measurement, or one of them is wrong and nobody can tell which."
    )
