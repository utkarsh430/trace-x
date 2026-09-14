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

**And the first version of this test then gave a believable but incorrect
result of its own.** `/metrics` serves the process-global registry:
`configure_telemetry` installs one `MeterProvider` per process, every
`HotPathMetrics()` constructed after it writes to the same instruments, and a
histogram's `_sum` on the exposition is therefore cumulative over every request
any test in the process has made -- a fresh `TestClient` isolates nothing. The
test compared one response's `latency_ms` against that sum. Locally the seven
requests an earlier test file makes summed to under a millisecond and the 2 ms
tolerance hid them; on a slower CI runner they summed to six and it failed,
repeatedly, on a defect that did not exist. The invariants below are stated as
before/after deltas over exactly one transaction, which is what they were
always about, and the tolerance is now the one the measurement justifies rather
than the one that happened to pass.

So the invariant is a RELATIONSHIP, not a magic number (docs/TESTING.md §2
rule 4): scoring is a strict part of the request, therefore scoring latency must
be strictly less than request latency whenever the request did more than score,
and `latency_ms` in the response must be the scoring figure rather than the
request figure.
"""

from __future__ import annotations

import datetime as dt
import re
import time
from dataclasses import dataclass
from typing import Any, Final

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

READ_DELAY_S: Final = 0.010
"""Added to the feature read, which is INSIDE scoring."""
WRITE_DELAY_S: Final = 0.010
"""Added to the replay-cache write, which is inside the request and OUTSIDE scoring. (The
feature-store write is no longer a separate step: since ADR-0046 it is part of the atomic
scoring read, so it sits on the other side of the boundary.)"""
CLOCK_SLACK_S: Final = 0.001
"""Timer granularity. `time.sleep` is a lower bound only in principle; a
millisecond of slack is an order of magnitude below either delay, so it cannot
mask a scope that has moved by one."""
AGREEMENT_S: Final = 1e-6
"""How far `latency_ms` may sit from the recorded scoring figure.

They are the SAME number: the pipeline computes `latency_ms` once and `_record`
records exactly that value divided by a thousand, which is the division this
test performs on the response. What separates them is float arithmetic on the
cumulative sum -- one ulp of a value measured in seconds -- so a microsecond is
six orders of magnitude of headroom. A tolerance in milliseconds would have let
a second clock read quietly replace the shared value, and then the caller and
the dashboard would once again be told two different things."""


class _SlowStore:
    """A feature store whose atomic scoring read is deliberately slow: INSIDE scoring."""

    def __init__(self) -> None:
        self.inner = ReferenceFeatureStore()

    def score(self, event: Any) -> Any:
        time.sleep(READ_DELAY_S)
        return self.inner.score(event)

    def snapshot(self, **kwargs: Any) -> Any:
        return self.inner.snapshot(**kwargs)


class _SlowReplayCache:
    """A replay cache whose write is deliberately slow: inside the request, OUTSIDE scoring.

    Two delays, one on each side of the boundary, because one cannot tell the instruments
    apart. With only a slow read, a scoring instrument that had silently widened to the whole
    request would trail the request instrument by microseconds of framework overhead -- an
    amount `request > scoring` could pass on by luck. With the cache write delayed as well, the
    gap between the two instruments is `WRITE_DELAY_S` wherever the boundary is right and ~0
    wherever it is wrong.
    """

    def lookup(self, idempotency_key: str, payload: object) -> None:
        return None

    def remember(self, idempotency_key: str, payload: object, response: str) -> None:
        time.sleep(WRITE_DELAY_S)


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
        idempotency=_SlowReplayCache(),  # type: ignore[arg-type]
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


# --- reading the exposition ------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _Histogram:
    """One instrument's `_sum` and `_count`, cumulative since the process started."""

    total: float
    count: int


_SERIES: Final = re.compile(r"^(?P<name>[a-z_]+)_(?P<part>sum|count)(?:\{[^}]*\})? (?P<value>\S+)$")


def _histogram(exposition: str, name: str) -> _Histogram:
    """The instrument's cumulative state, or zero if it has never been recorded.

    Exactly one series per part. The instruments carry no attributes of their
    own (the exporter adds only its `otel_scope_*` labels), so a second `_sum`
    line would mean a label had appeared and the series could no longer be read
    as one measurement -- which this refuses to do silently.
    """
    parts: dict[str, list[float]] = {"sum": [], "count": []}
    for line in exposition.splitlines():
        match = _SERIES.match(line)
        if match and match.group("name") == name:
            parts[match.group("part")].append(float(match.group("value")))
    for part, values in parts.items():
        assert len(values) <= 1, (
            f"{name}_{part} appears {len(values)} times on /metrics; the instrument has "
            f"grown a label and its series can no longer be compared as one measurement"
        )
    return _Histogram(
        total=parts["sum"][0] if parts["sum"] else 0.0,
        count=int(parts["count"][0]) if parts["count"] else 0,
    )


def _one_transaction(client: TestClient, key: str) -> tuple[Any, dict[str, _Histogram]]:
    """Post one transaction; return the response and each histogram's DELTA.

    Scraped before and after, because the registry is per process and the sums
    are cumulative: what one request contributed is `after - before`, and a
    test that reads `after` alone is reading every earlier test as well.
    """
    names = (TX_SCORE_LATENCY, REQUEST_LATENCY)
    before = {n: _histogram(client.get("/metrics").text, n) for n in names}
    response = client.post(
        "/v1/transactions",
        json=_transaction(),
        headers={"Authorization": f"Bearer {TOKEN}", "X-Idempotency-Key": key},
    )
    after = {n: _histogram(client.get("/metrics").text, n) for n in names}
    deltas = {
        n: _Histogram(
            total=after[n].total - before[n].total, count=after[n].count - before[n].count
        )
        for n in names
    }
    return response, deltas


def test_the_scrape_itself_records_nothing() -> None:
    """Reading /metrics twice must not move either histogram, or every delta
    measured through it would be off by the scrape's own observations."""
    with _client() as client:
        first = {
            n: _histogram(client.get("/metrics").text, n)
            for n in (TX_SCORE_LATENCY, REQUEST_LATENCY)
        }
        second = {
            n: _histogram(client.get("/metrics").text, n)
            for n in (TX_SCORE_LATENCY, REQUEST_LATENCY)
        }
    assert first == second


def test_request_latency_strictly_exceeds_scoring_latency() -> None:
    """The two instruments must not be measuring the same span.

    Asserting `latency_ms < round_trip` would be too weak to catch the original
    defect: a scoring figure that had silently widened to cover the whole
    request would still be under the round trip, because the client's own work
    is outside both. Comparing the two SERVER-side instruments to each other,
    over exactly one transaction, is the assertion that actually discriminates.
    """
    with _client() as client:
        response, deltas = _one_transaction(client, "scope-1")
    assert response.status_code == 200, response.text[:300]
    scoring, request = deltas[TX_SCORE_LATENCY], deltas[REQUEST_LATENCY]
    reported_s = response.json()["latency_ms"] / 1000.0

    # Exactly one observation each. Zero would mean an instrument was never
    # recorded (and its "sum" below would be a leftover from another test); two
    # would mean a request is being counted twice, which halves every rate
    # computed from it.
    assert scoring.count == 1, f"{TX_SCORE_LATENCY} recorded {scoring.count} observations"
    assert request.count == 1, f"{REQUEST_LATENCY} recorded {request.count} observations"

    assert scoring.total >= READ_DELAY_S - CLOCK_SLACK_S, (
        f"{TX_SCORE_LATENCY} recorded {scoring.total:.6f}s for a request whose feature read "
        f"slept {READ_DELAY_S}s. It is not measuring the feature read it claims to cover."
    )
    assert request.total - scoring.total >= WRITE_DELAY_S - CLOCK_SLACK_S, (
        f"{REQUEST_LATENCY} ({request.total:.6f}s) exceeds {TX_SCORE_LATENCY} "
        f"({scoring.total:.6f}s) by less than the {WRITE_DELAY_S}s the replay-cache write slept. "
        f"The write happens after the decision and inside the request, so either the scoring "
        f"instrument has widened to include it -- the original defect -- or the request "
        f"instrument has narrowed to exclude it. Both instruments now describe one span."
    )
    assert abs(reported_s - scoring.total) < AGREEMENT_S, (
        f"latency_ms ({reported_s:.9f}s) does not agree with {TX_SCORE_LATENCY} "
        f"({scoring.total:.9f}s). What the caller is told and what is graphed must be the "
        f"same measurement, or one of them is wrong and nobody can tell which."
    )


def test_the_latency_histograms_have_buckets_that_can_show_a_tail() -> None:
    """The declared second-scale boundaries must reach the exposition.

    Without an explicit advisory the SDK uses its default boundaries -- 5, 10,
    25 ... 10,000 -- which are millisecond-scale. Recording seconds against them
    puts every request in the first bucket, and a run with a client p99 of
    1.46 s showed all 298,413 requests under `le="5.0"`. The budget is 100 ms;
    a histogram that cannot distinguish 10 ms from 4 s cannot say whether the
    budget was met.
    """
    with _client() as client:
        client.post(
            "/v1/transactions",
            json=_transaction(),
            headers={"Authorization": f"Bearer {TOKEN}", "X-Idempotency-Key": "scope-buckets"},
        )
        exposition = client.get("/metrics").text
    for name in (TX_SCORE_LATENCY, REQUEST_LATENCY):
        buckets = [line for line in exposition.splitlines() if line.startswith(f"{name}_bucket")]
        assert any('le="0.1"' in b for b in buckets), (
            f"{name} has no 100 ms bucket; its boundaries are not the declared second-scale set. "
            f"Buckets seen: {[b.split('le=')[1].split(',')[0] for b in buckets][:6]}"
        )
        assert any('le="0.01"' in b for b in buckets), f"{name} has no 10 ms bucket"
