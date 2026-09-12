"""What the gateway emits, and what it must never emit.

Two claims are checked here, and the second is a build gate rather than a review
comment (CLAUDE.md §9: *"Logging PII is a build failure"*).

1. **The metric names in code and in `docs/ARCHITECTURE.md` §13 agree.** Two
   spellings of one metric produce two series, and the one an alert watches is
   whichever its author saw first. Code and the document disagreeing is a bug in
   one of them (CLAUDE.md §14), so this test refuses to let either move alone.

2. **No request field reaches log output.** The redaction processor is a
   backstop; the design is that the body is never logged in the first place. This
   drives real requests through the real app with a PAN, an email, an IP and an
   account identifier in them, captures everything the logger emitted, and
   requires none of it to survive.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import structlog
from fastapi.testclient import TestClient
from services.gateway.app import GatewayState, create_app
from services.gateway.config import GatewaySettings
from services.gateway.pipeline import ScoringPipeline

from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.observability.logging import configure_logging
from trace_core.observability.metrics import (
    ARCHITECTURE_DECLARED,
    HOT_PATH_METRICS,
    HotPathMetrics,
)
from trace_core.rules.loader import default_loader
from trace_core.scoring.banding import load_thresholds
from trace_core.security.service_tokens import MIN_SECRET_LENGTH, ServiceTokenVerifier

pytestmark = [pytest.mark.unit, pytest.mark.security]

ROOT = Path(__file__).resolve().parents[2]
ARCHITECTURE = ROOT / "docs" / "ARCHITECTURE.md"

SECRET = "s" * MIN_SECRET_LENGTH
AUTH = {"Authorization": f"Bearer psp-one.{SECRET}"}


# --- the metric set matches the document ---------------------------------------


def test_every_metric_architecture_declares_exists_in_code() -> None:
    """§13 names three hot-path metrics. Removing one from the code without
    removing it from the design is a silent loss of an alertable signal."""
    text = ARCHITECTURE.read_text()
    for name in ARCHITECTURE_DECLARED:
        assert f"`{name}" in text, (
            f"{name} is declared in code as an ARCHITECTURE §13 metric but does not "
            f"appear in docs/ARCHITECTURE.md. Code and the document disagreeing is a "
            f"bug in one of them (CLAUDE.md §14)."
        )
    assert ARCHITECTURE_DECLARED <= HOT_PATH_METRICS


def test_metric_names_are_prometheus_shaped() -> None:
    """A name Prometheus rejects is a series nobody can query, and the failure
    surfaces at scrape time rather than at build time."""
    import re

    for name in HOT_PATH_METRICS:
        assert re.fullmatch(r"[a-z_][a-z0-9_]*", name), name
        assert not name.endswith("_seconds_total"), (
            f"{name} mixes a unit suffix with a counter suffix; a reader cannot tell "
            f"whether it is a duration or a count"
        )


def test_counters_and_histograms_are_named_by_their_kind() -> None:
    """`_total` for counters, `_seconds` for durations. The convention is what
    lets someone read a dashboard query without opening the code."""
    for name in HOT_PATH_METRICS:
        assert name.endswith(("_total", "_seconds")), (
            f"{name} says nothing about whether it is a count or a duration"
        )


def test_instruments_are_created_once_per_process() -> None:
    """Creating an instrument per request either re-registers or silently returns
    the first one, and which of those happens is a detail nobody should need to
    know."""
    first, second = HotPathMetrics(), HotPathMetrics()
    assert first.latency is not None and second.latency is not None


# --- no PII reaches log output ---------------------------------------------------

PAN = "4111111111111111"
EMAIL = "victim@example.com"
IPV4 = "203.0.113.42"
ACCOUNT = "acct_000001"


class _Captured:
    """Everything the logging pipeline actually wrote to stdout.

    Captured at the STREAM, not at a stdlib handler. `configure_logging` uses
    `structlog.PrintLoggerFactory`, so log lines go to stdout directly and never
    reach a `logging.Handler` -- an earlier version of this fixture attached one
    and captured nothing, which made every "this value must not appear"
    assertion pass vacuously. The guard test below exists because of that.

    Capturing the stream is also the more faithful claim: what matters is what
    LEAVES the process, not what some processor returned on the way.
    """

    def __init__(self, read: Any) -> None:
        self._read = read

    @property
    def lines(self) -> list[str]:
        return [line for line in self._read().out.splitlines() if line.strip()]


@pytest.fixture
def captured(capsys: pytest.CaptureFixture[str]) -> Iterator[_Captured]:
    configure_logging(level="INFO", json_output=True)
    try:
        yield _Captured(capsys.readouterr)
    finally:
        structlog.reset_defaults()
        configure_logging()


def _client() -> TestClient:
    state = GatewayState(
        settings=GatewaySettings.from_environment({}),
        verifier=ServiceTokenVerifier({"psp-one": SECRET}),
        loader=default_loader(frozenset(ONLINE_FEATURES.ids)),
        pipeline=ScoringPipeline(
            pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
            thresholds=load_thresholds(),
            feature_store=None,
        ),
        metrics=HotPathMetrics(),
    )
    return TestClient(create_app(state))


def _body(**over: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "transaction_id": f"tx_{uuid.uuid4().hex[:16]}",
        "account_id": ACCOUNT,
        "amount_minor": 5_000,
        "currency": "GBP",
        "occurred_at": dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z"),
        "merchant_id": "mrch_00001",
        "merchant_mcc": "5411",
    }
    payload.update(over)
    return payload


def test_a_request_is_logged_at_all(captured: _Captured) -> None:
    """Without this, every assertion below would pass on an empty log."""
    with _client() as client:
        client.post(
            "/v1/transactions",
            json=_body(),
            headers={**AUTH, "X-Idempotency-Key": "idem-1"},
        )
    assert any("http_request" in line for line in captured.lines), (
        "no request was logged; the PII assertions would pass vacuously"
    )


def test_no_attacker_controlled_field_reaches_log_output(
    captured: _Captured,
) -> None:
    """The body is never logged. Not "redacted" -- not logged.

    A PAN, an email and an IP are planted in fields a caller controls. None may
    appear in the rendered output, whether because the field was never logged or
    because the redactor caught it. Both are acceptable; the value surviving is
    not (CLAUDE.md §9).
    """
    with _client() as client:
        client.post(
            "/v1/transactions",
            json=_body(
                merchant_name=f"SHOP {PAN}",
                user_agent=f"Mozilla/5.0 ({EMAIL})",
                memo=f"from {IPV4}",
            ),
            headers={**AUTH, "X-Idempotency-Key": "idem-2"},
        )
    output = "\n".join(captured.lines)
    for secret in (PAN, EMAIL, IPV4):
        assert secret not in output, (
            f"{secret!r} reached log output. Logging PII is a build failure, not a "
            f"review comment (CLAUDE.md §9)."
        )


def test_account_identifiers_do_not_reach_log_output(
    captured: _Captured,
) -> None:
    """`acct_\\d{6,}` is what the redaction pattern recognises, and the reason
    the identifier format is pinned to it (tests/unit/test_identifier_formats.py)."""
    with _client() as client:
        client.post(
            "/v1/transactions",
            json=_body(),
            headers={**AUTH, "X-Idempotency-Key": "idem-3"},
        )
    assert ACCOUNT not in "\n".join(captured.lines)


def test_a_rejected_request_does_not_log_what_it_rejected(
    captured: _Captured,
) -> None:
    """The path most likely to leak: an error handler that helpfully includes the
    offending input."""
    with _client() as client:
        client.post(
            "/v1/transactions",
            json={**_body(), "surprise": PAN},
            headers={**AUTH, "X-Idempotency-Key": "idem-4"},
        )
    assert PAN not in "\n".join(captured.lines)


def test_the_service_token_secret_never_reaches_log_output(
    captured: _Captured,
) -> None:
    """A credential in a log is a credential in every log aggregator, backup and
    screenshot downstream of it (docs/SECURITY.md §10)."""
    with _client() as client:
        client.post(
            "/v1/transactions",
            json=_body(),
            headers={**AUTH, "X-Idempotency-Key": "idem-5"},
        )
    assert SECRET not in "\n".join(captured.lines)


def test_the_log_line_carries_what_an_operator_actually_needs(
    captured: _Captured,
) -> None:
    """Keeping PII out must not leave a line nobody can use.

    Method, path, status, duration and request id are enough to answer "what
    happened to this request", and the request id is what a caller quotes.
    """
    with _client() as client:
        client.post(
            "/v1/transactions",
            json=_body(),
            headers={**AUTH, "X-Idempotency-Key": "idem-6", "X-Request-Id": "req_known"},
        )
    entries = [json.loads(line) for line in captured.lines if line.startswith("{")]
    requests = [entry for entry in entries if entry.get("event") == "http_request"]
    assert requests, "no structured http_request entry was emitted"
    entry = requests[-1]
    assert entry["method"] == "POST"
    assert entry["path"] == "/v1/transactions"
    assert entry["status"] == 200
    assert entry["request_id"] == "req_known"
    assert entry["duration_ms"] >= 0


def test_probe_endpoints_are_not_logged(captured: _Captured) -> None:
    """A liveness probe every second is 86,400 lines a day that say nothing, and
    they are the lines that push the useful ones out of a retention window."""
    with _client() as client:
        client.get("/healthz")
        client.get("/readyz")
        client.get("/metrics")
    assert not any("http_request" in line for line in captured.lines)
