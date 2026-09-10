"""Observability scaffold: trace propagation and log safety.

The two properties that matter operationally:
  1. one trace_id spans ingest to audit (docs/ARCHITECTURE.md §13)
  2. PII never reaches log output, structurally (CLAUDE.md §9)
"""

from __future__ import annotations

import json

import pytest
import structlog

from trace_core.observability import (
    PIIRedactingProcessor,
    build_processor_chain,
    carrier_extract,
    carrier_inject,
    configure_logging,
    configure_telemetry,
    current_span_id,
    current_trace_id,
    get_logger,
    get_tracer,
)

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def telemetry() -> None:
    configure_telemetry("trace-test", force=True)


# ----------------------------------------------------------------- tracing --


def test_no_trace_id_outside_a_span() -> None:
    assert current_trace_id() is None
    assert current_span_id() is None


def test_trace_id_is_available_inside_a_span() -> None:
    with get_tracer("t").start_as_current_span("op"):
        tid = current_trace_id()
    assert tid is not None
    assert len(tid) == 32
    int(tid, 16)  # must be valid hex


def test_nested_spans_share_one_trace_id() -> None:
    """An investigation is ONE distributed trace, not one per step."""
    tracer = get_tracer("t")
    with tracer.start_as_current_span("investigation"):
        outer = current_trace_id()
        with tracer.start_as_current_span("agent"):
            inner = current_trace_id()
            with tracer.start_as_current_span("tool_call"):
                deepest = current_trace_id()
    assert outer == inner == deepest


def test_span_ids_differ_within_one_trace() -> None:
    tracer = get_tracer("t")
    with tracer.start_as_current_span("outer"):
        a = current_span_id()
        with tracer.start_as_current_span("inner"):
            b = current_span_id()
    assert a != b


def test_context_survives_a_non_http_hop() -> None:
    """Kafka headers, MCP requests and Spark params have no auto-instrumentation."""
    with get_tracer("t").start_as_current_span("producer"):
        origin = current_trace_id()
        carrier = carrier_inject()

    assert "traceparent" in carrier
    assert origin is not None and origin in carrier["traceparent"]

    ctx = carrier_extract(carrier)
    with get_tracer("t").start_as_current_span("consumer", context=ctx):
        assert current_trace_id() == origin, "trace must survive the hop, not restart"


def test_carrier_inject_preserves_existing_keys() -> None:
    """Kafka headers already carry our own metadata; injection must not clobber it."""
    with get_tracer("t").start_as_current_span("op"):
        carrier = carrier_inject({"event_id": "abc"})
    assert carrier["event_id"] == "abc"
    assert "traceparent" in carrier


def test_telemetry_configures_without_a_collector() -> None:
    """A missing collector must never break the application."""
    configure_telemetry("trace-test", force=True)
    with get_tracer("t").start_as_current_span("op") as span:
        span.set_attribute("band", "HIGH")
        assert current_trace_id() is not None


# ----------------------------------------------------------------- logging --


def _capture(caplog_json: list[str]):
    class _Sink:
        def msg(self, s: str) -> None:
            caplog_json.append(s)

        info = debug = warning = error = critical = msg

    return _Sink()


def test_redaction_is_last_before_the_renderer() -> None:
    """Position is the control: nothing can add PII after redaction runs."""
    chain = build_processor_chain(json_output=True)
    types = [type(p).__name__ for p in chain]
    assert "PIIRedactingProcessor" in types
    assert types.index("PIIRedactingProcessor") == len(types) - 2, (
        f"redaction must sit immediately before the renderer, got {types}"
    )


def test_log_line_carries_trace_id_and_redacts_pii() -> None:
    captured: list[str] = []
    configure_logging(json_output=True)
    structlog.configure(
        processors=build_processor_chain(json_output=True),
        logger_factory=lambda *a, **k: _capture(captured),
        cache_logger_on_first_use=False,
    )
    log = get_logger("test")
    with get_tracer("t").start_as_current_span("score"):
        expected = current_trace_id()
        log.info("scored", band="HIGH", email="analyst@bank.example.com", pan="4111111111111111")

    assert captured, "no log line was emitted"
    record = json.loads(captured[-1])
    assert record["trace_id"] == expected
    assert "span_id" in record
    assert record["band"] == "HIGH"
    assert "analyst@bank.example.com" not in captured[-1]
    assert "4111111111111111" not in captured[-1]


def test_log_line_omits_trace_id_outside_a_span() -> None:
    """Absent, not fabricated -- an invalid trace_id is worse than none."""
    captured: list[str] = []
    structlog.configure(
        processors=build_processor_chain(json_output=True),
        logger_factory=lambda *a, **k: _capture(captured),
        cache_logger_on_first_use=False,
    )
    get_logger("test").info("startup")
    assert "trace_id" not in json.loads(captured[-1])


def test_processor_chain_is_json_or_console() -> None:
    assert type(build_processor_chain(json_output=True)[-1]).__name__ == "JSONRenderer"
    assert type(build_processor_chain(json_output=False)[-1]).__name__ == "ConsoleRenderer"


def test_pii_processor_is_the_same_one_the_redaction_tests_cover() -> None:
    chain = build_processor_chain(json_output=True)
    assert any(isinstance(p, PIIRedactingProcessor) for p in chain)
