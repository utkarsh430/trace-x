"""Observability: structured logging, metrics and tracing."""

from trace_core.observability.logging import build_processor_chain, configure_logging, get_logger
from trace_core.observability.redaction import PIIRedactingProcessor, redact
from trace_core.observability.telemetry import (
    carrier_extract,
    carrier_inject,
    configure_telemetry,
    current_span_id,
    current_trace_id,
    get_meter,
    get_tracer,
)

__all__ = [
    "PIIRedactingProcessor",
    "build_processor_chain",
    "carrier_extract",
    "carrier_inject",
    "configure_logging",
    "configure_telemetry",
    "current_span_id",
    "current_trace_id",
    "get_logger",
    "get_meter",
    "get_tracer",
    "redact",
]
