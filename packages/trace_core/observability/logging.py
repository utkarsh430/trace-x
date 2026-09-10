"""Structured logging.

Two properties are non-negotiable and are enforced by position in the processor
chain rather than by convention:

1. **PII never reaches log output** (CLAUDE.md §9). `PIIRedactingProcessor` runs
   immediately before rendering, so nothing added by an earlier processor can
   slip past it.
2. **Every line carries `trace_id`** when a span is active, so an operator can
   pivot from any log line to the distributed trace it belongs to.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

import structlog

from trace_core.observability.redaction import PIIRedactingProcessor
from trace_core.observability.telemetry import current_span_id, current_trace_id


def _add_trace_context(
    logger: Any,  # noqa: ARG001 - structlog processor signature
    method_name: str,  # noqa: ARG001 - structlog processor signature
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    if (tid := current_trace_id()) is not None:
        event_dict["trace_id"] = tid
        if (sid := current_span_id()) is not None:
            event_dict["span_id"] = sid
    return event_dict


def build_processor_chain(*, json_output: bool) -> list[Any]:
    """The processor chain, exposed so tests can assert its shape.

    Ordering matters: redaction is the LAST processor before the renderer, so no
    processor can introduce PII after it has run.
    """
    renderer: Any = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )
    return [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _add_trace_context,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        PIIRedactingProcessor(),  # must remain immediately before the renderer
        renderer,
    ]


def configure_logging(*, level: str | None = None, json_output: bool | None = None) -> None:
    """Install the logging configuration for this process. Idempotent."""
    resolved_level: str = (level or os.getenv("TRACE_LOG_LEVEL") or "INFO").upper()
    if json_output is None:
        json_output = os.getenv("TRACE_LOG_FORMAT", "json").lower() == "json"

    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=resolved_level)
    structlog.configure(
        processors=build_processor_chain(json_output=json_output),
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping().get(resolved_level, logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> Any:
    return structlog.get_logger(name)
