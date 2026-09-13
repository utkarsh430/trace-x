"""OpenTelemetry scaffold.

Design constraints that shaped this module:

* **A missing collector must never break the application.** Telemetry is
  observability, not a dependency. When no exporter is configured the providers
  are still installed so instrumentation code paths run and are exercised by
  tests -- spans simply go nowhere.
* **One trace_id spans ingest to audit** (docs/ARCHITECTURE.md §13). The trace
  context is propagated over HTTP, as a Kafka header, through Spark, into each
  agent node and tool call -- including across the MCP boundary -- and onto the
  action executor. `carrier_inject`/`carrier_extract` are the seams for the
  non-HTTP hops, which have no framework instrumentation to do it for them.
* **Resource attributes identify the emitting process**, because four
  entrypoints share one codebase (ADR-0001) and a metric is meaningless without
  knowing which one produced it.
"""

from __future__ import annotations

import os
from functools import cache
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.propagate import extract, inject
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, ParentBased, TraceIdRatioBased

from trace_core import __version__

SERVICE_NAMES = ("trace-gateway", "trace-api", "trace-worker", "trace-stream")

_configured = False


def _resource(service_name: str) -> Resource:
    return Resource.create(
        {
            "service.name": service_name,
            "service.version": __version__,
            "service.namespace": "tracex",
            "deployment.environment": os.getenv("TRACE_ENV", "local"),
        }
    )


def _sampler(ratio_env: str = "TRACE_OTEL_SAMPLE_RATIO") -> Any:
    """Sample everything locally; allow a ratio in higher-volume environments.

    ParentBased means a sampled inbound trace stays sampled through every hop --
    without it an investigation's trace would be sampled independently at each
    service and arrive fragmented, which is worse than not sampling at all.
    """
    raw = os.getenv(ratio_env)
    if raw is None:
        return ParentBased(ALWAYS_ON)
    try:
        ratio = float(raw)
    except ValueError:
        return ParentBased(ALWAYS_ON)
    return ParentBased(TraceIdRatioBased(max(0.0, min(1.0, ratio))))


def configure_telemetry(
    service_name: str, *, force: bool = False, prometheus: bool = False
) -> None:
    """Install tracer and meter providers for this process.

    Idempotent: calling it twice is a no-op, because a second TracerProvider
    would silently orphan every span created against the first.

    `prometheus=True` adds a scrape reader to the SAME meter provider, so
    `/metrics` serves the instruments the application already writes rather than
    a parallel set. Two metric pipelines would mean two definitions of every
    counter, and the one an alert watches would be whichever the author found
    first. ARCHITECTURE §14 requires `/metrics` to work with the `obs` profile
    down, which is exactly what a scrape endpoint with no collector gives.
    """
    global _configured
    if _configured and not force:
        return

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    resource = _resource(service_name)

    tracer_provider = TracerProvider(resource=resource, sampler=_sampler())
    metric_readers: list[Any] = []

    if prometheus:
        # Imported lazily for the same reason as the OTLP exporter: a process
        # that does not serve /metrics should not pay the import.
        from opentelemetry.exporter.prometheus import PrometheusMetricReader

        metric_readers.append(PrometheusMetricReader())

    if endpoint:
        # Imported lazily: the OTLP exporter pulls in grpc, and a process with
        # no collector configured should not pay that import cost.
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
            OTLPMetricExporter,
        )
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )

        tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
        metric_readers.append(PeriodicExportingMetricReader(OTLPMetricExporter(endpoint=endpoint)))

    trace.set_tracer_provider(tracer_provider)
    metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=metric_readers))
    _configured = True


@cache
def get_tracer(name: str) -> trace.Tracer:
    return trace.get_tracer(name, __version__)


@cache
def get_meter(name: str) -> metrics.Meter:
    return metrics.get_meter(name, __version__)


def current_trace_id() -> str | None:
    """The active trace id as a 32-char hex string, or None outside a span.

    Returned in the `trace_id` field of every structured log line and in the
    `trace_id` field of every RFC 9457 error body, so an operator can pivot from
    a log line or an API error straight to the distributed trace.
    """
    ctx = trace.get_current_span().get_span_context()
    if not ctx.is_valid:
        return None
    return format(ctx.trace_id, "032x")


def current_span_id() -> str | None:
    ctx = trace.get_current_span().get_span_context()
    if not ctx.is_valid:
        return None
    return format(ctx.span_id, "016x")


def carrier_inject(carrier: dict[str, str] | None = None) -> dict[str, str]:
    """Serialize the active trace context for a non-HTTP hop.

    Used for Kafka headers, MCP requests and Spark job parameters -- the hops
    where nothing instruments the boundary for us.
    """
    out: dict[str, str] = {} if carrier is None else carrier
    inject(out)
    return out


def carrier_extract(carrier: dict[str, str]) -> Any:
    """Rehydrate a trace context produced by `carrier_inject`."""
    return extract(carrier)
