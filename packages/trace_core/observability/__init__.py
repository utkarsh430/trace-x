"""Observability: structured logging, metrics and tracing scaffolds."""

from trace_core.observability.redaction import PIIRedactingProcessor, redact

__all__ = ["PIIRedactingProcessor", "redact"]
