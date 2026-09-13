"""RFC 9457 problem details — the only error shape this API returns.

`docs/API_CONTRACTS.md` §4 requires every error path to carry `type`, `title`,
`status`, `detail`, `instance`, `trace_id` and `request_id`. One shape means a
client writes one error handler; a service that returns three shapes teaches
callers to parse strings.

**Error `type` URIs are stable and enumerated.** Adding one is additive;
changing what an existing one *means* is breaking (§4), so they live in a closed
enum rather than being spelled inline at each raise site -- an inline string is
how two slightly different URIs come to mean the same thing.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Final, Self

from pydantic import Field

from trace_core.contracts.api.base import ApiModel

ERROR_BASE: Final = "https://tracex.dev/errors/"


class ErrorType(StrEnum):
    """The enumerated `type` URIs. Stable; a new member is additive."""

    MALFORMED_REQUEST = f"{ERROR_BASE}malformed-request"
    """400 -- the request does not match the schema: unknown field, wrong type,
    missing required field. Shape, not meaning."""

    INVALID_REQUEST = f"{ERROR_BASE}invalid-request"
    """422 -- the request is well-formed but semantically invalid: an out-of-enum
    value, an out-of-range amount, a timestamp implying impossible clock skew."""

    UNAUTHENTICATED = f"{ERROR_BASE}unauthenticated"
    """401 -- no service token, or one that does not authenticate."""

    IDEMPOTENCY_CONFLICT = f"{ERROR_BASE}idempotency-conflict"
    """409 -- the same idempotency key was reused with a different payload. A
    client bug, surfaced rather than absorbed (docs/API_CONTRACTS.md §5)."""

    RATE_LIMITED = f"{ERROR_BASE}rate-limited"
    """429 -- over the caller's budget. Carries `Retry-After`."""

    SERVICE_UNAVAILABLE = f"{ERROR_BASE}service-unavailable"
    """503 -- degraded past the usable threshold. For the gateway this means the
    system of record is unreachable, so a triage decision could not be durably
    recorded (ADR-0035)."""


TITLES: Final[dict[ErrorType, str]] = {
    ErrorType.MALFORMED_REQUEST: "Malformed request",
    ErrorType.INVALID_REQUEST: "Invalid request",
    ErrorType.UNAUTHENTICATED: "Unauthenticated",
    ErrorType.IDEMPOTENCY_CONFLICT: "Idempotency conflict",
    ErrorType.RATE_LIMITED: "Rate limited",
    ErrorType.SERVICE_UNAVAILABLE: "Service unavailable",
}

STATUSES: Final[dict[ErrorType, int]] = {
    ErrorType.MALFORMED_REQUEST: 400,
    ErrorType.INVALID_REQUEST: 422,
    ErrorType.UNAUTHENTICATED: 401,
    ErrorType.IDEMPOTENCY_CONFLICT: 409,
    ErrorType.RATE_LIMITED: 429,
    ErrorType.SERVICE_UNAVAILABLE: 503,
}


class Problem(ApiModel):
    """An RFC 9457 problem document."""

    type: str
    title: str
    status: Annotated[int, Field(ge=100, le=599)]
    detail: str
    instance: str
    trace_id: str
    """The W3C trace id, so an operator can pivot from a client's error report
    straight to the distributed trace (docs/ARCHITECTURE.md §13)."""
    request_id: str
    errors: list[str] = Field(default_factory=list)
    """Per-field validation messages, when there are any.

    Present so a `400` tells a client *which* field was wrong. Contains field
    names and error kinds only -- never the submitted values, which are
    attacker-controlled and may be PII (docs/SECURITY.md §10)."""

    @classmethod
    def of(
        cls,
        error: ErrorType,
        *,
        detail: str,
        instance: str,
        trace_id: str,
        request_id: str,
        errors: list[str] | None = None,
    ) -> Self:
        """Build a problem whose title and status match its type by construction."""
        return cls(
            type=str(error),
            title=TITLES[error],
            status=STATUSES[error],
            detail=detail,
            instance=instance,
            trace_id=trace_id,
            request_id=request_id,
            errors=errors or [],
        )
