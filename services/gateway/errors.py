"""Turning failures into RFC 9457 problem documents (docs/API_CONTRACTS.md §4).

**Why a validation error is not always the same status.** §4 and §6 give two
different codes for two different kinds of wrongness, and the distinction is
useful rather than pedantic:

* `400` — the request does not match the SHAPE: an unknown field, a wrong type, a
  missing required field. The client sent something this endpoint cannot read.
* `422` — the request is well formed but SEMANTICALLY invalid: an out-of-enum
  value, an amount outside the permitted range, a pattern that does not match.
  The client sent something readable that cannot be true.

A client fixes those two differently — the first is a serialisation bug, the
second is a data bug — so collapsing them into one code costs the caller the most
useful thing the response could have told them. §6.1 asks for `400` on an unknown
field and §6.6 for `422` on an out-of-enum value, which is exactly this split.

**Values are never echoed back.** A problem document names the field and the kind
of error, never the submitted value: request fields are attacker-controlled and
may carry PII, and an error response is a place people paste into tickets
(docs/SECURITY.md §10).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Final

from trace_core.contracts.api.problem import ErrorType, Problem

SHAPE_ERRORS: Final[frozenset[str]] = frozenset(
    {
        "extra_forbidden",
        "missing",
        "json_invalid",
        "json_type",
        "model_type",
        "model_attributes_type",
        "dict_type",
        "list_type",
        "int_type",
        "float_type",
        "string_type",
        "bool_type",
        "datetime_type",
        "int_parsing",
        "float_parsing",
        "bool_parsing",
    }
)
"""Pydantic error types that mean "this is not the right shape" -> 400.

Everything else -- enum, pattern, range, length, custom validators -- means "the
shape is right and the value is not" -> 422. Listing the 400 side explicitly and
defaulting to 422 is deliberate: a new pydantic error type should land on the
semantic side, where it is merely a less precise code, rather than silently
becoming a 400 that tells a client to look at their serialiser.
"""


def classify(errors: Sequence[Mapping[str, Any]]) -> ErrorType:
    """400 if ANY error is a shape error, else 422.

    Any, not all: a request with both problems cannot be parsed at all, so the
    shape error is the one the client must fix first.
    """
    return (
        ErrorType.MALFORMED_REQUEST
        if any(error.get("type") in SHAPE_ERRORS for error in errors)
        else ErrorType.INVALID_REQUEST
    )


def describe(errors: Sequence[Mapping[str, Any]]) -> list[str]:
    """`field: error kind` per error. Never the submitted value."""
    described: list[str] = []
    for error in errors:
        location = ".".join(str(part) for part in error.get("loc", ()) if part != "body")
        described.append(f"{location or '<body>'}: {error.get('type', 'invalid')}")
    return sorted(described)


def problem(
    error: ErrorType,
    *,
    detail: str,
    instance: str,
    trace_id: str,
    request_id: str,
    errors: list[str] | None = None,
) -> Problem:
    return Problem.of(
        error,
        detail=detail,
        instance=instance,
        trace_id=trace_id,
        request_id=request_id,
        errors=errors,
    )
