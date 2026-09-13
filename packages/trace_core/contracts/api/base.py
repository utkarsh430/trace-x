"""Base model for the HTTP API surface, and why it is not `StrictModel`.

`docs/API_CONTRACTS.md` §6.1 requires strict Pydantic v2 models with
`extra="forbid"`. `trace_core.contracts.base.StrictModel` sets `strict=True`
model-wide, and its own docstring records the consequence: strictness applies to
**both** validation modes, and they differ. In JSON mode an ISO-8601 string
becomes a datetime; in Python mode the same string is rejected.

FastAPI parses the request body to a `dict` and then validates in **Python
mode**. Measured, not assumed: a model-wide `strict=True` returns
`422 datetime_type: Input should be a valid datetime` for
`"2026-09-12T10:00:00Z"` -- i.e. it rejects every well-formed request.

So strictness is applied model-wide and relaxed **only** on the two types JSON
has no native spelling for, via `JsonDatetime` and `JSON_SPELLED`. Everything the
policy actually cares about stays strict, and
`tests/contract/test_api_strictness.py` proves each one:

* `amount_minor="1999"` and `amount_minor=19.99` are rejected -- money is
  integer minor units and never a float (CLAUDE.md §6);
* an unknown field is rejected, never ignored;
* a timezone-naive datetime is rejected;
* an out-of-enum value is rejected, never coerced to a default.

The alternative -- reading the raw body and calling `model_validate_json` -- keeps
full strictness but hides the request schema from FastAPI, and Phase 2 must emit
a committed OpenAPI document generated from these models (ADR-0028's direction of
truth, inverted for APIs). Losing the generated contract to win a config flag
would be the wrong trade.
"""

from __future__ import annotations

import datetime as dt
from typing import Annotated, Final

from pydantic import AwareDatetime, BaseModel, BeforeValidator, ConfigDict, Field

JSON_SPELLED: Final = Field(strict=False)
"""Marks a field whose JSON spelling differs from its Python type.

Applied to datetimes and to enums, and to nothing else. It relaxes only the
*spelling* a value may arrive in -- never the set of values, and never a type
the policy cares about. `latitude`/`longitude` also carry it so an integer
degree (`51`) is accepted as the float it plainly is.
"""


def _rfc3339_string_only(value: object) -> object:
    """Reject a numeric timestamp before pydantic guesses what it means.

    Found by a test: relaxing the spelling on `AwareDatetime` also admits a bare
    integer, which pydantic reads as a Unix epoch and disambiguates seconds from
    milliseconds **by magnitude heuristic**. So `1757671200` silently becomes
    2025-09-12 while `1757671200000` silently becomes the same instant -- and a
    client that sent the wrong unit gets a plausible date rather than an error.

    `occurred_at` drives every window and watermark in the system (ADR-0026), so
    a guessed timestamp is not a small inconvenience: it puts a transaction in
    the wrong window, and nothing downstream can detect that it happened.
    `docs/API_CONTRACTS.md` §6.3 says RFC 3339, which is a string.

    A real `datetime` passes through, so internal construction and Python-mode
    validation in tests are unaffected -- the ambiguity only exists on the wire.
    """
    if isinstance(value, dt.datetime | str):
        return value
    raise ValueError(
        "timestamps must be RFC 3339 strings with an explicit offset "
        "(docs/API_CONTRACTS.md §6.3); a numeric epoch is ambiguous between "
        "seconds and milliseconds and is never guessed at"
    )


JsonDatetime = Annotated[AwareDatetime, JSON_SPELLED, BeforeValidator(_rfc3339_string_only)]
"""An RFC 3339 timestamp with an explicit offset.

`AwareDatetime` keeps the naive-datetime rejection that `docs/API_CONTRACTS.md`
§6.3 requires; the relaxed spelling is what lets the string parse at all; and the
validator keeps a numeric epoch from being guessed at.
"""


class ApiModel(BaseModel):
    """Immutable, extra-forbidding, strict-by-default HTTP contract model."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        populate_by_name=True,
    )
