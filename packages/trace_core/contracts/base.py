"""Base model for every generated event contract.

CLAUDE.md §6: Pydantic v2 for all wire contracts, strict, `extra="forbid"`.

`extra="forbid"` is the load-bearing setting. Silently accepting an unknown
field means a producer can add a field nobody validates and a consumer will
never notice it is being ignored — which is how a "compatible" change turns
into a silent data loss. Rejecting loudly forces the change through the
versioning procedure in docs/EVENT_CONTRACTS.md §4.

`frozen=True` because an event is a record of something that already happened.
Mutating a parsed event in place would make the audit trail describe something
other than what arrived.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class StrictEventModel(BaseModel):
    """Immutable, extra-forbidding wire model."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        # Strict in *Python* mode, so an int is never silently accepted where a
        # str belongs. JSON mode still parses the JSON-native spellings (an ISO
        # 8601 string into a datetime), which is exactly what a wire contract
        # needs: strict about types, literate about JSON.
        strict=True,
        populate_by_name=True,
        ser_json_timedelta="iso8601",
    )
