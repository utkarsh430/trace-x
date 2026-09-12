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


class StrictModel(BaseModel):
    """Immutable, extra-forbidding contract model."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        # Strict, with one consequence worth knowing before you use these
        # models: strictness applies to BOTH validation modes, and they differ.
        #
        #   model_validate_json(text)  -- JSON mode. An ISO 8601 string becomes a
        #                                 datetime and a bare string becomes an
        #                                 enum member, because JSON has no native
        #                                 spelling for either. This is the
        #                                 intended entrypoint for wire data.
        #   model_validate(obj)        -- Python mode. The SAME ISO string is
        #                                 rejected; it wants a real datetime and a
        #                                 real enum member.
        #
        # So a dict that came from JSON must be validated as JSON, not as a
        # Python object. `GeneratorAdapter` does exactly that. The alternative --
        # dropping strict -- would buy dict support at the price of letting
        # `amount_minor="1999"` through, and money arriving as a string is
        # precisely the kind of thing a wire contract exists to refuse.
        strict=True,
        populate_by_name=True,
        ser_json_timedelta="iso8601",
    )


class StrictEventModel(StrictModel):
    """Base for the generated event models.

    A distinct name from `StrictModel` because `scripts/generate_event_models.py`
    passes it to `datamodel-codegen --base-class`: renaming it would silently
    change every generated file. It adds no configuration of its own today; it
    exists so event models and internal contracts can diverge later without a
    regeneration of everything.
    """
