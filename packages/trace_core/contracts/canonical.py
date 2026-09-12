"""`CanonicalTransaction` and the declared-coverage mechanism (ADR-0022).

Every inbound dataset is mapped onto this one shape by a `SourceAdapter`. The
datasets are not alike: the Track A generator supplies everything, while
IEEE-CIS (Track B) has **no true merchant id, no latitude/longitude and
obfuscated timedeltas** rather than timestamps.

The tempting shortcut is to fill what is missing with zeros so everything "just
works". That is how a cross-dataset generalization result becomes meaningless: a
velocity feature computed from imputed zeros is not a weak signal, it is a
**fabricated** one, and it looks exactly like a real feature in every downstream
report.

So coverage is **declared**, and the declaration is load-bearing:

* A field the source does not cover **must be absent**, enforced per row here.
* A feature whose `required_fields` are not covered evaluates to `UNAVAILABLE`
  and propagates as null — never 0, never a default.

The distinction ADR-0022 insists on is between "this source never provides it"
(not covered) and "this row happens to be missing it" (covered, but null on this
row). Both are representable, and they mean different things: the first
disqualifies a feature entirely, the second is ordinary sparsity.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Final, Self

from pydantic import AwareDatetime, Field, model_validator

from trace_core.contracts.base import StrictModel
from trace_core.domain.enums import (
    AuthorizationOutcome,
    EntryMode,
    TransactionChannel,
    TrustTier,
)


class CanonicalField(StrEnum):
    """The canonical vocabulary. `field_coverage` is a set of these.

    A `StrEnum` rather than bare strings so a typo in a coverage declaration or
    in a feature's `required_fields` is a type error, not a silently
    never-covered field that makes a feature quietly UNAVAILABLE forever.
    """

    TRANSACTION_ID = "transaction_id"
    ACCOUNT_ID = "account_id"
    CARD_ID = "card_id"
    DEVICE_ID = "device_id"
    MERCHANT_ID = "merchant_id"
    IP_ID = "ip_id"
    AMOUNT_MINOR = "amount_minor"
    CURRENCY = "currency"
    OCCURRED_AT = "occurred_at"
    INGESTED_AT = "ingested_at"
    CHANNEL = "channel"
    ENTRY_MODE = "entry_mode"
    MERCHANT_MCC = "merchant_mcc"
    MERCHANT_COUNTRY = "merchant_country"
    MERCHANT_NAME = "merchant_name"
    LATITUDE = "latitude"
    LONGITUDE = "longitude"
    USER_AGENT = "user_agent"
    MEMO = "memo"
    AUTHORIZATION_OUTCOME = "authorization_outcome"


ALWAYS_REQUIRED: Final[frozenset[CanonicalField]] = frozenset(
    {
        CanonicalField.TRANSACTION_ID,
        CanonicalField.ACCOUNT_ID,
        CanonicalField.AMOUNT_MINOR,
        CanonicalField.CURRENCY,
        CanonicalField.OCCURRED_AT,
        CanonicalField.INGESTED_AT,
    }
)
"""The irreducible minimum.

A source that cannot say *which account moved how much, in what currency, when*
cannot be scored or aggregated at all, so it cannot participate. Everything else
is optional and its absence is a measurable coverage gap rather than a
disqualification. IEEE-CIS clears this bar, which is why Track B is possible.
"""

UNTRUSTED_FIELDS: Final[frozenset[CanonicalField]] = frozenset(
    {
        CanonicalField.MERCHANT_NAME,
        CanonicalField.USER_AGENT,
        CanonicalField.MEMO,
    }
)
"""Attacker-controlled fields (docs/SECURITY.md §5.1).

`device_label` is the fourth field in that list; it arrives on
`device.events.v1` rather than on a transaction, so it is not a canonical
transaction field. The tier is stamped at ingestion and never removed.
"""


def wire_enum_to_domain[E: StrEnum](enum_cls: type[E], value: str | None) -> E | None:
    """Map a wire enum value onto its domain enum, degrading to UNKNOWN.

    `docs/EVENT_CONTRACTS.md` §4 classes "add an enum value" as compatible
    *provided consumers have an UNKNOWN branch*. This is that branch.

    **A caveat worth stating rather than hiding.** On the JSON path the released
    schemas enumerate their permitted values, so a value a newer producer adds is
    rejected by pydantic before it reaches here -- which means the §4 row is, on
    that path, optimistic. This function is therefore defence in depth for the
    paths that do not re-validate against the JSON Schema: a Delta read, a Spark
    row, a replay from a topic written by a newer producer. Closing the gap on
    the JSON path properly would mean relaxing the schema enums to patterns, and
    that is a contract change, not a Phase 1 decision.
    """
    if value is None:
        return None
    try:
        return enum_cls(value)
    except ValueError:
        unknown = enum_cls.__members__.get("UNKNOWN")
        if unknown is None:
            raise
        return unknown


def trust_tier_of(field: CanonicalField) -> TrustTier:
    """The provenance tier of a canonical field. A value never loses its tier."""
    return TrustTier.UNTRUSTED if field in UNTRUSTED_FIELDS else TrustTier.SYSTEM


class CanonicalTransaction(StrictModel):
    """One transaction, in the shape every source is mapped onto."""

    # --- provenance (always present; this is how a row is traced home) ------
    source_dataset: Annotated[str, Field(min_length=1, max_length=64)]
    source_row_id: Annotated[str, Field(min_length=1, max_length=128)]
    field_coverage: frozenset[CanonicalField]
    """Which canonical fields THIS SOURCE supplies. Declared by the adapter,
    identical for every row it produces, and checked against reality by
    `SourceAdapterConformanceSuite`."""

    # --- always required ----------------------------------------------------
    transaction_id: Annotated[str, Field(min_length=1, max_length=64)]
    account_id: Annotated[str, Field(min_length=1, max_length=64)]
    amount_minor: int
    """Integer minor units. Never a float (CLAUDE.md §6)."""
    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")]
    occurred_at: AwareDatetime
    """Event time. Drives every window and watermark (ADR-0026)."""
    ingested_at: AwareDatetime
    """Processing time. Lag measurement only; never business logic."""

    # --- coverage-dependent -------------------------------------------------
    card_id: Annotated[str, Field(max_length=64)] | None = None
    device_id: Annotated[str, Field(max_length=64)] | None = None
    merchant_id: Annotated[str, Field(max_length=64)] | None = None
    ip_id: Annotated[str, Field(max_length=64)] | None = None
    channel: TransactionChannel | None = None
    entry_mode: EntryMode | None = None
    merchant_mcc: Annotated[str, Field(pattern=r"^\d{4}$")] | None = None
    merchant_country: Annotated[str, Field(pattern=r"^[A-Z]{2}$")] | None = None
    merchant_name: Annotated[str, Field(max_length=128)] | None = None
    latitude: Annotated[float, Field(ge=-90, le=90)] | None = None
    longitude: Annotated[float, Field(ge=-180, le=180)] | None = None
    user_agent: Annotated[str, Field(max_length=256)] | None = None
    memo: Annotated[str, Field(max_length=256)] | None = None
    authorization_outcome: AuthorizationOutcome | None = None

    # --- source-specific opaque columns -------------------------------------
    opaque_features: dict[str, float] = Field(default_factory=dict)
    """Typed carrier for a source's own engineered columns.

    IEEE-CIS's `V1-V339`, `C1-C14`, `D1-D15` and `M1-M9` are anonymised and have
    no canonical meaning, so they cannot be mapped onto named fields without
    inventing semantics. They travel here, where a model may use them and a
    cross-dataset feature may not. Empty for Track A.
    """

    @model_validator(mode="after")
    def _coverage_must_describe_reality(self) -> Self:
        """Two checks, and both have caught the same class of bug in the wild.

        1. The irreducible minimum must be covered, or the row cannot be scored.
        2. **An uncovered field must be absent.** A populated field that the
           source claims not to supply means the coverage declaration is wrong,
           and every `UNAVAILABLE` decision downstream was made on a false
           premise.

        The converse is deliberately NOT enforced: a covered field may be null
        on a given row. That is ordinary sparsity, and conflating it with
        "this source never provides it" is precisely the distinction ADR-0022
        exists to preserve.
        """
        missing = ALWAYS_REQUIRED - self.field_coverage
        if missing:
            raise ValueError(
                f"field_coverage omits required field(s) {sorted(f.value for f in missing)}; "
                f"a source that cannot supply them cannot be scored at all"
            )
        contradictions = sorted(
            field.value
            for field in CanonicalField
            if field not in self.field_coverage and getattr(self, field.value, None) is not None
        )
        if contradictions:
            raise ValueError(
                f"{contradictions} are populated but not declared in field_coverage. "
                f"Declared coverage is what every UNAVAILABLE decision downstream rests on "
                f"(ADR-0022), so an inaccurate declaration silently invalidates them."
            )
        return self

    # ------------------------------------------------------------- helpers --

    def covers(self, field: CanonicalField) -> bool:
        """Whether THIS SOURCE supplies `field` at all."""
        return field in self.field_coverage

    def value_of(self, field: CanonicalField) -> object:
        """The field's value, or None.

        Returns None both for "not covered" and for "covered but null on this
        row". Callers that need to tell them apart ask `covers()` — which is the
        whole point of keeping the two representable.
        """
        return getattr(self, field.value, None)
