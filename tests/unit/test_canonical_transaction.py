"""`CanonicalTransaction` and the declared-coverage invariant (ADR-0022).

The invariant these tests protect is subtle and load-bearing: coverage
distinguishes "this source never provides the field" from "this row happens to
be missing it". Collapsing the two is what turns an honest gap into a fabricated
signal, and it is invisible in every downstream report.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from trace_core.contracts.canonical import (
    ALWAYS_REQUIRED,
    UNTRUSTED_FIELDS,
    CanonicalField,
    CanonicalTransaction,
    trust_tier_of,
    wire_enum_to_domain,
)
from trace_core.domain.enums import TransactionChannel, TrustTier

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = ROOT / "docs" / "contracts" / "events"

_REQUIRED_VALUES = {
    "transaction_id": "tx_1",
    "account_id": "acct_000001",
    "amount_minor": 1999,
    "currency": "USD",
    "occurred_at": dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
    "ingested_at": dt.datetime(2026, 1, 1, 0, 0, 1, tzinfo=dt.UTC),
}


def _row(**overrides: object) -> CanonicalTransaction:
    kwargs: dict[str, object] = {
        "source_dataset": "probe",
        "source_row_id": "row-1",
        "field_coverage": frozenset(ALWAYS_REQUIRED),
        **_REQUIRED_VALUES,
    }
    kwargs.update(overrides)
    return CanonicalTransaction(**kwargs)  # type: ignore[arg-type]


# -------------------------------------------------------- the invariant ----


def test_a_minimal_source_is_valid() -> None:
    """IEEE-CIS clears exactly this bar, which is what makes Track B possible."""
    row = _row()
    assert row.covers(CanonicalField.ACCOUNT_ID)
    assert not row.covers(CanonicalField.MERCHANT_ID)


def test_coverage_must_include_the_irreducible_minimum() -> None:
    with pytest.raises(ValidationError, match="cannot be scored at all"):
        _row(field_coverage=frozenset({CanonicalField.TRANSACTION_ID}))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (CanonicalField.MERCHANT_ID, "mrch_00001"),
        (CanonicalField.LATITUDE, 51.5),
        (CanonicalField.MERCHANT_NAME, "Acme"),
        (CanonicalField.CHANNEL, TransactionChannel.ATM),
    ],
)
def test_populating_an_undeclared_field_is_refused(field: CanonicalField, value: object) -> None:
    """A populated-but-undeclared field means the coverage declaration is wrong,
    and every UNAVAILABLE decision downstream was made on a false premise."""
    with pytest.raises(ValidationError, match="not declared in field_coverage"):
        _row(**{field.value: value})


def test_a_covered_field_may_be_null_on_a_given_row() -> None:
    """The converse is deliberately NOT enforced.

    "This source supplies merchant_id, but this particular row has none" is
    ordinary sparsity. Conflating it with "this source never supplies it" is the
    distinction ADR-0022 exists to preserve, so both must be representable.
    """
    row = _row(
        field_coverage=frozenset(ALWAYS_REQUIRED | {CanonicalField.MERCHANT_ID}),
        merchant_id=None,
    )
    assert row.covers(CanonicalField.MERCHANT_ID)
    assert row.value_of(CanonicalField.MERCHANT_ID) is None


def test_covers_and_value_of_answer_different_questions() -> None:
    """`value_of` returns None for both cases on purpose; `covers` disambiguates."""
    uncovered = _row()
    covered_but_null = _row(
        field_coverage=frozenset(ALWAYS_REQUIRED | {CanonicalField.MEMO}), memo=None
    )
    assert uncovered.value_of(CanonicalField.MEMO) is None
    assert covered_but_null.value_of(CanonicalField.MEMO) is None
    assert uncovered.covers(CanonicalField.MEMO) is False
    assert covered_but_null.covers(CanonicalField.MEMO) is True


def test_rows_are_immutable() -> None:
    with pytest.raises(ValidationError):
        _row().amount_minor = 1


def test_unknown_fields_are_refused() -> None:
    with pytest.raises(ValidationError):
        _row(not_a_canonical_field="x")


def test_money_must_be_an_integer() -> None:
    with pytest.raises(ValidationError):
        _row(amount_minor=19.99)


def test_naive_datetimes_are_refused() -> None:
    """`occurred_at` drives every window; an ambiguous value corrupts them all."""
    with pytest.raises(ValidationError):
        _row(occurred_at=dt.datetime(2026, 1, 1))


def test_opaque_features_default_to_empty_not_to_a_shared_dict() -> None:
    """A shared mutable default would let one row's opaque columns leak into
    every other row constructed in the process."""
    a, b = _row(), _row()
    assert a.opaque_features == b.opaque_features == {}
    assert a.opaque_features is not b.opaque_features


# ------------------------------------------------------------ trust tiers --


def test_attacker_controlled_fields_are_untrusted() -> None:
    for field in UNTRUSTED_FIELDS:
        assert trust_tier_of(field) is TrustTier.UNTRUSTED


def test_computed_fields_are_system_tier() -> None:
    for field in (
        CanonicalField.AMOUNT_MINOR,
        CanonicalField.ACCOUNT_ID,
        CanonicalField.OCCURRED_AT,
    ):
        assert trust_tier_of(field) is TrustTier.SYSTEM


def test_trust_tiers_agree_with_the_committed_schemas() -> None:
    """Three places name the attacker-controlled fields: SECURITY.md §5.1, the
    JSON Schemas' `x-trust-tier`, and `UNTRUSTED_FIELDS` here. Any two drifting
    apart would leave a field untagged somewhere, so they are tied together.
    """
    tagged: set[str] = set()

    def walk(node: object) -> None:
        if isinstance(node, dict):
            for name, sub in (node.get("properties") or {}).items():
                if isinstance(sub, dict) and sub.get("x-trust-tier") == "UNTRUSTED":
                    tagged.add(name)
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    for path in SCHEMA_DIR.glob("*.json"):
        walk(json.loads(path.read_text()))

    canonical_untrusted = {f.value for f in UNTRUSTED_FIELDS}
    # `device_label` is tagged in the schemas but arrives on device.events.v1,
    # so it is not a canonical TRANSACTION field. That asymmetry is expected and
    # is asserted rather than ignored.
    assert tagged - canonical_untrusted == {"device_label"}
    assert canonical_untrusted <= tagged


def test_security_md_names_the_same_fields() -> None:
    text = (ROOT / "docs" / "SECURITY.md").read_text()
    listed = set(re.findall(r"`(merchant_name|user_agent|memo|device_label)`", text))
    assert {f.value for f in UNTRUSTED_FIELDS} <= listed


# ----------------------------------------------------- enum degradation ----


def test_a_known_wire_value_maps_to_its_domain_member() -> None:
    assert wire_enum_to_domain(TransactionChannel, "ATM") is TransactionChannel.ATM


def test_an_unseen_wire_value_degrades_to_unknown() -> None:
    """EVENT_CONTRACTS.md §4: adding an enum value is compatible only if
    consumers have an UNKNOWN branch. This is that branch."""
    assert wire_enum_to_domain(TransactionChannel, "QUANTUM_TUNNEL") is TransactionChannel.UNKNOWN


def test_none_stays_none() -> None:
    assert wire_enum_to_domain(TransactionChannel, None) is None


def test_a_closed_enum_without_unknown_raises_rather_than_guessing() -> None:
    """Closed enums have no UNKNOWN member by design (ActionType, TrustTier).
    Degrading there would punch a hole in the action-safety control, so an
    unrecognised value must raise instead."""
    from trace_core.domain.enums import ActionType

    with pytest.raises(ValueError, match="not a valid ActionType"):
        wire_enum_to_domain(ActionType, "DRAIN_ACCOUNT")
