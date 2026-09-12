"""Domain enumerations, and the security boundary between two kinds of them.

The interesting assertion here is the one about which enums may carry UNKNOWN.
`docs/EVENT_CONTRACTS.md` §4 requires wire enums to tolerate an unseen value.
Applying that blanket rule to every enum would be a security regression:
`ActionType` guarantees a model cannot emit an out-of-enum action
(docs/SECURITY.md §5.5), and an UNKNOWN member is exactly somewhere for an
out-of-enum action to land.
"""

from __future__ import annotations

import re
from enum import StrEnum
from pathlib import Path

import pytest

from trace_core.domain.enums import (
    WIRE_ENUMS,
    ActionType,
    AuthorizationOutcome,
    DeviceEventType,
    EntryMode,
    EvidenceKind,
    FeatureSource,
    FraudPattern,
    IdentityEventType,
    RiskBand,
    RiskClassification,
    TransactionChannel,
    TrustTier,
    Verdict,
)

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]

CLOSED_ENUMS: tuple[type[StrEnum], ...] = (
    TrustTier,
    RiskBand,
    FeatureSource,
    FraudPattern,
    EvidenceKind,
    ActionType,
    RiskClassification,
    Verdict,
)


# Listed explicitly rather than derived from WIRE_ENUMS, so that adding a wire
# enum without adding it here fails `test_wire_enum_cases_match_production`
# instead of silently going untested.
WIRE_ENUM_CASES: tuple[type[StrEnum], ...] = (
    AuthorizationOutcome,
    DeviceEventType,
    EntryMode,
    IdentityEventType,
    TransactionChannel,
)


def test_wire_enum_cases_match_production() -> None:
    assert set(WIRE_ENUM_CASES) == WIRE_ENUMS, (
        "WIRE_ENUMS and the parametrised cases have diverged; a wire enum is untested"
    )


@pytest.mark.parametrize("enum_cls", WIRE_ENUM_CASES, ids=lambda e: e.__name__)
def test_wire_enums_tolerate_an_unseen_value(enum_cls: type[StrEnum]) -> None:
    """A newer producer may add a value; a consumer must not crash on it."""
    assert "UNKNOWN" in enum_cls.__members__, (
        f"{enum_cls.__name__} crosses the wire, so adding a value is a compatible "
        f"change only if consumers have an UNKNOWN branch (EVENT_CONTRACTS.md §4)"
    )


@pytest.mark.parametrize("enum_cls", CLOSED_ENUMS, ids=lambda e: e.__name__)
def test_closed_enums_have_no_unknown_escape_hatch(enum_cls: type[StrEnum]) -> None:
    """Closed enums are computed internally and must fail closed, not degrade."""
    assert "UNKNOWN" not in enum_cls.__members__, (
        f"{enum_cls.__name__} is computed inside TRACE-X and never parsed from a peer. "
        f"An UNKNOWN member would be a hole: for ActionType it gives raw model output "
        f"somewhere to land, and for TrustTier it means a value can travel without "
        f"established provenance."
    )


def test_wire_and_closed_enum_sets_are_disjoint() -> None:
    assert not WIRE_ENUMS & set(CLOSED_ENUMS)


def test_all_enum_values_equal_their_names() -> None:
    """Serialised form is the member name, so a rename is a visible wire break."""
    for enum_cls in (*WIRE_ENUMS, *CLOSED_ENUMS):
        for member in enum_cls:
            assert member.value == member.name, f"{enum_cls.__name__}.{member.name} diverges"


def test_ten_fraud_patterns_exactly() -> None:
    """ROADMAP Phase 1 exit condition: ten scenarios, no more and no fewer."""
    assert len(list(FraudPattern)) == 10


# ------------------------------------------------- doc <-> code agreement ----


def _roster_evidence_tokens() -> set[str]:
    """Evidence kinds named in the agent roster table of ARCHITECTURE.md §8."""
    body = (ROOT / "docs" / "ARCHITECTURE.md").read_text()
    roster = body.split("### Agent roster", 1)[1].split("###", 1)[0]
    return set(re.findall(r"`([A-Z][A-Z_]{2,})`", roster))


def test_every_evidence_kind_the_agent_roster_declares_exists() -> None:
    """CLAUDE.md §14: code and ARCHITECTURE.md disagreeing is a bug in one of them.

    An agent declaring evidence the enum does not have would leave a permanent
    open gap, and the router would never terminate that line of enquiry
    (ADR-0019).
    """
    known = {e.value for e in EvidenceKind}
    missing = _roster_evidence_tokens() - known
    assert not missing, f"ARCHITECTURE.md §8 names evidence kinds the enum lacks: {sorted(missing)}"


def test_no_undocumented_evidence_kind() -> None:
    """The reverse direction: an enum member no agent produces is dead vocabulary."""
    extra = {e.value for e in EvidenceKind} - _roster_evidence_tokens()
    # GEO_DISPERSION is not a roster row: it is introduced by the §8 worked
    # example as a Skeptic refutation gap, which is a real producer.
    assert extra <= {"GEO_DISPERSION"}, (
        f"EvidenceKind has members §8 never mentions: {sorted(extra)}"
    )
    for value in extra:
        assert value in (ROOT / "docs" / "ARCHITECTURE.md").read_text(), (
            f"{value} appears nowhere in ARCHITECTURE.md"
        )
