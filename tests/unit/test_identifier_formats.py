"""Identifier formats are pinned in three places at once, and they must agree.

Three independent things encode the same fact:

1. `docs/contracts/events/tx.raw.v1.json` -- a **released, immutable** schema
   whose patterns every producer must satisfy;
2. `trace_core.domain.identifiers` -- the formats the generator actually mints;
3. `trace_core.observability.redaction.ACCOUNT` -- the pattern that keeps account
   identifiers out of log output.

`docs/API_CONTRACTS.md` §6.4 stated a fourth, different set (`acc_`, `mer_`),
which was wrong in a way that would have been expensive. Validating inbound
transactions against `acc_` would have rejected every event the Phase 1
generator produces, and -- worse -- an `acc_`-prefixed identifier does not match
the redaction pattern, so it would have travelled straight into logs (CLAUDE.md
§9: logging PII is a build failure, not a review comment).

The released schema cannot change (docs/EVENT_CONTRACTS.md §4), so it is the
authority here and the other three are checked against it.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from trace_core.domain import identifiers
from trace_core.observability.redaction import ACCOUNT

pytestmark = [pytest.mark.unit, pytest.mark.security]

ROOT = Path(__file__).resolve().parents[2]
TX_SCHEMA = ROOT / "docs" / "contracts" / "events" / "tx.raw.v1.json"
API_CONTRACTS = ROOT / "docs" / "API_CONTRACTS.md"

# canonical field -> the module-level format constant that mints it
FORMATS = {
    "account_id": identifiers.ACCOUNT_FORMAT,
    "card_id": identifiers.CARD_FORMAT,
    "device_id": identifiers.DEVICE_FORMAT,
    "merchant_id": identifiers.MERCHANT_FORMAT,
    "ip_id": identifiers.IP_FORMAT,
}


@pytest.fixture(scope="module")
def schema_patterns() -> dict[str, str]:
    schema = json.loads(TX_SCHEMA.read_text())
    props = schema["properties"]["payload"]["properties"]
    return {name: props[name]["pattern"] for name in FORMATS if "pattern" in props[name]}


def test_every_minted_identifier_satisfies_the_released_schema(
    schema_patterns: dict[str, str],
) -> None:
    """The generator's output must be publishable against its own frozen contract."""
    assert set(schema_patterns) == set(FORMATS), (
        "the released schema constrains a different set of identifier fields than "
        "identifiers.py mints; one of them has drifted"
    )
    for field, pattern in schema_patterns.items():
        # Index 1 is the smallest a format is ever called with, and 999999 the
        # widest the zero-padded width covers -- the two ends of the range.
        for index in (0, 1, 42, 999_999):
            minted = FORMATS[field].format(index)
            assert re.fullmatch(pattern, minted), (
                f"{field}: identifiers.py mints {minted!r}, which does not match the "
                f"RELEASED schema pattern {pattern!r}. The schema is immutable "
                f"(docs/EVENT_CONTRACTS.md §4), so the format is what must change."
            )


def test_minted_account_ids_are_redacted_from_logs() -> None:
    """The closed hole: an identifier format the redactor does not recognise.

    `acct_000123` matches; `acc_000123` -- the prefix API_CONTRACTS used to state
    -- does not, and would have been logged verbatim.
    """
    for index in (1, 42, 999_999):
        minted = identifiers.ACCOUNT_FORMAT.format(index)
        assert ACCOUNT.search(minted), (
            f"{minted!r} is not matched by the PII redaction pattern. Account "
            f"identifiers would reach log output (CLAUDE.md §9)."
        )
    assert not ACCOUNT.search("acc_000123"), (
        "the redaction pattern has widened to cover `acc_`; if identifiers moved to "
        "that prefix, this test and the released schema must be revisited together"
    )


def test_api_contracts_states_the_released_prefixes() -> None:
    """The documented prefixes must be the ones the frozen contract requires."""
    text = API_CONTRACTS.read_text()
    line = next(
        (ln for ln in text.splitlines() if "Identifiers are typed and prefixed" in ln),
        None,
    )
    assert line is not None, "API_CONTRACTS.md §6.4 no longer states identifier prefixes"
    for prefix in ("acct_", "card_", "dev_", "mrch_", "ip_"):
        assert f"`{prefix}`" in line, (
            f"API_CONTRACTS.md §6.4 does not list {prefix!r}, which the released "
            f"tx.raw.v1 schema requires. A gateway validating the documented prefix "
            f"would reject every generated transaction."
        )
    for wrong in ("`acc_`", "`mer_`"):
        assert wrong not in line, (
            f"API_CONTRACTS.md §6.4 lists {wrong}, which contradicts the released schema "
            f"and escapes PII redaction"
        )
