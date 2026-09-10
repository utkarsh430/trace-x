"""PII must never reach log output (CLAUDE.md §9).

Logging PII is a build failure, not a review comment. These tests are the gate.
"""

from __future__ import annotations

import pytest

from trace_core.observability import PIIRedactingProcessor, redact

pytestmark = pytest.mark.unit

REDACTED = "«redacted»"


@pytest.mark.parametrize(
    ("raw", "leaked"),
    [
        ("contact analyst@bank.example.com now", "analyst@bank.example.com"),
        ("card 4111 1111 1111 1111 used", "4111 1111 1111 1111"),
        ("card 4111-1111-1111-1111 used", "4111-1111-1111-1111"),
        ("from 192.168.14.201", "192.168.14.201"),
        ("v6 2001:0db8:85a3:0000:0000:8a2e:0370:7334", "2001:0db8"),
        ("account acct_100482913 flagged", "acct_100482913"),
        ("iban GB33BUKB20201555555555 debited", "GB33BUKB20201555555555"),
    ],
)
def test_pii_is_removed_from_strings(raw: str, leaked: str) -> None:
    out = redact(raw)
    assert leaked not in out, f"PII leaked: {leaked!r} still present in {out!r}"
    assert REDACTED in out


def test_card_redaction_keeps_last_four_for_triage() -> None:
    """Analysts need last-four; the full PAN must still be gone."""
    out = redact("pan 4111111111111111")
    assert "4111111111111111" not in out
    assert "1111" in out


def test_non_card_long_numbers_survive_luhn_check() -> None:
    """A false positive destroys useful data; the Luhn check prevents it."""
    assert redact("amount_minor 1234567890123") == "amount_minor 1234567890123"


def test_sensitive_keys_are_redacted_regardless_of_value() -> None:
    out = PIIRedactingProcessor()(None, "info", {"password": "hunter2", "api_key": "sk-live-xyz"})
    assert out["password"] == REDACTED
    assert out["api_key"] == REDACTED
    assert "hunter2" not in str(out)
    assert "sk-live-xyz" not in str(out)


def test_redaction_reaches_nested_structures() -> None:
    """A PAN three dicts deep inside an exception payload must still be caught."""
    event = {
        "event": "scoring_failed",
        "ctx": {"customer": {"email": "x@y.co"}, "attempts": [{"pan": "4111111111111111"}]},
    }
    out = PIIRedactingProcessor()(None, "error", event)
    rendered = str(out)
    assert "x@y.co" not in rendered
    assert "4111111111111111" not in rendered


def test_processor_returns_a_dict_and_preserves_safe_fields() -> None:
    out = PIIRedactingProcessor()(
        None, "info", {"event": "scored", "band": "HIGH", "latency_ms": 42}
    )
    assert out["event"] == "scored"
    assert out["band"] == "HIGH"
    assert out["latency_ms"] == 42


def test_pathological_nesting_does_not_recurse_forever() -> None:
    deep: dict[str, object] = {"v": "a@b.co"}
    for _ in range(50):
        deep = {"v": deep}
    PIIRedactingProcessor()(None, "info", deep)  # must return, not blow the stack
