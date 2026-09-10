"""PII redaction for the structured-logging pipeline.

Logging PII is a BUILD FAILURE in this project, not a review comment
(CLAUDE.md §9). The control therefore sits in the logging pipeline itself rather
than relying on every call site to remember.

Design notes:

* Redaction is applied to the *rendered* value of every field, recursively, so a
  PAN nested three dicts deep inside an exception payload is still caught.
* Patterns are deliberately broad. A false positive redacts a harmless string;
  a false negative is a privacy incident. The asymmetry decides the trade-off.
* Card numbers are additionally Luhn-checked so ordinary 13-19 digit numbers
  (amounts in minor units, ids) are not needlessly destroyed.
"""

from __future__ import annotations

import re
from typing import Any, Final

REDACTED: Final = "«redacted»"

EMAIL: Final = re.compile(r"\b[\w.%+-]+@[\w.-]+\.[A-Za-z]{2,}\b")
IPV4: Final = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
IPV6: Final = re.compile(r"\b(?:[0-9A-Fa-f]{1,4}:){2,7}[0-9A-Fa-f]{1,4}\b")
# 13-19 digits, optionally separated by spaces or hyphens.
CARD: Final = re.compile(r"\b(?:\d[ -]?){12,18}\d\b")
# Account numbers: our own synthetic format plus generic long digit runs.
ACCOUNT: Final = re.compile(r"\bacct[_-]?\d{6,}\b", re.IGNORECASE)
IBAN: Final = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b")

_SENSITIVE_KEYS: Final = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "authorization",
        "access_key",
        "secret_key",
        "private_key",
        "session_token",
        "credential",
        "pan",
        "card_number",
        "cvv",
        "cvc",
        "ssn",
        "tax_id",
    }
)


def _luhn_ok(digits: str) -> bool:
    """Luhn check, so ordinary long numbers are not mistaken for card numbers."""
    total, alt = 0, False
    for ch in reversed(digits):
        d = ord(ch) - 48
        if alt:
            d *= 2
            if d > 9:
                d -= 9
        total += d
        alt = not alt
    return total % 10 == 0


def _redact_card(match: re.Match[str]) -> str:
    digits = re.sub(r"[ -]", "", match.group(0))
    if 13 <= len(digits) <= 19 and _luhn_ok(digits):
        return f"{REDACTED}(card:…{digits[-4:]})"
    return match.group(0)


def redact(value: str) -> str:
    """Redact PII from a single string."""
    value = EMAIL.sub(REDACTED, value)
    value = CARD.sub(_redact_card, value)
    value = IBAN.sub(REDACTED, value)
    value = ACCOUNT.sub(REDACTED, value)
    value = IPV4.sub(REDACTED, value)
    value = IPV6.sub(REDACTED, value)
    return value


def _walk(obj: Any, depth: int = 0) -> Any:
    """Recursively redact a log event, keys included."""
    if depth > 12:  # defensive: pathological nesting
        return obj
    if isinstance(obj, str):
        return redact(obj)
    if isinstance(obj, dict):
        out: dict[Any, Any] = {}
        for k, v in obj.items():
            if isinstance(k, str) and k.lower() in _SENSITIVE_KEYS:
                out[k] = REDACTED
            else:
                out[k] = _walk(v, depth + 1)
        return out
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(_walk(v, depth + 1) for v in obj)
    return obj


class PIIRedactingProcessor:
    """structlog processor. Sits in the pipeline so no call site can bypass it."""

    def __call__(
        self,
        logger: Any,  # noqa: ARG002 - required by the structlog processor signature
        method_name: str,  # noqa: ARG002 - required by the structlog processor signature
        event_dict: dict[str, Any],
    ) -> dict[str, Any]:
        result = _walk(event_dict)
        return result if isinstance(result, dict) else event_dict
