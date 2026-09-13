"""The envelope builder must satisfy the released envelope schema, exactly.

`envelope.v1.json` is released and immutable. Its `idempotency_key` field says
*"Deterministic hash of the semantic content. Equal keys mean equal meaning."* --
a claim with observable consequences, since duplicate detection rests on it.

These tests validate produced envelopes against the **committed JSON Schema
itself**, not against a Python mirror of it: a mirror can agree with the code and
still disagree with the contract.
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any
from uuid import UUID

import jsonschema
import pytest

from trace_core.contracts.canonical_json import content_hash
from trace_core.contracts.envelope import (
    ZERO_TRACE_ID,
    build_envelope,
    build_event,
    semantic_content,
)
from trace_core.domain.errors import ContractError
from trace_core.domain.identifiers import uuid7_millis
from trace_core.domain.time import event_time, processing_time, to_millis

pytestmark = pytest.mark.contract

ROOT = Path(__file__).resolve().parents[2]
ENVELOPE_SCHEMA = ROOT / "docs" / "contracts" / "events" / "envelope.v1.json"

PRODUCER = "trace-gateway@0.1.0"
OCCURRED = event_time(dt.datetime(2026, 9, 12, 10, 0, 0, tzinfo=dt.UTC))
PAYLOAD: dict[str, Any] = {
    "transaction_id": "tx_0000000001",
    "account_id": "acct_000001",
    "amount_minor": 1999,
    "currency": "GBP",
}


@pytest.fixture(scope="module")
def validator() -> jsonschema.protocols.Validator:
    schema = json.loads(ENVELOPE_SCHEMA.read_text())
    cls = jsonschema.validators.validator_for(schema)
    cls.check_schema(schema)
    return cls(schema)


def _envelope(**over: Any) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "event_type": "tx.raw",
        "occurred_at": OCCURRED,
        "payload": PAYLOAD,
        "producer": PRODUCER,
        "trace_id": ZERO_TRACE_ID,
        "correlation_id": "corr_0000000001",
    }
    kwargs.update(over)
    return build_envelope(**kwargs)


def test_a_built_envelope_satisfies_the_released_schema(
    validator: jsonschema.protocols.Validator,
) -> None:
    validator.validate(_envelope())


def test_every_required_envelope_field_is_present(
    validator: jsonschema.protocols.Validator,
) -> None:
    required = set(json.loads(ENVELOPE_SCHEMA.read_text())["required"])
    assert set(_envelope()) == required, (
        "the builder emits a different field set than the released envelope requires"
    )


def test_the_event_id_is_a_uuid7_carrying_the_event_time() -> None:
    """Time-ordered by construction: it is the dedup key in Redis and in Spark,
    and Delta index locality depends on it sorting by time."""
    envelope = _envelope()
    assert uuid7_millis(UUID(envelope["event_id"])) == to_millis(OCCURRED)


# --- idempotency_key is a content hash, not entropy -------------------------


def test_identical_semantic_content_yields_an_identical_key() -> None:
    """The contract's actual claim: equal keys mean equal meaning."""
    assert _envelope()["idempotency_key"] == _envelope()["idempotency_key"]


def test_the_key_ignores_fields_that_differ_between_deliveries() -> None:
    """Two deliveries of the same fact carry different ids, traces and arrival
    times. If any of those entered the hash, a retry would look like new data."""
    first = _envelope()
    second = _envelope(
        trace_id="a" * 32,
        correlation_id="corr_something_else",
        ingested_at=processing_time(dt.datetime(2027, 1, 1, tzinfo=dt.UTC)),
    )
    assert first["idempotency_key"] == second["idempotency_key"]


def test_the_key_changes_when_any_payload_field_changes() -> None:
    for field, value in (
        ("amount_minor", 2000),
        ("account_id", "acct_000002"),
        ("currency", "EUR"),
    ):
        assert (
            _envelope(payload={**PAYLOAD, field: value})["idempotency_key"]
            != (_envelope()["idempotency_key"])
        ), f"changing {field} did not change the idempotency key"


def test_the_key_changes_when_the_event_time_changes() -> None:
    """The same payload an hour later is a different event, not a duplicate."""
    later = event_time(dt.datetime(2026, 9, 12, 11, 0, 0, tzinfo=dt.UTC))
    assert _envelope(occurred_at=later)["idempotency_key"] != _envelope()["idempotency_key"]


def test_the_key_is_independent_of_payload_key_order() -> None:
    """A dict literal's insertion order is an implementation detail; letting it
    into the hash would make an unrelated refactor look like new data."""
    reordered = dict(reversed(list(PAYLOAD.items())))
    assert _envelope(payload=reordered)["idempotency_key"] == _envelope()["idempotency_key"]


def test_semantic_content_excludes_processing_time() -> None:
    content = semantic_content(event_type="tx.raw", occurred_at=OCCURRED, payload=PAYLOAD)
    assert "ingested_at" not in content
    assert "event_id" not in content
    assert "producer" not in content
    assert content["occurred_at"] == "2026-09-12T10:00:00Z"


# --- the two clocks stay apart ----------------------------------------------


def test_processing_time_defaults_to_now_and_is_not_the_event_time() -> None:
    envelope = _envelope()
    assert envelope["occurred_at"] == "2026-09-12T10:00:00Z"
    assert envelope["ingested_at"] != envelope["occurred_at"], (
        "ingested_at was set to the event time; the lag metric would read zero forever"
    )


def test_an_explicit_processing_time_is_honoured() -> None:
    observed = processing_time(dt.datetime(2026, 9, 12, 10, 0, 1, tzinfo=dt.UTC))
    assert _envelope(ingested_at=observed)["ingested_at"] == "2026-09-12T10:00:01Z"


# --- malformed inputs refuse rather than producing an invalid event ---------


@pytest.mark.parametrize("bad", ["", "xyz", "A" * 32, "0" * 31, "0" * 33])
def test_a_malformed_trace_id_is_refused(bad: str) -> None:
    """Producing it would publish an event that fails its own released schema."""
    with pytest.raises(ContractError):
        _envelope(trace_id=bad)


@pytest.mark.parametrize("bad", ["", "c" * 65])
def test_a_malformed_correlation_id_is_refused(bad: str) -> None:
    with pytest.raises(ContractError):
        _envelope(correlation_id=bad)


def test_build_event_wraps_envelope_and_payload_without_nesting() -> None:
    """Envelope fields are never nested inside payload, and payload never
    redefines one (docs/EVENT_CONTRACTS.md §2)."""
    event = build_event(
        event_type="tx.raw",
        occurred_at=OCCURRED,
        payload=PAYLOAD,
        producer=PRODUCER,
        trace_id=ZERO_TRACE_ID,
        correlation_id="corr_0000000001",
    )
    assert set(event) == {"envelope", "payload"}
    assert not set(event["envelope"]) & set(event["payload"])


# --- the recorded divergence in the Phase 1 generator -----------------------


def test_the_generator_envelope_diverges_and_that_is_recorded() -> None:
    """`data.generator` emits a RANDOM idempotency_key, not a content hash.

    This is a real divergence from the released schema's stated meaning, and it
    is deliberately not fixed: the key sits inside the canonical row JSON that
    the dataset digest covers, so changing it would move `eval-v1`'s frozen
    digest and invalidate every result citing it (ADR-0029,
    docs/EVALUATION.md §2). The test exists so the divergence cannot be
    forgotten, and so a future fix is a conscious dataset-version decision
    rather than an accident.
    """
    from data.generator.config import GeneratorConfig
    from data.generator.engine import generate_events

    config = GeneratorConfig(row_count=2, account_count=5, merchant_count=3)
    keys = [e["envelope"]["idempotency_key"] for e in generate_events(config)]
    assert all(k.startswith("sha256:") for k in keys), "shape still satisfies the schema"

    # If this ever becomes true, the generator has been changed to hash content --
    # which means eval-v1's digest has moved and a new dataset version is due.
    hashed = [
        semantic_content(
            event_type=e["envelope"]["event_type"],
            occurred_at=event_time(dt.datetime.fromisoformat(e["envelope"]["occurred_at"])),
            payload=e["payload"],
        )
        for e in generate_events(config)
    ]
    assert hashed, "generator produced nothing; the check would be vacuous"
    assert keys != [content_hash(h) for h in hashed], (
        "the generator now content-hashes its idempotency_key. That is the right "
        "behaviour, but it MOVES eval-v1's dataset digest: cut a new dataset version "
        "(docs/EVALUATION.md §2) and update eval/track_a/ rather than regenerating "
        "in place."
    )
