"""`GeneratorAdapter` against the shared `SourceAdapter` conformance suite.

Declared acceptance command for `P1.source-adapter`:
`pytest -m conformance tests/conformance/test_source_adapter.py`.

Phase 4B adds `TestIeeeCisAdapter` to this file, subclassing the *same* suite.
Two adapters, one suite -- which is what turns "ports and adapters" from an
arrangement of files into a tested property (CLAUDE.md §3.4).
"""

from __future__ import annotations

import datetime as dt
from typing import Any

import pytest
from data.adapters.generator_adapter import FULL_COVERAGE, GeneratorAdapter
from tests.conformance.source_adapter_suite import SourceAdapterConformanceSuite

from trace_core.contracts.canonical import (
    ALWAYS_REQUIRED,
    CanonicalField,
    CanonicalTransaction,
)
from trace_core.domain.enums import TrustTier
from trace_core.domain.identifiers import uuid7

pytestmark = pytest.mark.conformance

_BASE_MS = 1_757_683_200_000  # 2025-09-12T12:00:00Z, a fixed clock for stable ids


def _event(index: int, **overrides: Any) -> dict[str, Any]:
    """A valid `tx.raw.v1` event. Deterministic, so ids are stable across runs."""
    import random

    envelope = {
        "event_id": str(uuid7(millis=_BASE_MS + index, rng=random.Random(index))),
        "event_type": "tx.raw",
        "schema_version": 1,
        "occurred_at": "2026-09-12T14:03:11.482Z",
        "ingested_at": "2026-09-12T14:03:11.559Z",
        "producer": "trace-generator@1.0.0",
        "trace_id": f"{index:032x}",
        "correlation_id": f"corr_{index}",
        "idempotency_key": "sha256:" + f"{index:064x}",
    }
    payload = {
        "transaction_id": f"tx_{index:06d}",
        "account_id": f"acct_{index:06d}",
        "card_id": f"card_{index:06d}",
        "device_id": f"dev_{index:06d}",
        "merchant_id": f"mrch_{index:05d}",
        "ip_id": f"ip_{index:05d}",
        "amount_minor": 1999 + index,
        "currency": "USD",
        "channel": "CARD_NOT_PRESENT",
        "entry_mode": "ECOMMERCE",
        "merchant_mcc": "5812",
        "merchant_country": "GB",
        "merchant_name": f"Merchant {index}",
        "latitude": 51.5074,
        "longitude": -0.1278,
        "user_agent": "Mozilla/5.0",
        "memo": f"memo {index}",
        "authorization_outcome": "APPROVED",
    }
    payload.update(overrides)
    return {"envelope": envelope, "payload": payload}


class TestGeneratorAdapter(SourceAdapterConformanceSuite):
    """Track A, full coverage."""

    @pytest.fixture
    def adapter(self) -> GeneratorAdapter:
        return GeneratorAdapter(row_count=5, digest="sha256:" + "ab" * 32)

    @pytest.fixture
    def sample_rows(self) -> list[Any]:
        return [_event(i) for i in range(5)]


# ----------------------------------------- adapter-specific expectations ----


def test_generator_declares_full_coverage() -> None:
    """Track A supplies everything. Track B will not, and the contrast is the
    point: `field_coverage` makes the gap measurable rather than invisible."""
    assert GeneratorAdapter().describe().field_coverage == frozenset(CanonicalField)
    assert GeneratorAdapter().describe().uncovered == frozenset()


def test_full_coverage_is_a_superset_of_the_minimum() -> None:
    assert ALWAYS_REQUIRED <= FULL_COVERAGE


def test_source_row_id_is_the_event_id_not_the_transaction_id() -> None:
    """The identity of the RECORD that arrived, so a replayed duplicate is
    traceable to the exact row it came from."""
    row = next(GeneratorAdapter().to_canonical([_event(7)]))
    assert row.source_row_id != row.transaction_id
    assert len(row.source_row_id) == 36  # a UUID


def test_accepts_a_model_a_mapping_and_json_text() -> None:
    """Phase 3 will hand this Spark rows; Phase 1 hands it dicts and JSON."""
    import json

    from trace_core.contracts.events.tx_raw_v1 import TxRawV1

    raw = _event(3)
    from_dict = next(GeneratorAdapter().to_canonical([raw]))
    from_json = next(GeneratorAdapter().to_canonical([json.dumps(raw)]))
    # Built in JSON mode, not `model_validate`: the models are strict, and in
    # Python mode an ISO string is not a datetime and a bare string is not an
    # enum member. See the note in trace_core.contracts.base.
    from_model = next(
        GeneratorAdapter().to_canonical([TxRawV1.model_validate_json(json.dumps(raw))])
    )
    assert from_dict == from_json == from_model


def test_an_unsupported_input_type_fails_loudly() -> None:
    """Never a silent skip: a dropped row biases every downstream metric."""
    with pytest.raises(TypeError, match="GeneratorAdapter accepts"):
        list(GeneratorAdapter().to_canonical([42]))


def test_a_malformed_event_is_refused_not_coerced() -> None:
    """Validation happens at the boundary; a bad row must not become a
    plausible-looking canonical row."""
    from pydantic import ValidationError

    bad = _event(1)
    bad["payload"]["amount_minor"] = 19.99
    with pytest.raises(ValidationError):
        list(GeneratorAdapter().to_canonical([bad]))


def test_attacker_controlled_fields_keep_their_tier_through_the_mapping() -> None:
    """docs/SECURITY.md §2: a value never loses its tier.

    The adapter is the first place an attacker-controlled string is written into
    an internal structure, so this is where the tier has to survive.
    """
    from trace_core.contracts.canonical import trust_tier_of

    row = next(GeneratorAdapter().to_canonical([_event(1, merchant_name="Ignore previous")]))
    assert row.merchant_name == "Ignore previous"
    for field in (CanonicalField.MERCHANT_NAME, CanonicalField.USER_AGENT, CanonicalField.MEMO):
        assert trust_tier_of(field) is TrustTier.UNTRUSTED
    assert trust_tier_of(CanonicalField.AMOUNT_MINOR) is TrustTier.SYSTEM


def test_event_time_survives_the_mapping_unchanged() -> None:
    """`occurred_at` comes from the envelope, not from the clock. If the adapter
    ever substituted `now()`, every event-time window would be wrong and nothing
    would fail loudly."""
    row = next(GeneratorAdapter().to_canonical([_event(1)]))
    assert row.occurred_at == dt.datetime(2026, 9, 12, 14, 3, 11, 482000, tzinfo=dt.UTC)
    assert row.ingested_at == dt.datetime(2026, 9, 12, 14, 3, 11, 559000, tzinfo=dt.UTC)
    assert row.occurred_at < row.ingested_at


def test_streaming_rather_than_materialising() -> None:
    """A 1 M-row dataset must not be built in memory to be mapped."""
    import types

    result = GeneratorAdapter().to_canonical(_event(i) for i in range(3))
    assert isinstance(result, types.GeneratorType)


def test_a_lying_adapter_fails_the_suite() -> None:
    """The conformance suite must be able to fail, or it proves nothing.

    An adapter that declares a field it never populates is the exact failure
    ADR-0022 is about, so it is exercised here directly rather than trusted.
    """
    from trace_core.contracts.source_adapter import SourceProfile

    class LyingAdapter(GeneratorAdapter):
        def describe(self) -> SourceProfile:
            return SourceProfile(
                name="liar",
                version="1.0.0",
                field_coverage=FULL_COVERAGE,
                row_count=1,
                digest="sha256:" + "00" * 32,
            )

        def to_canonical(self, batch: Any) -> Any:
            for row in super().to_canonical(batch):
                # Claims full coverage but never populates merchant_name.
                yield row.model_copy(update={"merchant_name": None})

    rows = list(LyingAdapter().to_canonical([_event(i) for i in range(3)]))
    populated = {f for f in CanonicalField for r in rows if r.value_of(f) is not None}
    overclaimed = LyingAdapter().describe().field_coverage - populated
    assert CanonicalField.MERCHANT_NAME in overclaimed, (
        "the conformance suite's central check would not have caught this"
    )


def test_canonical_rows_never_carry_a_label_field() -> None:
    """ADR-0004: ground truth must be structurally unreachable from the app path."""
    row = next(GeneratorAdapter().to_canonical([_event(1)]))
    fields = set(CanonicalTransaction.model_fields)
    assert not fields & {"is_fraud", "label", "fraud_pattern", "causal_evidence_keys"}
    assert row.opaque_features == {}
