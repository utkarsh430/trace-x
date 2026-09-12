"""`GeneratorAdapter` — Track A, full coverage (ADR-0021, ADR-0022).

Maps `tx.raw.v1` events onto `CanonicalTransaction`.

Its input is the **released wire contract**, not the generator's internals. That
matters: it means Track A flows through exactly the path any external producer
would, so "ingestion adaptability" is tested against a real contract rather than
against a convenient in-memory structure. It also means this adapter was
writable before the generator existed.

Full coverage is a claim this adapter has to earn — `SourceAdapterConformanceSuite`
checks the declaration against the rows actually produced, so declaring a field
the generator does not populate fails the build rather than silently making a
downstream feature look available.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Iterator
from typing import Any, Final

from trace_core.contracts.canonical import (
    CanonicalField,
    CanonicalTransaction,
    wire_enum_to_domain,
)
from trace_core.contracts.events.tx_raw_v1 import TxRawV1
from trace_core.contracts.source_adapter import SourceProfile
from trace_core.domain.enums import AuthorizationOutcome, EntryMode, TransactionChannel

ADAPTER_NAME: Final = "generator"
ADAPTER_VERSION: Final = "1.0.0"
"""Semantic version of the MAPPING. Changing how a field is derived changes this,
and therefore changes `source_adapter_version` in every run manifest (ADR-0017)."""

SOURCE_DATASET: Final = "trace-x-generator"

FULL_COVERAGE: Final[frozenset[CanonicalField]] = frozenset(CanonicalField)
"""Track A supplies every canonical field. This is the contrast Track B exists to
provide: IEEE-CIS has no true merchant id and no lat/lon, so its coverage is a
strict subset and roughly half the canonical feature set is UNAVAILABLE there."""

EMPTY_DIGEST: Final = "sha256:" + hashlib.sha256(b"").hexdigest()


class GeneratorAdapter:
    """Track A adapter. Stateless apart from its declared profile."""

    def __init__(
        self,
        *,
        row_count: int = 0,
        digest: str = EMPTY_DIGEST,
        dataset: str = SOURCE_DATASET,
    ) -> None:
        self._row_count = row_count
        self._digest = digest
        self._dataset = dataset

    # ------------------------------------------------------------- port ----

    def describe(self) -> SourceProfile:
        return SourceProfile(
            name=ADAPTER_NAME,
            version=ADAPTER_VERSION,
            field_coverage=FULL_COVERAGE,
            row_count=self._row_count,
            digest=self._digest,
        )

    def to_canonical(self, batch: Iterable[Any]) -> Iterator[CanonicalTransaction]:
        for row in batch:
            yield self._one(row)

    def label_column(self) -> str:
        """The generator's label lives in `groundtruth.transaction_labels`.

        Only a name: this adapter never reads it. `trace_app` has no grant on
        that schema at all, so resolving it is structurally impossible from here
        (ADR-0004) -- which is the point.
        """
        return "is_fraud"

    # -------------------------------------------------------- internals ----

    def _one(self, row: Any) -> CanonicalTransaction:
        event = row if isinstance(row, TxRawV1) else self._parse(row)
        envelope, payload = event.envelope, event.payload
        return CanonicalTransaction(
            source_dataset=self._dataset,
            # The event_id, not the transaction_id: it is the unique identity of
            # the RECORD that arrived, so a replayed duplicate is traceable to
            # the exact row it came from.
            source_row_id=envelope.event_id,
            field_coverage=FULL_COVERAGE,
            transaction_id=payload.transaction_id,
            account_id=payload.account_id,
            amount_minor=payload.amount_minor,
            currency=payload.currency,
            occurred_at=envelope.occurred_at,
            ingested_at=envelope.ingested_at,
            card_id=payload.card_id,
            device_id=payload.device_id,
            merchant_id=payload.merchant_id,
            ip_id=payload.ip_id,
            # The wire enums generated from the schema are distinct types from
            # the domain enums; this is the one boundary where they meet.
            channel=wire_enum_to_domain(TransactionChannel, payload.channel.value),
            entry_mode=wire_enum_to_domain(EntryMode, payload.entry_mode.value),
            merchant_mcc=payload.merchant_mcc,
            merchant_country=payload.merchant_country,
            merchant_name=payload.merchant_name,
            latitude=payload.latitude,
            longitude=payload.longitude,
            user_agent=payload.user_agent,
            memo=payload.memo,
            authorization_outcome=wire_enum_to_domain(
                AuthorizationOutcome, payload.authorization_outcome.value
            ),
            opaque_features={},
        )

    @staticmethod
    def _parse(row: Any) -> TxRawV1:
        """Validate a wire row in JSON mode.

        A mapping is re-serialised rather than passed to `model_validate`,
        because the models are strict and strictness differs between modes: in
        Python mode an ISO 8601 string is NOT a datetime and a bare string is
        NOT an enum member, so a dict that came from JSON fails. Serialising and
        validating as JSON is the correct reading of such a dict.

        It costs a round trip, which is acceptable here and deliberately not
        relied on where it would matter: the generator constructs `TxRawV1`
        instances directly (the `isinstance` fast path above), and the hot path
        in Phase 2 never reaches this branch.
        """
        if isinstance(row, str | bytes | bytearray):
            return TxRawV1.model_validate_json(row)
        if isinstance(row, dict):
            return TxRawV1.model_validate_json(json.dumps(row, default=str))
        raise TypeError(
            f"GeneratorAdapter accepts TxRawV1, a mapping or JSON text; got {type(row).__name__}"
        )
