"""A `tx.scored.v1` delivery, read as the as-served side of parity (ADR-0051 §6; ADR-0056 §2).

Every scored delivery carries the transaction as served, the features the decision read, what the
online store did with the transaction, and the store position and epoch. The canonical transaction
the features were evaluated on is rebuilt from the payload, so both sides of a comparison evaluate
the same transaction with the same field coverage.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from eval.parity.comparator import Observed

from trace_core.contracts.canonical import CanonicalTransaction
from trace_core.contracts.events.tx_scored_v1 import TxScoredV1
from trace_core.domain.time import to_millis
from trace_core.observation.log import SEQ_HEADER, SESSION_HEADER
from trace_core.observation.scored_event import TRANSACTION_FIELDS


def parse_time(text: str) -> dt.datetime:
    moment = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        raise ValueError(f"{text!r} carries no timezone")
    return moment.astimezone(dt.UTC)


@dataclass(frozen=True)
class ServedScore:
    transaction_id: str
    canonical: CanonicalTransaction
    observe_outcome: str
    store_position: int | None
    store_epoch_ms: int | None
    degraded_reasons: tuple[str, ...]
    features: Mapping[str, Observed]
    session_id: str | None = None
    seq: int | None = None


def canonical_of(event: Mapping[str, Any]) -> CanonicalTransaction:
    """The transaction as the gateway evaluated it, from a scored event's payload."""
    envelope, payload = event["envelope"], event["payload"]
    fields = {k: v for k, v in payload.items() if k in TRANSACTION_FIELDS}
    document = {
        "source_dataset": "gateway",
        "source_row_id": payload["transaction_id"],
        "field_coverage": list(payload["field_coverage"]),
        "occurred_at": envelope["occurred_at"],
        "ingested_at": envelope["ingested_at"],
        **fields,
    }
    return CanonicalTransaction.model_validate_json(json.dumps(document))


def parse_scored(
    value: bytes | str | Mapping[str, Any],
    headers: Iterable[tuple[str, bytes | None]] = (),
) -> ServedScore:
    """Validate a delivery against the released contract, then read it."""
    text = json.dumps(value) if isinstance(value, Mapping) else value
    TxScoredV1.model_validate_json(text)
    event: dict[str, Any] = json.loads(text)
    payload = event["payload"]
    summary = payload["decision_summary"]
    by_header = dict(headers)
    session = by_header.get(SESSION_HEADER)
    seq = by_header.get(SEQ_HEADER)
    epoch = payload.get("store_epoch")
    features: dict[str, Observed] = {}
    for entry in payload["served_features"]:
        feature_id = str(entry["feature_id"])
        if feature_id in features:
            raise ValueError(f"{payload['transaction_id']}: {feature_id} served twice")
        features[feature_id] = Observed.served(entry)
    return ServedScore(
        transaction_id=str(payload["transaction_id"]),
        canonical=canonical_of(event),
        observe_outcome=str(payload["observe_outcome"]),
        store_position=payload.get("store_position"),
        store_epoch_ms=None if epoch is None else to_millis(parse_time(str(epoch))),
        degraded_reasons=tuple(str(r) for r in summary["degraded_reasons"]),
        features=features,
        session_id=None if session is None else session.decode(),
        seq=None if seq is None else int(seq.decode()),
    )
