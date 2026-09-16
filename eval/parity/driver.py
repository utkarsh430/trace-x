"""Drive the gateway with a partition, one request at a time, recording what it did (ADR-0056 §2).

**Why the driver's log is the as-served order.** The online store's position counter moves for every
newly recorded observation, but only `tx.scored.v1` publishes it. An identity event carries only
its session sequence number, and an authorization outcome, published through the outbox, carries
neither. The only complete record of the order in which the store saw observations is the order in
which one sequential client handed them to one gateway. The order is then verified against the
store at every scored delivery: the reference's receipt must report the same position, and the same
recorded and conflicting flags, as the event the gateway published. An order that fails that check
is refused (`run.OrderEvidenceError`), never compared.

`GatewayDriver` is an `eval.replay.faults.OverlayPublisher`, so `publish_overlay` keeps the
partition's schedule and arrival classes exactly as it does for Kafka.
"""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from typing import Any, Final, Protocol

from eval.parity.served import parse_time
from eval.replay.gateway_replay import STREAMS, _to_request

from trace_core.contracts.api.problem import ErrorType
from trace_core.contracts.canonical_json import canonical_bytes
from trace_core.contracts.topics import TX_AUTHORIZATION_V1
from trace_core.features.semantics import identity_stream

TRANSACTIONS: Final = "tx.raw.v1"
IDENTITY_EVENTS: Final = "identity.events.v1"
HEADER_IDEMPOTENCY: Final = "X-Idempotency-Key"


class HttpResponse(Protocol):
    status_code: int
    content: bytes

    def json(self) -> Any: ...


class HttpClient(Protocol):
    def post(self, url: str, *, json: Any, headers: Mapping[str, str]) -> Any: ...


class Applied(StrEnum):
    SCORED = "SCORED"
    """Scored now: exactly one `tx.scored.v1` delivery exists for it."""
    REPLAYED = "REPLAYED"
    """Answered from the replay cache (same key, same payload): no store call, no delivery."""
    OBSERVED = "OBSERVED"
    """An identity event or outcome handed to the online store."""
    NO_STATE = "NO_STATE"
    """Accepted, and changes no online state (ADR-0046 §4)."""
    REFUSED = "REFUSED"
    """Refused with 409 or 422 before any store call."""


class UnexpectedGatewayResponseError(RuntimeError):
    """A status the partition does not account for. The run stops: an unexplained answer makes the
    store's order unknowable."""


@dataclass(frozen=True)
class Posted:
    order: int
    topic: str
    request: Mapping[str, Any]
    idempotency_key: str | None
    status: int
    body: Mapping[str, Any] | None
    applied: Applied
    phase: str
    """`warmup` or `slice`."""
    fault: str | None = None


def classify(
    topic: str,
    request: Mapping[str, Any],
    key: str | None,
    status: int,
    body: Mapping[str, Any] | None,
    seen: set[bytes],
) -> Applied:
    """What the gateway did with one request, from its declared responses. `seen` holds the
    (key, payload) of every transaction scored so far, for the replay cache's contract."""
    problem = None if body is None else body.get("type")
    if topic == TRANSACTIONS:
        if status == 200:
            fingerprint = canonical_bytes({"key": key, "request": dict(request)})
            if fingerprint in seen:
                return Applied.REPLAYED
            seen.add(fingerprint)
            return Applied.SCORED
        if status in (409, 422):
            return Applied.REFUSED
    elif topic == IDENTITY_EVENTS:
        if status == 202:
            feeds = identity_stream(str(request["identity_event_type"])) is not None
            return Applied.OBSERVED if feeds else Applied.NO_STATE
        if status in (409, 422):
            return Applied.REFUSED
    elif topic == TX_AUTHORIZATION_V1:
        if status == 202:
            return Applied.OBSERVED
        if status == 409 and problem == ErrorType.AUTHORIZATION_ACCOUNT_MISMATCH.value:
            return Applied.OBSERVED
        if status in (409, 422):
            return Applied.REFUSED
    raise UnexpectedGatewayResponseError(
        f"{topic} answered {status} ({problem or 'no problem type'})"
    )


def iso_millis(moment: dt.datetime) -> str:
    utc = moment.astimezone(dt.UTC)
    return f"{utc:%Y-%m-%dT%H:%M:%S}.{utc.microsecond // 1000:03d}Z"


def shifted(event: Mapping[str, Any], shift: dt.timedelta) -> dict[str, Any]:
    """An event with both envelope clocks moved by one uniform shift (PHASE3_PLAN §4.3)."""
    moved: dict[str, Any] = json.loads(json.dumps(event))
    for field in ("occurred_at", "ingested_at"):
        moved["envelope"][field] = iso_millis(parse_time(moved["envelope"][field]) + shift)
    return moved


class GatewayDriver:
    """Posts each event to the gateway's ingress, in order, and records every answer."""

    allow_invalid_events: bool = False

    def __init__(self, client: HttpClient, *, token: str, shift: dt.timedelta) -> None:
        self._client = client
        self._token = token
        self.shift = shift
        self.posts: list[Posted] = []
        self.phase = "warmup"
        self._seen: set[bytes] = set()

    def publish(
        self,
        topic: str,
        value: bytes,
        *,
        key_event: dict[str, Any] | None = None,
        headers: Sequence[tuple[str, bytes]] | None = None,
        block: bool = True,
    ) -> bool:
        """`OverlayPublisher`: the overlay has already rendered envelope times for the schedule."""
        del key_event, headers, block
        self.post(topic, json.loads(value))
        return True

    def post(self, topic: str, event: Mapping[str, Any]) -> Posted:
        prepared: dict[str, Any] = json.loads(json.dumps(event))
        if topic == TX_AUTHORIZATION_V1:
            # The overlay moves envelope times only; the outcome's reference to its transaction's
            # time moves by the same uniform shift, or every outcome would precede its transaction.
            payload = prepared["payload"]
            payload["transaction_occurred_at"] = iso_millis(
                parse_time(payload["transaction_occurred_at"]) + self.shift
            )
        request = _to_request(topic, prepared)
        key = None if topic == TX_AUTHORIZATION_V1 else f"parity-{prepared['envelope']['event_id']}"
        headers = {"Authorization": f"Bearer {self._token}"}
        if key is not None:
            headers[HEADER_IDEMPOTENCY] = key
        response = self._client.post(STREAMS[topic], json=request, headers=headers)
        body = response.json() if response.content else None
        applied = classify(topic, request, key, int(response.status_code), body, self._seen)
        posted = Posted(
            order=len(self.posts),
            topic=topic,
            request=request,
            idempotency_key=key,
            status=int(response.status_code),
            body=body,
            applied=applied,
            phase=self.phase,
        )
        self.posts.append(posted)
        return posted

    def label_faults(self, faults: Sequence[str | None]) -> None:
        """Attach the overlay's fault class to each slice post, in publish order."""
        slice_posts = [p for p in self.posts if p.phase == "slice"]
        if len(slice_posts) != len(faults):
            raise RuntimeError(
                f"{len(slice_posts)} slice posts but the publish manifest lists {len(faults)}"
            )
        labelled = iter(faults)
        self.posts = [
            replace(p, fault=next(labelled)) if p.phase == "slice" else p for p in self.posts
        ]
