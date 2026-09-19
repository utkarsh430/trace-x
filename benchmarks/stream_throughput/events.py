"""The benchmark's events, built with the project's own builders and validated on publish.

- **`tx.scored.v1`** is what the gateway publishes per scored transaction (ADR-0051). Scoring
  every benchmark event through the real pipeline would make the producer, not the stream, the
  bottleneck, so `scored_templates` transactions are scored once, at start, by the real
  `ScoringPipeline` over a reference feature store, and each benchmark event re-stamps one of those
  outcomes with a fresh transaction id, account, card and times before `build_scored_event` builds
  it. The payload is therefore a real pipeline's output in shape and size; its decision and
  features describe the template, not the re-stamped account, which a throughput benchmark does not
  read and says so.
- **`tx.authorization.v1`**, through `trace_core.contracts.authorization.build_event`, is the
  outcome of a transaction this factory scored earlier (oldest first), with its account and event
  time, so Gold joins outcomes to real observations.
- **`identity.events.v1`**, through `trace_core.contracts.envelope.build_event`.

Every identifier embeds the run's nonce and the worker, so no two runs or workers collide, and no
benchmark event deduplicates against another. Event time is the host's clock at build time.
"""

from __future__ import annotations

import datetime as dt
import random
import uuid
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final

from benchmarks.stream_throughput.spec import PRODUCER_NAME

from trace_core.contracts import authorization
from trace_core.contracts.canonical_json import canonical_bytes
from trace_core.contracts.envelope import build_event
from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_AUTHORIZATION_V1, TX_SCORED_V1
from trace_core.domain.time import event_time, from_millis

IDENTITY_TYPES: Final = (
    "PASSWORD_CHANGE",
    "EMAIL_CHANGE",
    "PHONE_CHANGE",
    "ADDRESS_CHANGE",
    "MFA_RESET",
    "LOGIN_SUCCEEDED",
    "LOGIN_FAILED",
)
_TEMPLATE_EPOCH: Final = dt.datetime(2026, 3, 1, 12, 0, 0, tzinfo=dt.UTC)
_PENDING_CAP: Final = 100_000
"""Scored transactions awaiting an outcome. With more scored than outcome slots in the mix the
queue only grows, so it is capped; the cap bounds memory, not correctness."""


def mix_pattern(mix: dict[str, int], seed: int) -> tuple[str, ...]:
    """One cycle of the mix, shuffled deterministically: `mix[t]` slots of each topic."""
    slots = [topic for topic in sorted(mix) for _ in range(mix[topic])]
    random.Random(f"mix:{seed}").shuffle(slots)
    return tuple(slots)


@dataclass(frozen=True, slots=True)
class Template:
    outcome: Any
    """A `services.gateway.pipeline.ScoringOutcome` from the real pipeline."""


def score_templates(count: int, seed: int) -> list[Template]:
    """`count` generator-shaped transactions scored by the real pipeline, once."""
    from services.gateway.pipeline import ScoringPipeline

    from trace_core.contracts.api.transaction import TransactionRequest
    from trace_core.features.definitions import ONLINE_FEATURES
    from trace_core.features.reference import ReferenceFeatureStore
    from trace_core.rules.loader import default_loader
    from trace_core.scoring.banding import load_thresholds

    rng = random.Random(f"templates:{seed}")
    pipeline = ScoringPipeline(
        pack=default_loader(frozenset(ONLINE_FEATURES.ids)).load(),
        thresholds=load_thresholds(),
        feature_store=ReferenceFeatureStore(
            complete_since=event_time(_TEMPLATE_EPOCH - dt.timedelta(days=40))
        ),
    )
    templates: list[Template] = []
    for i in range(count):
        moment = _TEMPLATE_EPOCH + dt.timedelta(seconds=i)
        body: dict[str, Any] = {
            "transaction_id": f"tpl_{i:012d}",
            "account_id": f"acct_{rng.randrange(1, 50):09d}",
            "amount_minor": rng.randrange(100, 250_000),
            "currency": rng.choice(("GBP", "GBP", "GBP", "EUR", "USD")),
            "occurred_at": moment.isoformat().replace("+00:00", "Z"),
            "merchant_id": f"mrch_{rng.randrange(1, 5_000):06d}",
            "merchant_mcc": rng.choice(("5411", "5812", "5999", "4111", "5732")),
            "merchant_country": rng.choice(("GB", "GB", "FR", "DE", "US")),
            "device_id": f"dev_{rng.randrange(1, 100_000):09d}",
            "card_id": f"card_{rng.randrange(1, 100_000):09d}",
            "ip_id": f"ip_{rng.randrange(1, 100_000):07d}",
            "latitude": round(rng.uniform(49.9, 58.6), 6),
            "longitude": round(rng.uniform(-6.4, 1.8), 6),
            "channel": rng.choice(("CARD_PRESENT", "CARD_NOT_PRESENT")),
            "entry_mode": rng.choice(("CHIP", "CONTACTLESS", "ECOMMERCE")),
            "authorization_outcome": "APPROVED",
        }
        outcome = pipeline.score(TransactionRequest.model_validate(body), now=moment)
        templates.append(Template(outcome))
    return templates


@dataclass(frozen=True, slots=True)
class Scored:
    transaction_id: str
    account_id: str
    occurred_ms: int
    occurred_at: str


class EventFactory:
    """Builds the next event of a topic, as encoded bytes ready for `EventPublisher.publish`."""

    def __init__(
        self,
        *,
        templates: Sequence[Template],
        seed: int,
        worker: int,
        run_nonce: str,
        account_pool: int,
    ) -> None:
        if not templates:
            raise ValueError("at least one scored template is required")
        self._templates = list(templates)
        self._rng = random.Random(f"events:{seed}:{worker}")
        self._prefix = f"ls{run_nonce[:12]}w{worker}"
        self._accounts = account_pool
        self._seq = 0
        self._pending: deque[Scored] = deque()
        self.substituted = 0
        """Authorization slots that found no scored transaction yet and produced one instead."""

    def _next_id(self) -> str:
        self._seq += 1
        return f"{self._prefix}n{self._seq}"

    def _trace_id(self) -> str:
        return uuid.UUID(int=self._rng.getrandbits(128)).hex

    def build(self, topic: str, now_ms: int) -> tuple[str, bytes]:
        """The topic actually built (an authorization with nothing to refer to becomes a scored
        transaction, counted in `substituted`) and its encoded value."""
        if topic == TX_AUTHORIZATION_V1:
            if not self._pending:
                self.substituted += 1
                return TX_SCORED_V1, self._scored(now_ms)
            return topic, self._authorization(self._pending.popleft(), now_ms)
        if topic == TX_SCORED_V1:
            return topic, self._scored(now_ms)
        if topic == IDENTITY_EVENTS_V1:
            return topic, self._identity(now_ms)
        raise ValueError(f"no builder for {topic}")

    def _scored(self, now_ms: int) -> bytes:
        from trace_core.observation.scored_event import build_scored_event

        template = self._templates[self._rng.randrange(len(self._templates))].outcome
        number = self._rng.randrange(1, self._accounts + 1)
        transaction_id = self._next_id()
        account_id = f"acct_{number:09d}"
        moment = from_millis(now_ms)
        updates: dict[str, Any] = {
            "transaction_id": transaction_id,
            "account_id": account_id,
            "source_row_id": transaction_id,
            "occurred_at": moment,
            "ingested_at": moment,
        }
        if template.canonical.card_id is not None:
            updates["card_id"] = f"card_{number:09d}"
        canonical = template.canonical.model_copy(update=updates)
        decision = template.decision.model_copy(update={"transaction_id": transaction_id})
        event = build_scored_event(
            canonical=canonical,
            decision=decision,
            features=template.features,
            context=template.context,
            observe_outcome=template.observe_outcome.value,
            store_position=template.observe_position,
            store_epoch_ms=template.store_epoch_ms,
            producer=PRODUCER_NAME,
            trace_id=self._trace_id(),
        )
        if len(self._pending) < _PENDING_CAP:
            self._pending.append(
                Scored(transaction_id, account_id, now_ms, event["envelope"]["occurred_at"])
            )
        return canonical_bytes(event)

    def _authorization(self, scored: Scored, now_ms: int) -> bytes:
        decided_ms = max(now_ms, scored.occurred_ms)
        event = authorization.build_event(
            transaction_id=scored.transaction_id,
            account_id=scored.account_id,
            authorization_outcome="DECLINED" if self._rng.random() < 0.03 else "APPROVED",
            decided_ms=decided_ms,
            transaction_occurred_ms=scored.occurred_ms,
            transaction_occurred_at=scored.occurred_at,
            producer=PRODUCER_NAME,
            trace_id=self._trace_id(),
            correlation_id=scored.transaction_id,
            ingested_ms=decided_ms,
        )
        return canonical_bytes(event)

    def _identity(self, now_ms: int) -> bytes:
        number = self._rng.randrange(1, self._accounts + 1)
        event = build_event(
            event_type="identity.events",
            occurred_at=event_time(from_millis(now_ms)),
            payload={
                "account_id": f"acct_{number:09d}",
                "identity_event_type": self._rng.choice(IDENTITY_TYPES),
            },
            producer=PRODUCER_NAME,
            trace_id=self._trace_id(),
            correlation_id=self._next_id(),
        )
        return canonical_bytes(event)
