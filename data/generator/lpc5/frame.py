"""`LPC-5` §1-§2: the rows under test, normalised once, and the population knowledge `U`.

A generation is streamed once. Each row is reduced to the fields the criterion reads, with its group
(a scenario, or `LEGIT`) and its cluster (the scenario instance, or the account). Raw event dicts
are not retained: at acceptance scale they would not fit.

A dataset without a `tx.authorization.v1` stream has its outcome rows derived by ADR-0049 §7:
eval-v1, and any eval-v2 generation made before the generator emits the stream. The derivation uses
`data.generator.outcomes`, the same builder the generator uses, so a derived row has exactly the
shape of an emitted one.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

from pydantic import BaseModel, ValidationError

from data.generator import outcomes
from data.generator.lpc5 import declaration as d
from trace_core.contracts.events.device_events_v1 import DeviceEventV1
from trace_core.contracts.events.identity_events_v1 import IdentityEventV1
from trace_core.contracts.events.tx_raw_v1 import TxRawV1
from trace_core.domain.time import to_millis

if TYPE_CHECKING:  # pragma: no cover - typing only
    from data.generator.engine import GeneratedRow
    from data.generator.population import AccountProfile, Universe


def millis(text: str) -> int:
    """Envelope event time as integer epoch milliseconds, fractions floored (§2.4)."""
    return to_millis(dt.datetime.fromisoformat(text.replace("Z", "+00:00")))


@dataclass(frozen=True, slots=True)
class Envelope:
    occurred_at: str
    ingested_at: str | None
    event_id: str | None
    event_type: str | None
    schema_version: object
    producer: str | None
    trace_id: str | None
    correlation_id: str | None
    idempotency_key: str | None

    @classmethod
    def of(cls, raw: Mapping[str, Any]) -> Envelope:
        return cls(
            occurred_at=str(raw["occurred_at"]),
            ingested_at=raw.get("ingested_at"),
            event_id=raw.get("event_id"),
            event_type=raw.get("event_type"),
            schema_version=raw.get("schema_version"),
            producer=raw.get("producer"),
            trace_id=raw.get("trace_id"),
            correlation_id=raw.get("correlation_id"),
            idempotency_key=raw.get("idempotency_key"),
        )


@dataclass(slots=True)
class TxRow:
    order: int
    t: int
    account: str
    group: str
    cluster: str
    instance: str | None
    ordinal: int | None
    envelope: Envelope
    keys: str
    transaction_id: str
    card: str | None
    device: str | None
    merchant: str | None
    ip: str | None
    amount: int
    currency: str | None
    channel: str | None
    entry_mode: str | None
    mcc: str | None
    country: str | None
    latitude: float | None
    longitude: float | None
    user_agent: str | None
    memo: str | None
    outcome_field: str | None


@dataclass(slots=True)
class SideRow:
    """An identity (ID) or device (DEV) event."""

    order: int
    t: int
    account: str
    group: str
    cluster: str
    instance: str | None
    ordinal: int | None
    envelope: Envelope
    keys: str
    event_type: str
    device: str | None
    ip: str | None
    user_agent: str | None
    platform: str | None


@dataclass(slots=True)
class OutRow:
    order: int
    t: int
    account: str
    group: str
    cluster: str
    envelope: Envelope
    keys: str
    transaction_id: str
    authorization_outcome: str
    transaction_occurred_at: str | None
    derived: bool


@dataclass(frozen=True, slots=True)
class InstanceInfo:
    instance_id: str
    pattern: str
    causal_keys: frozenset[str]
    planned_transactions: int


@dataclass(slots=True)
class Frame:
    tx: list[TxRow]
    ident: list[SideRow]
    dev: list[SideRow]
    out: list[OutRow]
    instances: dict[str, InstanceInfo]
    outcomes_derived: bool
    violations: list[tuple[str, str, int, str]] = field(default_factory=list)
    """S2c rules 1 and 2, recorded at ingest when validation runs: (rule, topic, order, detail)."""
    validated: bool = False

    def rows(self, population: d.Population) -> list[TxRow] | list[SideRow] | list[OutRow]:
        if population is d.Population.TX:
            return self.tx
        if population is d.Population.ID:
            return self.ident
        if population is d.Population.DEV:
            return self.dev
        return self.out


@dataclass(frozen=True, slots=True)
class Knowledge:
    """`U` (§1.5): population facts, never labels."""

    seed: int
    start_ms: int
    end_ms: int
    home_devices: Mapping[str, frozenset[str]]
    home_ips: Mapping[str, frozenset[str]]
    home_point: Mapping[str, tuple[float, float]]
    country: Mapping[str, str]
    first_card: Mapping[str, str]
    habitual_merchants: Mapping[str, frozenset[str]]
    habitual_mccs: Mapping[str, frozenset[str]]
    amount_mu: Mapping[str, float]
    amount_sigma: Mapping[str, float]
    merchant_rank: Mapping[str, int]
    merchant_mcc: Mapping[str, str]
    ip_datacenter: Mapping[str, bool]
    device_platform: Mapping[str, str]
    profiles: Mapping[str, AccountProfile]


def knowledge(universe: Universe) -> Knowledge:
    """`U` for a generation's configuration. Merchant popularity rank is index + 1: the Zipf
    weights are built in index order (`population.build_universe`)."""
    config = universe.config
    merchant_mcc: dict[str, str] = {str(m.merchant_id): m.mcc for m in universe.merchants}
    profiles: dict[str, AccountProfile] = {str(p.account_id): p for p in universe.profiles}
    return Knowledge(
        seed=config.seed,
        start_ms=to_millis(config.start_at),
        end_ms=to_millis(config.end_at),
        home_devices={a: frozenset(p.home_devices) for a, p in profiles.items()},
        home_ips={a: frozenset(p.home_ips) for a, p in profiles.items()},
        home_point={
            a: (p.account.home.latitude, p.account.home.longitude) for a, p in profiles.items()
        },
        country={a: p.account.country for a, p in profiles.items()},
        first_card={a: p.cards[0].card_id for a, p in profiles.items()},
        habitual_merchants={a: frozenset(p.habitual_merchants) for a, p in profiles.items()},
        habitual_mccs={
            a: frozenset(merchant_mcc[m] for m in p.habitual_merchants if m in merchant_mcc)
            for a, p in profiles.items()
        },
        amount_mu={a: p.amount_mu for a, p in profiles.items()},
        amount_sigma={a: p.amount_sigma for a, p in profiles.items()},
        merchant_rank={m.merchant_id: index + 1 for index, m in enumerate(universe.merchants)},
        merchant_mcc=merchant_mcc,
        ip_datacenter={ip.ip_id: ip.is_datacenter for ip in universe.ips},
        device_platform={device.device_id: device.platform for device in universe.devices},
        profiles=profiles,
    )


_MODELS: Final[dict[str, type[BaseModel]]] = {
    d.TOPICS[d.Population.TX]: TxRawV1,
    d.TOPICS[d.Population.ID]: IdentityEventV1,
    d.TOPICS[d.Population.DEV]: DeviceEventV1,
}
_TOKENS: Final = re.compile("|".join(re.escape(t) for t in d.GROUND_TRUTH_TOKENS), re.IGNORECASE)


def _strings(value: Any) -> Iterator[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for item in value.values():
            yield from _strings(item)
    elif isinstance(value, list | tuple):
        for item in value:
            yield from _strings(item)


def _validate(frame: Frame, topic: str, event: Mapping[str, Any], order: int) -> None:
    """S2c rules 1 and 2 for one row: its released schema, and no ground-truth token."""
    model = _MODELS.get(topic)
    if model is None:
        frame.violations.append(("S2c/1", topic, order, f"{topic} has no released schema"))
    else:
        try:
            model.model_validate_json(json.dumps(event))
        except ValidationError as exc:
            frame.violations.append(("S2c/1", topic, order, str(exc.errors()[0])[:200]))
    for text in _strings(event):
        if _TOKENS.search(text):
            frame.violations.append(("S2c/2", topic, order, text[:80]))
            break


def _keys(payload: Mapping[str, Any]) -> str:
    return "|".join(sorted(payload))


def build_frame(
    rows: Iterable[GeneratedRow],
    *,
    seed: int,
    derive_outcomes: bool | None = None,
    validate: bool = False,
) -> Frame:
    """Normalise one generation.

    `derive_outcomes=None` derives outcome rows exactly when the stream carries none. A stream that
    carries some is never topped up: a partial outcome stream is a defect the S2c rules must see."""
    frame = Frame(tx=[], ident=[], dev=[], out=[], instances={}, outcomes_derived=False)
    frame.validated = validate
    raw_out: list[tuple[int, dict[str, Any]]] = []
    order = 0
    for row in rows:
        event = row.event
        envelope = Envelope.of(event["envelope"])
        payload = event["payload"]
        t = millis(envelope.occurred_at)
        instance = row.scenario_instance
        if instance is not None and instance.instance_id not in frame.instances:
            frame.instances[instance.instance_id] = InstanceInfo(
                instance_id=instance.instance_id,
                pattern=instance.pattern.value,
                causal_keys=frozenset(key.value for key in instance.causal_evidence_keys),
                planned_transactions=instance.transaction_count,
            )
        ordinal = getattr(row, "planned_ordinal", None)
        if validate:
            _validate(frame, row.topic, event, order)
        if row.topic == d.TOPICS[d.Population.TX]:
            label = row.label
            if label is None:
                raise ValueError(
                    f"transaction {payload.get('transaction_id')} has no label; LPC-5 cannot be "
                    f"computed over a partially labelled dataset"
                )
            account = str(payload["account_id"])
            if label.is_fraud:
                if label.fraud_pattern is None or label.scenario_instance_id is None:
                    raise ValueError(f"fraud label without a scenario: {label.transaction_id}")
                group = label.fraud_pattern.value
                cluster_instance: str | None = label.scenario_instance_id
            else:
                group = d.LEGIT
                cluster_instance = None
            frame.tx.append(
                TxRow(
                    order=order,
                    t=t,
                    account=account,
                    group=group,
                    cluster=cluster_instance or account,
                    instance=cluster_instance,
                    ordinal=ordinal,
                    envelope=envelope,
                    keys=_keys(payload),
                    transaction_id=str(payload["transaction_id"]),
                    card=payload.get("card_id"),
                    device=payload.get("device_id"),
                    merchant=payload.get("merchant_id"),
                    ip=payload.get("ip_id"),
                    amount=int(payload.get("amount_minor", 0)),
                    currency=payload.get("currency"),
                    channel=payload.get("channel"),
                    entry_mode=payload.get("entry_mode"),
                    mcc=payload.get("merchant_mcc"),
                    country=payload.get("merchant_country"),
                    latitude=payload.get("latitude"),
                    longitude=payload.get("longitude"),
                    user_agent=payload.get("user_agent"),
                    memo=payload.get("memo"),
                    outcome_field=payload.get("authorization_outcome"),
                )
            )
        elif row.topic in (d.TOPICS[d.Population.ID], d.TOPICS[d.Population.DEV]):
            account = str(payload["account_id"])
            group = instance.pattern.value if instance is not None else d.LEGIT
            side = SideRow(
                order=order,
                t=t,
                account=account,
                group=group,
                cluster=instance.instance_id if instance is not None else account,
                instance=instance.instance_id if instance is not None else None,
                ordinal=ordinal,
                envelope=envelope,
                keys=_keys(payload),
                event_type=str(
                    payload.get("identity_event_type") or payload.get("device_event_type")
                ),
                device=payload.get("device_id"),
                ip=payload.get("ip_id"),
                user_agent=payload.get("user_agent"),
                platform=payload.get("platform"),
            )
            if row.topic == d.TOPICS[d.Population.ID]:
                frame.ident.append(side)
            else:
                frame.dev.append(side)
        elif d.OUTCOME_STREAM is not None and row.topic == d.OUTCOME_STREAM:
            raw_out.append((order, event))
        else:
            raise ValueError(f"LPC-5 does not judge topic {row.topic!r}")
        order += 1

    tx_by_id = {tx.transaction_id: tx for tx in frame.tx}
    derive = not raw_out if derive_outcomes is None else derive_outcomes
    if derive:
        frame.outcomes_derived = True
        for tx in frame.tx:
            if tx.outcome_field not in outcomes.OUTCOMES:
                continue
            event = outcomes.outcome_event(
                seed,
                transaction_id=tx.transaction_id,
                account_id=tx.account,
                authorization_outcome=str(tx.outcome_field),
                transaction_occurred_at=tx.envelope.occurred_at,
                transaction_occurred_ms=tx.t,
                producer=tx.envelope.producer or "",
                trace_id=tx.envelope.trace_id or "",
                correlation_id=tx.envelope.correlation_id or "",
            )
            if validate:
                _validate(frame, d.OUTCOME_STREAM_LABEL, event, order)
            raw_out.append((order, event))
            order += 1
    for out_order, event in raw_out:
        envelope = Envelope.of(event["envelope"])
        payload = event["payload"]
        transaction_id = str(payload["transaction_id"])
        linked = tx_by_id.get(transaction_id)
        account = str(payload["account_id"])
        frame.out.append(
            OutRow(
                order=out_order,
                t=millis(envelope.occurred_at),
                account=account,
                group=linked.group if linked is not None else d.LEGIT,
                cluster=linked.cluster if linked is not None else account,
                envelope=envelope,
                keys=_keys(payload),
                transaction_id=transaction_id,
                authorization_outcome=str(payload["authorization_outcome"]),
                transaction_occurred_at=payload.get("transaction_occurred_at"),
                derived=derive,
            )
        )
    return frame
