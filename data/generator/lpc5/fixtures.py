"""Hand-built rows and population knowledge for `LPC-5` self-tests (§16.3) and diagnostics.

Every row carries a complete, well-formed envelope by default, so a test that wants a representation
difference has to create one on purpose.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from data.generator import outcomes
from data.generator.engine import GeneratedRow
from data.generator.labels import TransactionLabel
from data.generator.lpc5.frame import Knowledge
from data.generator.scenarios import ScenarioInstance
from trace_core.domain.enums import EvidenceKind, FraudPattern

T0 = 1_768_435_200_000
"""2026-01-15T00:00:00Z: inside the default knowledge window."""
START_MS = 1_767_225_600_000
"""2026-01-01T00:00:00Z."""
END_MS = 1_772_323_200_000
"""2026-03-01T00:00:00Z."""
HOME_POINT = (51.5, -0.12)
PRODUCER = "trace-generator@1.0.0"


def _millis(text: str) -> int:
    from data.generator.lpc5.frame import millis

    return millis(text)


def account(n: int) -> str:
    return f"acct_{n:06d}"


def _hex(seed: str, digits: int) -> str:
    return hashlib.sha256(seed.encode()).hexdigest()[:digits]


def uuid7_at(millis: int, salt: str) -> str:
    """A version-7 UUID string stamped with `millis`, deterministic in `salt`."""
    tail = _hex(salt, 20)
    head = f"{millis:012x}"
    return f"{head[:8]}-{head[8:12]}-7{tail[:3]}-8{tail[3:6]}-{tail[6:18]}"


@dataclass
class Rows:
    """Builds `GeneratedRow`s in emission order."""

    rows: list[GeneratedRow] = field(default_factory=list)
    _n: int = 0
    instances: dict[str, ScenarioInstance] = field(default_factory=dict)

    def _envelope(self, event_type: str, at_ms: int, salt: str, **overrides: Any) -> dict[str, Any]:
        self._n += 1
        envelope: dict[str, Any] = {
            "event_id": uuid7_at(at_ms, f"{salt}:{self._n}"),
            "event_type": event_type,
            "schema_version": 1,
            "occurred_at": outcomes.iso_millis(at_ms),
            "ingested_at": outcomes.iso_millis(at_ms + 70),
            "producer": PRODUCER,
            "trace_id": _hex(f"trace:{salt}:{self._n}", 32),
            "correlation_id": f"corr_{_hex(f'corr:{salt}:{self._n}', 12)}",
            "idempotency_key": "sha256:" + _hex(f"idem:{salt}:{self._n}", 64),
        }
        envelope.update(overrides)
        return envelope

    def instance(self, instance_id: str, pattern: FraudPattern) -> ScenarioInstance:
        found = self.instances.get(instance_id)
        if found is None:
            found = ScenarioInstance(
                instance_id=instance_id,
                pattern=pattern,
                causal_evidence_keys=frozenset({EvidenceKind.VELOCITY}),
                events=(),
                participants={},
            )
            self.instances[instance_id] = found
        return found

    def tx(
        self,
        at_ms: int,
        account_id: str,
        *,
        pattern: FraudPattern | None = None,
        instance: str = "fi_00000001",
        ordinal: int | None = None,
        envelope: dict[str, Any] | None = None,
        **payload_overrides: Any,
    ) -> GeneratedRow:
        transaction_id = payload_overrides.pop("transaction_id", f"tx_{self._n:012d}")
        payload: dict[str, Any] = {
            "transaction_id": transaction_id,
            "account_id": account_id,
            "card_id": f"card_{account_id.split('_')[1]}",
            "device_id": "dev_000001",
            "merchant_id": "mrch_00001",
            "ip_id": "ip_00001",
            "amount_minor": 1600,
            "currency": "GBP",
            "channel": "CARD_PRESENT",
            "entry_mode": "CHIP",
            "merchant_mcc": "5411",
            "merchant_country": "GB",
            "merchant_name": "Merchant 00001",
            "latitude": HOME_POINT[0] + 0.01,
            "longitude": HOME_POINT[1] + 0.01,
            "user_agent": "Mozilla/5.0 (Linux; Android 14)",
            "memo": "",
            "authorization_outcome": "APPROVED",
        }
        payload.update(payload_overrides)
        scenario = self.instance(instance, pattern) if pattern is not None else None
        label = (
            TransactionLabel(
                transaction_id=transaction_id,
                is_fraud=True,
                fraud_pattern=pattern,
                scenario_instance_id=instance,
                causal_evidence_keys=frozenset({EvidenceKind.VELOCITY}),
            )
            if pattern is not None
            else TransactionLabel(transaction_id=transaction_id, is_fraud=False)
        )
        row = GeneratedRow(
            topic="tx.raw.v1",
            event={
                "envelope": self._envelope("tx.raw", at_ms, transaction_id, **(envelope or {})),
                "payload": payload,
            },
            label=label,
            scenario_instance=scenario,
            planned_ordinal=ordinal,
        )
        self.rows.append(row)
        return row

    def outcome(
        self,
        transaction: GeneratedRow,
        authorization_outcome: str,
        *,
        seed: int = 42,
        envelope: dict[str, Any] | None = None,
        **payload_overrides: Any,
    ) -> GeneratedRow:
        """The transaction's `tx.authorization.v1` row, built by the generator's own builder."""
        tx_envelope = transaction.event["envelope"]
        tx_payload = transaction.event["payload"]
        event = outcomes.outcome_event(
            seed,
            transaction_id=tx_payload["transaction_id"],
            account_id=tx_payload["account_id"],
            authorization_outcome=authorization_outcome,
            transaction_occurred_at=tx_envelope["occurred_at"],
            transaction_occurred_ms=_millis(tx_envelope["occurred_at"]),
            producer=tx_envelope["producer"],
            trace_id=tx_envelope["trace_id"],
            correlation_id=tx_envelope["correlation_id"],
        )
        event["envelope"].update(envelope or {})
        event["payload"].update(payload_overrides)
        row = GeneratedRow(topic=outcomes.TOPIC, event=event)
        self.rows.append(row)
        return row

    def identity(
        self,
        at_ms: int,
        account_id: str,
        kind: str,
        *,
        pattern: FraudPattern | None = None,
        instance: str = "fi_00000001",
        device: str | None = "dev_000001",
        ip: str | None = None,
    ) -> GeneratedRow:
        payload: dict[str, Any] = {
            "account_id": account_id,
            "identity_event_type": kind,
            "user_agent": "Mozilla/5.0 (Linux; Android 14)",
        }
        if device is not None:
            payload["device_id"] = device
        if ip is not None:
            payload["ip_id"] = ip
        row = GeneratedRow(
            topic="identity.events.v1",
            event={"envelope": self._envelope("identity.events", at_ms, "id"), "payload": payload},
            scenario_instance=self.instance(instance, pattern) if pattern is not None else None,
        )
        self.rows.append(row)
        return row

    def device(
        self,
        at_ms: int,
        account_id: str,
        device: str,
        *,
        kind: str = "FIRST_SEEN",
        platform: str = "ios",
        pattern: FraudPattern | None = None,
        instance: str = "fi_00000001",
    ) -> GeneratedRow:
        row = GeneratedRow(
            topic="device.events.v1",
            event={
                "envelope": self._envelope("device.events", at_ms, "dev"),
                "payload": {
                    "device_id": device,
                    "account_id": account_id,
                    "device_event_type": kind,
                    "platform": platform,
                },
            },
            scenario_instance=self.instance(instance, pattern) if pattern is not None else None,
        )
        self.rows.append(row)
        return row


def knowledge(
    accounts: int = 40,
    *,
    seed: int = 42,
    home_devices: dict[str, frozenset[str]] | None = None,
    home_ips: dict[str, frozenset[str]] | None = None,
    datacenter_ips: frozenset[str] = frozenset(),
    merchant_ranks: dict[str, int] | None = None,
    habitual_merchants: frozenset[str] = frozenset({"mrch_00001"}),
    amount_mu: float = 7.378,
    amount_sigma: float = 1.0,
) -> Knowledge:
    """Population knowledge for `accounts` accounts, all alike unless overridden."""
    names = [account(n) for n in range(accounts)]
    return Knowledge(
        seed=seed,
        start_ms=START_MS,
        end_ms=END_MS,
        home_devices=home_devices or dict.fromkeys(names, frozenset({"dev_000001"})),
        home_ips=home_ips or dict.fromkeys(names, frozenset({"ip_00001"})),
        home_point=dict.fromkeys(names, HOME_POINT),
        country=dict.fromkeys(names, "GB"),
        first_card={name: f"card_{name.split('_')[1]}" for name in names},
        habitual_merchants=dict.fromkeys(names, habitual_merchants),
        habitual_mccs=dict.fromkeys(names, frozenset({"5411"})),
        amount_mu=dict.fromkeys(names, amount_mu),
        amount_sigma=dict.fromkeys(names, amount_sigma),
        merchant_rank=merchant_ranks or {"mrch_00001": 1, "mrch_00002": 50, "mrch_00003": 500},
        merchant_mcc={"mrch_00001": "5411", "mrch_00002": "5812", "mrch_00003": "7995"},
        ip_datacenter=dict.fromkeys(datacenter_ips, True),
        device_platform={"dev_000001": "ios", "dev_000002": "android", "dev_000003": "web"},
        profiles={},
    )
