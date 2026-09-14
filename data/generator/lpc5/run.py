"""`LPC-5` end to end: one generation in, one report out.

The acceptance command (Stage 2 step 11) regenerates the frozen candidate, verifies its digests, and
calls `evaluate`. Diagnostic runs call it too; their reports are never acceptance evidence (§1.3).

**Invalid, not failed.** A run is invalid when a check could not be computed:
- no availability provider (S6);
- validation not run (S2c rules 1 and 2).

Invalid runs are reported as such.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from data.generator import label_proxy
from data.generator.engine import GeneratedRow
from data.generator.labels import TransactionLabel
from data.generator.lpc5 import declaration as d
from data.generator.lpc5 import judge, rules
from data.generator.lpc5.attributes import AvailabilityProvider, compute_tables
from data.generator.lpc5.frame import Frame, Knowledge, build_frame
from data.generator.lpc5.judge import CheckResult, Finding
from trace_core.domain.enums import EvidenceKind, FraudPattern

if TYPE_CHECKING:  # pragma: no cover - typing only
    from data.generator.label_proxy import LabelProxyReport

CHECK_ORDER: tuple[str, ...] = (
    "S0",
    "R7",
    "R8",
    "R9",
    "S1-U",
    "S1-B",
    "S2",
    "S2c",
    "S3/pooled",
    "S3/thirds",
    "S4",
    "S5a",
    "S5b",
    "G3",
    "S6a",
    "S7a",
    "S7b",
    "S8/min-instances",
    "S8/disclosure",
    "S8/non-vacuity",
)


@dataclass(frozen=True, slots=True)
class Lpc5Report:
    checks: Mapping[str, CheckResult]
    s0: tuple[LabelProxyReport, LabelProxyReport, LabelProxyReport]
    invalid: tuple[str, ...]
    instance_counts: Mapping[str, int]
    mix: Mapping[str, tuple[int, int]] | None
    outcomes_derived: bool

    @property
    def passed(self) -> bool:
        return not self.invalid and all(result.passed for result in self.checks.values())

    def findings(self, prefix: str = "") -> list[Finding]:
        return [
            finding
            for result in self.checks.values()
            for finding in result.findings
            if finding.check.startswith(prefix)
        ]

    def format(self, *, limit: int = 40) -> str:
        lines = [
            f"{d.CRITERION_ID} revision {d.REVISION}: "
            f"{'PASS' if self.passed else 'INVALID' if self.invalid else 'FAIL'}"
        ]
        lines.extend(f"  invalid: {reason}" for reason in self.invalid)
        if self.outcomes_derived:
            lines.append("  outcome rows were derived (ADR-0049 §7), not emitted")
        for name in CHECK_ORDER:
            result = self.checks.get(name)
            if result is None:
                continue
            status = "pass" if result.passed else f"FAIL ({len(result.findings)})"
            lines.append(f"  {name:<18} judged {result.judged:>9}  {status}")
            for finding in result.findings[:limit]:
                cell = "/".join(
                    str(part)
                    for part in (
                        finding.population,
                        finding.attribute,
                        finding.value,
                        finding.group,
                    )
                    if part is not None
                )
                lines.append(f"      {finding.check} {cell}: {finding.detail}")
        return "\n".join(lines)


def _s0_rows(frame: Frame) -> Iterator[GeneratedRow]:
    """The rows LPC-1 to LPC-3 read, rebuilt from the frame in emission order.

    Rebuilt rather than re-generated: at acceptance scale a second generation costs as much as the
    first. Only the fields the criteria read are carried."""
    merged: list[tuple[int, GeneratedRow]] = []
    for tx in frame.tx:
        if tx.group == d.LEGIT:
            label = TransactionLabel(transaction_id=tx.transaction_id, is_fraud=False)
        else:
            info = frame.instances.get(tx.instance or "")
            keys = frozenset(
                EvidenceKind(key)
                for key in (info.causal_keys if info is not None else ())
                if key in EvidenceKind.__members__
            ) or frozenset({EvidenceKind.VELOCITY})
            label = TransactionLabel(
                transaction_id=tx.transaction_id,
                is_fraud=True,
                fraud_pattern=FraudPattern(tx.group),
                scenario_instance_id=tx.instance,
                causal_evidence_keys=keys,
            )
        payload = {
            "transaction_id": tx.transaction_id,
            "account_id": tx.account,
            "device_id": tx.device,
            "ip_id": tx.ip,
            "authorization_outcome": tx.outcome_field,
            "latitude": tx.latitude,
            "longitude": tx.longitude,
            "channel": tx.channel,
            "entry_mode": tx.entry_mode,
        }
        merged.append(
            (
                tx.order,
                GeneratedRow(
                    topic=d.TOPICS[d.Population.TX],
                    event={
                        "envelope": {"occurred_at": tx.envelope.occurred_at},
                        "payload": payload,
                    },
                    label=label,
                ),
            )
        )
    for population, rows in ((d.Population.ID, frame.ident), (d.Population.DEV, frame.dev)):
        for side in rows:
            side_payload: dict[str, object] = {"account_id": side.account}
            if population is d.Population.ID:
                side_payload["identity_event_type"] = side.event_type
            else:
                side_payload["device_event_type"] = side.event_type
            if side.device is not None:
                side_payload["device_id"] = side.device
            if side.ip is not None:
                side_payload["ip_id"] = side.ip
            merged.append(
                (
                    side.order,
                    GeneratedRow(
                        topic=d.TOPICS[population],
                        event={
                            "envelope": {"occurred_at": side.envelope.occurred_at},
                            "payload": side_payload,
                        },
                    ),
                )
            )
    merged.sort(key=lambda item: item[0])
    for _, row in merged:
        yield row


def _s0_check(report: LabelProxyReport) -> CheckResult:
    return CheckResult(
        "S0",
        len(report.rules),
        tuple(
            Finding(f"S0/{rule.rule}", d.Population.TX, rule.signal, detail=rule.detail)
            for rule in report.failures()
        ),
    )


def evaluate(
    rows: Iterable[GeneratedRow],
    know: Knowledge,
    *,
    availability: AvailabilityProvider | None = None,
    mix: Mapping[str, tuple[int, int]] | None = None,
    validate: bool = True,
) -> Lpc5Report:
    """Every check of `LPC-5` over one generation.

    `mix` maps a pattern to (instances the weighted mix produced, instances G2 added); the
    disclosure check of §14.4 fails without it."""
    frame = build_frame(rows, seed=know.seed, validate=validate)
    tables = compute_tables(frame, know, availability=availability)
    allow = judge.Allowlist()

    outcomes: dict[str, tuple[str, int, str]] = {}
    for out in frame.out:
        outcomes.setdefault(out.transaction_id, (out.authorization_outcome, out.t, out.account))
    population = label_proxy.PopulationView(
        home_devices=know.home_devices, home_points=know.home_point
    )
    s0 = label_proxy.evaluate_all(_s0_rows(frame), population, outcomes=outcomes)

    checks: dict[str, CheckResult] = {"S0": _s0_check(s0[2])}
    checks["R7"], checks["R8"] = judge.r7_r8(tables, allow)
    checks["R9"] = judge.r9(tables)
    checks["S1-U"] = judge.s1_unconditional(tables, allow)
    checks["S1-B"] = judge.s1_conditional(tables, allow)
    checks["S2"] = judge.s2_representation(tables)
    checks["S2c"] = rules.s2c_rows(frame, know)
    starts, legit_times = rules.s3_inputs(frame)
    checks["S3/pooled"], checks["S3/thirds"] = judge.s3_calendar(
        starts, legit_times, know.start_ms, know.end_ms
    )
    checks["S4"] = judge.s4_offsets(rules.s4_pairs(frame))
    checks["S5a"], checks["S5b"] = rules.s5_amounts(frame, tables, know)
    checks["G3"] = rules.g3_speed(frame)
    checks["S6a"] = rules.s6_availability(tables)
    checks["S7a"] = rules.s7_instances(frame, know)
    checks["S7b"] = judge.s7_effects(tables, allow)

    instance_counts = Counter({pattern: len(series) for pattern, series in starts.items()})
    short = [
        Finding(
            "S8/min-instances",
            d.Population.TX,
            group=pattern.value,
            detail=f"{instance_counts.get(pattern.value, 0)} instances with a transaction",
        )
        for pattern in FraudPattern
        if instance_counts.get(pattern.value, 0) < d.MIN_INSTANCES
    ]
    checks["S8/min-instances"] = CheckResult("S8/min-instances", len(FraudPattern), tuple(short))
    disclosure: list[Finding] = []
    if mix is None:
        disclosure.append(
            Finding("S8/disclosure", detail="the natural and G2-added instance counts are missing")
        )
    checks["S8/disclosure"] = CheckResult("S8/disclosure", 1, tuple(disclosure))

    vacuous: list[Finding] = []
    for name in ("S1-B", "S4", "S5b", "S7b"):
        vacuous.extend(
            Finding("S8/non-vacuity", detail=f"{name} judged nothing for {item}")
            for item in checks[name].unjudged
        )
    if checks["S3/pooled"].judged == 0:
        vacuous.append(Finding("S8/non-vacuity", detail="S3 judged no pooled slice"))
    checks["S8/non-vacuity"] = CheckResult("S8/non-vacuity", 5, tuple(vacuous))

    invalid: list[str] = []
    if availability is None:
        invalid.append("availability (S6, §4.6) was not computed")
    if not validate:
        invalid.append("schema validation and the ground-truth scan (S2c rules 1-2) did not run")
    return Lpc5Report(
        checks=checks,
        s0=s0,
        invalid=tuple(invalid),
        instance_counts=dict(instance_counts),
        mix=mix,
        outcomes_derived=frame.outcomes_derived,
    )
