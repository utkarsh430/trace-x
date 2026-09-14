"""`LPC-1` and `LPC-2`: the pre-declared label-proxy criteria (eval-v2, Phase 3 Step E).

Three layers, kept apart on purpose:

* **Declared thresholds, pinned.** Literal values, so loosening a threshold
  after seeing a result is a visible diff in review rather than a quiet edit.
* **Self-tests of the criteria** on hand-built rows: window boundaries, a
  planted perfect proxy, a signal that never fires, the pattern half biting, the
  transaction signals' boundaries, and the interval arithmetic. A criterion that
  cannot fail is not a criterion.
* **STAGE 1B DEMONSTRATION -- NOT ACCEPTANCE EVIDENCE.** Small in-memory
  generations at the scale declared in the eval-v2 ADR (draft §5.9), each judged
  by both criteria in one pass, with the required negative control (eval-v1
  configured, gate off) and a zero-rate control. The Q5 acceptance run is stage
  2: the frozen eval-v2, regenerated from its manifest, digest-verified, and
  judged by the same code.

Labels come from the generator's in-memory rows. Nothing here reads PostgreSQL,
and a structural test keeps the criteria out of every runtime package.
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any

import pytest
from data.generator import label_proxy
from data.generator.config import BaselineIdentityConfig, GeneratorConfig
from data.generator.engine import GeneratedRow, generate_dataset
from data.generator.label_proxy import (
    DAY_MS,
    HOUR_MS,
    LabelProxyReport,
    evaluate,
    evaluate_both,
    evaluate_lpc2,
    home_devices_by_account,
    wilson_interval,
)
from data.generator.labels import TransactionLabel
from data.generator.population import build_universe

from trace_core.domain.enums import EvidenceKind, FraudPattern
from trace_core.domain.time import from_millis

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]

# ====================================================== declared values =====


def test_the_declared_lpc1_thresholds_are_pinned() -> None:
    """Changing any of these requires an entry in the ADR's change record (§9)."""
    assert label_proxy.CRITERION_ID == "LPC-1"
    assert label_proxy.Z_ONE_SIDED_95 == 1.645
    assert label_proxy.MAX_PRECISION_UPPER_BOUND == 0.25
    assert label_proxy.MIN_LEGIT_FIRINGS == 30
    assert dict(label_proxy.LEGIT_FLOORS) == {
        "ANY_IDENTITY_24H": 0.05,
        "LOGIN_FAILED_1H": 0.0001,
        "IDENTITY_CHANGE_24H": 0.001,
        "DEVICE_FIRST_SEEN_24H": 0.001,
        "ANY_DEVICE_EVENT_24H": 0.001,
        "ANY_IDENTITY_OR_DEVICE_24H": 0.05,
    }
    assert dict(label_proxy.PATTERN_BASE) == {
        "STUFFING_BURST_1H": "ANY_IDENTITY_24H",
        "CHANGE_THEN_NEW_DEVICE_24H": "IDENTITY_CHANGE_24H",
    }
    assert label_proxy.PATTERN_MIN_FRAUD_FIRINGS == 5
    assert label_proxy.PATTERN_MIN_LIFT_LOWER_BOUND == 10.0
    assert label_proxy.PATTERN_OVER_PRESENCE == 3.0
    assert label_proxy.STUFFING_MIN_DISTINCT_ACCOUNTS == 5
    assert {
        "PASSWORD_CHANGE",
        "EMAIL_CHANGE",
        "PHONE_CHANGE",
        "ADDRESS_CHANGE",
        "MFA_RESET",
    } == label_proxy.IDENTITY_CHANGE_TYPES
    assert (HOUR_MS, DAY_MS) == (3_600_000, 86_400_000)


def test_the_declared_lpc2_additions_are_pinned() -> None:
    """LPC-2 is LPC-1 plus these, declared before any run including T1-T3 (§5b)."""
    assert label_proxy.LPC2_ID == "LPC-2"
    assert dict(label_proxy.TX_LEGIT_FLOORS) == {
        "TX_NON_HOME_DEVICE": 0.01,
        "TX_DEVICE_FIRST_USED_24H": 0.001,
        "TX_WHOLE_SECOND": 0.0005,
        "TX_DECLINED": 0.005,
        "TX_DECLINED_PRIOR_1H": 0.0005,
    }


# ================================================ criterion self-tests =====

T0 = 1_767_225_600_000 + 10 * DAY_MS
HOME = {f"acct_{n:06d}": frozenset({"dev_000001"}) for n in range(20)}


def _iso(millis: int) -> str:
    return from_millis(millis).isoformat().replace("+00:00", "Z")


class _Rows:
    """Hand-built rows carrying only the fields the criteria read."""

    def __init__(self) -> None:
        self.rows: list[GeneratedRow] = []
        self._tx = 0

    def tx(
        self,
        at_ms: int,
        account: str,
        *,
        fraud: bool = False,
        instance: str = "fi_00000000",
        device: str = "dev_000001",
        ip: str = "ip_00001",
        outcome: str | None = "APPROVED",
    ) -> None:
        transaction_id = f"tx_{self._tx:012d}"
        self._tx += 1
        label = (
            TransactionLabel(
                transaction_id=transaction_id,
                is_fraud=True,
                fraud_pattern=FraudPattern.ACCOUNT_TAKEOVER,
                scenario_instance_id=instance,
                causal_evidence_keys=frozenset({EvidenceKind.IDENTITY_CHANGE}),
            )
            if fraud
            else TransactionLabel(transaction_id=transaction_id, is_fraud=False)
        )
        payload: dict[str, Any] = {
            "transaction_id": transaction_id,
            "account_id": account,
            "device_id": device,
            "ip_id": ip,
        }
        if outcome is not None:
            payload["authorization_outcome"] = outcome
        self.rows.append(
            GeneratedRow(
                topic="tx.raw.v1",
                event={"envelope": {"occurred_at": _iso(at_ms)}, "payload": payload},
                label=label,
            )
        )

    def identity(
        self,
        at_ms: int,
        account: str,
        kind: str,
        *,
        device: str | None = None,
        ip: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {"account_id": account, "identity_event_type": kind}
        if device:
            payload["device_id"] = device
        if ip:
            payload["ip_id"] = ip
        self.rows.append(
            GeneratedRow(
                topic="identity.events.v1",
                event={"envelope": {"occurred_at": _iso(at_ms)}, "payload": payload},
            )
        )

    def device(self, at_ms: int, account: str, device: str, kind: str = "FIRST_SEEN") -> None:
        self.rows.append(
            GeneratedRow(
                topic="device.events.v1",
                event={
                    "envelope": {"occurred_at": _iso(at_ms)},
                    "payload": {
                        "account_id": account,
                        "device_id": device,
                        "device_event_type": kind,
                    },
                },
            )
        )


def test_the_window_is_half_open_and_strictly_before_the_transaction() -> None:
    rows = _Rows()
    rows.identity(T0 - DAY_MS, "acct_000001", "PASSWORD_CHANGE")  # at t - 24h: counts
    rows.tx(T0, "acct_000001")
    rows.identity(T0 - DAY_MS - 1, "acct_000002", "PASSWORD_CHANGE")  # just outside
    rows.tx(T0, "acct_000002")
    rows.identity(T0, "acct_000003", "PASSWORD_CHANGE")  # at t: not before
    rows.tx(T0, "acct_000003")
    rows.identity(T0 - 1, "acct_000004", "LOGIN_SUCCEEDED")  # not a Q4e change
    rows.tx(T0, "acct_000004")
    report = evaluate(rows.rows)
    # Account 1 (exactly t - 24h) only; 2 is 1 ms too early, 3 is at t, 4 is a login.
    assert report.signals["IDENTITY_CHANGE_24H"].legit == 1
    # Accounts 1 and 4: a login is still an identity event.
    assert report.signals["ANY_IDENTITY_24H"].legit == 2


def test_a_planted_perfect_proxy_fails_for_the_proxy_reason() -> None:
    rows = _Rows()
    for n in range(40):
        account = f"acct_{n:06d}"
        rows.identity(T0 + n * HOUR_MS, account, "PASSWORD_CHANGE")
        rows.tx(T0 + n * HOUR_MS + 60_000, account, fraud=True, instance=f"fi_{n:08d}")
    for n in range(40, 4_040):
        rows.tx(T0 + n * 1_000, f"acct_{n:06d}")
    report = evaluate(rows.rows)
    change = report.signals["IDENTITY_CHANGE_24H"]
    assert change.precision == 1.0
    assert change.lower > label_proxy.MAX_PRECISION_UPPER_BOUND
    assert not report.rule("R3", "IDENTITY_CHANGE_24H").passed
    assert not report.passed


def test_a_signal_that_never_fires_cannot_pass() -> None:
    rows = _Rows()
    for n in range(500):
        rows.tx(T0 + n * 1_000, f"acct_{n:06d}", fraud=n < 5, instance=f"fi_{n:08d}")
    report = evaluate(rows.rows)
    failed = report.signals["LOGIN_FAILED_1H"]
    assert failed.fired == 0
    assert (failed.lower, failed.upper) == (0.0, 1.0)
    for rule in ("R1", "R2", "R3"):
        assert not report.rule(rule, "LOGIN_FAILED_1H").passed


def test_presence_that_is_not_a_proxy_still_fails_without_the_patterns() -> None:
    """The second half bites: plenty of legitimate activity, no scenario pattern."""
    rows = _Rows()
    for n in range(2_000):
        account = f"acct_{n:06d}"
        at = T0 + n * 60_000
        rows.identity(at - 30 * 60_000, account, "LOGIN_FAILED", ip="ip_00009")
        rows.identity(at - 20 * 60_000, account, "PASSWORD_CHANGE", device="dev_000001")
        rows.device(at - 10 * 60_000, account, "dev_000001")
        rows.tx(at, account)
    for n in range(10):
        rows.tx(T0 + n * 7_000 + 13, f"acct_{900_000 + n:06d}", fraud=True, instance=f"fi_{n:08d}")
    report = evaluate(rows.rows)
    failing = {rule.rule for rule in report.failures()}
    assert failing, report.format()
    assert failing <= {"R4a", "R4b", "R4c"}, report.format()
    assert not report.rule("R4a", "STUFFING_BURST_1H").passed


def test_the_stuffing_pattern_counts_distinct_accounts_on_the_transactions_ip() -> None:
    rows = _Rows()
    for n in range(5):
        rows.identity(T0 - 10 * 60_000 + n, f"acct_{n:06d}", "LOGIN_FAILED", ip="ip_00042")
    rows.tx(T0, "acct_000000", fraud=True, ip="ip_00042")
    rows.tx(T0, "acct_000001", ip="ip_00043")  # other IP: no burst
    for _ in range(4):
        rows.identity(T0 - 5 * 60_000, "acct_000077", "LOGIN_FAILED", ip="ip_00050")  # one account
    rows.tx(T0, "acct_000077", ip="ip_00050")
    report = evaluate(rows.rows)
    assert report.signals["STUFFING_BURST_1H"].fraud == 1
    assert report.signals["STUFFING_BURST_1H"].legit == 0


def test_the_change_then_new_device_pattern_reads_no_future() -> None:
    rows = _Rows()
    rows.tx(T0 - 3 * DAY_MS, "acct_000001", device="dev_000010")  # known device
    rows.identity(T0 - HOUR_MS, "acct_000001", "PASSWORD_CHANGE")
    rows.tx(T0, "acct_000001", device="dev_000010")  # old device: no pattern
    rows.identity(T0 - HOUR_MS, "acct_000002", "EMAIL_CHANGE", device="dev_000020")
    rows.tx(T0, "acct_000002", device="dev_000020", fraud=True)  # new within 24h
    rows.tx(T0 + DAY_MS, "acct_000002", device="dev_000020")  # a day later: stale
    report = evaluate(rows.rows)
    assert report.signals["CHANGE_THEN_NEW_DEVICE_24H"].fraud == 1
    assert report.signals["CHANGE_THEN_NEW_DEVICE_24H"].legit == 0


def test_wilson_bounds_match_hand_computed_values() -> None:
    z2 = label_proxy.Z_ONE_SIDED_95**2
    lower, upper = wilson_interval(0, 10, 10)
    assert lower == 0.0
    assert upper == pytest.approx(z2 / (10 + z2), rel=1e-9)
    lower, upper = wilson_interval(5, 10, 10)
    assert lower + upper == pytest.approx(1.0)
    assert wilson_interval(3, 0, 0) == (0.0, 1.0)


def test_clustering_widens_the_interval() -> None:
    independent = wilson_interval(10, 100, 100)
    clustered = wilson_interval(10, 100, 10)
    assert clustered[0] < independent[0] and clustered[1] > independent[1]


def test_an_unlabelled_transaction_is_refused() -> None:
    row = GeneratedRow(
        topic="tx.raw.v1",
        event={
            "envelope": {"occurred_at": _iso(T0)},
            "payload": {
                "account_id": "acct_000001",
                "device_id": "dev_000001",
                "ip_id": "ip_00001",
            },
        },
    )
    with pytest.raises(ValueError, match="no label"):
        evaluate([row])


def test_lpc2_carries_every_lpc1_rule_unchanged() -> None:
    rows = _Rows()
    for n in range(60):
        rows.identity(T0 + n * 1_000 - 5_000, f"acct_{n % 20:06d}", "LOGIN_SUCCEEDED")
        rows.tx(T0 + n * 1_000 + 7, f"acct_{n % 20:06d}", fraud=n < 3, instance=f"fi_{n:08d}")
    lpc1, lpc2 = evaluate_both(rows.rows, HOME)
    alone = evaluate(rows.rows)
    assert lpc1.rules == alone.rules
    assert lpc1.format() == alone.format()
    lpc1_keys = [(r.rule, r.signal) for r in lpc1.rules]
    assert [(r.rule, r.signal) for r in lpc2.rules][: len(lpc1_keys)] == lpc1_keys
    added = {r.signal for r in lpc2.rules} - {r.signal for r in lpc1.rules}
    assert added == set(label_proxy.TX_SIGNALS)
    assert lpc2.criterion == "LPC-2" and lpc1.criterion == "LPC-1"


def test_device_signals_measure_payment_against_home_devices() -> None:
    rows = _Rows()
    rows.tx(T0, "acct_000001", device="dev_000001")  # home: neither signal
    rows.tx(T0 - 2 * DAY_MS, "acct_000002", device="dev_000009")  # first payment: both
    rows.tx(T0, "acct_000002", device="dev_000009")  # non-home, first paid > 24h ago
    rows.tx(T0 - DAY_MS, "acct_000003", device="dev_000008")  # first payment: both
    rows.tx(T0, "acct_000003", device="dev_000008")  # first paid exactly t - 24h: both
    rows.identity(T0 - HOUR_MS, "acct_000004", "LOGIN_SUCCEEDED", device="dev_000007")
    rows.tx(T0, "acct_000004", device="dev_000007")  # a login is not a payment: both
    report = evaluate_lpc2(rows.rows, HOME)
    assert report.signals["TX_NON_HOME_DEVICE"].legit == 5
    assert report.signals["TX_DEVICE_FIRST_USED_24H"].legit == 4


def test_whole_second_and_decline_signals_respect_their_boundaries() -> None:
    rows = _Rows()
    rows.tx(T0, "acct_000001")  # whole second
    rows.tx(T0 + 1, "acct_000002")
    rows.tx(T0 - HOUR_MS + 7, "acct_000003", outcome="DECLINED")
    rows.tx(T0 + 7, "acct_000003")  # decline exactly t - 1h: fires
    rows.tx(T0 - HOUR_MS + 6, "acct_000004", outcome="DECLINED")
    rows.tx(T0 + 7, "acct_000004")  # decline 1 ms too early: does not fire
    rows.tx(T0 + 7, "acct_000005", outcome="DECLINED")
    rows.tx(T0 + 7, "acct_000005")  # decline at t itself: not before
    report = evaluate_lpc2(rows.rows, HOME)
    assert report.signals["TX_WHOLE_SECOND"].legit == 1
    assert report.signals["TX_DECLINED"].legit == 3
    assert report.signals["TX_DECLINED_PRIOR_1H"].legit == 1


def test_lpc2_refuses_unknown_accounts_and_missing_outcomes() -> None:
    unknown = _Rows()
    unknown.tx(T0, "acct_999999")
    with pytest.raises(ValueError, match="no home devices"):
        evaluate_lpc2(unknown.rows, HOME)
    missing = _Rows()
    missing.tx(T0, "acct_000001", outcome=None)
    with pytest.raises(ValueError, match="authorization_outcome"):
        evaluate_lpc2(missing.rows, HOME)


def test_no_runtime_module_imports_the_criteria() -> None:
    """Labels feed these reports; the reports must never feed scoring."""
    scanned = 0
    offenders: list[str] = []
    for root in ("packages", "services", "mcp_servers"):
        base = ROOT / root
        if not base.is_dir():
            continue
        for path in base.rglob("*.py"):
            scanned += 1
            if "label_proxy" in path.read_text():
                offenders.append(str(path.relative_to(ROOT)))
    for name in ("engine.py", "baseline.py", "emit.py", "cli.py", "scenarios.py"):
        scanned += 1
        if "label_proxy" in (ROOT / "data" / "generator" / name).read_text():
            offenders.append(f"data/generator/{name}")
    assert scanned > 20
    assert not offenders, offenders


# ======================================================= LPC-3 (stage 1c) =====


def test_the_declared_lpc3_additions_are_pinned() -> None:
    """LPC-3 is LPC-2 plus these, declared before any run including M1-M3 (§5c)."""
    assert label_proxy.LPC3_ID == "LPC-3"
    assert dict(label_proxy.ARTEFACT_LEGIT_FLOORS) == {
        "TX_REPEATED_EXACT_COORDINATES": 0.001,
        "TX_EXACT_HOME_POINT": 0.0001,
        "TX_CNP_ECOMMERCE": 0.05,
    }
    assert frozenset({"TX_EXACT_HOME_POINT"}) == label_proxy.ABSENCE_ALTERNATIVE_SIGNALS
    assert dict(label_proxy.ENRICHMENT_SIGNALS) == {"TX_CNP_ECOMMERCE": "CARD_NOT_PRESENT"}
    assert label_proxy.ENRICHMENT_MAX_RATIO == 2.0


HOME_POINT = (51.5, -0.12)


def _population(accounts: int = 20) -> label_proxy.PopulationView:
    names = [f"acct_{n:06d}" for n in range(accounts)]
    return label_proxy.PopulationView(
        home_devices=dict.fromkeys(names, frozenset({"dev_000001"})),
        home_points=dict.fromkeys(names, HOME_POINT),
    )


class _FullRows:
    """Hand-built transactions carrying every field LPC-3 reads."""

    def __init__(self) -> None:
        self.rows: list[GeneratedRow] = []
        self._n = 0

    def tx(
        self,
        at_ms: int,
        account: str,
        *,
        point: tuple[float, float] | None = None,
        channel: str = "CARD_PRESENT",
        entry_mode: str = "CHIP",
        fraud: bool = False,
        instance: str = "fi_00000000",
    ) -> None:
        n = self._n
        self._n += 1
        latitude, longitude = point if point is not None else (50.0 + n * 1e-4, 1.0 + n * 1e-4)
        transaction_id = f"tx_{n:012d}"
        label = (
            TransactionLabel(
                transaction_id=transaction_id,
                is_fraud=True,
                fraud_pattern=FraudPattern.ACCOUNT_TAKEOVER,
                scenario_instance_id=instance,
                causal_evidence_keys=frozenset({EvidenceKind.IDENTITY_CHANGE}),
            )
            if fraud
            else TransactionLabel(transaction_id=transaction_id, is_fraud=False)
        )
        self.rows.append(
            GeneratedRow(
                topic="tx.raw.v1",
                event={
                    "envelope": {"occurred_at": _iso(at_ms)},
                    "payload": {
                        "transaction_id": transaction_id,
                        "account_id": account,
                        "device_id": "dev_000001",
                        "ip_id": "ip_00001",
                        "authorization_outcome": "APPROVED",
                        "latitude": latitude,
                        "longitude": longitude,
                        "channel": channel,
                        "entry_mode": entry_mode,
                    },
                },
                label=label,
            )
        )


def test_lpc3_carries_every_lpc2_rule_unchanged() -> None:
    rows = _FullRows()
    for n in range(60):
        rows.tx(T0 + n * 1_000 + 7, f"acct_{n % 20:06d}", fraud=n < 3, instance=f"fi_{n:08d}")
    lpc1, lpc2, lpc3 = label_proxy.evaluate_all(rows.rows, _population())
    both1, both2 = evaluate_both(rows.rows, HOME)
    assert (lpc1.rules, lpc1.format()) == (both1.rules, both1.format())
    assert (lpc2.rules, lpc2.format()) == (both2.rules, both2.format())
    assert lpc3.rules[: len(lpc2.rules)] == lpc2.rules
    added = {r.signal for r in lpc3.rules} - {r.signal for r in lpc2.rules}
    assert added == set(label_proxy.ARTEFACT_SIGNALS)
    assert lpc3.criterion == "LPC-3"


def test_repeated_coordinates_need_an_exact_match_strictly_earlier_on_the_account() -> None:
    place = (51.7, -0.3)
    rows = _FullRows()
    rows.tx(T0, "acct_000001", point=place)
    rows.tx(T0 + 1_000, "acct_000001", point=place)  # repeats an earlier one: fires
    rows.tx(T0 + 2_000, "acct_000001", point=(51.7, -0.300001))  # not exact
    rows.tx(T0 + 3_000, "acct_000002", point=place)  # another account
    rows.tx(T0, "acct_000003", point=(52.0, 0.5))
    rows.tx(T0, "acct_000003", point=(52.0, 0.5))  # same millisecond: not earlier
    _, _, lpc3 = label_proxy.evaluate_all(rows.rows, _population())
    assert lpc3.signals["TX_REPEATED_EXACT_COORDINATES"].legit == 1


def test_home_point_must_match_exactly() -> None:
    rows = _FullRows()
    rows.tx(T0, "acct_000001", point=HOME_POINT)
    rows.tx(T0, "acct_000002", point=(51.5, -0.120001))
    _, _, lpc3 = label_proxy.evaluate_all(rows.rows, _population())
    assert lpc3.signals["TX_EXACT_HOME_POINT"].legit == 1


def test_cnp_ecommerce_fires_only_on_card_not_present_ecommerce() -> None:
    rows = _FullRows()
    rows.tx(T0, "acct_000001", channel="CARD_NOT_PRESENT", entry_mode="ECOMMERCE")
    rows.tx(T0, "acct_000002", channel="CARD_NOT_PRESENT", entry_mode="TOKEN")
    rows.tx(T0, "acct_000003", channel="CARD_PRESENT", entry_mode="ECOMMERCE")
    _, _, lpc3 = label_proxy.evaluate_all(rows.rows, _population())
    assert lpc3.signals["TX_CNP_ECOMMERCE"].legit == 1
    enrichment = lpc3.enrichment["TX_CNP_ECOMMERCE"]
    assert (enrichment.legit_eligible, enrichment.legit_fired) == (2, 1)


def test_the_home_point_absence_alternative_forgives_only_zero_fraud() -> None:
    clean = _FullRows()
    for n in range(40):
        clean.tx(T0 + n * 1_000 + 7, f"acct_{n % 20:06d}", fraud=n < 2, instance=f"fi_{n:08d}")
    _, _, lpc3 = label_proxy.evaluate_all(clean.rows, _population())
    for rule in ("R1'", "R2'", "R3'"):
        assert lpc3.rule(rule, "TX_EXACT_HOME_POINT").passed

    dirty = _FullRows()
    for n in range(40):
        dirty.tx(T0 + n * 1_000 + 7, f"acct_{n % 20:06d}")
    dirty.tx(T0 + 99_000, "acct_000001", point=HOME_POINT, fraud=True, instance="fi_99999999")
    _, _, lpc3 = label_proxy.evaluate_all(dirty.rows, _population())
    assert not lpc3.rule("R1'", "TX_EXACT_HOME_POINT").passed
    assert not lpc3.rule("R3'", "TX_EXACT_HOME_POINT").passed


def test_enrichment_parity_detects_a_doubled_planted_share() -> None:
    def judged(fraud_ecommerce: int) -> LabelProxyReport:
        rows = _FullRows()
        for n in range(30):
            rows.tx(
                T0 + n * 1_000 + 7,
                f"acct_{n:06d}",
                channel="CARD_NOT_PRESENT",
                entry_mode="ECOMMERCE" if n < fraud_ecommerce else "TOKEN",
                fraud=True,
                instance=f"fi_{n:08d}",
            )
        for n in range(3_000):
            rows.tx(
                T0 + 100_000 + n * 1_000 + 7,
                f"acct_{n % 300:06d}",
                channel="CARD_NOT_PRESENT",
                entry_mode=("ECOMMERCE", "TOKEN", "MANUAL")[n % 3],
            )
        return label_proxy.evaluate_all(rows.rows, _population(300))[2]

    skewed = judged(24)
    assert not skewed.rule("R6", "TX_CNP_ECOMMERCE").passed
    assert skewed.enrichment["TX_CNP_ECOMMERCE"].ratio == pytest.approx(0.8 * 3)
    assert judged(10).rule("R6", "TX_CNP_ECOMMERCE").passed


def test_lpc3_refuses_transactions_without_location_or_entry_mode() -> None:
    rows = _Rows()
    rows.tx(T0, "acct_000001")
    with pytest.raises(ValueError, match="location, channel or entry mode"):
        label_proxy.evaluate_all(rows.rows, _population())


# ============================= STAGE 1C DEMONSTRATION (not acceptance) =====

_Reports = tuple[LabelProxyReport, LabelProxyReport, LabelProxyReport]


def _stage1(**overrides: Any) -> GeneratorConfig:
    """The stage-1 scale declared in the ADR (§5.9) before any gated run."""
    base: dict[str, Any] = {
        "seed": 42,
        "row_count": 120_000,
        "account_count": 4_800,
        "merchant_count": 360,
        "device_count": 5_760,
        "ip_count": 2_400,
        "fraud_rate": 0.005,
        "start_at": dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
        "end_at": dt.datetime(2026, 3, 1, tzinfo=dt.UTC),
    }
    base.update(overrides)
    return GeneratorConfig(**base)


ZERO_RATE_FIELDS = frozenset(
    {
        "login_rate_per_account_day",
        "abandoned_burst_rate_per_account_day",
        "password_change_rate_per_account_year",
        "email_change_rate_per_account_year",
        "phone_change_rate_per_account_year",
        "address_change_rate_per_account_year",
        "mfa_reset_rate_per_account_year",
        "mfa_enrolled_rate_per_account_year",
        "new_device_rate_per_account_year",
        "attribute_change_rate_per_device_year",
        "fingerprint_change_rate_per_device_year",
        "new_device_payment_share",
        "secondary_device_account_share",
        "secondary_device_transaction_share",
        "decline_share_per_transaction",
        "decline_retry_share_per_transaction",
    }
)
"""Every identity, device, T1 and T3 rate, named. A rate added later must be
added here too, or the control would quietly stop being a zero-activity control."""


def _zero_rates() -> BaselineIdentityConfig:
    """Every rate zero, every other setting at its default: the block, no activity."""
    fields = set(BaselineIdentityConfig.model_fields)
    assert fields >= ZERO_RATE_FIELDS
    suffixed = {
        name
        for name in fields
        if name.endswith(("_per_account_day", "_per_account_year", "_per_device_year"))
    }
    assert suffixed <= ZERO_RATE_FIELDS
    return BaselineIdentityConfig.model_validate(
        {**BaselineIdentityConfig().model_dump(), **dict.fromkeys(ZERO_RATE_FIELDS, 0.0)}
    )


def _reports(config: GeneratorConfig) -> _Reports:
    """LPC-1, LPC-2 and LPC-3 from one streaming pass over one generation."""
    universe = build_universe(config)
    return label_proxy.evaluate_all(
        generate_dataset(config, universe), label_proxy.population_view(universe)
    )


@pytest.fixture(scope="module")
def eval_v2_configured() -> _Reports:
    return _reports(_stage1(baseline_identity=BaselineIdentityConfig()))


@pytest.fixture(scope="module")
def negative_control() -> _Reports:
    return _reports(_stage1())


@pytest.fixture(scope="module")
def zero_rate_control() -> _Reports:
    return _reports(_stage1(baseline_identity=_zero_rates()))


def test_stage1c_the_three_generations_share_their_transaction_and_fraud_counts(
    eval_v2_configured: _Reports, negative_control: _Reports, zero_rate_control: _Reports
) -> None:
    """The controls differ from the positive run in the gated behaviour only."""
    counts = {
        (report.transactions, report.fraudulent)
        for reports in (eval_v2_configured, negative_control, zero_rate_control)
        for report in reports
    }
    assert len(counts) == 1


def test_stage1c_home_devices_view_matches_the_lpc2_helper() -> None:
    """LPC-3's population view must hand LPC-2 exactly the home devices it had."""
    universe = build_universe(_stage1(row_count=1_000, account_count=50, device_count=60))
    assert dict(label_proxy.population_view(universe).home_devices) == home_devices_by_account(
        universe
    )


@pytest.mark.parametrize("index", [0, 1, 2], ids=["LPC-1", "LPC-2", "LPC-3"])
def test_stage1c_eval_v2_configured_generation_passes(
    eval_v2_configured: _Reports, index: int
) -> None:
    report = eval_v2_configured[index]
    assert report.passed, report.format()


def test_stage1c_negative_control_fails_lpc1_because_presence_is_a_proxy(
    negative_control: _Reports,
) -> None:
    """Required: gate off must fail -- and fail on evidence that presence IS a
    proxy (a precision LOWER bound above 0.25), not merely for want of samples."""
    lpc1 = negative_control[0]
    assert not lpc1.passed, lpc1.format()
    for signal in ("IDENTITY_CHANGE_24H", "DEVICE_FIRST_SEEN_24H"):
        assert lpc1.signals[signal].lower > label_proxy.MAX_PRECISION_UPPER_BOUND, lpc1.format()


def test_stage1c_negative_control_fails_lpc2_for_the_proxy_reason_on_every_tx_signal(
    negative_control: _Reports,
) -> None:
    """Declared in §5b: on each transaction signal, R3 fails AND the precision point
    estimate itself exceeds 0.25, so the failure is not interval width alone."""
    lpc2 = negative_control[1]
    assert not lpc2.passed, lpc2.format()
    for signal in label_proxy.TX_SIGNALS:
        stats = lpc2.signals[signal]
        assert not lpc2.rule("R3", signal).passed, lpc2.format()
        assert stats.precision is not None, lpc2.format()
        assert stats.precision > label_proxy.MAX_PRECISION_UPPER_BOUND, lpc2.format()


def test_stage1c_negative_control_fails_lpc3_for_the_proxy_reason_on_every_marker(
    negative_control: _Reports,
) -> None:
    """Declared in §5c: M1 and M2 fail their precision rule with a point precision
    above 0.25; M3 fails R6 with a point ratio above 2."""
    lpc3 = negative_control[2]
    assert not lpc3.passed, lpc3.format()
    for signal, rule in (
        ("TX_REPEATED_EXACT_COORDINATES", "R3"),
        ("TX_EXACT_HOME_POINT", "R3'"),
    ):
        stats = lpc3.signals[signal]
        assert not lpc3.rule(rule, signal).passed, lpc3.format()
        assert stats.precision is not None, lpc3.format()
        assert stats.precision > label_proxy.MAX_PRECISION_UPPER_BOUND, lpc3.format()
    enrichment = lpc3.enrichment["TX_CNP_ECOMMERCE"]
    assert not lpc3.rule("R6", "TX_CNP_ECOMMERCE").passed, lpc3.format()
    assert enrichment.ratio is not None, lpc3.format()
    assert enrichment.ratio > label_proxy.ENRICHMENT_MAX_RATIO, lpc3.format()


def test_stage1c_zero_rate_control_fails_every_criterion(zero_rate_control: _Reports) -> None:
    """The gate's corrections alone -- unique envelopes, coherent platform, planted
    sub-second timing, planted locations and entry modes -- must pass no criterion."""
    for report in zero_rate_control:
        assert not report.passed, report.format()
