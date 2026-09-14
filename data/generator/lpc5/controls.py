"""`LPC-5` §14.1-§14.3: whether a control run failed for the reasons the criterion names.

A control that fails for some other reason is not evidence that its check can fail. Each evaluator
returns the unmet expectations, and an empty list means the control behaved as declared.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Final

from data.generator import label_proxy
from data.generator.config import CORRECTIONS, BaselineIdentityConfig, GeneratorConfig
from data.generator.lpc5 import declaration as d
from data.generator.lpc5.run import Lpc5Report
from trace_core.domain.enums import FraudPattern

FP = FraudPattern
TX = d.Population.TX


def has_finding(
    report: Lpc5Report,
    checks: Iterable[str],
    *,
    attributes: Iterable[str] = (),
    values: Iterable[str] = (),
    groups: Iterable[str] = (),
) -> bool:
    wanted_checks = tuple(checks)
    wanted_attributes = set(attributes)
    wanted_values = set(values)
    wanted_groups = set(groups)
    for result in report.checks.values():
        for finding in result.findings:
            if not finding.check.startswith(wanted_checks):
                continue
            if wanted_attributes and finding.attribute not in wanted_attributes:
                continue
            if wanted_values and finding.value not in wanted_values:
                continue
            if wanted_groups and finding.group not in wanted_groups:
                continue
            return True
    return False


def _rule_failed(report: Lpc5Report, rules: Iterable[str], signal: str) -> bool:
    lpc3 = report.s0[2]
    return any(not r.passed for r in lpc3.rules if r.signal == signal and r.rule in set(rules))


def _precision_above(report: Lpc5Report, signal: str, bound: float) -> bool:
    precision = report.s0[2].signals[signal].precision
    return precision is not None and precision > bound


def negative_control_unmet(report: Lpc5Report) -> list[str]:
    """§14.1: the eval-v1 control must fail every named check, on the named cell."""
    unmet: list[str] = []
    lpc1 = report.s0[0]
    for signal in ("IDENTITY_CHANGE_24H", "DEVICE_FIRST_SEEN_24H"):
        if lpc1.rule("R3", signal).passed or lpc1.signals[signal].lower <= 0.25:
            unmet.append(f"S0 LPC-1 R3 on {signal} with a lower bound above 0.25")
    for signal in label_proxy.TX_SIGNALS:
        if not _rule_failed(report, ("R3",), signal) or not _precision_above(report, signal, 0.25):
            unmet.append(f"S0 LPC-2 R3 on {signal} with point precision above 0.25")
    if not _rule_failed(report, ("R3",), "TX_REPEATED_EXACT_COORDINATES") or not _precision_above(
        report, "TX_REPEATED_EXACT_COORDINATES", 0.25
    ):
        unmet.append("S0 LPC-3 R3 on TX_REPEATED_EXACT_COORDINATES with point precision above 0.25")
    if not _rule_failed(report, ("R3'",), "TX_EXACT_HOME_POINT") or not _precision_above(
        report, "TX_EXACT_HOME_POINT", 0.25
    ):
        unmet.append("S0 LPC-3 R3' on TX_EXACT_HOME_POINT with point precision above 0.25")
    enrichment = report.s0[2].enrichment.get("TX_CNP_ECOMMERCE")
    if (
        not _rule_failed(report, ("R6",), "TX_CNP_ECOMMERCE")
        or enrichment is None
        or enrichment.ratio is None
        or enrichment.ratio <= 2
    ):
        unmet.append("S0 LPC-3 R6 on TX_CNP_ECOMMERCE with a point ratio above 2")
    named: tuple[tuple[str, bool], ...] = (
        ("R7 on hour or daypart", has_finding(report, ("R7",), attributes=("hour", "daypart"))),
        (
            "R7 on merchant_habitual or merchant_popularity",
            has_finding(report, ("R7",), attributes=("merchant_habitual", "merchant_popularity")),
        ),
        (
            "S1-U(a) on TX ip_home = not",
            has_finding(report, ("S1-U(a)",), attributes=("ip_home",), values=("not",)),
        ),
        (
            "S1-U(a) on TX distance_home for ACCOUNT_TAKEOVER",
            has_finding(
                report,
                ("S1-U(a)",),
                attributes=("distance_home",),
                values=("[100,500)", "≥500"),
                groups=(FP.ACCOUNT_TAKEOVER.value,),
            ),
        ),
        (
            "S2a or S2b on TX subsecond",
            has_finding(report, ("S2a", "S2b"), attributes=("subsecond",)),
        ),
        ("S2c rule 3", has_finding(report, ("S2c/3",))),
        ("S2c rule 7", has_finding(report, ("S2c/7",))),
        ("S3 pooled on slice 20", has_finding(report, ("S3/pooled",), values=("slice 20",))),
        (
            "S4 K2 for ACCOUNT_TAKEOVER",
            has_finding(report, ("S4/K2",), groups=(FP.ACCOUNT_TAKEOVER.value,)),
        ),
        (
            "S5b for VELOCITY_ATTACK or UNUSUAL_LOCATION_DEVICE",
            has_finding(
                report,
                ("S5b",),
                groups=(FP.VELOCITY_ATTACK.value, FP.UNUSUAL_LOCATION_DEVICE.value),
            ),
        ),
    )
    unmet.extend(description for description, met in named if not met)
    return unmet


def zero_rate_unmet(report: Lpc5Report) -> list[str]:
    """§14.2: with every legitimate-baseline rate zero, at least one LPC-1 R1 must fail."""
    lpc1 = report.s0[0]
    if any(not r.passed for r in lpc1.rules if r.rule == "R1"):
        return []
    return ["no S0 LPC-1 R1 failure"]


ABLATION_CHECKS: Mapping[str, Callable[[Lpc5Report], bool]] = {
    "T1": lambda r: _rule_failed(r, ("R1", "R2"), "TX_NON_HOME_DEVICE"),
    "T2": lambda r: has_finding(r, ("S2a", "S2b"), attributes=("subsecond",)),
    "T3": lambda r: _rule_failed(r, ("R1", "R2"), "TX_DECLINED"),
    "M1": lambda r: _rule_failed(r, ("R3",), "TX_REPEATED_EXACT_COORDINATES"),
    "M2": lambda r: _rule_failed(r, ("R1'", "R2'", "R3'"), "TX_EXACT_HOME_POINT"),
    "M3": lambda r: _rule_failed(r, ("R6",), "TX_CNP_ECOMMERCE"),
    "M4": lambda r: has_finding(r, ("R7",), attributes=("hour", "daypart")),
    "M5": lambda r: has_finding(r, ("R7",), attributes=("merchant_popularity",)),
    "M6": lambda r: has_finding(
        r,
        ("R7",),
        attributes=("channel",),
        groups=tuple(p.value for p in FP if p is not FP.IMPOSSIBLE_TRAVEL),
    ),
    "N1": lambda r: has_finding(r, ("S1-U(a)",), attributes=("ip_home",), values=("not",)),
    "N2": lambda r: has_finding(
        r,
        ("S1-U(a)",),
        attributes=("distance_home",),
        values=("[100,500)", "≥500"),
        groups=(FP.ACCOUNT_TAKEOVER.value,),
    ),
    "N3": lambda r: has_finding(
        r,
        ("S1-U(a)",),
        attributes=("gap_prev",),
        values=("<10s", "[10s,60s)"),
        groups=(FP.CARD_TESTING.value, FP.VELOCITY_ATTACK.value),
    ),
    "N4": lambda r: has_finding(
        r,
        ("S1-U(a)",),
        attributes=("merchant_same_amount_accounts_24h",),
        values=("5+",),
        groups=(FP.MERCHANT_COLLUSION.value,),
    ),
    "N5": lambda r: has_finding(
        r, ("S1-U(a)",), attributes=("joint_link",), values=("yes",), groups=(FP.FRAUD_RING.value,)
    ),
    "N6": lambda r: has_finding(r, ("S3/pooled",), values=("slice 20",)),
    "N7": lambda r: has_finding(r, ("S4/K2",), groups=(FP.ACCOUNT_TAKEOVER.value,)),
    "N8": lambda r: has_finding(r, ("S4/K1",), groups=(FP.DEVICE_FARM.value,)),
    "N9": lambda r: has_finding(r, ("S5a",)),
    "N10": lambda r: has_finding(r, ("S2c/5",)),
    "N11": lambda r: (
        has_finding(r, ("S2c/7",)) or has_finding(r, ("S2b",), attributes=("tie_rank",))
    ),
    "N12": lambda r: has_finding(r, ("S2c/3",)),
}
"""§14.3, one evaluator per correction."""


def ablation_unmet(correction: str, report: Lpc5Report) -> list[str]:
    expectation = d.ABLATIONS[correction]
    if ABLATION_CHECKS[correction](report):
        return []
    return [f"{correction}: expected {expectation.check} {expectation.note}".rstrip()]


ZERO_RATE_FIELDS: Final[frozenset[str]] = frozenset(
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
        "transaction_away_ip_share",
        "travel_trips_per_account_year",
        "micro_session_share_per_transaction",
        "fixed_price_merchant_share",
        "fixed_price_purchase_share",
        "household_account_share",
    }
)
"""§14.2: every legitimate-baseline rate, by name -- identity and device activity, T1, T3 and N1-N5.

Conditional shares (of logins, bursts or new devices) are left alone: with their rates zero, nothing
they apply to happens. A rate added to `BaselineIdentityConfig` must be named here, or the control
would quietly stop being a zero-activity control; `zero_rates` refuses a per-day or per-year rate
it does not name."""

_RATE_SUFFIXES: Final = ("_per_account_day", "_per_account_year", "_per_device_year")


def zero_rates(block: BaselineIdentityConfig) -> BaselineIdentityConfig:
    """`block` with every legitimate-baseline rate zero and every other setting unchanged."""
    fields = set(BaselineIdentityConfig.model_fields)
    unknown = sorted(ZERO_RATE_FIELDS - fields)
    unnamed = sorted(
        name for name in fields if name.endswith(_RATE_SUFFIXES) and name not in ZERO_RATE_FIELDS
    )
    if unknown or unnamed:
        raise ValueError(
            f"the zero-rate fields are stale: {unknown} do not exist, and the rates {unnamed} are "
            f"not named"
        )
    return BaselineIdentityConfig.model_validate(
        {**block.model_dump(), **dict.fromkeys(ZERO_RATE_FIELDS, 0.0)}
    )


def _candidate_block(config: GeneratorConfig) -> BaselineIdentityConfig:
    block = config.baseline_identity
    if block is None:
        raise ValueError(
            "a §14.2 or §14.3 control is taken from a candidate, which has the eval-v2 gate on"
        )
    if block.disabled_corrections:
        raise ValueError(
            f"a candidate disables no correction; this one disables "
            f"{list(block.disabled_corrections)}"
        )
    return block


def zero_rate_control(config: GeneratorConfig) -> GeneratorConfig:
    """§14.2: the candidate with every legitimate-baseline rate zero; the seed and all else kept."""
    return config.model_copy(update={"baseline_identity": zero_rates(_candidate_block(config))})


def ablation_control(config: GeneratorConfig, correction: str) -> GeneratorConfig:
    """§14.3: the candidate with `correction` alone disabled; the seed and all else kept, so a
    failed check is attributable to that correction."""
    if correction not in CORRECTIONS:
        raise ValueError(f"unknown correction {correction!r}")
    block = BaselineIdentityConfig.model_validate(
        {**_candidate_block(config).model_dump(), "disabled_corrections": (correction,)}
    )
    return config.model_copy(update={"baseline_identity": block})
