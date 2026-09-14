"""`LPC-1`, `LPC-2` and `LPC-3` -- the pre-declared label-proxy criteria for eval-v2.

**What they decide.** `LPC-1`: whether the mere *presence* of identity or device
events before a transaction determines that transaction's fraud label
(docs/PHASE3_PLAN.md §3 Q5). In `eval-v1` it does, because those events exist
only inside fraud scenarios. `LPC-2`: every `LPC-1` rule, plus whether five
transaction-row facts do -- a non-home device, a device first used within a day,
a whole-second timestamp, a declined outcome, a recent decline. `LPC-3`: every
`LPC-2` rule, plus three planted-row markers -- repeated exact coordinates, the
exact home point, an `ECOMMERCE` entry mode among card-not-present transactions.
Each criterion was declared -- signals, statistics, thresholds -- before any
generation it judges existed (the eval-v2 ADR, draft eval/track_a/drafts/eval-v2-adr-draft.md §5,
§5b and §5c), and a test pins every threshold as a literal, so loosening one is a
visible diff rather than a quiet edit.

**Evaluation-side only.** Labels come from the generator's in-memory
`GeneratedRow`s, and home devices and home points from its population. Nothing
here reads PostgreSQL, and nothing under `packages/`, `services/` or
`mcp_servers/` may import this module (asserted by test). A report computed from
labels must never become an input to scoring.

**Two halves.** Presence signals must not be proxies (R1-R3), and the scenarios'
*patterns* must stay detectable (R4). Without the second half, the first could be
passed by drowning the scenarios in look-alike legitimate behaviour.
"""

from __future__ import annotations

import bisect
import datetime as dt
import math
from collections import defaultdict
from collections.abc import Collection, Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from trace_core.domain.time import to_millis

if TYPE_CHECKING:  # pragma: no cover - typing only
    from data.generator.engine import GeneratedRow
    from data.generator.population import Universe

CRITERION_ID: Final = "LPC-1"
LPC2_ID: Final = "LPC-2"
LPC3_ID: Final = "LPC-3"

HOUR_MS: Final = 3_600_000
DAY_MS: Final = 86_400_000

TX_TOPIC: Final = "tx.raw.v1"
IDENTITY_TOPIC: Final = "identity.events.v1"
DEVICE_TOPIC: Final = "device.events.v1"

# ---- LPC-1 declared thresholds (see the ADR §5 for why each value) ----------
Z_ONE_SIDED_95: Final = 1.645
MAX_PRECISION_UPPER_BOUND: Final = 0.25
MIN_LEGIT_FIRINGS: Final = 30
LEGIT_FLOORS: Final[Mapping[str, float]] = MappingProxyType(
    {
        "ANY_IDENTITY_24H": 0.05,
        "LOGIN_FAILED_1H": 0.0001,
        "IDENTITY_CHANGE_24H": 0.001,
        "DEVICE_FIRST_SEEN_24H": 0.001,
        "ANY_DEVICE_EVENT_24H": 0.001,
        "ANY_IDENTITY_OR_DEVICE_24H": 0.05,
    }
)
PRESENCE_SIGNALS: Final[tuple[str, ...]] = tuple(LEGIT_FLOORS)

PATTERN_BASE: Final[Mapping[str, str]] = MappingProxyType(
    {
        "STUFFING_BURST_1H": "ANY_IDENTITY_24H",
        "CHANGE_THEN_NEW_DEVICE_24H": "IDENTITY_CHANGE_24H",
    }
)
PATTERN_SIGNALS: Final[tuple[str, ...]] = tuple(PATTERN_BASE)
PATTERN_MIN_FRAUD_FIRINGS: Final = 5
PATTERN_MIN_LIFT_LOWER_BOUND: Final = 10.0
PATTERN_OVER_PRESENCE: Final = 3.0
STUFFING_MIN_DISTINCT_ACCOUNTS: Final = 5

IDENTITY_CHANGE_TYPES: Final = frozenset(
    {"PASSWORD_CHANGE", "EMAIL_CHANGE", "PHONE_CHANGE", "ADDRESS_CHANGE", "MFA_RESET"}
)
"""The Q4e set. LOGIN_SUCCEEDED, MFA_ENROLLED and UNKNOWN are not changes."""
LOGIN_TYPES: Final = frozenset({"LOGIN_FAILED", "LOGIN_SUCCEEDED"})

# ---- LPC-2 declared additions (see the ADR §5b) -----------------------------
TX_LEGIT_FLOORS: Final[Mapping[str, float]] = MappingProxyType(
    {
        "TX_NON_HOME_DEVICE": 0.01,
        "TX_DEVICE_FIRST_USED_24H": 0.001,
        "TX_WHOLE_SECOND": 0.0005,
        "TX_DECLINED": 0.005,
        "TX_DECLINED_PRIOR_1H": 0.0005,
    }
)
TX_SIGNALS: Final[tuple[str, ...]] = tuple(TX_LEGIT_FLOORS)

# ---- LPC-3 declared additions (see the ADR §5c) -----------------------------
ARTEFACT_LEGIT_FLOORS: Final[Mapping[str, float]] = MappingProxyType(
    {
        "TX_REPEATED_EXACT_COORDINATES": 0.001,
        "TX_EXACT_HOME_POINT": 0.0001,
        "TX_CNP_ECOMMERCE": 0.05,
    }
)
ARTEFACT_SIGNALS: Final[tuple[str, ...]] = tuple(ARTEFACT_LEGIT_FLOORS)
ABSENCE_ALTERNATIVE_SIGNALS: Final = frozenset({"TX_EXACT_HOME_POINT"})
"""Signals whose R1-R3 are each also satisfied when no fraudulent transaction fires."""
ENRICHMENT_SIGNALS: Final[Mapping[str, str]] = MappingProxyType(
    {"TX_CNP_ECOMMERCE": "CARD_NOT_PRESENT"}
)
"""Signal -> the channel whose transactions form R6's eligible population."""
ENRICHMENT_MAX_RATIO: Final = 2.0

LPC1_SIGNALS: Final[tuple[str, ...]] = (*PRESENCE_SIGNALS, *PATTERN_SIGNALS)
LPC2_SIGNALS: Final[tuple[str, ...]] = (*LPC1_SIGNALS, *TX_SIGNALS)
LPC3_SIGNALS: Final[tuple[str, ...]] = (*LPC2_SIGNALS, *ARTEFACT_SIGNALS)

_EMPTY: Final[list[int]] = []


@dataclass(frozen=True, slots=True)
class SignalStats:
    """One signal evaluated over every transaction."""

    name: str
    fired: int
    fraud: int
    legit: int
    clusters: int
    """Distinct clusters among firing transactions: the interval's effective n."""
    precision: float | None
    legit_share: float
    fraud_share: float
    lift: float | None
    lower: float
    upper: float


@dataclass(frozen=True, slots=True)
class EnrichmentStats:
    """R6: a signal's firing share among one channel's transactions, by class."""

    name: str
    channel: str
    fraud_eligible: int
    fraud_fired: int
    fraud_clusters: int
    fraud_share: float | None
    fraud_upper: float
    legit_eligible: int
    legit_fired: int
    legit_clusters: int
    legit_share: float | None
    legit_lower: float
    ratio: float | None
    """Point ratio of the fraudulent to the legitimate firing share."""


@dataclass(frozen=True, slots=True)
class RuleResult:
    rule: str
    signal: str
    passed: bool
    detail: str


@dataclass(frozen=True, slots=True)
class LabelProxyReport:
    criterion: str
    transactions: int
    fraudulent: int
    base_rate: float
    signals: Mapping[str, SignalStats]
    rules: tuple[RuleResult, ...]
    enrichment: Mapping[str, EnrichmentStats] = field(default_factory=lambda: MappingProxyType({}))

    @property
    def passed(self) -> bool:
        return bool(self.rules) and all(rule.passed for rule in self.rules)

    def failures(self) -> list[RuleResult]:
        return [rule for rule in self.rules if not rule.passed]

    def rule(self, rule: str, signal: str) -> RuleResult:
        return next(r for r in self.rules if r.rule == rule and r.signal == signal)

    def format(self) -> str:
        """A table a reviewer can read in a test failure or a run record."""
        lines = [
            f"{self.criterion}: {'PASS' if self.passed else 'FAIL'}  "
            f"transactions={self.transactions} fraudulent={self.fraudulent} "
            f"base_rate={self.base_rate:.5f}",
            f"  {'signal':<30}{'fired':>8}{'fraud':>7}{'legit':>8}{'clust':>7}"
            f"{'prec':>8}{'lower':>8}{'upper':>8}{'legit%':>9}{'lift':>8}",
        ]
        for stats in self.signals.values():
            precision = "-" if stats.precision is None else f"{stats.precision:.4f}"
            lift = "-" if stats.lift is None else f"{stats.lift:.1f}"
            lines.append(
                f"  {stats.name:<30}{stats.fired:>8}{stats.fraud:>7}{stats.legit:>8}"
                f"{stats.clusters:>7}{precision:>8}{stats.lower:>8.4f}{stats.upper:>8.4f}"
                f"{stats.legit_share * 100:>8.3f}%{lift:>8}"
            )
        for e in self.enrichment.values():
            fraud = "-" if e.fraud_share is None else f"{e.fraud_share:.4f}"
            legit = "-" if e.legit_share is None else f"{e.legit_share:.4f}"
            ratio = "-" if e.ratio is None else f"{e.ratio:.2f}"
            lines.append(
                f"  R6 {e.name} within {e.channel}: fraud {e.fraud_fired}/{e.fraud_eligible}"
                f" = {fraud} (upper {e.fraud_upper:.4f}, clusters {e.fraud_clusters});"
                f" legit {e.legit_fired}/{e.legit_eligible} = {legit}"
                f" (lower {e.legit_lower:.4f}, clusters {e.legit_clusters}); ratio {ratio}"
            )
        for failure in self.failures():
            lines.append(f"  FAIL {failure.rule} {failure.signal}: {failure.detail}")
        return "\n".join(lines)


def wilson_interval(
    successes: int, rows: int, clusters: int, z: float = Z_ONE_SIDED_95
) -> tuple[float, float]:
    """One-sided Wilson score bounds on `successes / rows`, with n = `clusters`.

    The point estimate uses every row; the width uses the number of independent
    clusters, so one takeover's several transactions count as one observation.
    With no rows or no clusters there is no evidence at all: `(0.0, 1.0)`.
    """
    if rows <= 0 or clusters <= 0:
        return 0.0, 1.0
    p = successes / rows
    n = float(clusters)
    z2 = z * z
    denominator = 1.0 + z2 / n
    centre = (p + z2 / (2.0 * n)) / denominator
    half = z * math.sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n)) / denominator
    return max(0.0, centre - half), min(1.0, centre + half)


def home_devices_by_account(universe: Universe) -> dict[str, frozenset[str]]:
    """account_id -> the account's home devices, from the generator's population."""
    return {profile.account_id: frozenset(profile.home_devices) for profile in universe.profiles}


@dataclass(frozen=True, slots=True)
class PopulationView:
    """The population facts the transaction criteria read. Never a label."""

    home_devices: Mapping[str, Collection[str]]
    home_points: Mapping[str, tuple[float, float]]
    """account_id -> the account's home (latitude, longitude), exactly."""


def population_view(universe: Universe) -> PopulationView:
    return PopulationView(
        home_devices=home_devices_by_account(universe),
        home_points={
            profile.account_id: (profile.account.home.latitude, profile.account.home.longitude)
            for profile in universe.profiles
        },
    )


@dataclass(frozen=True, slots=True)
class _Tx:
    occurred_ms: int
    account_id: str
    device_id: str
    ip_id: str
    is_fraud: bool
    cluster: str
    outcome: str | None
    latitude: float | None
    longitude: float | None
    channel: str | None
    entry_mode: str | None
    transaction_id: str = ""


def _millis(timestamp: str) -> int:
    return to_millis(dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00")))


def _fired_in(times: list[int], start_ms: int, end_ms: int) -> bool:
    """Whether any time lies in `[start_ms, end_ms)`. `times` is sorted."""
    index = bisect.bisect_left(times, start_ms)
    return index < len(times) and times[index] < end_ms


def _has(
    account_times: Mapping[str, list[int]] | None, category: str, at_ms: int, window_ms: int
) -> bool:
    """Whether the account has an event of `category` in `[at_ms - window_ms, at_ms)`."""
    if account_times is None:
        return False
    return _fired_in(account_times.get(category, _EMPTY), at_ms - window_ms, at_ms)


def _declined_before(series: list[tuple[int, str]] | None, own: str, at_ms: int) -> bool:
    """Whether another transaction's DECLINED outcome was decided in `[at_ms - 1 h, at_ms)`."""
    if not series:
        return False
    index = bisect.bisect_left(series, (at_ms - HOUR_MS, ""))
    while index < len(series) and series[index][0] < at_ms:
        if series[index][1] != own:
            return True
        index += 1
    return False


def _stuffing_burst(logins: list[tuple[int, str]] | None, at_ms: int) -> bool:
    if not logins:
        return False
    low = bisect.bisect_left(logins, (at_ms - HOUR_MS, ""))
    high = bisect.bisect_left(logins, (at_ms, ""))
    accounts: set[str] = set()
    for _, account in logins[low:high]:
        accounts.add(account)
        if len(accounts) >= STUFFING_MIN_DISTINCT_ACCOUNTS:
            return True
    return False


@dataclass(frozen=True, slots=True)
class _Tally:
    signals: dict[str, SignalStats]
    transactions: int
    fraudulent: int
    base_rate: float
    enrichment: dict[str, EnrichmentStats]


def _tally(
    rows: Iterable[GeneratedRow],
    home_devices: Mapping[str, Collection[str]] | None,
    home_points: Mapping[str, tuple[float, float]] | None = None,
    outcomes: Mapping[str, tuple[str, int, str]] | None = None,
) -> _Tally:
    """One pass over the rows: `LPC-1` signals always, `LPC-2` transaction signals
    when `home_devices` is given, `LPC-3` artefact signals when `home_points` is."""
    with_transactions = home_devices is not None
    with_artefacts = home_points is not None
    if with_artefacts and not with_transactions:  # pragma: no cover - API misuse
        raise ValueError("LPC-3 signals need the LPC-2 population as well")
    transactions: list[_Tx] = []
    times: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    ip_logins: dict[str, list[tuple[int, str]]] = defaultdict(list)
    first_reference: dict[tuple[str, str], int] = {}
    first_payment: dict[tuple[str, str], int] = {}
    first_coordinates: dict[tuple[str, float, float], int] = {}
    declined_times: dict[str, list[int]] = defaultdict(list)

    def reference(account: str, device: str | None, at_ms: int) -> None:
        if not device:
            return
        key = (account, device)
        if at_ms < first_reference.get(key, at_ms + 1):
            first_reference[key] = at_ms

    for row in rows:
        payload = row.event["payload"]
        at_ms = _millis(row.event["envelope"]["occurred_at"])
        if row.topic == TX_TOPIC:
            label = row.label
            if label is None:
                raise ValueError(
                    f"transaction {payload.get('transaction_id')} has no label; the criterion "
                    f"cannot be computed over a partially labelled dataset"
                )
            account = payload["account_id"]
            cluster = (
                f"scenario:{label.scenario_instance_id}" if label.is_fraud else f"account:{account}"
            )
            if outcomes is not None:
                decided = outcomes.get(str(payload.get("transaction_id", "")))
                outcome = decided[0] if decided is not None else None
            else:
                outcome = payload.get("authorization_outcome")
            if with_transactions and outcome is None and outcomes is None:
                raise ValueError(
                    f"transaction {payload.get('transaction_id')} has no authorization_outcome; "
                    f"LPC-2 cannot judge declines without it"
                )
            latitude = payload.get("latitude")
            longitude = payload.get("longitude")
            channel = payload.get("channel")
            entry_mode = payload.get("entry_mode")
            if with_artefacts and None in (latitude, longitude, channel, entry_mode):
                raise ValueError(
                    f"transaction {payload.get('transaction_id')} lacks a location, channel or "
                    f"entry mode; LPC-3 cannot judge the planted-row markers without them"
                )
            device = payload["device_id"]
            transactions.append(
                _Tx(
                    at_ms,
                    account,
                    device,
                    payload["ip_id"],
                    label.is_fraud,
                    cluster,
                    outcome,
                    latitude,
                    longitude,
                    channel,
                    entry_mode,
                    transaction_id=str(payload.get("transaction_id", "")),
                )
            )
            reference(account, device, at_ms)
            if at_ms < first_payment.get((account, device), at_ms + 1):
                first_payment[(account, device)] = at_ms
            if outcomes is None and outcome == "DECLINED":
                declined_times[account].append(at_ms)
            if with_artefacts and latitude is not None and longitude is not None:
                place = (account, latitude, longitude)
                if at_ms < first_coordinates.get(place, at_ms + 1):
                    first_coordinates[place] = at_ms
        elif row.topic == IDENTITY_TOPIC:
            account = payload["account_id"]
            kind = payload["identity_event_type"]
            times[account]["identity"].append(at_ms)
            if kind == "LOGIN_FAILED":
                times[account]["login_failed"].append(at_ms)
            if kind in IDENTITY_CHANGE_TYPES:
                times[account]["identity_change"].append(at_ms)
            if kind in LOGIN_TYPES and payload.get("ip_id"):
                ip_logins[payload["ip_id"]].append((at_ms, account))
            reference(account, payload.get("device_id"), at_ms)
        elif row.topic == DEVICE_TOPIC:
            account = payload["account_id"]
            times[account]["device"].append(at_ms)
            if payload["device_event_type"] == "FIRST_SEEN":
                times[account]["first_seen"].append(at_ms)
            reference(account, payload["device_id"], at_ms)

    for per_account in times.values():
        for series in per_account.values():
            series.sort()
    for series_by_ip in ip_logins.values():
        series_by_ip.sort()
    for series_declined in declined_times.values():
        series_declined.sort()
    # LPC-5 §5.2 (U7): with outcome rows given, the decline signals read those rows. The account
    # is the outcome row's own, and the time is when the outcome was decided.
    declined_outcomes: dict[str, list[tuple[int, str]]] = defaultdict(list)
    if outcomes is not None:
        for transaction_id, (value, decided_ms, outcome_account) in outcomes.items():
            if value == "DECLINED":
                declined_outcomes[outcome_account].append((decided_ms, transaction_id))
        for series_outcomes in declined_outcomes.values():
            series_outcomes.sort()

    if with_artefacts:
        names = LPC3_SIGNALS
    elif with_transactions:
        names = LPC2_SIGNALS
    else:
        names = LPC1_SIGNALS
    fraud_counts = dict.fromkeys(names, 0)
    legit_counts = dict.fromkeys(names, 0)
    cluster_sets: dict[str, set[str]] = {name: set() for name in names}
    eligible_counts: dict[tuple[str, bool], int] = defaultdict(int)
    eligible_fired: dict[tuple[str, bool], int] = defaultdict(int)
    eligible_clusters: dict[tuple[str, bool], set[str]] = defaultdict(set)

    for tx in transactions:
        t = tx.occurred_ms
        account_times = times.get(tx.account_id)
        fired = {
            "ANY_IDENTITY_24H": _has(account_times, "identity", t, DAY_MS),
            "LOGIN_FAILED_1H": _has(account_times, "login_failed", t, HOUR_MS),
            "IDENTITY_CHANGE_24H": _has(account_times, "identity_change", t, DAY_MS),
            "DEVICE_FIRST_SEEN_24H": _has(account_times, "first_seen", t, DAY_MS),
            "ANY_DEVICE_EVENT_24H": _has(account_times, "device", t, DAY_MS),
        }
        fired["ANY_IDENTITY_OR_DEVICE_24H"] = (
            fired["ANY_IDENTITY_24H"] or fired["ANY_DEVICE_EVENT_24H"]
        )
        fired["STUFFING_BURST_1H"] = _stuffing_burst(ip_logins.get(tx.ip_id), t)
        # The transaction itself references its device, so the first reference
        # is never later than t: this reads no future information.
        fired["CHANGE_THEN_NEW_DEVICE_24H"] = (
            fired["IDENTITY_CHANGE_24H"]
            and first_reference[(tx.account_id, tx.device_id)] >= t - DAY_MS
        )

        if home_devices is not None:
            home = home_devices.get(tx.account_id)
            if home is None:
                raise ValueError(
                    f"account {tx.account_id} has no home devices in the population given; "
                    f"every device would read as non-home"
                )
            non_home = tx.device_id not in home
            fired["TX_NON_HOME_DEVICE"] = non_home
            # The transaction itself is a payment on its device, so the first
            # payment is never later than t: no future information.
            fired["TX_DEVICE_FIRST_USED_24H"] = (
                non_home and first_payment[(tx.account_id, tx.device_id)] >= t - DAY_MS
            )
            fired["TX_WHOLE_SECOND"] = t % 1000 == 0
            fired["TX_DECLINED"] = tx.outcome == "DECLINED"
            if outcomes is None:
                fired["TX_DECLINED_PRIOR_1H"] = _fired_in(
                    declined_times.get(tx.account_id, _EMPTY), t - HOUR_MS, t
                )
            else:
                fired["TX_DECLINED_PRIOR_1H"] = _declined_before(
                    declined_outcomes.get(tx.account_id), tx.transaction_id, t
                )

        if home_points is not None and tx.latitude is not None and tx.longitude is not None:
            home_point = home_points.get(tx.account_id)
            if home_point is None:
                raise ValueError(
                    f"account {tx.account_id} has no home point in the population given"
                )
            # Strictly earlier: a transaction is not its own precedent, and two at
            # the same millisecond are not earlier than each other.
            first_here = first_coordinates[(tx.account_id, tx.latitude, tx.longitude)]
            fired["TX_REPEATED_EXACT_COORDINATES"] = first_here < t
            fired["TX_EXACT_HOME_POINT"] = (tx.latitude, tx.longitude) == home_point
            fired["TX_CNP_ECOMMERCE"] = (
                tx.channel == "CARD_NOT_PRESENT" and tx.entry_mode == "ECOMMERCE"
            )
            for name, channel in ENRICHMENT_SIGNALS.items():
                if tx.channel != channel:
                    continue
                key = (name, tx.is_fraud)
                eligible_counts[key] += 1
                eligible_clusters[key].add(tx.cluster)
                if fired[name]:
                    eligible_fired[key] += 1

        for name, is_set in fired.items():
            if not is_set:
                continue
            if tx.is_fraud:
                fraud_counts[name] += 1
            else:
                legit_counts[name] += 1
            cluster_sets[name].add(tx.cluster)

    total = len(transactions)
    fraudulent = sum(1 for tx in transactions if tx.is_fraud)
    legitimate = total - fraudulent
    base_rate = fraudulent / total if total else 0.0

    signals: dict[str, SignalStats] = {}
    for name in names:
        fraud = fraud_counts[name]
        legit = legit_counts[name]
        fired_count = fraud + legit
        clusters = len(cluster_sets[name])
        lower, upper = wilson_interval(fraud, fired_count, clusters)
        precision = fraud / fired_count if fired_count else None
        signals[name] = SignalStats(
            name=name,
            fired=fired_count,
            fraud=fraud,
            legit=legit,
            clusters=clusters,
            precision=precision,
            legit_share=legit / legitimate if legitimate else 0.0,
            fraud_share=fraud / fraudulent if fraudulent else 0.0,
            lift=(precision / base_rate) if precision is not None and base_rate else None,
            lower=lower,
            upper=upper,
        )

    enrichment: dict[str, EnrichmentStats] = {}
    if with_artefacts:
        for name, channel in ENRICHMENT_SIGNALS.items():
            f_key, l_key = (name, True), (name, False)
            _, fraud_upper = wilson_interval(
                eligible_fired[f_key], eligible_counts[f_key], len(eligible_clusters[f_key])
            )
            legit_lower, _ = wilson_interval(
                eligible_fired[l_key], eligible_counts[l_key], len(eligible_clusters[l_key])
            )
            fraud_share = (
                eligible_fired[f_key] / eligible_counts[f_key] if eligible_counts[f_key] else None
            )
            legit_share = (
                eligible_fired[l_key] / eligible_counts[l_key] if eligible_counts[l_key] else None
            )
            enrichment[name] = EnrichmentStats(
                name=name,
                channel=channel,
                fraud_eligible=eligible_counts[f_key],
                fraud_fired=eligible_fired[f_key],
                fraud_clusters=len(eligible_clusters[f_key]),
                fraud_share=fraud_share,
                fraud_upper=fraud_upper,
                legit_eligible=eligible_counts[l_key],
                legit_fired=eligible_fired[l_key],
                legit_clusters=len(eligible_clusters[l_key]),
                legit_share=legit_share,
                legit_lower=legit_lower,
                ratio=(
                    fraud_share / legit_share if fraud_share is not None and legit_share else None
                ),
            )
    return _Tally(signals, total, fraudulent, base_rate, enrichment)


def evaluate(rows: Iterable[GeneratedRow]) -> LabelProxyReport:
    """Compute `LPC-1` over a generated dataset, from in-memory labels."""
    tally = _tally(rows, None)
    return _lpc1_report(tally)


def evaluate_lpc2(
    rows: Iterable[GeneratedRow], home_devices: Mapping[str, Collection[str]]
) -> LabelProxyReport:
    """Compute `LPC-2` over a generated dataset."""
    return evaluate_both(rows, home_devices)[1]


def evaluate_both(
    rows: Iterable[GeneratedRow], home_devices: Mapping[str, Collection[str]]
) -> tuple[LabelProxyReport, LabelProxyReport]:
    """`LPC-1` and `LPC-2` from one pass over the rows.

    The `LPC-1` report is identical to `evaluate(rows)`: same signals, same rules.
    """
    tally = _tally(rows, home_devices)
    return _lpc1_report(tally), _lpc2_report(tally)


def evaluate_all(
    rows: Iterable[GeneratedRow],
    population: PopulationView,
    *,
    outcomes: Mapping[str, tuple[str, int, str]] | None = None,
) -> tuple[LabelProxyReport, LabelProxyReport, LabelProxyReport]:
    """`LPC-1`, `LPC-2` and `LPC-3` from one pass over the rows.

    The `LPC-1` and `LPC-2` reports are identical to `evaluate_both`'s.

    `outcomes` maps a transaction id to (authorization outcome, decided ms, the outcome row's
    account). With it, the two decline signals read the outcome stream, as `LPC-5` §5.2 declares
    for U7. Without it they read the transaction field, as LPC-2 was declared.
    """
    tally = _tally(rows, population.home_devices, population.home_points, outcomes)
    lpc2 = _lpc2_report(tally)
    lpc3 = LabelProxyReport(
        criterion=LPC3_ID,
        transactions=tally.transactions,
        fraudulent=tally.fraudulent,
        base_rate=tally.base_rate,
        signals=MappingProxyType({name: tally.signals[name] for name in LPC3_SIGNALS}),
        rules=lpc2.rules + _artefact_rules(tally.signals, tally.enrichment),
        enrichment=MappingProxyType(dict(tally.enrichment)),
    )
    return _lpc1_report(tally), lpc2, lpc3


def _lpc1_report(tally: _Tally) -> LabelProxyReport:
    return LabelProxyReport(
        criterion=CRITERION_ID,
        transactions=tally.transactions,
        fraudulent=tally.fraudulent,
        base_rate=tally.base_rate,
        signals=MappingProxyType({name: tally.signals[name] for name in LPC1_SIGNALS}),
        rules=_rules(tally.signals, tally.base_rate),
    )


def _lpc2_report(tally: _Tally) -> LabelProxyReport:
    return LabelProxyReport(
        criterion=LPC2_ID,
        transactions=tally.transactions,
        fraudulent=tally.fraudulent,
        base_rate=tally.base_rate,
        signals=MappingProxyType({name: tally.signals[name] for name in LPC2_SIGNALS}),
        rules=_rules(tally.signals, tally.base_rate) + _transaction_rules(tally.signals),
    )


def _presence_rules(stats: SignalStats, floor: float, results: list[RuleResult], name: str) -> None:
    results.append(
        RuleResult(
            "R1",
            name,
            stats.legit >= MIN_LEGIT_FIRINGS,
            f"legitimate firings {stats.legit}, need >= {MIN_LEGIT_FIRINGS}",
        )
    )
    results.append(
        RuleResult(
            "R2",
            name,
            stats.legit_share >= floor,
            f"legitimate share {stats.legit_share:.6f}, need >= {floor}",
        )
    )
    results.append(
        RuleResult(
            "R3",
            name,
            stats.upper <= MAX_PRECISION_UPPER_BOUND,
            f"precision upper bound {stats.upper:.4f}, need <= {MAX_PRECISION_UPPER_BOUND}",
        )
    )


def _absence_rules(stats: SignalStats, floor: float, results: list[RuleResult], name: str) -> None:
    """R1'-R3': R1-R3, each also satisfied when no fraudulent transaction fires."""
    absent = stats.fraud == 0
    alternative = f"; or no fraudulent firing (fraudulent firings {stats.fraud})"
    results.append(
        RuleResult(
            "R1'",
            name,
            stats.legit >= MIN_LEGIT_FIRINGS or absent,
            f"legitimate firings {stats.legit}, need >= {MIN_LEGIT_FIRINGS}{alternative}",
        )
    )
    results.append(
        RuleResult(
            "R2'",
            name,
            stats.legit_share >= floor or absent,
            f"legitimate share {stats.legit_share:.6f}, need >= {floor}{alternative}",
        )
    )
    results.append(
        RuleResult(
            "R3'",
            name,
            stats.upper <= MAX_PRECISION_UPPER_BOUND or absent,
            f"precision upper bound {stats.upper:.4f}, need <= "
            f"{MAX_PRECISION_UPPER_BOUND}{alternative}",
        )
    )


def _rules(signals: Mapping[str, SignalStats], base_rate: float) -> tuple[RuleResult, ...]:
    """Every `LPC-1` rule."""
    results: list[RuleResult] = []
    for name in PRESENCE_SIGNALS:
        _presence_rules(signals[name], LEGIT_FLOORS[name], results, name)
    for name, base in PATTERN_BASE.items():
        stats = signals[name]
        presence = signals[base]
        results.append(
            RuleResult(
                "R4a",
                name,
                stats.fraud >= PATTERN_MIN_FRAUD_FIRINGS,
                f"fraudulent firings {stats.fraud}, need >= {PATTERN_MIN_FRAUD_FIRINGS}",
            )
        )
        results.append(
            RuleResult(
                "R4b",
                name,
                base_rate > 0 and stats.lower >= PATTERN_MIN_LIFT_LOWER_BOUND * base_rate,
                f"precision lower bound {stats.lower:.4f}, need >= "
                f"{PATTERN_MIN_LIFT_LOWER_BOUND} x base rate {base_rate:.5f}",
            )
        )
        results.append(
            RuleResult(
                "R4c",
                name,
                stats.lower >= PATTERN_OVER_PRESENCE * presence.upper,
                f"precision lower bound {stats.lower:.4f}, need >= {PATTERN_OVER_PRESENCE} x "
                f"{base} upper bound {presence.upper:.4f}",
            )
        )
    return tuple(results)


def _transaction_rules(signals: Mapping[str, SignalStats]) -> tuple[RuleResult, ...]:
    """The `LPC-2` additions: R1-R3 analogues for each transaction-row signal."""
    results: list[RuleResult] = []
    for name in TX_SIGNALS:
        _presence_rules(signals[name], TX_LEGIT_FLOORS[name], results, name)
    return tuple(results)


def _artefact_rules(
    signals: Mapping[str, SignalStats], enrichment: Mapping[str, EnrichmentStats]
) -> tuple[RuleResult, ...]:
    """The `LPC-3` additions: R1-R3 (or R1'-R3') per signal, and R6 where declared."""
    results: list[RuleResult] = []
    for name in ARTEFACT_SIGNALS:
        stats = signals[name]
        floor = ARTEFACT_LEGIT_FLOORS[name]
        if name in ABSENCE_ALTERNATIVE_SIGNALS:
            _absence_rules(stats, floor, results, name)
        else:
            _presence_rules(stats, floor, results, name)
        if name in ENRICHMENT_SIGNALS:
            e = enrichment[name]
            results.append(
                RuleResult(
                    "R6",
                    name,
                    e.fraud_upper <= ENRICHMENT_MAX_RATIO * e.legit_lower,
                    f"within {e.channel}: fraudulent share upper bound {e.fraud_upper:.4f}, need "
                    f"<= {ENRICHMENT_MAX_RATIO} x legitimate share lower bound "
                    f"{e.legit_lower:.4f}",
                )
            )
    return tuple(results)
