"""`LPC-5`'s exact and per-instance checks, and the frame-derived inputs of S3 and S4.

**Row-by-row checks.** These hold for every row or every instance, with no statistics, so one
failing row fails the check:
- §8 S2c;
- §11 S5a and G3;
- §12 S6a;
- §13 S7a.

Each finding names its rule, the number of violations and the first example.

**Also here.** S5b's amount-shape comparison sits here, beside the G1 mechanism it compares against.
G1 is recomputed from the declaration, independently of the generator's implementation, so a
mismatch between the two cannot hide.
"""

from __future__ import annotations

import math
import re
from bisect import bisect_right
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from itertools import pairwise

from data.generator.behavior import sample_amount_minor
from data.generator.lpc5 import declaration as d
from data.generator.lpc5 import stats
from data.generator.lpc5.attributes import Table
from data.generator.lpc5.frame import Frame, Knowledge, OutRow, SideRow, TxRow
from data.generator.lpc5.judge import CheckResult, Finding, Pair
from data.generator.outcomes import decided_ms
from data.generator.population import AccountProfile
from data.generator.rng import derive
from data.generator.scenarios import SCENARIOS_BY_PATTERN
from trace_core.domain.enums import FraudPattern
from trace_core.domain.geo import GeoPoint, haversine_km, implied_speed_kmh

FP = FraudPattern
PAYOFF_GROUP = f"{FP.CARD_TESTING.value}:payoff"
_TIMESTAMP = re.compile(d.TIMESTAMP_PATTERN)


class Violations:
    """Per rule: how many rows violated it, and the first."""

    def __init__(self) -> None:
        self.count: Counter[str] = Counter()
        self.first: dict[str, str] = {}

    def add(self, rule: str, example: str) -> None:
        self.count[rule] += 1
        self.first.setdefault(rule, example)

    def findings(self, population: d.Population | None = None) -> tuple[Finding, ...]:
        return tuple(
            Finding(
                rule,
                population,
                detail=f"{self.count[rule]} violation(s); first: {self.first[rule]}",
            )
            for rule in sorted(self.count)
        )


def _all_rows(frame: Frame) -> Iterable[TxRow | SideRow | OutRow]:
    yield from frame.tx
    yield from frame.ident
    yield from frame.dev
    yield from frame.out


def _stamp(row: TxRow | SideRow | OutRow) -> tuple[int, int]:
    return (row.t, row.order)


# ---------------------------------------------------------------------------- S2c ------------
def s2c_rows(frame: Frame, know: Knowledge) -> CheckResult:
    """§8 S2c rules 1-8. Rules 1 and 2 were recorded at ingest when validation ran."""
    v = Violations()
    if not frame.validated:
        v.add("S2c/1", "schema validation was not run")
        v.add("S2c/2", "the ground-truth token scan was not run")
    for rule, topic, order, detail in frame.violations:
        v.add(rule, f"{topic} row {order}: {detail}")

    for tx in frame.tx:
        if tx.outcome_field != "UNKNOWN":
            v.add("S2c/3", f"{tx.transaction_id}: authorization_outcome {tx.outcome_field!r}")

    tx_by_id: dict[str, TxRow] = {}
    for tx in frame.tx:
        tx_by_id.setdefault(tx.transaction_id, tx)
    outs: dict[str, list[OutRow]] = defaultdict(list)
    for out in frame.out:
        outs[out.transaction_id].append(out)
    for tx in frame.tx:
        count = len(outs.get(tx.transaction_id, ()))
        if count != 1:
            v.add("S2c/4", f"{tx.transaction_id}: {count} outcome rows")
    for out in frame.out:
        linked = tx_by_id.get(out.transaction_id)
        if linked is None:
            v.add("S2c/4", f"outcome row for unknown transaction {out.transaction_id}")
            continue
        if out.account != linked.account:
            v.add("S2c/4", f"{out.transaction_id}: outcome account {out.account}")
        if out.transaction_occurred_at != linked.envelope.occurred_at:
            v.add("S2c/4", f"{out.transaction_id}: transaction_occurred_at differs")
        expected = decided_ms(know.seed, linked.transaction_id, linked.t)
        if out.t != expected:
            v.add("S2c/4", f"{out.transaction_id}: decided at {out.t}, DM-1 gives {expected}")
        if out.order <= linked.order:
            v.add("S2c/4", f"{out.transaction_id}: emitted before its transaction")

    correlation: Counter[str] = Counter()
    trace: Counter[str] = Counter()
    event_ids: Counter[str] = Counter()
    keys: Counter[str] = Counter()
    for row in _all_rows(frame):
        envelope = row.envelope
        for counter, value in (
            (correlation, envelope.correlation_id),
            (trace, envelope.trace_id),
            (event_ids, envelope.event_id),
            (keys, envelope.idempotency_key),
        ):
            if value is not None:
                counter[value] += 1
    for tx in frame.tx:
        linked_outs = outs.get(tx.transaction_id, [])
        members = 2 if linked_outs else 1
        for name, value, counter, partners in (
            (
                "correlation_id",
                tx.envelope.correlation_id,
                correlation,
                [o.envelope.correlation_id for o in linked_outs],
            ),
            ("trace_id", tx.envelope.trace_id, trace, [o.envelope.trace_id for o in linked_outs]),
        ):
            if value is None or counter[value] != members or any(p != value for p in partners):
                v.add("S2c/5", f"{tx.transaction_id}: {name} is not exactly its business flow's")
    for side in (*frame.ident, *frame.dev):
        for name, value, counter in (
            ("correlation_id", side.envelope.correlation_id, correlation),
            ("trace_id", side.envelope.trace_id, trace),
        ):
            if value is None or counter[value] != 1:
                v.add("S2c/5", f"{side.event_type} row {side.order}: {name} is shared")

    for row in _all_rows(frame):
        envelope = row.envelope
        if envelope.event_id is None or event_ids[envelope.event_id] != 1:
            v.add("S2c/6", f"row {row.order}: event_id {envelope.event_id!r} is not unique")
        if envelope.idempotency_key is None or keys[envelope.idempotency_key] != 1:
            v.add("S2c/6", f"row {row.order}: idempotency_key is not unique")
        ingested = envelope.ingested_at
        if (
            _TIMESTAMP.fullmatch(envelope.occurred_at) is None
            or ingested is None
            or _TIMESTAMP.fullmatch(ingested) is None
        ):
            v.add("S2c/7", f"row {row.order}: {envelope.occurred_at!r} / {ingested!r}")
    windowed_rows: list[TxRow | SideRow] = [*frame.tx, *frame.ident, *frame.dev]
    for windowed in windowed_rows:
        if not know.start_ms <= windowed.t < know.end_ms:
            v.add("S2c/8", f"row {windowed.order} at {windowed.t} is outside the window")
    judged = len(frame.tx) + len(frame.ident) + len(frame.dev) + len(frame.out)
    return CheckResult("S2c", judged, v.findings())


# ---------------------------------------------------------------------------- S3, S4 inputs --
def s3_inputs(frame: Frame) -> tuple[dict[str, list[int]], list[int]]:
    """Per scenario, each instance's start (its least TX time); and every legitimate TX time."""
    first: dict[str, tuple[str, int]] = {}
    for tx in frame.tx:
        if tx.instance is None:
            continue
        current = first.get(tx.instance)
        if current is None or tx.t < current[1]:
            first[tx.instance] = (tx.group, tx.t)
    starts: dict[str, list[int]] = defaultdict(list)
    for group, t in first.values():
        starts[group].append(t)
    return dict(starts), [tx.t for tx in frame.tx if tx.group == d.LEGIT]


def _pair(
    first: TxRow | SideRow, second: TxRow | SideRow, kinds: dict[str, list[Pair]], kind: str
) -> None:
    offset = second.t - first.t
    if offset < 0 or offset > d.S4_MAX_OFFSET_MS:
        return
    if first.group == d.LEGIT and second.group == d.LEGIT:
        kinds[kind].append(Pair(d.LEGIT, first.account, offset))
    elif (
        first.group != d.LEGIT and first.instance is not None and first.instance == second.instance
    ):
        kinds[kind].append(Pair(first.group, first.instance, offset))


def _next_after(
    sources: Iterable[TxRow | SideRow],
    targets: list[TxRow],
    kinds: dict[str, list[Pair]],
    kind: str,
) -> None:
    """Pair each source with the first target strictly after it in (t, order)."""
    stamps = [_stamp(t) for t in targets]
    for source in sources:
        index = bisect_right(stamps, _stamp(source))
        if index < len(targets):
            _pair(source, targets[index], kinds, kind)


def s4_pairs(frame: Frame) -> dict[str, list[Pair]]:
    """Every pair of the eight kinds of §10, planted (one instance) or legitimate (both rows)."""
    kinds: dict[str, list[Pair]] = {kind: [] for kind in d.PAIR_KINDS}
    tx_by_account: dict[str, list[TxRow]] = defaultdict(list)
    tx_by_device: dict[str, list[TxRow]] = defaultdict(list)
    tx_by_merchant: dict[str, list[TxRow]] = defaultdict(list)
    tx_by_account_device: dict[tuple[str, str], list[TxRow]] = defaultdict(list)
    for tx in frame.tx:
        tx_by_account[tx.account].append(tx)
        if tx.device is not None:
            tx_by_device[tx.device].append(tx)
            tx_by_account_device[(tx.account, tx.device)].append(tx)
        if tx.merchant is not None:
            tx_by_merchant[tx.merchant].append(tx)
    for bucket in (tx_by_account, tx_by_device, tx_by_merchant, tx_by_account_device):
        for series in bucket.values():
            series.sort(key=_stamp)

    for series in tx_by_account.values():
        for first, second in pairwise(series):
            _pair(first, second, kinds, "K1")
    for series in tx_by_merchant.values():
        for first, second in pairwise(series):
            _pair(first, second, kinds, "K8")
    for series in tx_by_device.values():
        # The next row on the device paid by a different account, found in one backward pass.
        count = len(series)
        next_other = [count] * count
        for index in range(count - 2, -1, -1):
            if series[index + 1].account != series[index].account:
                next_other[index] = index + 1
            else:
                next_other[index] = next_other[index + 1]
        for index, first in enumerate(series):
            if next_other[index] < count:
                _pair(first, series[next_other[index]], kinds, "K5")

    changes: dict[str, list[SideRow]] = defaultdict(list)
    identities: dict[str, list[SideRow]] = defaultdict(list)
    logins: dict[str, list[SideRow]] = defaultdict(list)
    for event in frame.ident:
        identities[event.account].append(event)
        if event.event_type in d.IDENTITY_CHANGE_TYPES:
            changes[event.account].append(event)
        if event.event_type in d.LOGIN_TYPES and event.ip is not None:
            logins[event.ip].append(event)
    first_seen: dict[str, list[SideRow]] = defaultdict(list)
    device_events: dict[tuple[str, str], list[SideRow]] = defaultdict(list)
    for event in frame.dev:
        if event.event_type == "FIRST_SEEN":
            first_seen[event.account].append(event)
        if event.device is not None:
            device_events[(event.account, event.device)].append(event)
    for bucket_side in (changes, identities, logins, first_seen):
        for side_series in bucket_side.values():
            side_series.sort(key=_stamp)
    for account, change_rows in changes.items():
        targets = first_seen.get(account, [])
        stamps = [_stamp(t) for t in targets]
        for change in change_rows:
            index = bisect_right(stamps, _stamp(change))
            if index < len(targets):
                _pair(change, targets[index], kinds, "K2")
    for account, rows in identities.items():
        _next_after(rows, tx_by_account.get(account, []), kinds, "K3")
    for key, rows in device_events.items():
        _next_after(sorted(rows, key=_stamp), tx_by_account_device.get(key, []), kinds, "K4")
    for series_logins in logins.values():
        for earlier, later in pairwise(series_logins):
            _pair(earlier, later, kinds, "K6")

    tx_by_id = {tx.transaction_id: tx for tx in frame.tx}
    for out in frame.out:
        paid = tx_by_id.get(out.transaction_id)
        if paid is None:
            continue
        offset = out.t - paid.t
        if offset < 0 or offset > d.S4_MAX_OFFSET_MS:
            continue
        if paid.group == d.LEGIT:
            kinds["K7"].append(Pair(d.LEGIT, paid.account, offset))
        elif paid.instance is not None:
            kinds["K7"].append(Pair(paid.group, paid.instance, offset))
    return kinds


# ---------------------------------------------------------------------------- S5 -------------
def in_region(amount: int, typical: float, rule: d.AmountRule) -> bool:
    if rule.lower_multiple is not None:
        bound = rule.lower_multiple * typical
        if amount < bound or (rule.lower_exclusive and amount <= bound):
            return False
    return rule.upper_multiple is None or amount < rule.upper_multiple * typical


def g1_amount(
    seed: int,
    instance_id: str,
    ordinal: int,
    pattern: FraudPattern,
    profile: AccountProfile,
    *,
    payoff: bool = False,
) -> int | None:
    """G1 (§11): the amount the declaration gives a planted transaction. None when a region's
    draws are exhausted, which fails the generation."""
    rng = derive(seed, d.G1_AMOUNT_NAMESPACE, f"{instance_id}:{ordinal}")
    typical = math.exp(profile.amount_mu)
    rule = d.G1_CARD_TESTING_PAYOFF if payoff else d.G1_RULES[pattern]
    mechanism = rule.mechanism
    if mechanism is d.AmountMechanism.ORDINARY:
        return sample_amount_minor(rng, profile)
    if mechanism is d.AmountMechanism.ORDINARY_REGION:
        for _ in range(d.G1_MAX_DRAWS):
            amount = sample_amount_minor(rng, profile)
            if in_region(amount, typical, rule):
                return amount
        return None
    if mechanism is d.AmountMechanism.PROBE:
        return rng.randrange(*d.G1_PROBE_RANGE)
    if mechanism is d.AmountMechanism.HIGH_VALUE:
        return int(typical * rng.uniform(*d.G1_HIGH_VALUE_MULTIPLE)) + 1
    price_rng = derive(seed, d.G1_AMOUNT_NAMESPACE, f"{instance_id}:price")
    price = int(math.exp(price_rng.uniform(*d.G6_PRICE_LOG_RANGE)))
    return int(price * rng.uniform(*d.G6_PRICE_MULTIPLIER))


_ORDINARY_MECHANISMS = (d.AmountMechanism.ORDINARY, d.AmountMechanism.ORDINARY_REGION)


def s5_amounts(
    frame: Frame, tables: Mapping[d.Population, Table], know: Knowledge
) -> tuple[CheckResult, CheckResult]:
    """S5a: every planted amount is G1's. S5b: ordinary amounts keep the legitimate shape."""
    v = Violations()
    payoff_ordinal: dict[str, int] = {}
    for tx in frame.tx:
        if tx.group == FP.CARD_TESTING.value and tx.instance is not None and tx.ordinal is not None:
            payoff_ordinal[tx.instance] = max(payoff_ordinal.get(tx.instance, -1), tx.ordinal)
    groups: dict[str, list[int]] = defaultdict(list)
    judged_a = 0
    for index, tx in enumerate(frame.tx):
        if tx.group == d.LEGIT:
            continue
        judged_a += 1
        pattern = FP(tx.group)
        payoff = (
            pattern is FP.CARD_TESTING
            and tx.instance is not None
            and tx.ordinal is not None
            and tx.ordinal == payoff_ordinal.get(tx.instance)
        )
        rule = d.G1_CARD_TESTING_PAYOFF if payoff else d.G1_RULES[pattern]
        if rule.mechanism in _ORDINARY_MECHANISMS:
            groups[PAYOFF_GROUP if payoff else tx.group].append(index)
        profile = know.profiles.get(tx.account)
        if tx.instance is None or tx.ordinal is None:
            v.add("S5a", f"{tx.transaction_id} ({tx.group}): no planned ordinal")
            continue
        if profile is None:
            v.add("S5a", f"{tx.transaction_id}: no profile for {tx.account}")
            continue
        expected = g1_amount(know.seed, tx.instance, tx.ordinal, pattern, profile, payoff=payoff)
        if expected != tx.amount:
            v.add(
                "S5a", f"{tx.transaction_id} ({tx.group}): amount {tx.amount}, G1 gives {expected}"
            )
    s5a = CheckResult("S5a", judged_a, v.findings(d.Population.TX))

    amount_z = tables[d.Population.TX].column("amount_z")
    legit = [i for i, tx in enumerate(frame.tx) if tx.group == d.LEGIT]
    typical = {account: math.exp(mu) for account, mu in know.amount_mu.items()}
    expected_groups = [
        pattern.value for pattern in FP if d.G1_RULES[pattern].mechanism in _ORDINARY_MECHANISMS
    ] + [PAYOFF_GROUP]
    findings: list[Finding] = []
    unjudged: list[str] = []
    judged_b = 0
    for group in sorted(expected_groups):
        rows = [i for i in groups.get(group, []) if amount_z.get(i) is not None]
        if not rows:
            unjudged.append(group)
            continue
        rule = d.G1_CARD_TESTING_PAYOFF if group == PAYOFF_GROUP else d.G1_RULES[FP(group)]
        reference = [
            i
            for i in legit
            if amount_z.get(i) is not None
            and in_region(frame.tx[i].amount, typical[frame.tx[i].account], rule)
        ]
        planted_counts = Counter(amount_z.get(i) for i in rows)
        legit_counts = Counter(amount_z.get(i) for i in reference)
        planted_clusters = len({frame.tx[i].cluster for i in rows})
        legit_clusters = len({frame.tx[i].account for i in reference})
        for value in sorted(v_ for v_ in {*planted_counts, *legit_counts} if v_ is not None):
            judged_b += 1
            share = stats.Share(planted_counts[value], len(rows), planted_clusters)
            legit_share = stats.Share(legit_counts[value], len(reference), legit_clusters)
            if stats.differs(share, legit_share, d.S5_TOLERANCE):
                findings.append(
                    Finding(
                        "S5b",
                        d.Population.TX,
                        "amount_z",
                        value,
                        group,
                        None,
                        f"{share.x}/{share.r} (lo {share.lo:.4f}, hi {share.hi:.4f}) vs legitimate "
                        f"{legit_share.x}/{legit_share.r} in the same region "
                        f"(lo {legit_share.lo:.4f}, hi {legit_share.hi:.4f})",
                    )
                )
    return s5a, CheckResult("S5b", judged_b, tuple(findings), tuple(unjudged))


# ---------------------------------------------------------------------------- G3 -------------
def too_fast(distance_km: float, elapsed_ms: int) -> bool:
    """G3's bound: more than 900 km/h. Zero elapsed time is too fast only between two places."""
    if elapsed_ms <= 0:
        return distance_km > 0
    return distance_km / (elapsed_ms / d.HOUR_MS) > d.G3_MAX_SPEED_KMH


def g3_speed(frame: Frame) -> CheckResult:
    v = Violations()
    by_account: dict[str, list[TxRow]] = defaultdict(list)
    for tx in frame.tx:
        by_account[tx.account].append(tx)
    judged = 0
    scenarios = {pattern.value for pattern in d.G3_SCENARIOS}
    for series in by_account.values():
        series.sort(key=_stamp)
        times = [tx.t for tx in series]
        for tx in series:
            if tx.group not in scenarios:
                continue
            judged += 1
            before = bisect_right(times, tx.t - 1) - 1
            after = bisect_right(times, tx.t)
            neighbours = []
            if before >= 0:
                neighbours.append(series[before])
            if after < len(series):
                neighbours.append(series[after])
            for other in neighbours:
                elapsed = abs(tx.t - other.t)
                if elapsed >= d.DAY_MS or None in (
                    tx.latitude,
                    tx.longitude,
                    other.latitude,
                    other.longitude,
                ):
                    continue
                distance = haversine_km(
                    GeoPoint(tx.latitude, tx.longitude),  # type: ignore[arg-type]
                    GeoPoint(other.latitude, other.longitude),  # type: ignore[arg-type]
                )
                if too_fast(distance, elapsed):
                    v.add(
                        "G3",
                        f"{tx.transaction_id} ({tx.group}): {distance:.0f} km in {elapsed} ms "
                        f"from {other.transaction_id}",
                    )
    return CheckResult("G3", judged, v.findings(d.Population.TX))


# ---------------------------------------------------------------------------- S6a ------------
def s6_availability(tables: Mapping[d.Population, Table]) -> CheckResult:
    table = tables[d.Population.TX]
    missing = [f for f in d.RELEASED_FEATURES if f"{d.AVAIL_PREFIX}{f}" not in table.columns]
    if missing:
        return CheckResult(
            "S6a",
            0,
            (
                Finding(
                    "S6a",
                    d.Population.TX,
                    detail=f"availability was not computed for {len(missing)} features",
                ),
            ),
        )
    findings: list[Finding] = []
    for feature in d.RELEASED_FEATURES:
        name = f"{d.AVAIL_PREFIX}{feature}"
        count = sum(1 for value in table.column(name).values() if value == "UNAVAILABLE")
        if count:
            findings.append(
                Finding("S6a", d.Population.TX, name, "UNAVAILABLE", detail=f"{count} rows")
            )
    return CheckResult("S6a", len(d.RELEASED_FEATURES) * table.size, tuple(findings))


# ---------------------------------------------------------------------------- S7a ------------
def _typical(know: Knowledge, account: str) -> float:
    return math.exp(know.amount_mu[account])


def _home_distance(know: Knowledge, tx: TxRow) -> float | None:
    home = know.home_point.get(tx.account)
    if home is None or tx.latitude is None or tx.longitude is None:
        return None
    return haversine_km(GeoPoint(*home), GeoPoint(tx.latitude, tx.longitude))


def s7_instances(frame: Frame, know: Knowledge) -> CheckResult:
    """§13 S7a: every instance's emitted rows keep its scenario's invariants."""
    v = Violations()
    tx_by_instance: dict[str, list[TxRow]] = defaultdict(list)
    for tx in frame.tx:
        if tx.instance is not None:
            tx_by_instance[tx.instance].append(tx)
    side_by_instance: dict[str, list[SideRow]] = defaultdict(list)
    for event in frame.ident:
        if event.instance is not None:
            side_by_instance[event.instance].append(event)
    outcome_by_id = {out.transaction_id: out.authorization_outcome for out in frame.out}
    card_testing_seen = False
    probe_declined = False
    instances = sorted(set(frame.instances) | set(tx_by_instance))
    for instance in instances:
        info = frame.instances.get(instance)
        txs = sorted(tx_by_instance.get(instance, []), key=_stamp)
        identities = side_by_instance.get(instance, [])
        pattern = FP(info.pattern if info is not None else txs[0].group)
        where = f"{instance} ({pattern.value})"
        if not txs:
            v.add("S7a/11", f"{where}: no transaction row")
        accounts = {tx.account for tx in txs}
        devices = {tx.device for tx in txs}
        merchants = {tx.merchant for tx in txs}

        if pattern is FP.ACCOUNT_TAKEOVER:
            changes = [e.t for e in identities if e.event_type in d.IDENTITY_CHANGE_TYPES]
            if txs and (not changes or min(changes) >= txs[0].t):
                v.add("S7a/1", f"{where}: no identity change before every transaction")
            for tx in txs:
                if tx.device in know.home_devices.get(tx.account, frozenset()):
                    v.add("S7a/1", f"{where}: {tx.transaction_id} on a home device")
                if not tx.amount > 2.5 * _typical(know, tx.account):
                    v.add("S7a/1", f"{where}: {tx.transaction_id} not above 2.5 x typical")
                distance = _home_distance(know, tx)
                if distance is None or not distance > 100:
                    v.add("S7a/1", f"{where}: {tx.transaction_id} not more than 100 km from home")
        elif pattern is FP.CARD_TESTING:
            card_testing_seen = True
            probes = [tx for tx in txs if tx.amount < 250]
            if len(devices) != 1:
                v.add("S7a/2", f"{where}: {len(devices)} devices")
            if len(probes) < 9:
                v.add("S7a/2", f"{where}: {len(probes)} probes")
            if len({p.merchant for p in probes}) < 5:
                v.add("S7a/2", f"{where}: probes span fewer than 5 merchants")
            if probes and probes[-1].t - probes[0].t >= 1_800_000:
                v.add("S7a/2", f"{where}: probes span 30 minutes or more")
            if not any(tx.amount > 1000 for tx in txs):
                v.add("S7a/2", f"{where}: no transaction above 1000")
            if any(outcome_by_id.get(p.transaction_id) == "DECLINED" for p in probes):
                probe_declined = True
        elif pattern is FP.IMPOSSIBLE_TRAVEL:
            if len(txs) != 2:
                v.add("S7a/3", f"{where}: {len(txs)} transactions")
            else:
                first, second = txs
                if first.channel != "CARD_PRESENT" or second.channel != "CARD_PRESENT":
                    v.add("S7a/3", f"{where}: a leg is not card-present")
                if None in (first.latitude, first.longitude, second.latitude, second.longitude):
                    v.add("S7a/3", f"{where}: a leg has no location")
                else:
                    speed = implied_speed_kmh(
                        GeoPoint(first.latitude, first.longitude),  # type: ignore[arg-type]
                        GeoPoint(second.latitude, second.longitude),  # type: ignore[arg-type]
                        (second.t - first.t) / 1000,
                    )
                    if not speed > 900:
                        v.add("S7a/3", f"{where}: implied {speed:.0f} km/h")
        elif pattern is FP.VELOCITY_ATTACK:
            if len(txs) < 16 or len(accounts) != 1:
                v.add("S7a/4", f"{where}: {len(txs)} transactions on {len(accounts)} accounts")
            if txs and txs[-1].t - txs[0].t >= 2_400_000:
                v.add("S7a/4", f"{where}: span of 40 minutes or more")
            for tx in txs:
                if not tx.amount < 3 * _typical(know, tx.account):
                    v.add("S7a/4", f"{where}: {tx.transaction_id} not below 3 x typical")
        elif pattern is FP.DEVICE_FARM:
            if len(devices) != 1 or len(accounts) < 6:
                v.add("S7a/5", f"{where}: {len(devices)} devices, {len(accounts)} accounts")
        elif pattern is FP.FRAUD_RING:
            if len(accounts) < 3 or len(devices) >= len(accounts) or len(merchants) > 3:
                v.add(
                    "S7a/6",
                    f"{where}: {len(accounts)} accounts, {len(devices)} devices, "
                    f"{len(merchants)} merchants",
                )
        elif pattern is FP.MERCHANT_COLLUSION:
            amounts = [tx.amount for tx in txs]
            if len(merchants) != 1 or len(accounts) < 10:
                v.add("S7a/7", f"{where}: {len(merchants)} merchants, {len(accounts)} accounts")
            if (
                amounts
                and (max(amounts) - min(amounts)) / max(1, sum(amounts) / len(amounts)) >= 0.2
            ):
                v.add("S7a/7", f"{where}: amounts spread 0.2 or more")
            for tx in txs:
                if (
                    tx.amount <= 0
                    or abs(math.log(tx.amount) - know.amount_mu[tx.account])
                    > know.amount_sigma[tx.account] + d.G6_S7A_LOG_SLACK
                ):
                    v.add("S7a/7", f"{where}: {tx.transaction_id} not ordinary for its payer")
        elif pattern is FP.CREDENTIAL_STUFFING:
            failed = sum(1 for e in identities if e.event_type == "LOGIN_FAILED")
            succeeded = sum(1 for e in identities if e.event_type == "LOGIN_SUCCEEDED")
            if len(identities) < 16 or len({e.account for e in identities}) < 10:
                v.add("S7a/8", f"{where}: {len(identities)} identity rows")
            if len({e.ip for e in identities}) > 3:
                v.add("S7a/8", f"{where}: more than 3 IPs")
            if not failed > succeeded:
                v.add("S7a/8", f"{where}: failures do not outnumber successes")
            if any(e.ip is None or not know.ip_datacenter.get(e.ip, False) for e in identities):
                v.add("S7a/8", f"{where}: an identity row not from a datacenter IP")
            if any(e.event_type in d.IDENTITY_CHANGE_TYPES for e in identities):
                v.add("S7a/8", f"{where}: an identity change")
        elif pattern is FP.ANOMALOUS_HIGH_VALUE:
            if len(txs) != 1:
                v.add("S7a/9", f"{where}: {len(txs)} transactions")
            for tx in txs:
                if not tx.amount >= 20 * _typical(know, tx.account):
                    v.add("S7a/9", f"{where}: {tx.transaction_id} below 20 x typical")
        elif pattern is FP.UNUSUAL_LOCATION_DEVICE:
            if len(txs) != 1:
                v.add("S7a/10", f"{where}: {len(txs)} transactions")
            for tx in txs:
                typical = _typical(know, tx.account)
                if not 0.5 * typical < tx.amount < 2 * typical:
                    v.add("S7a/10", f"{where}: {tx.transaction_id} outside (0.5, 2) x typical")
                if tx.device in know.home_devices.get(tx.account, frozenset()):
                    v.add("S7a/10", f"{where}: {tx.transaction_id} on a home device")

        if info is not None:
            declared = d.G7_GATED_CAUSAL_KEYS.get(pattern)
            expected = (
                declared
                if declared is not None
                else frozenset(
                    key.value for key in SCENARIOS_BY_PATTERN[pattern].causal_evidence_keys()
                )
            )
            if info.causal_keys != expected:
                v.add("S7a/11a", f"{where}: causal keys {sorted(info.causal_keys)}")
    if card_testing_seen and not probe_declined:
        v.add("S7a/2", "no card-testing probe was declined, pooled over every instance")
    return CheckResult("S7a", len(instances), v.findings())
