"""`LPC-5` §4: every attribute of every population, computed once over a normalised frame.

**Layout.** A table holds, per population:
- per row, a group (a scenario value or `LEGIT`), a cluster and an account;
- per attribute, an encoded column: one unsigned 16-bit code per row, with the column's value
  labels.

Code 0 means "not applicable", and such a row takes no part in that attribute's cells. At acceptance
scale (a million transactions and as many side events) a column costs 2 bytes a row, where a list of
strings would cost 8.

**Definitions are the frozen document's**, not re-derived here. Where the document says "strictly
earlier", `(t - W, t]` or `[t - W, t)`, the code says exactly that, and
`tests/unit/test_lpc5_attributes.py` pins each boundary.
"""

from __future__ import annotations

import datetime as dt
import math
import re
from array import array
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass

from data.generator.lpc5 import declaration as d
from data.generator.lpc5.frame import Envelope, Frame, Knowledge, OutRow, SideRow, TxRow, millis
from trace_core.domain.geo import GeoPoint, haversine_km

WEEK_MS = 7 * d.DAY_MS
AvailabilityProvider = Callable[[Frame], Mapping[str, Sequence[str]]]

_ACCOUNT = re.compile(r"acct_\d{6,}")
_CARD = re.compile(r"card_\d{6,}")
_DEVICE = re.compile(r"dev_\d{6,}")
_MERCHANT = re.compile(r"mrch_\d{5,}")
_IP = re.compile(r"ip_\d{5,}")
_TIMESTAMP = re.compile(d.TIMESTAMP_PATTERN)
_SHAPE = re.compile(r"[A-Za-z0-9]+")


class Column:
    """One attribute over one population, encoded."""

    __slots__ = ("_index", "codes", "vocab")

    def __init__(self, size: int) -> None:
        self.codes = array("H", bytes(2 * size))
        self.vocab: list[str | None] = [None]
        self._index: dict[str, int] = {}

    def put(self, row: int, value: str) -> None:
        code = self._index.get(value)
        if code is None:
            code = len(self.vocab)
            if code > 0xFFFF:  # pragma: no cover - no declared attribute has this many values
                raise ValueError(f"column vocabulary exceeds 65535 values at {value!r}")
            self._index[value] = code
            self.vocab.append(value)
        self.codes[row] = code

    def get(self, row: int) -> str | None:
        return self.vocab[self.codes[row]]

    def values(self) -> list[str | None]:
        return [self.vocab[code] for code in self.codes]


@dataclass(slots=True)
class Table:
    population: d.Population
    size: int
    groups: list[str]
    clusters: list[str]
    accounts: list[str]
    columns: dict[str, Column]

    def column(self, name: str) -> Column:
        try:
            return self.columns[name]
        except KeyError:
            raise KeyError(f"{self.population} has no computed attribute {name!r}") from None


# ---------------------------------------------------------------------------- binning ---------
def band(value: float, edges: Sequence[float], labels: Sequence[str]) -> str:
    """`labels[i]` for the first edge the value is below; the last label otherwise."""
    for edge, label in zip(edges, labels, strict=False):
        if value < edge:
            return label
    return labels[-1]


def count_bin(n: int) -> str:
    return band(n, (2, 3, 5, 10, 20), d.TX_COUNT)


def _members(n: int) -> str:
    return "1" if n <= 1 else "2" if n == 2 else "3+"


_CALENDAR: dict[int, tuple[str, str, str]] = {}


def calendar(t: int) -> tuple[str, str, str]:
    """(hour, daypart, weekday), UTC."""
    key = t // d.HOUR_MS
    cached = _CALENDAR.get(key)
    if cached is None:
        moment = dt.datetime.fromtimestamp(key * 3600, tz=dt.UTC)
        cached = (str(moment.hour), d.DAYPARTS[moment.hour // 6], d.WEEKDAYS[moment.weekday()])
        _CALENDAR[key] = cached
    return cached


def decimals(value: float) -> int:
    text = repr(float(value))
    if "e" in text or "E" in text:
        mantissa, exponent = text.lower().split("e")
        places = len(mantissa.split(".")[1]) if "." in mantissa else 0
        return max(0, places - int(exponent))
    return len(text.split(".")[1]) if "." in text else 0


def _age(delta_ms: int) -> str:
    return band(delta_ms, (d.HOUR_MS, d.DAY_MS, WEEK_MS), ("<1h", "[1h,24h)", "[1d,7d)", "≥7d"))


def _ok(pattern: re.Pattern[str], value: str | None) -> bool:
    return value is not None and pattern.fullmatch(value) is not None


# ---------------------------------------------------------------------------- shared indexes --
@dataclass(slots=True)
class _Global:
    """Counts over every row of every population: envelope uniqueness, membership, ties."""

    event_ids: Counter[int]
    idempotency: Counter[int]
    correlation: Counter[int]
    trace: Counter[int]
    tie_rank: dict[str, list[str]]
    """Per population, each row's rank among rows sharing its millisecond, by row index."""


def _global(frame: Frame) -> _Global:
    event_ids: Counter[int] = Counter()
    idempotency: Counter[int] = Counter()
    correlation: Counter[int] = Counter()
    trace: Counter[int] = Counter()
    by_time: dict[int, list[tuple[int, str, int]]] = defaultdict(list)
    populations: tuple[tuple[str, Sequence[TxRow | SideRow | OutRow]], ...] = (
        ("TX", frame.tx),
        ("ID", frame.ident),
        ("DEV", frame.dev),
        ("OUT", frame.out),
    )
    for name, rows in populations:
        for index, row in enumerate(rows):
            envelope = row.envelope
            if envelope.event_id is not None:
                event_ids[hash(envelope.event_id)] += 1
            if envelope.idempotency_key is not None:
                idempotency[hash(envelope.idempotency_key)] += 1
            if envelope.correlation_id is not None:
                correlation[hash(envelope.correlation_id)] += 1
            if envelope.trace_id is not None:
                trace[hash(envelope.trace_id)] += 1
            by_time[row.t].append((row.order, name, index))
    # One list per population, not a dict keyed by (population, row): at acceptance scale the
    # tuple keys cost more than a gigabyte (Stage 2 step 11).
    tie_rank: dict[str, list[str]] = {name: [""] * len(rows) for name, rows in populations}
    for group in by_time.values():
        if len(group) == 1:
            _, name, index = group[0]
            tie_rank[name][index] = "alone"
            continue
        group.sort()
        for position, (_, name, index) in enumerate(group):
            tie_rank[name][index] = "first" if position == 0 else "later"
    return _Global(event_ids, idempotency, correlation, trace, tie_rank)


def _put_common(
    columns: dict[str, Column],
    i: int,
    population: str,
    row_t: int,
    envelope: Envelope,
    keys: str,
    identifiers_ok: bool,
    shared: _Global,
    know: Knowledge,
    *,
    in_window: bool,
) -> None:
    if in_window:
        columns["in_window"].put(i, "inside" if know.start_ms <= row_t < know.end_ms else "outside")
    columns["subsecond"].put(i, "whole" if row_t % 1000 == 0 else "fractional")
    formats_ok = _TIMESTAMP.fullmatch(envelope.occurred_at) is not None and (
        envelope.ingested_at is not None and _TIMESTAMP.fullmatch(envelope.ingested_at) is not None
    )
    columns["timestamp_format"].put(i, "ms-z" if formats_ok else "other")
    if envelope.ingested_at is not None:
        lag = millis(envelope.ingested_at) - row_t
        columns["ingest_lag"].put(
            i, "<0" if lag < 0 else band(lag, (40, 80, 120, 160), d.INGEST_LAG[1:])
        )
    columns["tie_rank"].put(i, shared.tie_rank[population][i])
    columns["payload_keys"].put(i, keys)
    columns["identifier_formats"].put(i, "ok" if identifiers_ok else "not")
    columns["envelope_constants"].put(
        i, f"{envelope.event_type}|{envelope.schema_version}|{envelope.producer}"
    )
    if envelope.event_id is not None and envelope.idempotency_key is not None:
        unique = (
            shared.event_ids[hash(envelope.event_id)] == 1
            and shared.idempotency[hash(envelope.idempotency_key)] == 1
        )
        columns["envelope_unique"].put(i, "unique" if unique else "shared")
    if envelope.event_id is not None:
        try:
            stamped = int(envelope.event_id.replace("-", "")[:12], 16)
        except ValueError:
            stamped = -1
        columns["event_id_time"].put(i, "equal" if stamped == row_t else "not")
    if envelope.correlation_id is not None:
        columns["correlation_shape"].put(i, _SHAPE.sub("x", envelope.correlation_id))
        columns["correlation_members"].put(
            i, _members(shared.correlation[hash(envelope.correlation_id)])
        )
    if envelope.trace_id is not None:
        columns["trace_shape"].put(i, _SHAPE.sub("x", envelope.trace_id))
        columns["trace_members"].put(i, _members(shared.trace[hash(envelope.trace_id)]))


def _sorted_by_time(indices: list[int], rows: Sequence[TxRow]) -> list[int]:
    indices.sort(key=lambda i: (rows[i].t, rows[i].order))
    return indices


def _sliding(
    indices: Sequence[int],
    rows: Sequence[TxRow],
    width_ms: int,
    key: Callable[[int], Hashable],
    out: list[int],
) -> None:
    """For each row, the number of distinct keys among rows at `(t - width, t]`, ties included."""
    counts: Counter[Hashable] = Counter()
    left = 0
    k = 0
    n = len(indices)
    while k < n:
        t = rows[indices[k]].t
        end = k
        while end < n and rows[indices[end]].t == t:
            counts[key(indices[end])] += 1
            end += 1
        while left < end and rows[indices[left]].t <= t - width_ms:
            stale = key(indices[left])
            counts[stale] -= 1
            if counts[stale] == 0:
                del counts[stale]
            left += 1
        for j in range(k, end):
            out[indices[j]] = len(counts)
        k = end


# ---------------------------------------------------------------------------- TX --------------
def _tx_table(
    frame: Frame,
    know: Knowledge,
    shared: _Global,
    availability: Mapping[str, Sequence[str]] | None,
) -> Table:
    txs = frame.tx
    n = len(txs)
    columns = {
        spec.name: Column(n)
        for spec in d.ATTRIBUTES[d.Population.TX]
        if spec.klass is not d.Klass.AVAILABILITY
    }
    table = Table(
        d.Population.TX,
        n,
        [tx.group for tx in txs],
        [tx.cluster for tx in txs],
        [tx.account for tx in txs],
        columns,
    )
    for account in {tx.account for tx in txs}:
        if account not in know.amount_mu:
            raise ValueError(f"account {account} is not in the population knowledge given")

    legit_amounts = sorted(tx.amount for tx in txs if tx.group == d.LEGIT)
    edges: list[int] = []
    if legit_amounts:
        for percentile in range(10, 100, 10):
            nearest_rank = math.ceil(percentile / 100 * len(legit_amounts))
            edges.append(legit_amounts[max(nearest_rank, 1) - 1])

    out_by_id = {out.transaction_id: out for out in frame.out}
    points = [
        GeoPoint(tx.latitude, tx.longitude)
        if tx.latitude is not None and tx.longitude is not None
        else None
        for tx in txs
    ]

    # --- per-row attributes -------------------------------------------------------------
    for i, tx in enumerate(txs):
        hour, daypart, weekday = calendar(tx.t)
        columns["hour"].put(i, hour)
        columns["daypart"].put(i, daypart)
        columns["weekday"].put(i, weekday)
        columns["channel"].put(i, tx.channel or "absent")
        columns["entry_mode"].put(i, tx.entry_mode or "absent")
        columns["currency"].put(i, tx.currency or "absent")
        mu = know.amount_mu[tx.account]
        sigma = know.amount_sigma[tx.account]
        columns["amount_vs_account"].put(
            i, band(tx.amount / math.exp(mu), (0.1, 0.5, 2, 5, 20), d.AMOUNT_VS_ACCOUNT)
        )
        if edges:
            columns["amount_decile"].put(i, str(bisect_right(edges, tx.amount)))
        if tx.amount <= 0:
            columns["amount_z"].put(i, "nonpositive")
        else:
            z = (math.log(tx.amount) - mu) / sigma
            columns["amount_z"].put(i, band(z, (-2, -1, -0.5, 0, 0.5, 1, 2), d.AMOUNT_Z[:8]))
        magnitude = abs(tx.amount)
        columns["amount_last_digit"].put(i, str(magnitude % 10))
        columns["amount_roundness"].put(
            i,
            "x1000"
            if magnitude % 1000 == 0
            else "x100"
            if magnitude % 100 == 0
            else "x10"
            if magnitude % 10 == 0
            else "other",
        )
        columns["outcome_field"].put(i, tx.outcome_field or "absent")
        outcome = out_by_id.get(tx.transaction_id)
        columns["outcome_event"].put(i, outcome.authorization_outcome if outcome else "none")
        columns["card"].put(i, "first" if tx.card == know.first_card.get(tx.account) else "other")
        columns["user_agent"].put(i, tx.user_agent or "absent")
        columns["memo"].put(i, "non-empty" if tx.memo else "absent-or-empty")
        if tx.merchant is not None:
            habitual = tx.merchant in know.habitual_merchants.get(tx.account, frozenset())
            columns["merchant_habitual"].put(i, "habitual" if habitual else "unhabitual")
            rank = know.merchant_rank.get(tx.merchant)
            if rank is not None:
                columns["merchant_popularity"].put(i, band(rank, (2, 4, 11, 31, 101), d.POPULARITY))
        columns["merchant_mcc"].put(i, tx.mcc or "absent")
        if tx.mcc is not None:
            unused = tx.mcc not in know.habitual_mccs.get(tx.account, frozenset())
            columns["mcc_habitual"].put(i, "unhabitual" if unused else "habitual")
        columns["merchant_country"].put(i, tx.country or "absent")
        if tx.country is not None:
            columns["merchant_country_home"].put(
                i, "home" if tx.country == know.country.get(tx.account) else "other"
            )
        point = points[i]
        home = know.home_point.get(tx.account)
        if point is not None and home is not None:
            distance = haversine_km(GeoPoint(*home), point)
            columns["distance_home"].put(i, band(distance, (5, 25, 100, 500), d.DISTANCE_HOME))
            places = max(decimals(point.latitude), decimals(point.longitude))
            columns["coordinate_decimals"].put(i, band(places, (3, 6), d.COORDINATE_DECIMALS))
            exact_home = (point.latitude, point.longitude) == home
            columns["coordinate_home"].put(i, "exact" if exact_home else "not")
        if tx.device is not None:
            at_home = tx.device in know.home_devices.get(tx.account, frozenset())
            columns["device_home"].put(i, "home" if at_home else "not")
        if tx.ip is not None:
            at_home = tx.ip in know.home_ips.get(tx.account, frozenset())
            columns["ip_home"].put(i, "home" if at_home else "not")
            columns["ip_datacenter"].put(
                i, "datacenter" if know.ip_datacenter.get(tx.ip, False) else "not"
            )
        identifiers_ok = (
            1 <= len(tx.transaction_id) <= 64
            and _ok(_ACCOUNT, tx.account)
            and _ok(_CARD, tx.card)
            and _ok(_DEVICE, tx.device)
            and _ok(_MERCHANT, tx.merchant)
            and _ok(_IP, tx.ip)
        )
        _put_common(
            columns,
            i,
            "TX",
            tx.t,
            tx.envelope,
            tx.keys,
            identifiers_ok,
            shared,
            know,
            in_window=True,
        )

    # --- per-account histories ---------------------------------------------------------
    by_account: dict[str, list[int]] = defaultdict(list)
    by_card: dict[str, list[int]] = defaultdict(list)
    by_device: dict[str, list[int]] = defaultdict(list)
    by_ip: dict[str, list[int]] = defaultdict(list)
    by_merchant_currency: dict[tuple[str, str], list[int]] = defaultdict(list)
    for i, tx in enumerate(txs):
        by_account[tx.account].append(i)
        if tx.card is not None:
            by_card[tx.card].append(i)
        if tx.device is not None:
            by_device[tx.device].append(i)
        if tx.ip is not None:
            by_ip[tx.ip].append(i)
        if tx.merchant is not None:
            by_merchant_currency[(tx.merchant, tx.currency or "")].append(i)
    for bucket in (by_account, by_card, by_device, by_ip):
        for indices in bucket.values():
            _sorted_by_time(indices, txs)
    for indices in by_merchant_currency.values():
        _sorted_by_time(indices, txs)

    windows = (
        ("tx_count_1m", 60_000),
        ("tx_count_5m", 300_000),
        ("tx_count_1h", d.HOUR_MS),
        ("tx_count_24h", d.DAY_MS),
    )
    for indices in by_account.values():
        times = [txs[i].t for i in indices]
        coordinates_seen: set[tuple[float, float]] = set()
        position = 0
        while position < len(indices):
            t = times[position]
            block_end = bisect_right(times, t)
            earlier = bisect_left(times, t)
            for j in range(position, block_end):
                i = indices[j]
                upto = block_end
                for name, width in windows:
                    columns[name].put(i, count_bin(upto - bisect_right(times, t - width)))
                start_1h = bisect_right(times, t - d.HOUR_MS)
                start_5m = bisect_right(times, t - 300_000)
                start_24h = bisect_right(times, t - d.DAY_MS)
                merchants = {txs[k].merchant for k in indices[start_1h:upto]}
                columns["distinct_merchants_1h"].put(
                    i, band(len(merchants), (2, 3, 5, 10), d.DISTINCT_MERCHANTS)
                )
                mccs = {txs[k].mcc for k in indices[start_5m:upto]}
                columns["distinct_mcc_5m"].put(i, band(len(mccs), (2, 3, 5), d.DISTINCT_MCC))
                devices = {txs[k].device for k in indices[start_24h:upto]}
                columns["distinct_devices_24h"].put(i, band(len(devices), (2, 3), d.DISTINCT_FEW))
                countries = {txs[k].country for k in indices[start_24h:upto]}
                columns["distinct_countries_24h"].put(
                    i, band(len(countries), (2, 3), d.DISTINCT_FEW)
                )
                columns["profile_depth"].put(i, band(earlier, (1, 3, 20, 128), d.PROFILE_DEPTH))
                columns["account_activity"].put(
                    i, band(len(indices), (11, 21, 31, 51), d.ACCOUNT_ACTIVITY)
                )
                if earlier == 0:
                    columns["gap_prev"].put(i, "none")
                    columns["leg_speed"].put(i, "none")
                else:
                    previous = txs[indices[earlier - 1]]
                    gap = t - previous.t
                    columns["gap_prev"].put(
                        i,
                        band(
                            gap,
                            (10_000, 60_000, 600_000, d.HOUR_MS, d.DAY_MS),
                            d.GAP_PREV[1:],
                        ),
                    )
                    here = points[i]
                    there = points[indices[earlier - 1]]
                    if gap >= d.DAY_MS or here is None or there is None:
                        columns["leg_speed"].put(i, "none")
                    else:
                        distance = haversine_km(there, here)
                        if gap == 0:
                            speed_label = "≥1000" if distance > 0 else "<100"
                        else:
                            speed = distance / (gap / d.HOUR_MS)
                            speed_label = band(speed, (100, 500, 1000), d.LEG_SPEED[1:])
                        columns["leg_speed"].put(i, speed_label)
                here = points[i]
                if here is None:
                    continue
                if earlier == 0:
                    columns["location_novel"].put(i, "no-prior")
                else:
                    nearest = min(
                        (
                            haversine_km(points[k], here)  # type: ignore[arg-type]
                            for k in indices[:earlier]
                            if points[k] is not None
                        ),
                        default=None,
                    )
                    columns["location_novel"].put(
                        i,
                        "no-prior"
                        if nearest is None
                        else band(nearest, (25, 100, 500), d.LOCATION_NOVEL[1:]),
                    )
                repeat = (here.latitude, here.longitude) in coordinates_seen
                columns["coordinate_repeat"].put(i, "repeat" if repeat else "new")
            for j in range(position, block_end):
                point = points[indices[j]]
                if point is not None:
                    coordinates_seen.add((point.latitude, point.longitude))
            position = block_end

    for indices in by_card.values():
        times = [txs[i].t for i in indices]
        for i in indices:
            t = txs[i].t
            columns["card_count_5m"].put(
                i, count_bin(bisect_right(times, t) - bisect_right(times, t - 300_000))
            )

    # --- devices, IPs, merchants: dataset-wide and sliding ------------------------------
    device_accounts = {device: {txs[i].account for i in idx} for device, idx in by_device.items()}
    ip_accounts = {ip: {txs[i].account for i in idx} for ip, idx in by_ip.items()}
    first_payment: dict[tuple[str, str], tuple[int, int]] = {}
    per_account_device: Counter[tuple[str, str]] = Counter()
    for tx in txs:
        if tx.device is None:
            continue
        key = (tx.account, tx.device)
        per_account_device[key] += 1
        stamp = (tx.t, tx.order)
        if key not in first_payment or stamp < first_payment[key]:
            first_payment[key] = stamp

    counts24 = [0] * n
    for indices in by_device.values():
        _sliding(indices, txs, d.DAY_MS, lambda k: txs[k].account, counts24)
    counts1h = [0] * n
    for indices in by_ip.values():
        _sliding(indices, txs, d.HOUR_MS, lambda k: txs[k].account, counts1h)
    merchant1h = [0] * n
    by_merchant: dict[str, list[int]] = defaultdict(list)
    for (merchant, _), indices in by_merchant_currency.items():
        by_merchant[merchant].extend(indices)
    for indices in by_merchant.values():
        _sorted_by_time(indices, txs)
        _sliding(indices, txs, d.HOUR_MS, lambda k: txs[k].account, merchant1h)

    for i, tx in enumerate(txs):
        if tx.device is not None:
            key = (tx.account, tx.device)
            if first_payment[key] == (tx.t, tx.order):
                columns["device_age"].put(i, "first-use")
            else:
                columns["device_age"].put(i, _age(tx.t - first_payment[key][0]))
            columns["device_account_tx"].put(
                i, band(per_account_device[key], (2, 3, 6), d.ACCOUNTS_DATASET)
            )
            columns["device_accounts"].put(
                i, band(len(device_accounts[tx.device]), (2, 3, 6), d.ACCOUNTS_DATASET)
            )
            columns["device_accounts_24h"].put(
                i, band(counts24[i], (2, 3, 5), d.DEVICE_ACCOUNTS_24H)
            )
        if tx.ip is not None:
            columns["ip_accounts"].put(
                i, band(len(ip_accounts[tx.ip]), (2, 3, 6), d.ACCOUNTS_DATASET)
            )
            columns["ip_accounts_1h"].put(i, band(counts1h[i], (2, 3, 5, 10), d.IP_ACCOUNTS_1H))
        if tx.merchant is not None:
            columns["merchant_accounts_1h"].put(
                i, band(merchant1h[i], (5, 20), d.MERCHANT_ACCOUNTS_1H)
            )

    for indices in by_merchant_currency.values():
        _merchant_amounts(indices, txs, columns)

    _links(txs, device_accounts, ip_accounts, columns)
    _identity_context(frame, txs, by_account, columns)
    _prior_outcomes(frame, txs, columns)

    if availability is not None:
        for feature in d.RELEASED_FEATURES:
            values = availability[feature]
            if len(values) != n:
                raise ValueError(f"availability for {feature} has {len(values)} rows, need {n}")
            column = Column(n)
            for i, value in enumerate(values):
                column.put(i, value)
            columns[f"{d.AVAIL_PREFIX}{feature}"] = column
    return table


def _merchant_amounts(indices: list[int], txs: Sequence[TxRow], columns: dict[str, Column]) -> None:
    """`merchant_amount_cv_24h` and `merchant_same_amount_accounts_24h`, over `(t - 24 h, t]`."""
    count = 0
    total = 0
    squares = 0
    left = 0
    k = 0
    n = len(indices)
    while k < n:
        t = txs[indices[k]].t
        end = k
        while end < n and txs[indices[end]].t == t:
            amount = txs[indices[end]].amount
            count += 1
            total += amount
            squares += amount * amount
            end += 1
        while left < end and txs[indices[left]].t <= t - d.DAY_MS:
            amount = txs[indices[left]].amount
            count -= 1
            total -= amount
            squares -= amount * amount
            left += 1
        if count < 2 or total == 0:
            cv_label = "n<2" if count < 2 else "≥0.2"
        else:
            cv = math.sqrt(max(count * squares - total * total, 0)) / abs(total)
            cv_label = band(cv, (0.05, 0.2), d.MERCHANT_CV[1:])
        window = indices[left:end]
        for j in range(k, end):
            i = indices[j]
            columns["merchant_amount_cv_24h"].put(i, cv_label)
            amount = abs(txs[i].amount)
            accounts = {
                txs[w].account for w in window if 50 * abs(abs(txs[w].amount) - amount) <= amount
            }
            columns["merchant_same_amount_accounts_24h"].put(
                i, band(len(accounts), (2, 5), d.SAME_AMOUNT_ACCOUNTS)
            )
        k = end


def _links(
    txs: Sequence[TxRow],
    device_accounts: Mapping[str, set[str]],
    ip_accounts: Mapping[str, set[str]],
    columns: dict[str, Column],
) -> None:
    """`joint_link` and `shared_merchant_link` (dataset-wide, TX rows)."""
    devices_of: dict[str, set[str]] = defaultdict(set)
    ips_of: dict[str, set[str]] = defaultdict(set)
    merchant_times: dict[tuple[str, str], list[int]] = defaultdict(list)
    for tx in txs:
        if tx.device is not None:
            devices_of[tx.account].add(tx.device)
        if tx.ip is not None:
            ips_of[tx.account].add(tx.ip)
        if tx.merchant is not None:
            merchant_times[(tx.merchant, tx.account)].append(tx.t)
    for series in merchant_times.values():
        series.sort()
    joint: dict[str, bool] = {}
    linked: dict[str, set[str]] = {}
    for account in devices_of.keys() | ips_of.keys():
        mine_ips = ips_of.get(account, set())
        via_devices: set[str] = set()
        for device in devices_of.get(account, set()):
            via_devices |= device_accounts[device]
        via_devices.discard(account)
        joint[account] = any(mine_ips & ips_of.get(other, set()) for other in via_devices)
        via_ips: set[str] = set()
        for ip in mine_ips:
            via_ips |= ip_accounts[ip]
        via_ips.discard(account)
        linked[account] = via_devices | via_ips
    for i, tx in enumerate(txs):
        columns["joint_link"].put(i, "yes" if joint.get(tx.account, False) else "no")
        if tx.merchant is None:
            continue
        found = 0
        for other in linked.get(tx.account, set()):
            times = merchant_times.get((tx.merchant, other))
            if not times:
                continue
            position = bisect_right(times, tx.t - WEEK_MS)
            if position < len(times) and times[position] < tx.t + WEEK_MS:
                found += 1
                if found >= 2:
                    break
        columns["shared_merchant_link"].put(i, "yes" if found >= 2 else "no")


def _identity_context(
    frame: Frame,
    txs: Sequence[TxRow],
    by_account: Mapping[str, list[int]],
    columns: dict[str, Column],
) -> None:
    changes: dict[str, list[int]] = defaultdict(list)
    failed: dict[str, list[int]] = defaultdict(list)
    other: dict[str, list[int]] = defaultdict(list)
    device_events: dict[str, list[int]] = defaultdict(list)
    logins_by_ip: dict[str, list[tuple[int, str]]] = defaultdict(list)
    for event in frame.ident:
        if event.event_type in d.IDENTITY_CHANGE_TYPES:
            changes[event.account].append(event.t)
        elif event.event_type == "LOGIN_FAILED":
            failed[event.account].append(event.t)
        else:
            other[event.account].append(event.t)
        if event.event_type in d.LOGIN_TYPES and event.ip is not None:
            logins_by_ip[event.ip].append((event.t, event.account))
    for event in frame.dev:
        device_events[event.account].append(event.t)
    for bucket in (changes, failed, other, device_events):
        for series in bucket.values():
            series.sort()
    for series_by_ip in logins_by_ip.values():
        series_by_ip.sort()
    login_times = {ip: [t for t, _ in series] for ip, series in logins_by_ip.items()}

    def present(series: list[int] | None, low: int, high: int) -> bool:
        if not series:
            return False
        position = bisect_left(series, low)
        return position < len(series) and series[position] < high

    for account, indices in by_account.items():
        account_changes = changes.get(account)
        account_failed = failed.get(account)
        for i in indices:
            t = txs[i].t
            low = t - d.DAY_MS
            if present(account_changes, low, t):
                prior = "identity-change"
            elif present(device_events.get(account), low, t):
                prior = "device-event"
            elif present(account_failed, low, t):
                prior = "failed-login"
            elif present(other.get(account), low, t):
                prior = "other-identity"
            else:
                prior = "none"
            columns["prior_events_24h"].put(i, prior)
            latest = None
            if account_changes:
                position = bisect_left(account_changes, t) - 1
                if position >= 0 and account_changes[position] >= low:
                    latest = account_changes[position]
            columns["hours_since_identity_change"].put(
                i,
                "none"
                if latest is None
                else band(t - latest, (d.HOUR_MS, 6 * d.HOUR_MS), d.HOURS_SINCE_CHANGE[1:]),
            )
            count = 0
            if account_failed:
                count = bisect_left(account_failed, t) - bisect_left(account_failed, t - d.HOUR_MS)
            columns["failed_logins_1h"].put(i, band(count, (1, 5, 20), d.FAILED_LOGINS))
            ip = txs[i].ip
            if ip is None:
                continue
            times = login_times.get(ip)
            if not times:
                columns["ip_login_accounts_1h"].put(i, "0")
                continue
            login_series = logins_by_ip[ip]
            start = bisect_left(times, t - d.HOUR_MS)
            stop = bisect_left(times, t)
            accounts = {login_series[k][1] for k in range(start, stop)}
            columns["ip_login_accounts_1h"].put(
                i, band(len(accounts), (1, 2, 5), d.IP_LOGIN_ACCOUNTS_1H)
            )


def _prior_outcomes(frame: Frame, txs: Sequence[TxRow], columns: dict[str, Column]) -> None:
    """`prior_decisions_1h` and `prior_declined_share_1h`.

    OUT rows at `(t - 1 h, t)`, the transaction's own outcome excluded."""
    by_account: dict[str, list[OutRow]] = defaultdict(list)
    for out in frame.out:
        by_account[out.account].append(out)
    times_by_account: dict[str, list[int]] = {}
    for account, series in by_account.items():
        series.sort(key=lambda o: (o.t, o.order))
        times_by_account[account] = [o.t for o in series]
    for i, tx in enumerate(txs):
        series = by_account.get(tx.account, [])
        times = times_by_account.get(tx.account, [])
        start = bisect_right(times, tx.t - d.HOUR_MS)
        stop = bisect_left(times, tx.t)
        known = [o for o in series[start:stop] if o.transaction_id != tx.transaction_id]
        declined = sum(1 for o in known if o.authorization_outcome == "DECLINED")
        columns["prior_decisions_1h"].put(i, band(len(known), (1, 2, 5, 10), d.PRIOR_DECISIONS))
        if not known:
            share = "none"
        elif declined == 0:
            share = "0"
        else:
            share = "(0,0.4)" if declined / len(known) < 0.4 else "≥0.4"
        columns["prior_declined_share_1h"].put(i, share)


# ---------------------------------------------------------------------------- ID and DEV ------
def _first_references(frame: Frame) -> dict[tuple[str, str], tuple[int, int]]:
    first: dict[tuple[str, str], tuple[int, int]] = {}

    def note(account: str, device: str | None, t: int, order: int) -> None:
        if device is None:
            return
        key = (account, device)
        stamp = (t, order)
        if key not in first or stamp < first[key]:
            first[key] = stamp

    for tx in frame.tx:
        note(tx.account, tx.device, tx.t, tx.order)
    for event in (*frame.ident, *frame.dev):
        note(event.account, event.device, event.t, event.order)
    return first


def _side_table(
    population: d.Population,
    rows: Sequence[SideRow],
    frame: Frame,
    know: Knowledge,
    shared: _Global,
    first_reference: Mapping[tuple[str, str], tuple[int, int]],
) -> Table:
    n = len(rows)
    columns = {spec.name: Column(n) for spec in d.ATTRIBUTES[population]}
    table = Table(
        population,
        n,
        [row.group for row in rows],
        [row.cluster for row in rows],
        [row.account for row in rows],
        columns,
    )
    login_accounts: dict[str, set[str]] = defaultdict(set)
    device_login_accounts: dict[str, set[str]] = defaultdict(set)
    for event in frame.ident:
        if event.event_type in d.LOGIN_TYPES and event.ip is not None:
            login_accounts[event.ip].add(event.account)
        if event.device is not None:
            device_login_accounts[event.device].add(event.account)
    label = "ID" if population is d.Population.ID else "DEV"
    # §4.8 (revision 3): an attribute of an optional field is not applicable to an event type no row
    # of which carries the field, when the type occurs among legitimate rows. Such rows stay unset.
    not_applicable: dict[str, set[str]] = {}
    optional = d.OPTIONAL_FIELD_ATTRIBUTES.get(population, {})
    if optional:
        legitimate_types = {row.event_type for row in rows if row.group == d.LEGIT}
        carrying: dict[str, set[str]] = defaultdict(set)
        for row in rows:
            for source in optional:
                if getattr(row, source) is not None:
                    carrying[source].add(row.event_type)
        for source, names in optional.items():
            absent_types = legitimate_types - carrying[source]
            for name in names:
                not_applicable[name] = absent_types

    def applicable(name: str, event_type: str) -> bool:
        return event_type not in not_applicable.get(name, ())

    for i, row in enumerate(rows):
        columns["event_type"].put(i, row.event_type)
        hour, daypart, weekday = calendar(row.t)
        columns["hour"].put(i, hour)
        columns["daypart"].put(i, daypart)
        columns["weekday"].put(i, weekday)
        if row.device is not None:
            at_home = row.device in know.home_devices.get(row.account, frozenset())
            columns["device_home"].put(i, "home" if at_home else "not")
            stamp = first_reference[(row.account, row.device)]
            columns["device_age"].put(
                i, "first-reference" if stamp == (row.t, row.order) else _age(row.t - stamp[0])
            )
        if population is d.Population.ID:
            if row.user_agent or applicable("user_agent", row.event_type):
                columns["user_agent"].put(i, row.user_agent or "absent")
            if row.ip is None:
                for name in ("ip_datacenter", "ip_login_accounts"):
                    if applicable(name, row.event_type):
                        columns[name].put(i, "absent")
            else:
                columns["ip_datacenter"].put(
                    i, "datacenter" if know.ip_datacenter.get(row.ip, False) else "not"
                )
                accounts = len(login_accounts.get(row.ip, set()))
                columns["ip_login_accounts"].put(
                    i, "absent" if accounts == 0 else band(accounts, (2, 3, 6), d.ACCOUNTS_DATASET)
                )
            if row.device is None:
                for name in ("device_home", "device_age", "device_login_accounts"):
                    if applicable(name, row.event_type):
                        columns[name].put(i, "absent")
            else:
                columns["device_login_accounts"].put(
                    i,
                    band(len(device_login_accounts[row.device]), (2, 3, 6), d.ACCOUNTS_DATASET),
                )
        else:
            columns["platform"].put(i, row.platform or "absent")
            expected = know.device_platform.get(row.device or "")
            columns["platform_consistent"].put(
                i, "equal" if row.platform is not None and row.platform == expected else "not"
            )
        identifiers_ok = (
            _ok(_ACCOUNT, row.account)
            and (row.device is None or _ok(_DEVICE, row.device))
            and (row.ip is None or _ok(_IP, row.ip))
        )
        _put_common(
            columns,
            i,
            label,
            row.t,
            row.envelope,
            row.keys,
            identifiers_ok,
            shared,
            know,
            in_window=True,
        )
    return table


# ---------------------------------------------------------------------------- OUT -------------
def _out_table(frame: Frame, know: Knowledge, shared: _Global) -> Table:
    rows = frame.out
    n = len(rows)
    columns = {spec.name: Column(n) for spec in d.ATTRIBUTES[d.Population.OUT]}
    table = Table(
        d.Population.OUT,
        n,
        [row.group for row in rows],
        [row.cluster for row in rows],
        [row.account for row in rows],
        columns,
    )
    tx_counts = Counter(tx.transaction_id for tx in frame.tx)
    tx_by_id = {tx.transaction_id: tx for tx in frame.tx}
    for i, row in enumerate(rows):
        columns["authorization_outcome"].put(i, row.authorization_outcome)
        hour, daypart, weekday = calendar(row.t)
        columns["hour"].put(i, hour)
        columns["daypart"].put(i, daypart)
        columns["weekday"].put(i, weekday)
        one = tx_counts.get(row.transaction_id, 0) == 1
        columns["tx_link"].put(i, "one" if one else "not")
        tx = tx_by_id.get(row.transaction_id)
        if tx is not None and one:
            latency = row.t - tx.t
            columns["decision_latency"].put(
                i,
                "<0"
                if latency < 0
                else band(latency, (40, 100, 250, 500, 1000), d.DECISION_LATENCY[1:]),
            )
            columns["account_match"].put(i, "equal" if row.account == tx.account else "not")
            columns["transaction_time_match"].put(
                i, "equal" if row.transaction_occurred_at == tx.envelope.occurred_at else "not"
            )
        identifiers_ok = 1 <= len(row.transaction_id) <= 64 and _ok(_ACCOUNT, row.account)
        _put_common(
            columns,
            i,
            "OUT",
            row.t,
            row.envelope,
            row.keys,
            identifiers_ok,
            shared,
            know,
            in_window=False,
        )
    return table


def compute_tables(
    frame: Frame, know: Knowledge, *, availability: AvailabilityProvider | None = None
) -> dict[d.Population, Table]:
    """Every attribute of §4 over the frame. Availability is computed only when a provider is
    given (§4.6); without one the `avail:*` columns are absent and S6 cannot be judged."""
    shared = _global(frame)
    first_reference = _first_references(frame)
    return {
        d.Population.TX: _tx_table(
            frame, know, shared, availability(frame) if availability is not None else None
        ),
        d.Population.ID: _side_table(
            d.Population.ID, frame.ident, frame, know, shared, first_reference
        ),
        d.Population.DEV: _side_table(
            d.Population.DEV, frame.dev, frame, know, shared, first_reference
        ),
        d.Population.OUT: _out_table(frame, know, shared),
    }


__all__ = [
    "AvailabilityProvider",
    "Column",
    "Table",
    "band",
    "calendar",
    "compute_tables",
    "count_bin",
    "decimals",
]
