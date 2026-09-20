"""A deterministic, seeded generator of rows shaped like `gold.observations` (ADR-0055 §2).

**Why `gold.observations`.** It is the Gold table that holds every observation, so it is the
largest, and it is the only Gold table carrying every column the query mix filters on: `stream` and
`event_id` (what `gold.read_context_rows` filters, `packages/trace_core/stream/gold.py`),
`account_id` and `occurred_at` (ADR-0015's point lookup and time-range scan). Its schema is taken
from the released declaration (`gold_features.observations_schema`), never restated: `COLUMNS` is
asserted equal to it by the unit tests.

**Pure and stdlib-only, on purpose.** Row `i` is a function of `(spec, vocabulary, i)` alone, built
from a SplitMix64 hash of the seed, a per-field salt and the index. Nothing depends on the order
rows are produced in, on how Spark splits the range, or on process state, so

- a Spark task anywhere produces exactly the rows the driver's unit tests produce, and
- the dataset is identical for every layout variant and every rerun with the same spec.

The module imports nothing from `trace_core` or pyspark: Spark pickles `rows_between` by value into
its Python workers. The vocabulary (stream names, namespaces, source tables, enum values) is read
from `trace_core` on the driver (`spec.vocabulary`) and passed in.

**Distributions** (documented choices, not measurements; the Phase 1 generator's `eval-v1`
inspection, `benchmarks/generator/eval-v1-distributions.md`, is the reference for their shape):

- **Stream mix.** Every transaction has one observed authorization outcome (APPROVED or
  DECLINED, ADR-0049); `identity_share` of rows are identity events. Gold observes an outcome per
  decided transaction, and identity events are a minority stream.
- **Arrival.** Uniform over `days` from `start_ms`, with a jitter inside each transaction's slot.
  No diurnal shape: partitions by day are equal-sized, which neither favours nor penalises
  partitioning.
- **Transaction ids.** `tx_{t:012d}` with `t` in event-time order, as the Phase 1 generator's
  `tx_{position:012d}` (`data/generator/engine.py`), so ids are time-correlated as in the real
  data. An outcome is dated 50 ms to 2.05 s after its transaction.
- **Accounts.** `floor(accounts * u ** account_skew)` with skew 2.0: the busiest 1 % of accounts
  carry 10 % of activity. A uniform draw would flatter any account-keyed layout.
- **Merchants.** The same law with skew 2.43: the busiest 1 % take 15 % of volume, as in
  `eval-v1` (15.0 %).
- **Cardinalities.** accounts = rows / 50 (about 25 transactions per account), merchants =
  rows / 667, devices 1.2 x and IPs 0.8 x accounts: `eval-v1`'s 1 M transactions over 40 k
  accounts and 3 k merchants, scaled.
- **Devices, IPs.** The account's home device 92 % and home IP 85 % of the time, otherwise
  uniform.
- **Amount.** Log-normal, median 1,808 minor units, sigma 1.05 (`eval-v1`: p50 1,808, p95/p50
  about 5.6).
- **Channel, MCC, country.** `eval-v1`'s channel shares; its top-15 MCC and country shares, fixed
  per merchant.
- **Identity events.** 80 % `IDENTITY_FAILED_LOGIN`, 20 % `IDENTITY_CHANGE`; id `idev_` + 32 hex,
  as the gateway mints.
"""

from __future__ import annotations

import bisect
import datetime as dt
import hashlib
import json
import math
from collections.abc import Iterator, Sequence
from dataclasses import asdict, dataclass
from statistics import NormalDist
from typing import Final

GENERATOR_VERSION: Final = "delta-layout-gen-v1"
"""Changes whenever a row for a given spec would change. Recorded in every run record."""

COLUMNS: Final = (
    "stream",
    "identity_namespace",
    "event_id",
    "observation_identity",
    "occurred_at",
    "occurred_ms",
    "account_id",
    "currency",
    "amount_minor",
    "card_id",
    "device_id",
    "merchant_id",
    "ip_id",
    "merchant_mcc",
    "merchant_country",
    "latitude",
    "longitude",
    "channel",
    "authorization_outcome",
    "verification",
    "correlation_id",
    "source_table",
)
"""`gold_features.observations_schema()`'s field order (asserted by the unit tests)."""

DAY_MS: Final = 86_400_000
DEFAULT_START_MS: Final = 1_767_225_600_000
"""2026-01-01T00:00:00Z, the Phase 1 generator's default `start_at`."""

_MASK: Final = (1 << 64) - 1
_GOLDEN: Final = 0x9E3779B97F4A7C15
_EPOCH: Final = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)
_UNIT: Final = 2.0**-53

# One salt per independent draw: two fields sharing a salt would be correlated.
_S_TX_TIME, _S_ACCOUNT, _S_MERCHANT, _S_AMOUNT, _S_CHANNEL = 1, 2, 3, 4, 5
_S_DEVICE, _S_DEVICE_PICK, _S_IP, _S_IP_PICK, _S_LAT, _S_LON = 6, 7, 8, 9, 10, 11
_S_CORR_HI, _S_CORR_LO, _S_OUTCOME, _S_OUTCOME_TIME = 12, 13, 14, 15
_S_ID_TIME, _S_ID_ACCOUNT, _S_ID_STREAM, _S_ID_DEVICE, _S_ID_IP, _S_ID_HI, _S_ID_LO = (
    16,
    17,
    18,
    19,
    20,
    21,
    22,
)
_S_MCC, _S_COUNTRY, _S_HOME_LAT, _S_HOME_LON = 23, 24, 25, 26

CHANNEL_WEIGHTS: Final = (
    ("CARD_PRESENT", 0.4589),
    ("CARD_NOT_PRESENT", 0.4413),
    ("ATM", 0.0601),
    ("RECURRING", 0.0397),
)
MCC_WEIGHTS: Final = (
    ("5411", 18.97),
    ("5999", 11.61),
    ("5812", 11.54),
    ("6011", 8.15),
    ("5541", 8.10),
    ("4121", 6.02),
    ("5691", 5.93),
    ("7011", 4.89),
    ("5732", 4.55),
    ("5912", 4.36),
    ("5942", 3.93),
    ("4511", 3.65),
    ("7372", 3.62),
    ("7995", 2.89),
    ("5967", 1.78),
)
COUNTRY_WEIGHTS: Final = (
    ("GB", 64.05),
    ("DE", 7.98),
    ("IE", 7.98),
    ("FR", 7.93),
    ("ES", 6.46),
    ("NL", 5.60),
)


@dataclass(frozen=True, slots=True)
class Vocabulary:
    """The released names a Gold observation carries, read from `trace_core` on the driver."""

    stream_transaction: str
    stream_outcome: str
    stream_failed_login: str
    stream_identity_change: str
    namespace_transaction: str
    namespace_outcome: str
    namespace_identity: str
    source_transactions: str
    source_outcomes: str
    source_identity: str
    outcome_approved: str
    outcome_declined: str
    verification_verified: str
    channels: tuple[str, ...]
    """Every `TransactionChannel` value, which must include each of `CHANNEL_WEIGHTS`."""


@dataclass(frozen=True, slots=True)
class GeneratorSpec:
    """Everything that determines the dataset. Equal specs generate identical rows."""

    seed: int
    rows: int
    start_ms: int = DEFAULT_START_MS
    days: int = 60
    accounts: int = 0
    merchants: int = 0
    devices: int = 0
    ips: int = 0
    identity_share: float = 0.06
    decline_share: float = 0.05
    account_skew: float = 2.0
    merchant_skew: float = 2.43
    home_device_share: float = 0.92
    home_ip_share: float = 0.85
    amount_median_minor: int = 1808
    amount_sigma: float = 1.05
    currency: str = "GBP"

    @classmethod
    def for_rows(cls, rows: int, seed: int, *, days: int = 60) -> GeneratorSpec:
        """The documented cardinalities for `rows` (module docstring)."""
        accounts = max(1_000, rows // 50)
        return cls(
            seed=seed,
            rows=rows,
            days=days,
            accounts=accounts,
            merchants=max(200, rows // 667),
            devices=accounts * 6 // 5,
            ips=accounts * 4 // 5,
        )

    def validate(self) -> None:
        problems = []
        if self.rows < 3:
            problems.append(f"rows must be at least 3, got {self.rows}")
        if not 0 <= self.seed <= _MASK:
            problems.append(f"seed must fit in 64 bits, got {self.seed}")
        for name in ("days", "accounts", "merchants", "devices", "ips"):
            if getattr(self, name) < 1:
                problems.append(f"{name} must be positive, got {getattr(self, name)}")
        for name in ("identity_share", "decline_share", "home_device_share", "home_ip_share"):
            if not 0.0 <= getattr(self, name) < 1.0:
                problems.append(f"{name} must be in [0, 1), got {getattr(self, name)}")
        if self.amount_median_minor < 1 or self.amount_sigma <= 0:
            problems.append("the amount distribution needs a positive median and sigma")
        if problems:
            raise ValueError("invalid generator spec: " + "; ".join(problems))

    @property
    def span_ms(self) -> int:
        return self.days * DAY_MS

    @property
    def identity_rows(self) -> int:
        """Identity-event rows: the requested share, plus one when the pairs leave a remainder."""
        return self.rows - 2 * self.transactions

    @property
    def transactions(self) -> int:
        """Transactions; each contributes a transaction row and an outcome row."""
        return (self.rows - round(self.rows * self.identity_share)) // 2

    def digest(self, vocabulary: Vocabulary) -> str:
        payload = {
            "generator_version": GENERATOR_VERSION,
            "spec": asdict(self),
            "vocabulary": asdict(vocabulary),
            "channels": CHANNEL_WEIGHTS,
            "mccs": MCC_WEIGHTS,
            "countries": COUNTRY_WEIGHTS,
        }
        text = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return "sha256:" + hashlib.sha256(text.encode()).hexdigest()


# ------------------------------------------------------------------ hashing ---


def _mix(x: int) -> int:
    """SplitMix64: a bijective 64-bit mixer with full avalanche."""
    z = (x + _GOLDEN) & _MASK
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _MASK
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _MASK
    return z ^ (z >> 31)


def draw(seed: int, salt: int, index: int) -> int:
    """A 64-bit value determined by `(seed, salt, index)` alone."""
    return _mix(_mix((seed ^ (salt * _GOLDEN)) & _MASK) ^ (index & _MASK))


def unit(seed: int, salt: int, index: int) -> float:
    """A uniform draw in [0, 1)."""
    return (draw(seed, salt, index) >> 11) * _UNIT


def _open_unit(seed: int, salt: int, index: int) -> float:
    """A uniform draw in (0, 1), for the inverse normal CDF."""
    return ((draw(seed, salt, index) >> 11) + 0.5) * _UNIT


def skewed_index(n: int, u: float, skew: float) -> int:
    """`floor(n * u ** skew)`: index 0 is the busiest; P(index < q * n) = q ** (1 / skew)."""
    return min(n - 1, int(n * u**skew))


def _cumulative(weights: Sequence[tuple[str, float]]) -> tuple[tuple[str, ...], tuple[float, ...]]:
    total = sum(w for _, w in weights)
    running, bounds = 0.0, []
    for _, w in weights:
        running += w / total
        bounds.append(running)
    bounds[-1] = 1.0
    return tuple(v for v, _ in weights), tuple(bounds)


def weighted(table: tuple[tuple[str, ...], tuple[float, ...]], u: float) -> str:
    values, bounds = table
    return values[min(len(values) - 1, bisect.bisect_right(bounds, u))]


_CHANNELS: Final = _cumulative(CHANNEL_WEIGHTS)
_MCCS: Final = _cumulative(MCC_WEIGHTS)
_COUNTRIES: Final = _cumulative(COUNTRY_WEIGHTS)
_NORMAL: Final = NormalDist()


# --------------------------------------------------------------- the fields ---


def transaction_id(t: int) -> str:
    return f"tx_{t:012d}"


def account_id(index: int) -> str:
    """`trace_core.domain.identifiers.ACCOUNT_FORMAT`, which the log redactor recognises."""
    return f"acct_{index:06d}"


def transaction_ms(spec: GeneratorSpec, t: int) -> int:
    step = max(1, spec.span_ms // spec.transactions)
    return (
        spec.start_ms
        + (t * spec.span_ms) // spec.transactions
        + draw(spec.seed, _S_TX_TIME, t) % step
    )


def transaction_account(spec: GeneratorSpec, t: int) -> int:
    return skewed_index(spec.accounts, unit(spec.seed, _S_ACCOUNT, t), spec.account_skew)


def identity_ms(spec: GeneratorSpec, j: int) -> int:
    n = max(1, spec.identity_rows)
    step = max(1, spec.span_ms // n)
    return spec.start_ms + (j * spec.span_ms) // n + draw(spec.seed, _S_ID_TIME, j) % step


def timestamp(ms: int) -> dt.datetime:
    """UTC-aware; exact to the millisecond (no float seconds)."""
    return _EPOCH + dt.timedelta(milliseconds=ms)


def _correlation(spec: GeneratorSpec, t: int) -> str:
    hi, lo = draw(spec.seed, _S_CORR_HI, t), draw(spec.seed, _S_CORR_LO, t)
    return f"corr_{hi:016x}{lo:016x}"


def _home(spec: GeneratorSpec, account: int) -> tuple[float, float]:
    lat = 50.0 + (draw(spec.seed, _S_HOME_LAT, account) % 800_000) / 100_000
    lon = -5.5 + (draw(spec.seed, _S_HOME_LON, account) % 750_000) / 100_000
    return lat, lon


def _device(spec: GeneratorSpec, account: int, salt_pick: int, salt: int, i: int) -> str:
    if unit(spec.seed, salt_pick, i) < spec.home_device_share:
        return f"dev_{account % spec.devices:06d}"
    return f"dev_{draw(spec.seed, salt, i) % spec.devices:06d}"


def _ip(spec: GeneratorSpec, account: int, salt_pick: int, salt: int, i: int) -> str:
    if unit(spec.seed, salt_pick, i) < spec.home_ip_share:
        return f"ip_{account % spec.ips:05d}"
    return f"ip_{draw(spec.seed, salt, i) % spec.ips:05d}"


Row = tuple[object, ...]


def _transaction_rows(spec: GeneratorSpec, vocab: Vocabulary, t: int) -> tuple[Row, Row]:
    seed = spec.seed
    tx_id = transaction_id(t)
    ms = transaction_ms(spec, t)
    account = transaction_account(spec, t)
    merchant = skewed_index(spec.merchants, unit(seed, _S_MERCHANT, t), spec.merchant_skew)
    z = _NORMAL.inv_cdf(_open_unit(seed, _S_AMOUNT, t))
    amount = max(1, round(spec.amount_median_minor * math.exp(spec.amount_sigma * z)))
    home_lat, home_lon = _home(spec, account)
    latitude = round(home_lat + (unit(seed, _S_LAT, t) - 0.5) * 0.2, 6)
    longitude = round(home_lon + (unit(seed, _S_LON, t) - 0.5) * 0.2, 6)
    correlation = _correlation(spec, t)
    acct = account_id(account)
    transaction: Row = (
        vocab.stream_transaction,
        vocab.namespace_transaction,
        tx_id,
        f"{vocab.namespace_transaction}:{tx_id}",
        timestamp(ms),
        ms,
        acct,
        spec.currency,
        amount,
        f"card_{account:06d}",
        _device(spec, account, _S_DEVICE_PICK, _S_DEVICE, t),
        f"mrch_{merchant:05d}",
        _ip(spec, account, _S_IP_PICK, _S_IP, t),
        weighted(_MCCS, unit(seed, _S_MCC, merchant)),
        weighted(_COUNTRIES, unit(seed, _S_COUNTRY, merchant)),
        latitude,
        longitude,
        weighted(_CHANNELS, unit(seed, _S_CHANNEL, t)),
        None,
        None,
        correlation,
        vocab.source_transactions,
    )
    decided = ms + 50 + draw(seed, _S_OUTCOME_TIME, t) % 2_000
    declined = unit(seed, _S_OUTCOME, t) < spec.decline_share
    outcome: Row = (
        vocab.stream_outcome,
        vocab.namespace_outcome,
        tx_id,
        f"{vocab.namespace_outcome}:{tx_id}",
        timestamp(decided),
        decided,
        acct,
        "",
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        vocab.outcome_declined if declined else vocab.outcome_approved,
        vocab.verification_verified,
        correlation,
        vocab.source_outcomes,
    )
    return transaction, outcome


def _identity_row(spec: GeneratorSpec, vocab: Vocabulary, j: int) -> Row:
    seed = spec.seed
    ms = identity_ms(spec, j)
    account = skewed_index(spec.accounts, unit(seed, _S_ID_ACCOUNT, j), spec.account_skew)
    stream = (
        vocab.stream_failed_login
        if unit(seed, _S_ID_STREAM, j) < 0.8
        else vocab.stream_identity_change
    )
    idev = f"idev_{draw(seed, _S_ID_HI, j):016x}{draw(seed, _S_ID_LO, j):016x}"
    return (
        stream,
        vocab.namespace_identity,
        idev,
        f"{vocab.namespace_identity}:{idev}",
        timestamp(ms),
        ms,
        account_id(account),
        "",
        0,
        None,
        _device(spec, account, _S_ID_DEVICE, _S_ID_DEVICE + 100, j),
        None,
        _ip(spec, account, _S_ID_IP, _S_ID_IP + 100, j),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        idev,
        vocab.source_identity,
    )


def row(spec: GeneratorSpec, vocab: Vocabulary, i: int) -> Row:
    """Row `i` of the dataset: rows `2t` and `2t + 1` are transaction `t` and its outcome, and
    the rows after every pair are identity events."""
    if not 0 <= i < spec.rows:
        raise IndexError(f"row {i} is outside [0, {spec.rows})")
    pairs = 2 * spec.transactions
    if i < pairs:
        return _transaction_rows(spec, vocab, i // 2)[i % 2]
    return _identity_row(spec, vocab, i - pairs)


def rows_between(spec: GeneratorSpec, vocab: Vocabulary, start: int, stop: int) -> Iterator[Row]:
    """Rows `[start, stop)`, in index order. A transaction's two rows are built once."""
    pairs = 2 * spec.transactions
    i = max(0, start)
    stop = min(stop, spec.rows)
    while i < stop:
        if i < pairs:
            transaction, outcome = _transaction_rows(spec, vocab, i // 2)
            if i % 2 == 0:
                yield transaction
                if i + 1 < stop:
                    yield outcome
                i += 2
            else:
                yield outcome
                i += 1
        else:
            yield _identity_row(spec, vocab, i - pairs)
            i += 1


def rows_for_slice(spec: GeneratorSpec, vocab: Vocabulary, bounds: tuple[int, int]) -> list[Row]:
    """One Spark task's rows: `bounds` is `(start, stop)`."""
    return list(rows_between(spec, vocab, bounds[0], bounds[1]))


def split(start: int, stop: int, parts: int) -> list[tuple[int, int]]:
    """`[start, stop)` as at most `parts` contiguous, non-empty, covering ranges."""
    if stop <= start:
        return []
    parts = max(1, min(parts, stop - start))
    size, extra = divmod(stop - start, parts)
    out, lo = [], start
    for k in range(parts):
        hi = lo + size + (1 if k < extra else 0)
        out.append((lo, hi))
        lo = hi
    return out


def batches(rows: int, batch_rows: int) -> list[tuple[int, int]]:
    """The bounded write batches covering `[0, rows)`, each at most `batch_rows` rows."""
    if batch_rows < 1:
        raise ValueError(f"batch_rows must be positive, got {batch_rows}")
    return [(lo, min(rows, lo + batch_rows)) for lo in range(0, rows, batch_rows)]


__all__ = [
    "COLUMNS",
    "DAY_MS",
    "DEFAULT_START_MS",
    "GENERATOR_VERSION",
    "GeneratorSpec",
    "Row",
    "Vocabulary",
    "account_id",
    "batches",
    "draw",
    "row",
    "rows_between",
    "rows_for_slice",
    "skewed_index",
    "split",
    "timestamp",
    "transaction_account",
    "transaction_id",
    "transaction_ms",
    "unit",
]
