"""The Redis online feature store (ADR-0003, ADR-0034, ADR-0044, ADR-0046).

**One atomic script per scored transaction.** `score` records the transaction and reads its whole
context in a single Lua script, so the transaction is inside its own transactional windows
(ADR-0046 §2) and no other observation can land between the write and the read. Redis refuses a
writing script before it starts when the store is full (`noeviction`), so a refused write changes
nothing -- it never leaves the counters and the distinct sets disagreeing.

**What is stored, and for how long.** Derived from the released declarations, never guessed:

* **Every observation once, under its namespaced identity** (ADR-0046 §1). The first delivery is
  the observation whatever a redelivery carries -- another amount, another account -- so identity
  is checked across the whole store, not per account.
* **Account transactions, and identity events one set per stream, raw**, for the widest account
  lookback plus the late-arrival margin (25 h). A score-time read is bounded at any depth
  (ADR-0046 §8): every window count is a range count, the previous transaction and the latest
  identity change are one bounded lookup each, and only the most recent `SCORE_READ_CAP`
  transactions at or before `as_of` are decoded. Sums, distinct counts and the profile are computed
  from those by the reference implementation's own reductions (`reference.window_state`,
  `previous_observation`, `lifetime_profile`), so the two cannot drift. A window holding more than
  the cap keeps its count and serves its content as absent; a capped profile keeps what is still
  exact (`reference.depth_capped_profile`).
* **Beyond that, account transactions are folded, in event-time order, into a bounded profile
  prefix**: the current lifetime run's start, visit counters, known devices, the last 128 amounts
  per currency and the last 20 located points. A profile is the prefix plus the raw lifetime.
* **Cards**: identities scored by event time. **Devices**: account and identity, scored by event
  time; a device window holding more than the cap serves no distinct-account count. **IPs and
  merchants**: five-minute HyperLogLogs of accounts (the declared APPROXIMATE estimand).
* **Authorization outcomes** (ADR-0049 §6): each once, under its namespaced identity, with a
  verdict. One whose transaction the store holds with the same account is verified and joins the
  account's verified outcomes, and its declines, scored by outcome time. One whose transaction is
  not held waits as pending, and that transaction's own record verifies or rejects it. Kept for the
  widest outcome window plus the late-arrival margin; PostgreSQL, not this store, decides
  duplicates and conflicts.
* **Merchant amounts**: per-currency minute buckets of count, sum and sum of squares, for the
  minute-aligned coefficient of variation. Sums are kept as base-1e9 limbs: the square of the
  largest released amount does not fit Redis's 64-bit `HINCRBY`, and a script that fails part-way
  is not rolled back.

Trims are relative to the observation's event time, never ahead of the clock. Expiry only collects
keys nothing writes any more: it is relative to the later of event time and the Redis clock, and a
late observation never shortens it.

**Declared limits** (ADR-0046 §5):

* An observation folded into the prefix is no longer recognised as a redelivery, and one that
  arrives behind the prefix's frontier cannot be placed in order. Either way the fold marks that
  lifetime run inexact, and profile features read over it are absent rather than wrong.
* Each structure keeps what its widest window reads, plus the late-arrival margin, behind the
  newest observation. A read further behind -- a late arrival, or a redelivery read at its first
  delivery's time -- gets no value for a declared window reaching behind what is still held, and
  its context stops vouching for that window (`history_incomplete`): absent, never a lower bound.
  A previous observation is served only when nothing that could be later has been folded away.
* An outcome is verified only against a transaction the store still holds. One arriving after its
  transaction was folded away -- decided a day or more after it, or delivered further behind than
  the margin -- stays pending as served where a complete history verifies it; so does one whose
  transaction arrives after the pending outcome expired, the margin past the later of its outcome
  time and the clock. No released producer does either: DM-1 decides within seconds.
* A capped profile sees the folded prefix only when nothing unread can hide a lifetime gap between
  them. Where a late read has folded past the raw history, or a snapshot still holds older raw
  transactions, it serves an absence where a complete as-served log still has a value.
* Identity events recorded before ADR-0046 §8 shared one account set. It is never migrated: an
  account's identity windows are not held while it exists, it is given an expiry within the 25 hours
  identity events are held, and a read reaching behind what it held stays unheld after it has gone.
* The script touches keys of several entities, so it cannot run on a Redis Cluster as written.

Redis is authoritative for nothing (ADR-0003): it holds derived state that Phase 3 can rebuild.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from redis.exceptions import OutOfMemoryError

from trace_core.contracts.api.transaction import AMOUNT_MINOR_MAX
from trace_core.domain.enums import AuthorizationOutcome, FeatureSource, TransactionChannel
from trace_core.domain.errors import ContractError, FeatureWriteFailedError
from trace_core.domain.time import EventTime, from_millis, to_millis
from trace_core.features.context import FeatureContext, Observation, Profile, WindowState
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.features.observation import (
    Event,
    IdentityNamespace,
    ObserveReceipt,
    ServedRead,
    Verification,
)
from trace_core.features.profile_math import geodesic_medoid, robust_centre
from trace_core.features.reference import (
    ReadScope,
    depth_capped_profile,
    lifetime_profile,
    previous_observation,
    window_state,
)
from trace_core.features.semantics import (
    ALIGNED_MINUTE_MS,
    AMOUNT_SAMPLE_SIZE,
    APPROXIMATE_BUCKET_MS,
    HABITUAL_MIN_VISITS,
    HOME_SAMPLE_SIZE,
    LATE_ARRIVAL_MARGIN_S,
    PROFILE_LIFETIME_GAP_S,
    SCORE_READ_CAP,
    Aggregation,
    CardinalityStorage,
    CurrentObservation,
    Dimension,
    Entity,
    EvaluationMode,
    PairwiseWithPrevious,
    ProfileAttribute,
    Stream,
    Window,
    WindowedAggregate,
    reads_observation_content,
)
from trace_core.features.state_plan import PLAN

if TYPE_CHECKING:  # pragma: no cover - typing only
    from redis import Redis

BUCKET_TRIM_SWEEP: Final = 3
"""How many just-expired minute buckets each merchant write deletes (Phase 2's measured fix)."""

LIMB_BASE: Final = 10**9
LIMBS: Final = len(str(AMOUNT_MINOR_MAX**2)) // 9 + 1
"""Base-1e9 digits per merchant sum: enough for the square of the largest released amount. Each
digit field overflows only after nine billion increments in one minute."""

_GAP_MS: Final = PROFILE_LIFETIME_GAP_S * 1000
IDENTITY_RAW_KEYS: Final[dict[Stream, str]] = {
    Stream.IDENTITY_FAILED_LOGIN: "fl",
    Stream.IDENTITY_CHANGE: "ic",
}
"""Each identity stream's raw set, `<ns>:ie:<account>:<suffix>` (ADR-0046 §8). The account set
`<ns>:ie:<account>` both streams shared before it is legacy: read as not held, never written."""
_WRITABLE_STREAMS: Final = frozenset(
    {Stream.TRANSACTION, Stream.AUTHORIZATION_OUTCOME, *IDENTITY_RAW_KEYS}
)
_NON_ACCOUNT_SHAPES: Final = frozenset(
    {
        (Entity.CARD, Aggregation.COUNT, None, None),
        (Entity.DEVICE, Aggregation.DISTINCT_COUNT, Dimension.ACCOUNT, CardinalityStorage.EXACT),
        (Entity.IP, Aggregation.DISTINCT_COUNT, Dimension.ACCOUNT, CardinalityStorage.APPROXIMATE),
        (
            Entity.MERCHANT,
            Aggregation.DISTINCT_COUNT,
            Dimension.ACCOUNT,
            CardinalityStorage.APPROXIMATE,
        ),
        (Entity.MERCHANT, Aggregation.AMOUNT_CV, None, None),
    }
)


def unsupported_declarations() -> list[str]:
    """Every released feature shape this store cannot serve. Must be empty (a test asserts it):
    a feature the store does not know how to answer would otherwise read as absent forever."""
    problems: list[str] = []
    for spec in ONLINE_FEATURES:
        shape = spec.semantics
        if isinstance(shape, WindowedAggregate):
            if shape.entity is Entity.ACCOUNT:
                if shape.stream is Stream.AUTHORIZATION_OUTCOME:
                    if shape.aggregation is not Aggregation.DECLINED_RATIO:
                        problems.append(f"{spec.feature_id}: outcome state answers only the ratio")
                elif shape.stream is Stream.TRANSACTION:
                    if shape.aggregation is not Aggregation.COUNT and not (
                        reads_observation_content(shape)
                    ):
                        problems.append(
                            f"{spec.feature_id}: account transactions are served as range counts "
                            f"or capped content, not {shape.aggregation}"
                        )
                elif shape.aggregation is not Aggregation.COUNT:
                    problems.append(f"{spec.feature_id}: identity streams are range counts only")
                continue
            key = (shape.entity, shape.aggregation, shape.dimension, shape.storage)
            if key not in _NON_ACCOUNT_SHAPES or shape.stream is not Stream.TRANSACTION:
                problems.append(f"{spec.feature_id}: no primitive for {key} on {shape.stream}")
            if shape.current_observation is not CurrentObservation.INCLUDED:
                problems.append(f"{spec.feature_id}: only INCLUDED non-account windows are stored")
        elif isinstance(shape, (PairwiseWithPrevious, ProfileAttribute)):
            if isinstance(shape, PairwiseWithPrevious) and shape.stream not in (
                Stream.TRANSACTION,
                *IDENTITY_RAW_KEYS,
            ):
                problems.append(f"{spec.feature_id}: no previous observation on {shape.stream}")
            if shape.entity is not Entity.ACCOUNT:
                problems.append(
                    f"{spec.feature_id}: only account profiles and previous observations"
                )
    return problems


def _windows_of(entity: Entity, aggregation: Aggregation) -> tuple[Window, ...]:
    found = {
        spec.semantics.window
        for spec in ONLINE_FEATURES
        if isinstance(spec.semantics, WindowedAggregate)
        and spec.semantics.entity is entity
        and spec.semantics.aggregation is aggregation
    }
    return tuple(sorted(found, key=lambda w: w.seconds))


def _account_windows(stream: Stream) -> tuple[Window, ...]:
    found = {
        spec.semantics.window
        for spec in ONLINE_FEATURES
        if isinstance(spec.semantics, WindowedAggregate)
        and spec.semantics.entity is Entity.ACCOUNT
        and spec.semantics.stream is stream
    }
    return tuple(sorted(found, key=lambda w: w.seconds))


def _excluded(entity: Entity, stream: Stream, window: Window) -> bool:
    """Whether the released features declare this window without the current observation."""
    return any(
        isinstance(spec.semantics, WindowedAggregate)
        and (spec.semantics.entity, spec.semantics.stream, spec.semantics.window)
        == (entity, stream, window)
        and spec.semantics.current_observation is CurrentObservation.EXCLUDED
        for spec in ONLINE_FEATURES
    )


def _retention_ms(windows: tuple[Window, ...], extra_ms: int = 0) -> int:
    """How long a structure keeps what its widest window reads: that window plus the margin."""
    widest = max((w.seconds for w in windows), default=0)
    return (widest + LATE_ARRIVAL_MARGIN_S) * 1000 + extra_ms


@dataclass(frozen=True, slots=True)
class Layout:
    """Retentions and read windows, derived from the declarations."""

    raw_tx_ms: int
    raw_ie_ms: int
    card_windows: tuple[Window, ...]
    device_windows: tuple[Window, ...]
    ip_windows: tuple[Window, ...]
    merchant_distinct_windows: tuple[Window, ...]
    cv_windows: tuple[Window, ...]
    outcome_windows: tuple[Window, ...]
    account_windows: dict[Stream, tuple[Window, ...]]
    """Every declared account window on each raw stream, each served by a range count (§8)."""
    identity_streams: tuple[Stream, ...] = tuple(IDENTITY_RAW_KEYS)

    @property
    def card_ms(self) -> int:
        return _retention_ms(self.card_windows)

    @property
    def device_ms(self) -> int:
        return _retention_ms(self.device_windows)

    @property
    def sketch_ms(self) -> int:
        """A sketch bucket is read whole when any part of it is inside the window."""
        return _retention_ms(
            self.ip_windows + self.merchant_distinct_windows, APPROXIMATE_BUCKET_MS
        )

    @property
    def cv_ms(self) -> int:
        return _retention_ms(self.cv_windows, ALIGNED_MINUTE_MS)

    @property
    def outcome_ms(self) -> int:
        """Verified outcomes, and pending ones past the later of outcome time and the clock."""
        return _retention_ms(self.outcome_windows)

    def config_json(self) -> str:
        def ms(windows: tuple[Window, ...]) -> list[int]:
            return [w.seconds * 1000 for w in windows]

        def ranged(stream: Stream) -> list[list[int]]:
            """`[window ms, 1 when the current observation is excluded]`, per account window."""
            return [
                [w.seconds * 1000, int(_excluded(Entity.ACCOUNT, stream, w))]
                for w in self.account_windows[stream]
            ]

        def previous_ms(stream: Stream) -> int:
            lookback = PLAN.previous.get((Entity.ACCOUNT, stream))
            return 0 if lookback is None else lookback.seconds * 1000

        return json.dumps(
            {
                "raw_tx_ms": self.raw_tx_ms,
                "raw_ie_ms": self.raw_ie_ms,
                "profile_ms": _GAP_MS + LATE_ARRIVAL_MARGIN_S * 1000,
                "gap_ms": _GAP_MS,
                "amount_sample": AMOUNT_SAMPLE_SIZE,
                "home_sample": HOME_SAMPLE_SIZE,
                "limbs": LIMBS,
                "card_ms": self.card_ms,
                "dev_ms": self.device_ms,
                "hll_bucket_ms": APPROXIMATE_BUCKET_MS,
                "hll_ms": self.sketch_ms,
                "minute_ms": ALIGNED_MINUTE_MS,
                "cv_ms": self.cv_ms,
                "cv_sweep": BUCKET_TRIM_SWEEP,
                "card_windows": ms(self.card_windows),
                "dev_windows": ms(self.device_windows),
                "ip_windows": ms(self.ip_windows),
                "mer_windows": ms(self.merchant_distinct_windows),
                "cv_windows": ms(self.cv_windows),
                "ao_ms": self.outcome_ms,
                "ao_windows": ms(self.outcome_windows),
                "cap": SCORE_READ_CAP,
                "tx_windows": ranged(Stream.TRANSACTION),
                "tx_prev_ms": previous_ms(Stream.TRANSACTION),
                "ie_keys": {stream.value: suffix for stream, suffix in IDENTITY_RAW_KEYS.items()},
                "ie_streams": [
                    {
                        "key": IDENTITY_RAW_KEYS[stream],
                        "windows": ranged(stream),
                        "prev_ms": previous_ms(stream),
                    }
                    for stream in self.identity_streams
                ],
                "tx_ns": IdentityNamespace.TRANSACTION.value,
                "ao_ns": IdentityNamespace.TRANSACTION_AUTHORIZATION.value,
            },
            separators=(",", ":"),
        )


def _layout() -> Layout:
    identity_streams = tuple(IDENTITY_RAW_KEYS)
    return Layout(
        raw_tx_ms=PLAN.account_raw_ms((Stream.TRANSACTION,)),
        raw_ie_ms=PLAN.account_raw_ms(identity_streams),
        card_windows=_windows_of(Entity.CARD, Aggregation.COUNT),
        device_windows=_windows_of(Entity.DEVICE, Aggregation.DISTINCT_COUNT),
        ip_windows=_windows_of(Entity.IP, Aggregation.DISTINCT_COUNT),
        merchant_distinct_windows=_windows_of(Entity.MERCHANT, Aggregation.DISTINCT_COUNT),
        cv_windows=_windows_of(Entity.MERCHANT, Aggregation.AMOUNT_CV),
        outcome_windows=_windows_of(Entity.ACCOUNT, Aggregation.DECLINED_RATIO),
        account_windows={
            stream: _account_windows(stream) for stream in (Stream.TRANSACTION, *identity_streams)
        },
        identity_streams=identity_streams,
    )


LAYOUT: Final = _layout()
_DECLARED_WINDOWS: Final = frozenset(
    (spec.semantics.entity, spec.semantics.stream, spec.semantics.window)
    for spec in ONLINE_FEATURES
    if isinstance(spec.semantics, WindowedAggregate)
)

_COMMON_LUA: Final = r"""
local ns = ARGV[1]
local cfg = cjson.decode(ARGV[2])
local function key(...)
  return ns .. ':' .. table.concat({...}, ':')
end
local function fmt(x)
  return string.format('%.0f', x)
end
local function present(v)
  return v ~= nil and v ~= cjson.null and v ~= false and v ~= ''
end
local function bytes_le(a, b)
  local la, lb = #a, #b
  for i = 1, math.min(la, lb) do
    local ca, cb = string.byte(a, i), string.byte(b, i)
    if ca ~= cb then return ca < cb end
  end
  return la <= lb
end
local function observations(identities)
  local out = {}
  for i = 1, #identities, 200 do
    local chunk = {}
    for j = i, math.min(i + 199, #identities) do chunk[#chunk + 1] = key('obs', identities[j]) end
    local got = redis.call('MGET', unpack(chunk))
    for j = 1, #chunk do
      if got[j] then out[#out + 1] = got[j] end
    end
  end
  return out
end
"""

_READ_LUA: Final = r"""
local function read(as_of, acct, card, dev, mer, ip, cur, own)
  local reply = {}
  reply[1] = redis.call('GET', key('epoch')) or ''
  local tx = key('tx', acct)
  -- The most recent `cap` transactions at or before as_of, newest first: the only transaction
  -- content a read decodes, at any depth (ADR-0046 §8).
  local newest = redis.call('ZREVRANGEBYSCORE', tx, fmt(as_of), '-inf', 'LIMIT', 0, cfg.cap)
  reply[2] = observations(newest)
  -- Identity events still in the account set they shared before §8: not held while it exists.
  reply[3] = redis.call('ZCOUNT', key('ie', acct), '-inf', '+inf')
  reply[4] = redis.call('HGETALL', key('pf', acct))
  reply[5] = redis.call('HGETALL', key('pfh', acct))
  reply[6] = redis.call('LRANGE', key('pfa', acct, cur), 0, -1)
  reply[7] = redis.call('LRANGE', key('pfl', acct), 0, -1)
  local card_counts = {}
  for i, w in ipairs(cfg.card_windows) do
    if present(card) then
      card_counts[i] = redis.call('ZCOUNT', key('card', card), '(' .. fmt(as_of - w), fmt(as_of))
    else
      card_counts[i] = 0
    end
  end
  reply[8] = card_counts
  local dev_counts = {}
  for i, w in ipairs(cfg.dev_windows) do
    local n, distinct = 0, 0
    if present(dev) then
      local dk = key('dev', dev)
      n = redis.call('ZCOUNT', dk, '(' .. fmt(as_of - w), fmt(as_of))
      if n > cfg.cap then
        distinct = -1  -- capped: the count is exact, the members are not read (ADR-0046 §8)
      else
        local seen = {}
        for _, m in ipairs(redis.call('ZRANGEBYSCORE', dk, '(' .. fmt(as_of - w), fmt(as_of))) do
          -- `<length of account>|<account><identity>`: no separator can collide with an id.
          local sep = string.find(m, '|', 1, true)
          local a = string.sub(m, sep + 1, sep + tonumber(string.sub(m, 1, sep - 1)))
          if not seen[a] then
            seen[a] = true
            distinct = distinct + 1
          end
        end
      end
    end
    dev_counts[i] = {n, distinct}
  end
  reply[9] = dev_counts
  local function hll(entity, id, windows)
    local counts = {}
    for i, w in ipairs(windows) do
      if present(id) then
        local keys = {}
        local first = math.floor((as_of - w) / cfg.hll_bucket_ms)
        local last = math.floor(as_of / cfg.hll_bucket_ms)
        for b = first, last do keys[#keys + 1] = key('hll', entity, id, fmt(b)) end
        counts[i] = redis.call('PFCOUNT', unpack(keys))
      else
        counts[i] = 0
      end
    end
    return counts
  end
  reply[10] = hll('IP', ip, cfg.ip_windows)
  reply[11] = hll('MERCHANT', mer, cfg.mer_windows)
  local cv = {}
  for i, w in ipairs(cfg.cv_windows) do
    local picked = {}
    if present(mer) and present(cur) then
      local fields = redis.call('HGETALL', key('mcv', mer, cur))
      local lower = math.floor((as_of - w) / cfg.minute_ms)
      local upper = math.floor(as_of / cfg.minute_ms)
      for j = 1, #fields, 2 do
        local m = tonumber(string.match(fields[j], '^(-?%d+):'))
        if m and m > lower and m < upper then
          picked[#picked + 1] = fields[j]
          picked[#picked + 1] = fields[j + 1]
        end
      end
    end
    cv[i] = picked
  end
  reply[12] = cv
  reply[13] = redis.call('GET', key('position')) or '0'
  reply[14] = redis.call('GET', key('hw')) or ''
  -- Verified outcomes decided strictly inside (as_of - W, as_of), and the declines among them,
  -- without the scored transaction's own outcome whatever its time (ADR-0049 §5).
  local outcomes = {}
  for i, w in ipairs(cfg.ao_windows) do
    local counts = {}
    for j, set in ipairs({key('ao', acct), key('aod', acct)}) do
      local n = redis.call('ZCOUNT', set, '(' .. fmt(as_of - w), '(' .. fmt(as_of))
      if present(own) then
        local s = tonumber(redis.call('ZSCORE', set, own) or '')
        if s ~= nil and s > as_of - w and s < as_of then n = n - 1 end
      end
      counts[j] = n
    end
    outcomes[i] = counts
  end
  reply[15] = outcomes
  -- Exact at any depth (ADR-0046 §8): window counts are range counts, and the previous transaction
  -- and each identity stream's latest observation are one bounded lookup each.
  local function range_count(k, w)
    local upper = (w[2] == 1 and '(' or '') .. fmt(as_of)
    return redis.call('ZCOUNT', k, '(' .. fmt(as_of - w[1]), upper)
  end
  local tx_counts = {}
  for i, w in ipairs(cfg.tx_windows) do tx_counts[i] = range_count(tx, w) end
  reply[16] = tx_counts
  local oldest = redis.call('ZRANGEBYSCORE', tx, '-inf', fmt(as_of), 'WITHSCORES', 'LIMIT', 0, 1)
  reply[17] = {redis.call('ZCOUNT', tx, '-inf', fmt(as_of)), oldest[1] or '', oldest[2] or ''}
  local previous = {}
  if cfg.tx_prev_ms > 0 then
    previous = observations(redis.call('ZREVRANGEBYSCORE', tx, '(' .. fmt(as_of),
      '(' .. fmt(as_of - cfg.tx_prev_ms), 'LIMIT', 0, 1))
  end
  reply[18] = previous
  local streams = {}
  for i, s in ipairs(cfg.ie_streams) do
    local k = key('ie', acct, s.key)
    local counts, latest = {}, {}
    for j, w in ipairs(s.windows) do counts[j] = range_count(k, w) end
    if s.prev_ms > 0 then
      latest = redis.call('ZREVRANGEBYSCORE', k, '(' .. fmt(as_of), '(' .. fmt(as_of - s.prev_ms),
        'WITHSCORES', 'LIMIT', 0, 1)
    end
    streams[i] = {counts, latest}
  end
  reply[19] = streams
  return reply
end
"""

_WRITE_LUA: Final = r"""
local function expire_at(k, event_ms, clock, retention_ms)
  -- Relative to the later of event time and the clock, and never shortened: an observation
  -- accepted ahead of the clock stays readable at its own time, and a late one cannot cut short
  -- a key that newer observations keep alive. NX gives a new key its first expiry; GT extends.
  local at = fmt(math.max(event_ms, clock) + retention_ms)
  if redis.call('PEXPIREAT', k, at, 'NX') == 0 then
    redis.call('PEXPIREAT', k, at, 'GT')
  end
end
local function reset_run(acct)
  redis.call('DEL', key('pfh', acct), key('pfl', acct))
  local currencies = redis.call('SMEMBERS', key('pfc', acct))
  for _, c in ipairs(currencies) do redis.call('DEL', key('pfa', acct, c)) end
  redis.call('DEL', key('pfc', acct))
  redis.call('HDEL', key('pf', acct), 'run_start_ms', 'inexact')
end
local function fold_one(acct, id, ms, js)
  local pf = key('pf', acct)
  local through = tonumber(redis.call('HGET', pf, 'folded_through_ms') or '')
  if through == nil or ms > through then redis.call('HSET', pf, 'folded_through_ms', fmt(ms)) end
  local last_ms = tonumber(redis.call('HGET', pf, 'last_ms') or '')
  if last_ms ~= nil then
    local last_id = redis.call('HGET', pf, 'last_id') or ''
    if ms < last_ms or (ms == last_ms and bytes_le(id, last_id)) then
      redis.call('HSET', pf, 'inexact', '1')
      return
    end
    if ms - last_ms >= cfg.gap_ms then reset_run(acct) end
  end
  if not redis.call('HGET', pf, 'run_start_ms') then
    redis.call('HSET', pf, 'run_start_ms', fmt(ms))
  end
  local r = cjson.decode(js)
  local counters = key('pfh', acct)
  if present(r.mer) then redis.call('HINCRBY', counters, 'm:' .. r.mer, 1) end
  if present(r.mcc) then redis.call('HINCRBY', counters, 'c:' .. r.mcc, 1) end
  if present(r.dev) then redis.call('HINCRBY', counters, 'd:' .. r.dev, 1) end
  local amounts = key('pfa', acct, r.cur)
  redis.call('RPUSH', amounts, fmt(math.abs(r.amt)))
  redis.call('LTRIM', amounts, -cfg.amount_sample, -1)
  redis.call('SADD', key('pfc', acct), r.cur)
  if present(r.lat) and present(r.lon) then
    local located = key('pfl', acct)
    redis.call('RPUSH', located, fmt(ms) .. '\t' .. string.format('%.17g', r.lat) .. '\t' ..
      string.format('%.17g', r.lon) .. '\t' .. id)
    redis.call('LTRIM', located, -cfg.home_sample, -1)
  end
  redis.call('HSET', pf, 'last_ms', fmt(ms), 'last_id', id)
end
local function fold(acct, raw, cutoff)
  local old = redis.call('ZRANGEBYSCORE', raw, '-inf', '(' .. fmt(cutoff), 'WITHSCORES')
  if #old == 0 then return end
  for i = 1, #old, 2 do
    local id = old[i]
    local js = redis.call('GET', key('obs', id))
    if js then
      fold_one(acct, id, tonumber(old[i + 1]), js)
      redis.call('DEL', key('obs', id))
    end
  end
  redis.call('ZREMRANGEBYSCORE', raw, '-inf', '(' .. fmt(cutoff))
end
local function drop(acct, raw, cutoff)
  local old = redis.call('ZRANGEBYSCORE', raw, '-inf', '(' .. fmt(cutoff), 'WITHSCORES')
  if #old == 0 then return end
  local pf = key('pf', acct)
  local through = tonumber(redis.call('HGET', pf, 'dropped_through_ms') or '')
  for i = 1, #old, 2 do
    redis.call('DEL', key('obs', old[i]))
    local ms = tonumber(old[i + 1])
    if through == nil or ms > through then through = ms end
  end
  redis.call('HSET', pf, 'dropped_through_ms', fmt(through))
  redis.call('ZREMRANGEBYSCORE', raw, '-inf', '(' .. fmt(cutoff))
end
local function cv_fields(minute)
  local prefix = fmt(minute) .. ':'
  local fields = {prefix .. 'c'}
  for k = 0, cfg.limbs - 1 do
    fields[#fields + 1] = prefix .. 's' .. k
    fields[#fields + 1] = prefix .. 'q' .. k
  end
  return fields
end
local function add_verified(acct, id, ms, outcome, ref, clock)
  local sets = {key('ao', acct)}
  if outcome == 'DECLINED' then sets[2] = key('aod', acct) end
  for _, k in ipairs(sets) do
    -- NX: an outcome is added once, at its first delivery's outcome time.
    redis.call('ZADD', k, 'NX', fmt(ms), id)
    redis.call('ZREMRANGEBYSCORE', k, '-inf', '(' .. fmt(ref - cfg.ao_ms))
    expire_at(k, ms, clock, cfg.ao_ms)
  end
end
local function reconcile(acct, id, ref, clock)
  -- A transaction recorded while an outcome for it is pending verifies or rejects that outcome by
  -- its account. Its own read never counts it: it is its own outcome (ADR-0049 §6).
  local verdict = key('aov', id)
  if redis.call('GET', verdict) ~= 'PENDING' then return end
  local pending = redis.call('GET', key('obs', cfg.ao_ns .. ':' .. id))
  if not pending then return end
  local o = cjson.decode(pending)
  if o.a == acct then
    add_verified(acct, id, o.ms, o.out, ref, clock)
    redis.call('SET', verdict, 'VERIFIED', 'KEEPTTL')
  else
    redis.call('SET', verdict, 'REJECTED', 'KEEPTTL')
  end
end
local function record(ev, identity, amount, amount_sq, now, js)
  local observation = key('obs', identity)
  local is_outcome = ev.s == 'AUTHORIZATION_OUTCOME'
  local existing = redis.call('GET', observation)
  if existing then
    return 0, existing, is_outcome and (redis.call('GET', key('aov', ev.id)) or '') or ''
  end
  local t = redis.call('TIME')
  local clock = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  local acct = ev.a
  redis.call('INCR', key('position'))
  redis.call('SET', key('epoch'), fmt(now), 'NX')
  redis.call('SET', observation, js)
  local ref = math.min(ev.ms, now)
  -- The newest instant any trim has been relative to: nothing later than
  -- `hw - retention` has been trimmed from a structure with that retention.
  local high = tonumber(redis.call('GET', key('hw')) or '')
  if high == nil or ref > high then redis.call('SET', key('hw'), fmt(ref)) end
  if is_outcome then
    -- Verified only against the transaction the store holds, with the same account.
    expire_at(observation, ev.ms, clock, cfg.ao_ms)
    local verdict = 'PENDING'
    local transaction = redis.call('GET', key('obs', cfg.tx_ns .. ':' .. ev.id))
    if transaction then
      if cjson.decode(transaction).a == acct then
        verdict = 'VERIFIED'
        add_verified(acct, ev.id, ev.ms, ev.out, ref, clock)
      else
        verdict = 'REJECTED'
      end
    end
    local vk = key('aov', ev.id)
    redis.call('SET', vk, verdict)
    expire_at(vk, ev.ms, clock, cfg.ao_ms)
    return 1, js, verdict
  end
  local is_tx = ev.s == 'TRANSACTION'
  -- Identity events: one set per stream (ADR-0046 §8), so a failed-login count reads no changes.
  local raw = is_tx and key('tx', acct) or key('ie', acct, cfg.ie_keys[ev.s])
  expire_at(observation, ev.ms, clock, cfg.profile_ms)
  redis.call('ZADD', raw, fmt(ev.ms), identity)
  if is_tx then
    if present(ev.card) then
      local ck = key('card', ev.card)
      redis.call('ZADD', ck, fmt(ev.ms), identity)
      redis.call('ZREMRANGEBYSCORE', ck, '-inf', '(' .. fmt(ref - cfg.card_ms))
      expire_at(ck, ev.ms, clock, cfg.card_ms)
    end
    if present(ev.dev) then
      local dk = key('dev', ev.dev)
      redis.call('ZADD', dk, fmt(ev.ms), #acct .. '|' .. acct .. identity)
      redis.call('ZREMRANGEBYSCORE', dk, '-inf', '(' .. fmt(ref - cfg.dev_ms))
      expire_at(dk, ev.ms, clock, cfg.dev_ms)
    end
    local bucket = fmt(math.floor(ev.ms / cfg.hll_bucket_ms))
    if present(ev.ip) then
      local hk = key('hll', 'IP', ev.ip, bucket)
      redis.call('PFADD', hk, acct)
      expire_at(hk, ev.ms, clock, cfg.hll_ms)
    end
    if present(ev.mer) then
      local mk = key('hll', 'MERCHANT', ev.mer, bucket)
      redis.call('PFADD', mk, acct)
      expire_at(mk, ev.ms, clock, cfg.hll_ms)
      local cvk = key('mcv', ev.mer, ev.cur)
      local prefix = fmt(math.floor(ev.ms / cfg.minute_ms)) .. ':'
      redis.call('HINCRBY', cvk, prefix .. 'c', 1)
      for k = 1, cfg.limbs do
        if amount[k] ~= '0' then
          redis.call('HINCRBY', cvk, prefix .. 's' .. (k - 1), amount[k])
        end
        if amount_sq[k] ~= '0' then
          redis.call('HINCRBY', cvk, prefix .. 'q' .. (k - 1), amount_sq[k])
        end
      end
      local oldest_kept = math.floor((ref - cfg.cv_ms) / cfg.minute_ms)
      for m = oldest_kept - cfg.cv_sweep, oldest_kept - 1 do
        redis.call('HDEL', cvk, unpack(cv_fields(m)))
      end
      expire_at(cvk, ev.ms, clock, cfg.cv_ms)
    end
    reconcile(acct, ev.id, ref, clock)
    fold(acct, raw, ref - cfg.raw_tx_ms)
  else
    drop(acct, raw, ref - cfg.raw_ie_ms)
  end
  -- The account set identity events shared before ADR-0046 §8 is never migrated -- moving a deep
  -- set in one script is the stall §8 removes -- and never written again; reads treat it as not
  -- held. Everything it held is marked dropped, so a read reaching behind it stays unheld once it
  -- has gone, and it is given an expiry within the time identity events are held.
  local legacy = key('ie', acct)
  local legacy_newest = redis.call('ZREVRANGEBYSCORE', legacy, '+inf', '-inf', 'WITHSCORES',
    'LIMIT', 0, 1)
  if #legacy_newest > 0 then
    local pf = key('pf', acct)
    local through = tonumber(redis.call('HGET', pf, 'dropped_through_ms') or '')
    local newest_ms = tonumber(legacy_newest[2])
    if through == nil or newest_ms > through then
      redis.call('HSET', pf, 'dropped_through_ms', fmt(newest_ms))
    end
    redis.call('PEXPIREAT', legacy, fmt(clock + cfg.raw_ie_ms), 'LT')
  end
  -- One lifetime for every key of the account, whichever stream wrote last: a profile hash that
  -- outlived its counters would read as an exact prefix with nothing in it.
  local account_keys = {key('tx', acct), key('pf', acct), key('pfh', acct), key('pfl', acct),
    key('pfc', acct)}
  for _, suffix in pairs(cfg.ie_keys) do
    account_keys[#account_keys + 1] = key('ie', acct, suffix)
  end
  for _, c in ipairs(redis.call('SMEMBERS', key('pfc', acct))) do
    account_keys[#account_keys + 1] = key('pfa', acct, c)
  end
  for _, k in ipairs(account_keys) do expire_at(k, ev.ms, clock, cfg.profile_ms) end
  return 1, js, ''
end
"""

_WRITE_MAIN_LUA: Final = r"""
local mode = ARGV[3]
local now = tonumber(ARGV[4])
local js = ARGV[5]
local identity = ARGV[6]
local ev = cjson.decode(js)
local recorded, current_js, verdict = record(
  ev, identity, cjson.decode(ARGV[7]), cjson.decode(ARGV[8]), now, js)
local position = redis.call('GET', key('position')) or '0'
if mode == 'observe' then return {recorded, position, current_js, verdict} end
local c = cjson.decode(current_js)
return {recorded, position, current_js, verdict,
  read(c.ms, c.a, c.card, c.dev, c.mer, c.ip, c.cur, c.id)}
"""

_READ_MAIN_LUA: Final = r"""
return read(tonumber(ARGV[3]), ARGV[4], ARGV[5], ARGV[6], ARGV[7], ARGV[8], ARGV[9], '')
"""

WRITE_SCRIPT: Final = "#!lua\n" + _COMMON_LUA + _READ_LUA + _WRITE_LUA + _WRITE_MAIN_LUA
READ_SCRIPT: Final = "#!lua flags=allow-oom\n" + _COMMON_LUA + _READ_LUA + _READ_MAIN_LUA
"""Runs on a full store, which is when it is needed: the snapshot a refused write degrades to.

Not `no-writes`: Redis flags `PFCOUNT` as a write, because it caches the sketch's cardinality in
place. That cache is the only thing this script can change, and it allocates nothing, so
`allow-oom` cannot grow a full store. A unit test pins that no other write command appears here."""
WITHDRAW_SCRIPT: Final = r"""#!lua
local current = tonumber(redis.call('GET', ARGV[1]) or '')
if current == nil or current < tonumber(ARGV[2]) then
  redis.call('SET', ARGV[1], ARGV[2])
end
return redis.call('GET', ARGV[1])
"""

HYDRATION_START_SCRIPT: Final = r"""#!lua
-- Hydration's marker is created only on a store nothing has written: no epoch, no position
-- (ADR-0057 §1). A store that holds a marker already reports it, so the caller can resume it.
local marker = ARGV[1] .. ':hydration'
if redis.call('EXISTS', marker) == 1 then
  return {'marker', redis.call('HGETALL', marker)}
end
local epoch = redis.call('GET', ARGV[1] .. ':epoch')
local position = redis.call('GET', ARGV[1] .. ':position')
if epoch or position then
  return {'occupied', {epoch or '', position or ''}}
end
local fields = cjson.decode(ARGV[2])
for i = 1, #fields, 2 do redis.call('HSET', marker, fields[i], fields[i + 1]) end
return {'created', redis.call('HGETALL', marker)}
"""
"""Create the hydration marker on an untouched store, or report what the store already holds."""

HYDRATION_UPDATE_SCRIPT: Final = r"""#!lua
-- Compare-and-set on the marker's run and state: a run that lost the marker writes nothing.
local marker = ARGV[1] .. ':hydration'
if redis.call('HGET', marker, 'run_id') ~= ARGV[2] then return 0 end
if redis.call('HGET', marker, 'state') ~= ARGV[3] then return 0 end
local fields = cjson.decode(ARGV[4])
for i = 1, #fields, 2 do redis.call('HSET', marker, fields[i], fields[i + 1]) end
return 1
"""
"""Update the marker only while `run_id` and `state` are still what the caller last saw."""

HYDRATION_CLAIM_SCRIPT: Final = r"""#!lua
-- The one place an epoch moves earlier (ADR-0057 §5): only out of hydration's own withdrawal
-- marker B, only while the epoch is still exactly B and nothing else has written since.
local marker = ARGV[1] .. ':hydration'
if redis.call('HGET', marker, 'run_id') ~= ARGV[2] then return 'marker' end
if redis.call('HGET', marker, 'state') ~= 'replaying' then return 'marker' end
if redis.call('HGET', marker, 'begin_epoch_ms') ~= ARGV[3] then return 'marker' end
if redis.call('GET', ARGV[1] .. ':epoch') ~= ARGV[3] then return 'epoch' end
if (redis.call('GET', ARGV[1] .. ':position') or '0') ~= ARGV[4] then return 'position' end
if tonumber(ARGV[5]) >= tonumber(ARGV[3]) then return 'not_earlier' end
redis.call('SET', ARGV[1] .. ':epoch', ARGV[5])
redis.call('HSET', marker, 'state', 'claimed', 'claimed_ms', ARGV[5])
return 'claimed'
"""
"""Set the epoch to the claim iff the marker, the epoch (still B) and the position all match."""


class RedisOnlineFeatureStore:
    """Reads and writes the online feature state for one Redis instance.

    Redis access is confined to this module: business logic sees `FeatureContext`.
    """

    def __init__(self, client: Redis, *, namespace: str = "f") -> None:
        self._redis = client
        self._ns = namespace
        self._config = LAYOUT.config_json()
        self._write = client.register_script(WRITE_SCRIPT)
        self._read = client.register_script(READ_SCRIPT)
        self._withdraw = client.register_script(WITHDRAW_SCRIPT)
        self._hydration_start = client.register_script(HYDRATION_START_SCRIPT)
        self._hydration_update = client.register_script(HYDRATION_UPDATE_SCRIPT)
        self._hydration_claim = client.register_script(HYDRATION_CLAIM_SCRIPT)

    @property
    def epoch_key(self) -> str:
        """When this store began recording continuously, in event-time ms (ADR-0044)."""
        return f"{self._ns}:epoch"

    def establish_epoch(self, *, at: EventTime | None = None) -> EventTime:
        """Record when continuous recording began, if not already recorded (NX)."""
        moment = at if at is not None else EventTime(dt.datetime.now(dt.UTC))
        self._redis.set(self.epoch_key, to_millis(moment), nx=True)
        stored = self._redis.get(self.epoch_key)
        return EventTime(from_millis(int(_text(stored)))) if stored is not None else moment

    def withdraw_completeness(self, *, resume_at: EventTime) -> None:
        """Vouch for no window that began before `resume_at`, keeping any later epoch."""
        try:
            self._withdraw(args=[self.epoch_key, str(to_millis(resume_at))])
        except OutOfMemoryError as exc:
            raise FeatureWriteFailedError(str(exc)) from exc

    # -- reconstruction (ADR-0057) ---------------------------------------------

    @property
    def hydration_key(self) -> str:
        """Hydration's own marker: not a feature primitive, and read by no feature."""
        return f"{self._ns}:hydration"

    @property
    def namespace(self) -> str:
        return self._ns

    def stored_epoch_ms(self) -> int | None:
        """The epoch as stored, in event-time ms, or None."""
        stored = self._redis.get(self.epoch_key)
        return None if stored is None else int(_text(stored))

    def stored_position(self) -> int:
        """The store-wide observation counter as stored; 0 before the first recorded write."""
        stored = self._redis.get(f"{self._ns}:position")
        return 0 if stored is None else int(_text(stored))

    def hydration_marker(self) -> dict[str, str]:
        """The marker's fields; empty when there is none."""
        stored: Mapping[Any, Any] = self._redis.hgetall(self.hydration_key)
        return {_text(key): _text(value) for key, value in stored.items()}

    def start_hydration(self, fields: Mapping[str, str]) -> tuple[str, dict[str, str]]:
        """Create the marker on an untouched store.

        Returns `("created", marker)`, `("marker", existing marker)` when one already exists, or
        `("occupied", {"epoch": ..., "position": ...})` when something other than hydration has
        written: nothing is created then.
        """
        flat = [item for pair in sorted(fields.items()) for item in pair]
        try:
            outcome, detail = self._hydration_start(args=[self._ns, json.dumps(flat)])
        except OutOfMemoryError as exc:
            raise FeatureWriteFailedError(str(exc)) from exc
        kind = _text(outcome)
        if kind == "occupied":
            epoch, position = (_text(value) for value in detail)
            return kind, {"epoch": epoch, "position": position}
        return kind, _pairs(detail)

    def update_hydration(self, *, run_id: str, state: str, fields: Mapping[str, str]) -> bool:
        """Set `fields` on the marker iff it still belongs to `run_id` in `state`."""
        flat = [item for pair in sorted(fields.items()) for item in pair]
        try:
            return bool(self._hydration_update(args=[self._ns, run_id, state, json.dumps(flat)]))
        except OutOfMemoryError as exc:
            raise FeatureWriteFailedError(str(exc)) from exc

    def claim_completeness(
        self, *, run_id: str, begin_epoch_ms: int, expected_position: int, since_ms: int
    ) -> str:
        """Move the epoch from hydration's own `begin_epoch_ms` to the earlier `since_ms`.

        Returns `claimed`, or why not: `marker` (not this run's replaying marker), `epoch` (the
        epoch is no longer B), `position` (another writer recorded something) or `not_earlier`.
        """
        try:
            outcome = self._hydration_claim(
                args=[
                    self._ns,
                    run_id,
                    str(begin_epoch_ms),
                    str(expected_position),
                    str(since_ms),
                ]
            )
        except OutOfMemoryError as exc:
            raise FeatureWriteFailedError(str(exc)) from exc
        return _text(outcome)

    def discard_unfinished_hydration(self, *, run_id: str, begin_epoch_ms: int) -> int:
        """Delete everything an unfinished hydration wrote, keeping only its marker and epoch B.

        Allowed only while the marker is `run_id`'s and the epoch is still B. The marker is set to
        `discarding` first, so a crash part-way leaves a state a re-run finishes, never a store
        that looks resumable with primitives missing. Returns how many keys were deleted.
        """
        if self.stored_epoch_ms() != begin_epoch_ms:
            return -1
        if (
            not self.update_hydration(
                run_id=run_id, state="replaying", fields={"state": "discarding"}
            )
            and self.hydration_marker().get("state") != "discarding"
        ):
            return -1
        if self.hydration_marker().get("run_id") != run_id:
            return -1
        keep = {self.hydration_key, self.epoch_key}
        pattern = _glob_escape(self._ns) + ":*"
        deleted = 0
        batch: list[str] = []
        for raw in self._redis.scan_iter(match=pattern, count=1000):
            name = _text(raw)
            if name in keep:
                continue
            batch.append(name)
            if len(batch) >= 1000:
                deleted += int(self._redis.unlink(*batch))
                batch = []
        if batch:
            deleted += int(self._redis.unlink(*batch))
        reset = {"state": "replaying", "cursor": "", "count": "0", "position": "0", "digest": ""}
        if not self.update_hydration(run_id=run_id, state="discarding", fields=reset):
            return -1
        return deleted

    # -- writes ---------------------------------------------------------------

    def observe(self, event: Event) -> ObserveReceipt:
        """Record an observation unless its identity already was (ADR-0046 §1)."""
        recorded, position, current_json, verdict = self._run("observe", event)
        return _receipt(event, recorded, position, current_json, verdict)

    def score(self, event: Event) -> ServedRead:
        """Record the scored transaction and read its context, atomically (ADR-0046 §5)."""
        recorded, position, current_json, verdict, reply = self._run("score", event)
        current = _decode(_text(current_json))
        context = self._assemble(
            reply,
            as_of_ms=current.occurred_ms,
            currency=current.currency,
            ids=_ids(current),
            current=current,
        )
        epoch_text = _text(reply[0])
        return ServedRead(
            receipt=_receipt(event, recorded, position, current, verdict),
            context=context,
            store_epoch_ms=int(epoch_text) if epoch_text else None,
        )

    def _run(self, mode: str, event: Event) -> Any:
        if event.stream not in _WRITABLE_STREAMS:
            # Refused before the script: a stream it has no raw set for would fail part-way.
            raise ContractError(f"the online store records no {event.stream} observation")
        amount = event.amount_minor
        args = [
            self._ns,
            self._config,
            mode,
            str(to_millis(dt.datetime.now(dt.UTC))),
            _encode(event),
            event.identity,
            _limbs(amount),
            _limbs(amount * amount),
        ]
        try:
            return self._write(args=args)
        except OutOfMemoryError as exc:
            # Refused before the script ran: nothing was written. Withdrawing completeness is
            # the gateway's CompletenessGuard's job, which also survives a restart.
            raise FeatureWriteFailedError(str(exc)) from exc

    # -- reads ----------------------------------------------------------------

    def snapshot(
        self,
        *,
        as_of: EventTime,
        account_id: str,
        currency: str,
        card_id: str | None = None,
        device_id: str | None = None,
        merchant_id: str | None = None,
        ip_id: str | None = None,
    ) -> FeatureContext:
        """A read-only read with no current observation (served when a write is refused)."""
        as_of_ms = to_millis(as_of)
        reply = self._read(
            args=[
                self._ns,
                self._config,
                str(as_of_ms),
                account_id,
                card_id or "",
                device_id or "",
                merchant_id or "",
                ip_id or "",
                currency,
            ]
        )
        ids = {
            Entity.ACCOUNT: account_id,
            Entity.CARD: card_id,
            Entity.DEVICE: device_id,
            Entity.MERCHANT: merchant_id,
            Entity.IP: ip_id,
        }
        return self._assemble(reply, as_of_ms=as_of_ms, currency=currency, ids=ids, current=None)

    def _assemble(
        self,
        reply: Sequence[Any],
        *,
        as_of_ms: int,
        currency: str,
        ids: dict[Entity, str | None],
        current: Event | None,
    ) -> FeatureContext:
        (
            epoch,
            newest_transactions,
            legacy_identity_events,
            pf_flat,
            pfh_flat,
            prefix_amounts,
            prefix_located,
            card_counts,
            device_counts,
            ip_counts,
            merchant_counts,
            cv_fields,
            _position,
            high_watermark,
            outcome_counts,
            transaction_counts,
            raw_depth,
            previous_transaction,
            identity_streams,
        ) = reply
        read = ReadScope(
            as_of_ms=as_of_ms, currency=currency, current=current, mode=EvaluationMode.AS_SERVED
        )
        scalars = _pairs(pf_flat)
        dropped_through = _optional_int(scalars.get("dropped_through_ms"))
        missing_through = {
            Stream.TRANSACTION: _optional_int(scalars.get("folded_through_ms")),
            Stream.IDENTITY_FAILED_LOGIN: dropped_through,
            Stream.IDENTITY_CHANGE: dropped_through,
        }
        high_ms = _optional_int(_text(high_watermark))
        unheld_ms: list[int] = []

        def held(entity: Entity, stream: Stream, window: Window, through_ms: int | None) -> bool:
            """Does the store still hold every observation `(as_of - W, as_of]` contains?

            `through_ms`: nothing later than it can have been trimmed, folded or dropped. A declared
            window that is not held is left out AND no longer vouched for, so it reads as absent
            rather than as the lower bound that is left (ADR-0046 §5).
            """
            if through_ms is None or as_of_ms - window.seconds * 1000 >= through_ms:
                return True
            if (entity, stream, window) in _DECLARED_WINDOWS:
                unheld_ms.append(window.seconds * 1000)
            return False

        def trimmed_through(retention_ms: int) -> int | None:
            return None if high_ms is None else high_ms - retention_ms

        # The most recent `SCORE_READ_CAP` transactions at or before `as_of` (ADR-0046 §8).
        transactions = [_decode(_text(item)) for item in newest_transactions]
        tx = Stream.TRANSACTION
        windows: dict[tuple[Entity, str, Stream, str], WindowState] = {}
        previous: dict[tuple[Entity, str, Stream], Observation] = {}
        account = ids[Entity.ACCOUNT]
        if account is not None:
            for window, count in zip(LAYOUT.account_windows[tx], transaction_counts, strict=True):
                if not held(Entity.ACCOUNT, tx, window, missing_through[tx]):
                    continue
                observed = int(count)
                state: WindowState | None
                if observed > SCORE_READ_CAP:
                    # The count stays exact; the content past the cap is not read.
                    state = WindowState(count=observed, content_capped=True)
                else:
                    state = window_state(
                        transactions, read=read, entity=Entity.ACCOUNT, stream=tx, window=window
                    )
                if state is not None:
                    windows[(Entity.ACCOUNT, account, tx, window.label)] = dataclasses.replace(
                        state, count=observed
                    )
            # Identity events still in the account set both streams shared before §8 are not in
            # the per-stream sets, so neither stream is held while that set exists.
            legacy = int(legacy_identity_events) > 0
            for stream, (counts, latest) in zip(
                LAYOUT.identity_streams, identity_streams, strict=True
            ):
                through_ms = as_of_ms if legacy else missing_through[stream]
                for window, count in zip(LAYOUT.account_windows[stream], counts, strict=True):
                    if held(Entity.ACCOUNT, stream, window, through_ms) and int(count):
                        windows[(Entity.ACCOUNT, account, stream, window.label)] = WindowState(
                            count=int(count)
                        )
                lookback = PLAN.previous.get((Entity.ACCOUNT, stream))
                if lookback is None:
                    continue
                if legacy:
                    unheld_ms.append(lookback.seconds * 1000)
                elif latest:
                    # The latest held is the latest there was only if nothing later was dropped.
                    occurred_ms = int(float(_text(latest[1])))
                    if through_ms is None or occurred_ms > through_ms:
                        previous[(Entity.ACCOUNT, account, stream)] = Observation(
                            occurred_at=EventTime(from_millis(occurred_ms))
                        )
            tx_lookback = PLAN.previous.get((Entity.ACCOUNT, tx))
            if tx_lookback is not None and previous_transaction:
                last = _decode(_text(previous_transaction[0]))
                observation = previous_observation([last], read, tx_lookback)
                through_ms = missing_through[tx]
                if observation is not None and (
                    through_ms is None or to_millis(observation.occurred_at) > through_ms
                ):
                    previous[(Entity.ACCOUNT, account, tx)] = observation
            # The reference's outcome window, counted by the script from verified outcomes,
            # without the scored transaction's own (ADR-0049 §6).
            outcome = Stream.AUTHORIZATION_OUTCOME
            for window, pair in zip(LAYOUT.outcome_windows, outcome_counts, strict=True):
                known, declined = int(pair[0]), int(pair[1])
                held_through = trimmed_through(LAYOUT.outcome_ms)
                if held(Entity.ACCOUNT, outcome, window, held_through) and known:
                    windows[(Entity.ACCOUNT, account, outcome, window.label)] = WindowState(
                        count=known, declined_count=declined, outcome_known_count=known
                    )

        card = ids[Entity.CARD]
        if card is not None:
            for window, count in zip(LAYOUT.card_windows, card_counts, strict=True):
                if held(Entity.CARD, tx, window, trimmed_through(LAYOUT.card_ms)) and int(count):
                    windows[(Entity.CARD, card, tx, window.label)] = WindowState(count=int(count))
        device = ids[Entity.DEVICE]
        if device is not None:
            for window, pair in zip(LAYOUT.device_windows, device_counts, strict=True):
                members, distinct = int(pair[0]), int(pair[1])
                if held(Entity.DEVICE, tx, window, trimmed_through(LAYOUT.device_ms)) and members:
                    windows[(Entity.DEVICE, device, tx, window.label)] = (
                        WindowState(count=members, content_capped=True)
                        if members > SCORE_READ_CAP
                        else WindowState(count=members, distinct={Dimension.ACCOUNT: distinct})
                    )
        for entity, entity_windows, counts in (
            (Entity.IP, LAYOUT.ip_windows, ip_counts),
            (Entity.MERCHANT, LAYOUT.merchant_distinct_windows, merchant_counts),
        ):
            entity_id = ids[entity]
            if entity_id is None:
                continue
            for window, count in zip(entity_windows, counts, strict=True):
                if held(entity, tx, window, trimmed_through(LAYOUT.sketch_ms)) and int(count):
                    _merge(
                        windows,
                        (entity, entity_id, tx, window.label),
                        distinct={Dimension.ACCOUNT: int(count)},
                    )
        merchant = ids[Entity.MERCHANT]
        if merchant is not None:
            for window, fields in zip(LAYOUT.cv_windows, cv_fields, strict=True):
                if not held(Entity.MERCHANT, tx, window, trimmed_through(LAYOUT.cv_ms)):
                    continue
                c, s, q = _aligned_totals(fields)
                if (
                    current is not None
                    and current.merchant_id == merchant
                    and current.currency == currency
                ):
                    c, s, q = c + 1, s + current.amount_minor, q + current.amount_minor**2
                if c:
                    _merge(
                        windows,
                        (Entity.MERCHANT, merchant, tx, window.label),
                        aligned_count=c,
                        aligned_amount_sum_minor=s,
                        aligned_amount_sum_squares=q,
                    )

        profiles: dict[tuple[Entity, str], Profile] = {}
        if account is not None:
            prefix = _prefix(scalars, pfh_flat, prefix_amounts, prefix_located)
            profile: Profile | None
            if int(raw_depth[0]) > SCORE_READ_CAP:
                oldest_identity, oldest_score = _text(raw_depth[1]), _text(raw_depth[2])
                profile = capped_account_profile(
                    transactions,
                    read,
                    prefix,
                    oldest_raw=(int(float(oldest_score)), oldest_identity)
                    if oldest_identity
                    else None,
                    raw_from_ms=as_of_ms - LAYOUT.raw_tx_ms,
                )
            else:
                profile = account_profile(transactions, read, prefix)
            if profile is not None:
                profiles[(Entity.ACCOUNT, account)] = profile

        epoch_text = _text(epoch)
        complete_ms = int(epoch_text) if epoch_text else None
        if complete_ms is not None and unheld_ms:
            complete_ms = max(complete_ms, as_of_ms - min(unheld_ms) + 1)
        return FeatureContext(
            as_of=EventTime(from_millis(as_of_ms)),
            source=FeatureSource.ONLINE_ONLY,
            windows=windows,
            profiles=profiles,
            previous=previous,
            complete_since=None if complete_ms is None else EventTime(from_millis(complete_ms)),
            distinct_dimensions={e: PLAN.distinct_dimensions(e) for e in Entity},
        )


@dataclass(frozen=True, slots=True)
class ProfilePrefix:
    """An account's folded history: the lifetime run the raw records continue."""

    run_start_ms: int | None
    last_ms: int
    last_id: str
    inexact: bool
    merchants: dict[str, int]
    mccs: dict[str, int]
    devices: frozenset[str]
    amounts: tuple[int, ...]
    """The scored currency's last folded amounts, oldest first."""
    located: tuple[tuple[int, str, float, float], ...]


def _prefix(
    scalars: dict[str, str],
    counters_flat: Sequence[Any],
    amounts: Sequence[Any],
    located: Sequence[Any],
) -> ProfilePrefix | None:
    if "last_ms" not in scalars:
        return None
    counters = _pairs(counters_flat)
    points = []
    for item in located:
        ms, lat, lon, identity = _text(item).split("\t", 3)
        points.append((int(ms), identity, float(lat), float(lon)))
    return ProfilePrefix(
        run_start_ms=_optional_int(scalars.get("run_start_ms")),
        last_ms=int(scalars["last_ms"]),
        last_id=scalars.get("last_id", ""),
        inexact=scalars.get("inexact") == "1",
        merchants={k[2:]: int(v) for k, v in counters.items() if k.startswith("m:")},
        mccs={k[2:]: int(v) for k, v in counters.items() if k.startswith("c:")},
        devices=frozenset(k[2:] for k in counters if k.startswith("d:")),
        amounts=tuple(int(_text(a)) for a in amounts),
        located=tuple(points),
    )


def account_profile(
    transactions: Sequence[Event], read: ReadScope, prefix: ProfilePrefix | None
) -> Profile | None:
    """The account's profile from raw history and the folded prefix it continues.

    Exactly `reference.lifetime_profile` whenever the lifetime lies within raw history. When it
    reaches into the prefix, the prefix's summaries are combined with the raw lifetime; when the
    combination cannot be exact -- the prefix holds observations at or after `as_of`, a raw record
    interleaves with the prefix, or the prefix run was marked inexact -- there is no profile.
    """
    if prefix is None:
        return lifetime_profile(transactions, read)
    history = sorted(
        (e for e in transactions if e.occurred_ms < read.as_of_ms and e.identity != read.identity),
        key=lambda e: e.order_key,
    )
    frontier = (prefix.last_ms, prefix.last_id)
    lifetime_raw: list[Event]
    if not history:
        if prefix.last_ms >= read.as_of_ms:
            return None
        if read.as_of_ms - prefix.last_ms >= _GAP_MS:
            return None
        lifetime_raw = []
    else:
        if read.as_of_ms - history[-1].occurred_ms >= _GAP_MS:
            return None
        for index in range(len(history) - 1, 0, -1):
            if history[index].occurred_ms - history[index - 1].occurred_ms >= _GAP_MS:
                return lifetime_profile(transactions, read)
        if history[0].occurred_ms - prefix.last_ms >= _GAP_MS:
            return lifetime_profile(transactions, read)
        if (history[0].occurred_ms, history[0].identity) <= frontier:
            return None
        lifetime_raw = history
    if prefix.inexact or prefix.run_start_ms is None:
        return None
    return _combined_profile(prefix, prefix.run_start_ms, lifetime_raw, read)


def capped_account_profile(
    capped: Sequence[Event],
    read: ReadScope,
    prefix: ProfilePrefix | None,
    *,
    oldest_raw: tuple[int, str] | None,
    raw_from_ms: int,
) -> Profile | None:
    """The profile a capped read serves (ADR-0046 §8): the reference's capped `lifetime_profile`.

    `capped` is the most recent `SCORE_READ_CAP` transactions at or before `as_of`, and `oldest_raw`
    the oldest raw transaction at or before it. The folded prefix is part of what the read sees only
    when nothing unread can hide a 30-day gap between them: its run is exact and ends before the raw
    history, the oldest raw transaction follows its frontier within the gap, and the read's oldest
    follows that within the gap too. Otherwise the read alone is seen and tenure is absent -- an
    absence where a complete as-served log may still have a value, never another number.
    """
    history = sorted(
        (e for e in capped if e.occurred_ms < read.as_of_ms and e.identity != read.identity),
        key=lambda e: e.order_key,
    )
    if not history:
        return Profile(depth_capped=True)
    if read.as_of_ms - history[-1].occurred_ms >= _GAP_MS:
        return None
    start = 0
    for index in range(len(history) - 1, 0, -1):
        if history[index].occurred_ms - history[index - 1].occurred_ms >= _GAP_MS:
            start = index
            break
    in_read = history[start:]
    run_start_ms = None if prefix is None or prefix.inexact else prefix.run_start_ms
    continues = (
        start == 0
        and prefix is not None
        and run_start_ms is not None
        and prefix.last_ms < raw_from_ms
        and oldest_raw is not None
        and oldest_raw > (prefix.last_ms, prefix.last_id)
        and oldest_raw[0] - prefix.last_ms < _GAP_MS
        and in_read[0].occurred_ms - oldest_raw[0] < _GAP_MS
    )
    seen: Profile | None
    if continues and prefix is not None and run_start_ms is not None:
        seen = _combined_profile(prefix, run_start_ms, in_read, read)
    else:
        seen = lifetime_profile(in_read, read)
    if seen is None:
        return Profile(depth_capped=True)
    return depth_capped_profile(
        seen, in_read=in_read, currency=read.currency, start_in_prefix=continues
    )


def _combined_profile(
    prefix: ProfilePrefix, run_start_ms: int, lifetime_raw: Sequence[Event], read: ReadScope
) -> Profile:
    """The folded prefix's summaries, continued by the raw lifetime that follows it."""
    merchants: Counter[str] = Counter(prefix.merchants)
    merchants.update(e.merchant_id for e in lifetime_raw if e.merchant_id is not None)
    mccs: Counter[str] = Counter(prefix.mccs)
    mccs.update(e.merchant_mcc for e in lifetime_raw if e.merchant_mcc is not None)
    devices = set(prefix.devices) | {e.device_id for e in lifetime_raw if e.device_id is not None}
    amounts = list(prefix.amounts) + [
        abs(e.amount_minor) for e in lifetime_raw if e.currency == read.currency
    ]
    sample = amounts[-AMOUNT_SAMPLE_SIZE:]
    centre = robust_centre(sample)
    located = list(prefix.located) + [
        (e.occurred_ms, e.identity, e.latitude, e.longitude)
        for e in lifetime_raw
        if e.latitude is not None and e.longitude is not None
    ]
    home = geodesic_medoid(located[-HOME_SAMPLE_SIZE:])
    return Profile(
        first_seen_at=EventTime(from_millis(run_start_ms)),
        observation_count=len(sample),
        amount_median_minor=None if centre is None else centre[0],
        amount_mad_minor=None if centre is None else centre[1],
        habitual_merchants=frozenset(k for k, n in merchants.items() if n >= HABITUAL_MIN_VISITS),
        habitual_mccs=frozenset(k for k, n in mccs.items() if n >= HABITUAL_MIN_VISITS),
        known_devices=frozenset(devices),
        home_latitude=None if home is None else home[0],
        home_longitude=None if home is None else home[1],
    )


def _receipt(
    event: Event, recorded: Any, position: Any, first: Event | Any, verdict: Any
) -> ObserveReceipt:
    """The store's answer, with the first delivery compared only when this one was not recorded.

    An outcome's verdict is the first delivery's, as the store stands after this call; one the store
    no longer knows is pending, the direction that counts nothing."""
    was_recorded = bool(int(recorded))
    conflicting = False
    if not was_recorded:
        first_event = first if isinstance(first, Event) else _decode(_text(first))
        conflicting = first_event.recorded_form() != event.recorded_form()
    verification = None
    if event.stream is Stream.AUTHORIZATION_OUTCOME:
        verification = Verification(_text(verdict) or Verification.PENDING.value)
    return ObserveReceipt(
        position=int(_text(position)),
        recorded=was_recorded,
        conflicting=conflicting,
        verification=verification,
    )


def _merge(
    windows: dict[tuple[Entity, str, Stream, str], WindowState],
    key: tuple[Entity, str, Stream, str],
    **fields: Any,
) -> None:
    existing = windows.get(key, WindowState())
    windows[key] = dataclasses.replace(existing, **fields)


def _limbs(value: int) -> str:
    """`value` as `LIMBS` signed base-1e9 digits, least significant first."""
    sign = -1 if value < 0 else 1
    magnitude = abs(value)
    digits = []
    for _ in range(LIMBS):
        magnitude, digit = divmod(magnitude, LIMB_BASE)
        digits.append(str(sign * digit))
    if magnitude:
        raise ContractError(
            f"a merchant amount sum term of {value} exceeds what the store keeps exactly; "
            f"released amounts are bounded by {AMOUNT_MINOR_MAX} minor units (tx.raw.v1)"
        )
    return json.dumps(digits)


def _aligned_totals(fields: Sequence[Any]) -> tuple[int, int, int]:
    count = total = squares = 0
    for name, value in zip(fields[0::2], fields[1::2], strict=True):
        kind = _text(name).rsplit(":", 1)[1]
        number = int(_text(value))
        if kind == "c":
            count += number
        elif kind[0] == "s":
            total += number * LIMB_BASE ** int(kind[1:])
        elif kind[0] == "q":
            squares += number * LIMB_BASE ** int(kind[1:])
    return count, total, squares


def _optional_int(text: str | None) -> int | None:
    return int(text) if text else None


def _pairs(flat: Sequence[Any]) -> dict[str, str]:
    items = [_text(x) for x in flat]
    return dict(zip(items[0::2], items[1::2], strict=True))


def _ids(event: Event) -> dict[Entity, str | None]:
    return {entity: event.entity_id(entity) for entity in Entity}


def _encode(event: Event) -> str:
    return json.dumps(
        {
            "s": event.stream.value,
            "ms": event.occurred_ms,
            "a": event.account_id,
            "id": event.event_id,
            "cur": event.currency,
            "amt": event.amount_minor,
            "card": event.card_id,
            "dev": event.device_id,
            "mer": event.merchant_id,
            "ip": event.ip_id,
            "mcc": event.merchant_mcc,
            "ctry": event.merchant_country,
            "lat": event.latitude,
            "lon": event.longitude,
            "ch": None if event.channel is None else event.channel.value,
            "out": None
            if event.authorization_outcome is None
            else event.authorization_outcome.value,
        },
        separators=(",", ":"),
        allow_nan=False,
    )


def _decode(text: str) -> Event:
    r = json.loads(text)
    return Event(
        stream=Stream(r["s"]),
        occurred_at=EventTime(from_millis(int(r["ms"]))),
        account_id=r["a"],
        event_id=r["id"],
        currency=r["cur"] or "",
        amount_minor=int(r["amt"]),
        card_id=r["card"],
        device_id=r["dev"],
        merchant_id=r["mer"],
        ip_id=r["ip"],
        merchant_mcc=r["mcc"],
        merchant_country=r["ctry"],
        latitude=r["lat"],
        longitude=r["lon"],
        channel=TransactionChannel(r["ch"]) if r["ch"] else None,
        authorization_outcome=AuthorizationOutcome(r["out"]) if r["out"] else None,
    )


def _glob_escape(text: str) -> str:
    """`text` as a literal inside a Redis MATCH pattern."""
    return "".join("\\" + ch if ch in "*?[]\\" else ch for ch in text)


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


__all__ = [
    "BUCKET_TRIM_SWEEP",
    "HYDRATION_CLAIM_SCRIPT",
    "HYDRATION_START_SCRIPT",
    "HYDRATION_UPDATE_SCRIPT",
    "IDENTITY_RAW_KEYS",
    "LAYOUT",
    "LIMBS",
    "LIMB_BASE",
    "READ_SCRIPT",
    "WITHDRAW_SCRIPT",
    "WRITE_SCRIPT",
    "ProfilePrefix",
    "RedisOnlineFeatureStore",
    "account_profile",
    "capped_account_profile",
    "unsupported_declarations",
]
