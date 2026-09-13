"""The Redis online feature store (ADR-0003, ADR-0034).

**One round trip, not twenty-six.** Every feature a transaction needs is read in
a single pipeline and frozen into a `FeatureContext`; the features themselves are
pure functions of that snapshot (ADR-0032). Twenty-six sequential reads could not
fit a 100 ms p99 budget, and a snapshot also means the whole feature vector
describes one consistent instant rather than a smear across the read.

**Event time is the score, everywhere.** Sorted sets are scored by `occurred_at`
and read as `(as_of - W, as_of]`. Processing time appears nowhere in a window:
using it would silently corrupt every counter under out-of-order or replayed
traffic, and the corruption is invisible until someone recomputes by hand
(ADR-0026). Out-of-order inserts land at their own score, so the answer does not
depend on arrival order.

**Distinct counts use two representations, declared per feature** (ADR-0034):

* `EXACT` — a sorted set keyed by the counted *value*, `ZADD … GT` so a late
  arrival cannot move a value's timestamp backwards. `ZCOUNT` over the window is
  then exactly the distinct count, because each value carries its latest
  observation. Memory is O(distinct cardinality).
* `APPROXIMATE` — one HyperLogLog per time bucket, unioned by `PFCOUNT`. Memory
  is bounded regardless of cardinality, at the cost of two compounding error
  sources: HLL's own, and the bucket boundary.

The classification is static and lives on the `FeatureSpec`. Nothing here reads
observed cardinality to pick a representation: a feature that changed its own
error characteristics under load would be unattributable exactly when it
mattered.

**Redis is authoritative for nothing** (ADR-0003). It holds derived state,
rebuildable from Delta, and Phase 3's Gold reconciliation overwrites it. Eviction
under `allkeys-lru` therefore degrades a feature to absent — which the rules tier
treats as `UNKNOWN` and abstains on — rather than to a wrong number. That is the
fail-safe direction, and it is why this module never returns a default.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import statistics
from typing import TYPE_CHECKING, Any, Final

from redis.exceptions import OutOfMemoryError

from trace_core.domain.enums import AuthorizationOutcome, FeatureSource
from trace_core.domain.errors import FeatureWriteFailedError
from trace_core.domain.time import EventTime, from_millis, to_millis
from trace_core.features.context import (
    MIN_OBSERVATIONS_FOR_ROBUST_Z,
    FeatureContext,
    Observation,
    Profile,
    WindowState,
)
from trace_core.features.reference import HABITUAL_MIN_VISITS, Event
from trace_core.features.semantics import (
    CardinalityStorage,
    Dimension,
    Entity,
    Stream,
    Window,
)
from trace_core.features.state_plan import PLAN

if TYPE_CHECKING:  # pragma: no cover - typing only
    from redis import Redis

MINUTE_MS: Final = 60_000
HLL_BUCKET_MS: Final = 300_000
"""Five-minute HyperLogLog buckets.

Chosen so the widest approximate window (1 h) unions 12-13 buckets: few enough
that `PFCOUNT` stays sub-millisecond, fine enough that the boundary error stays
small relative to HLL's own. The resulting total error is MEASURED, not assumed
-- see `benchmarks/features/REPORT.md` and ADR-0034."""

PROFILE_SAMPLE_SIZE: Final = 128
"""How many recent amounts the robust-z estimator keeps per account.

An exact median needs the whole history, which is unbounded. A bounded recent
sample is why `amount_zscore_vs_account` is declared APPROXIMATE: the feature's
MEANING is identical to the offline version, only the estimator differs, and that
difference is what `feature_parity_drift` measures rather than hides."""

BUCKET_TRIM_SWEEP: Final = 3
"""How many just-expired minute buckets each write deletes.

The bucket hash has one field set per active minute and nothing else removes
them: the key's TTL is refreshed by every write, so a continuously active
merchant accumulated five fields per minute for as long as it stayed active --
unbounded growth, discovered by the memory model. Each write now deletes the
buckets that fell out of retention in the last few minutes. Three is enough to
keep a steadily active entity trimmed; a bursty one leaves orphans that the
key's own TTL clears once it goes quiet."""


def _key(*parts: str) -> str:
    return ":".join(parts)


class RedisOnlineFeatureStore:
    """Reads and writes the online feature state for one Redis instance.

    Redis access is confined to this module: nothing in `features/`, `rules/` or
    `services/` imports a Redis client. Business logic sees `FeatureContext`, and
    the store is the only thing that knows a sorted set from a HyperLogLog.
    """

    def __init__(self, client: Redis, *, namespace: str = "f") -> None:
        self._redis = client
        self._ns = namespace

    # -- key schema ---------------------------------------------------------
    #
    # Written as one block so the layout is reviewable in one place. Every key
    # carries its entity, its id and its stream: two features must never share a
    # key by accident, because the symptom would be a plausible wrong number.

    def _velocity_key(self, entity: Entity, entity_id: str, stream: Stream) -> str:
        return _key(self._ns, "v", entity.value, entity_id, stream.value)

    def _bucket_key(self, entity: Entity, entity_id: str, stream: Stream, currency: str) -> str:
        return _key(self._ns, "b", entity.value, entity_id, stream.value, currency)

    def _exact_distinct_key(self, entity: Entity, entity_id: str, dimension: Dimension) -> str:
        return _key(self._ns, "dx", entity.value, entity_id, dimension.value)

    def _hll_key(self, entity: Entity, entity_id: str, dimension: Dimension, bucket: int) -> str:
        return _key(self._ns, "da", entity.value, entity_id, dimension.value, str(bucket))

    def _profile_key(self, entity: Entity, entity_id: str, currency: str) -> str:
        return _key(self._ns, "p", entity.value, entity_id, currency)

    def _amounts_key(self, entity: Entity, entity_id: str, currency: str) -> str:
        return _key(self._ns, "pa", entity.value, entity_id, currency)

    def _previous_key(self, entity: Entity, entity_id: str, stream: Stream) -> str:
        return _key(self._ns, "l", entity.value, entity_id, stream.value)

    @property
    def epoch_key(self) -> str:
        """When this store began recording continuously, in event-time ms.

        One key, no TTL, set with `NX` by every write and read by every
        snapshot. Its absence means the store is empty -- never written, or
        restarted and wiped -- and its presence is what lets an absent feature
        key mean "the entity did nothing" instead of "we do not know". A write
        that the store REFUSES deletes it, because an unrecorded observation is
        a hole and every window spanning the hole is no longer complete."""
        return _key(self._ns, "epoch")

    def establish_epoch(self, *, at: EventTime | None = None) -> EventTime:
        """Record when continuous recording began, if not already recorded.

        `NX`: an existing epoch is never moved forward, since that would claim
        completeness for a period the store did not watch. Returns the epoch in
        force. The gateway calls this at start-up so a store that has been
        running for a day is not re-dated by a gateway restart; every write also
        issues it, so a Redis restart mid-run re-establishes an epoch at the
        first write after it rather than at the next gateway restart.
        """
        moment = at if at is not None else EventTime(dt.datetime.now(dt.UTC))
        self._redis.set(self.epoch_key, to_millis(moment), nx=True)
        stored = self._redis.get(self.epoch_key)
        return EventTime(from_millis(int(_text(stored)))) if stored is not None else moment

    # -- writes -------------------------------------------------------------

    def observe(self, event: Event) -> None:
        """Record one observation. Idempotent per `(entity, dedup_key)`.

        Everything is issued in one pipeline: a partial write would leave the
        counters and the distinct sets disagreeing, and nothing downstream could
        tell that had happened.

        **Only what a released feature reads is written** (ADR-0044). Which
        entity/stream pairs get a velocity set, which entities get buckets and
        which get a previous-observation hash all come from `PLAN`, and each key
        keeps exactly the retention its widest reader needs. A feature that is
        not released has no claim on this store; a feature released later
        declares what it needs and warms up (the backfill contract).

        Raises `FeatureWriteFailedError` if the store refuses -- under
        `noeviction`, the only way memory pressure can present. The epoch is
        deleted first, because an observation that was not recorded is a hole
        in the history.
        """
        occurred_ms = to_millis(event.occurred_at)
        pipe = self._redis.pipeline(transaction=False)
        dedup = event.dedup_key

        # NX: never moves an existing epoch. Cheap, and it is what re-establishes
        # completeness at the first write after a Redis restart.
        pipe.set(self.epoch_key, to_millis(EventTime(dt.datetime.now(dt.UTC))), nx=True)

        for entity in Entity:
            entity_id = event.entity_id(entity)
            if entity_id is None:
                continue

            if (entity, event.stream) in PLAN.velocity:
                retention = PLAN.velocity_retention(entity, event.stream).seconds
                vkey = self._velocity_key(entity, entity_id, event.stream)
                # One member per observation, so a repeat of the SAME observation
                # updates in place rather than double-counting.
                pipe.zadd(vkey, {dedup: occurred_ms})
                pipe.zremrangebyscore(vkey, "-inf", occurred_ms - retention * 1000)
                pipe.expire(vkey, retention)

            if event.stream is Stream.TRANSACTION:
                if entity in PLAN.buckets:
                    self._observe_amounts(pipe, entity, entity_id, event, occurred_ms)
                self._observe_distinct(pipe, entity, entity_id, event, occurred_ms)

            if (entity, event.stream) in PLAN.previous:
                retention = PLAN.previous_retention(entity, event.stream).seconds
                lkey = self._previous_key(entity, entity_id, event.stream)
                # `GT`-guarded via a read-modify-write would need a transaction;
                # instead the stored timestamp is compared on read, so an
                # out-of-order write is harmless.
                pipe.hset(
                    lkey,
                    mapping={
                        "occurred_ms": occurred_ms,
                        "latitude": "" if event.latitude is None else event.latitude,
                        "longitude": "" if event.longitude is None else event.longitude,
                        "card_present": int(event.card_present),
                    },
                )
                pipe.expire(lkey, retention)

        if event.stream is Stream.TRANSACTION and Entity.ACCOUNT in PLAN.profiles:
            self._observe_profile(pipe, event, occurred_ms)

        try:
            pipe.execute()
        except OutOfMemoryError as exc:
            # The store is up and answering; it is full. Not an outage, and the
            # breaker must not treat it as one. The epoch goes first: whatever
            # this pipeline did or did not manage to write, the history now has
            # a hole in it, and no window spanning the hole is complete.
            with contextlib.suppress(Exception):
                self._redis.delete(self.epoch_key)
            raise FeatureWriteFailedError(str(exc)) from exc

    def _observe_amounts(
        self, pipe: Any, entity: Entity, entity_id: str, event: Event, occurred_ms: int
    ) -> None:
        """Minute-bucketed integer aggregates.

        Integer counters throughout (`HINCRBY`): a running float sum of squares
        loses precision at exactly the scale where uniform-amount laundering
        lives, and money is integer minor units anyway (CLAUDE.md §6).
        """
        retention = PLAN.bucket_retention(entity).seconds
        bucket = occurred_ms // MINUTE_MS
        key = self._bucket_key(entity, entity_id, event.stream, event.currency)
        amount = event.amount_minor
        pipe.hincrby(key, f"{bucket}:c", 1)
        pipe.hincrby(key, f"{bucket}:s", amount)
        pipe.hincrby(key, f"{bucket}:q", amount * amount)
        if event.authorization_outcome is not None:
            pipe.hincrby(key, f"{bucket}:k", 1)
            if event.authorization_outcome is AuthorizationOutcome.DECLINED:
                pipe.hincrby(key, f"{bucket}:d", 1)
        # Trim the buckets that just fell out of retention. Without this the
        # hash grew one field set per active minute for as long as the entity
        # stayed active, because every write refreshed the key's TTL.
        oldest_kept = (occurred_ms - retention * 1000) // MINUTE_MS
        stale = [
            f"{b}:{kind}"
            for b in range(oldest_kept - BUCKET_TRIM_SWEEP, oldest_kept)
            for kind in ("c", "s", "q", "k", "d")
        ]
        pipe.hdel(key, *stale)
        pipe.expire(key, retention)

    def _observe_distinct(
        self, pipe: Any, entity: Entity, entity_id: str, event: Event, occurred_ms: int
    ) -> None:
        for dimension in Dimension:
            value = event.dimension_value(dimension)
            if value is None:
                continue
            storage = PLAN.storage(entity, dimension)
            if storage is None:
                continue
            if storage is CardinalityStorage.EXACT:
                retention = PLAN.exact_distinct_retention(entity, dimension).seconds
                key = self._exact_distinct_key(entity, entity_id, dimension)
                # GT: a late arrival must not move a value's timestamp BACKWARDS,
                # which would drop it out of a window it genuinely belongs to.
                pipe.zadd(key, {value: occurred_ms}, gt=True)
                pipe.zremrangebyscore(key, "-inf", occurred_ms - retention * 1000)
                pipe.expire(key, retention)
            else:
                retention = PLAN.approx_distinct_retention(entity, dimension).seconds
                bucket = occurred_ms // HLL_BUCKET_MS
                key = self._hll_key(entity, entity_id, dimension, bucket)
                pipe.pfadd(key, value)
                pipe.expire(key, retention)

    def _observe_profile(self, pipe: Any, event: Event, occurred_ms: int) -> None:
        retention = PLAN.profile_retention(Entity.ACCOUNT).seconds
        key = self._profile_key(Entity.ACCOUNT, event.account_id, event.currency)
        pipe.hsetnx(key, "first_seen_ms", occurred_ms)
        pipe.hincrby(key, "observations", 1)
        if event.merchant_id is not None:
            pipe.hincrby(key, f"m:{event.merchant_id}", 1)
        if event.merchant_mcc is not None:
            pipe.hincrby(key, f"c:{event.merchant_mcc}", 1)
        if event.device_id is not None:
            pipe.hincrby(key, f"d:{event.device_id}", 1)
        if event.latitude is not None and event.longitude is not None:
            pipe.hset(key, mapping={"lat": event.latitude, "lon": event.longitude})
        pipe.expire(key, retention)

        # A bounded sample of recent amounts, for the robust z-score. Capped, so
        # memory per account is O(1) -- and declared APPROXIMATE because of it.
        akey = self._amounts_key(Entity.ACCOUNT, event.account_id, event.currency)
        pipe.zadd(akey, {f"{occurred_ms}:{event.dedup_key}:{abs(event.amount_minor)}": occurred_ms})
        pipe.zremrangebyrank(akey, 0, -(PROFILE_SAMPLE_SIZE + 1))
        pipe.expire(akey, retention)

    # -- reads --------------------------------------------------------------

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
        """Read every value the feature set needs, in one pipeline."""
        as_of_ms = to_millis(as_of)
        ids: dict[Entity, str | None] = {
            Entity.ACCOUNT: account_id,
            Entity.CARD: card_id,
            Entity.DEVICE: device_id,
            Entity.MERCHANT: merchant_id,
            Entity.IP: ip_id,
        }
        pipe = self._redis.pipeline(transaction=False)
        plan: list[tuple[str, Any]] = []

        # The epoch first, in the same round trip: a snapshot that could not
        # say whether the store was complete would have to assume it was not.
        pipe.get(self.epoch_key)
        plan.append(("epoch", None))

        for entity, entity_id in ids.items():
            if entity_id is None:
                continue
            for stream in Stream:
                # Only what a declared feature reads (ADR-0038, now from PLAN).
                windows = PLAN.velocity.get((entity, stream), ())
                if windows:
                    vkey = self._velocity_key(entity, entity_id, stream)
                    for window in windows:
                        lower = as_of_ms - window.seconds * 1000
                        pipe.zcount(vkey, f"({lower}", as_of_ms)
                        plan.append(("count", (entity, entity_id, stream, window)))
                if (entity, stream) in PLAN.previous:
                    pipe.hgetall(self._previous_key(entity, entity_id, stream))
                    plan.append(("previous", (entity, entity_id, stream)))

            if entity in PLAN.buckets:
                pipe.hgetall(self._bucket_key(entity, entity_id, Stream.TRANSACTION, currency))
                plan.append(("buckets", (entity, entity_id, currency)))

            for dimension in Dimension:
                storage = PLAN.storage(entity, dimension)
                if storage is None:
                    continue
                if storage is CardinalityStorage.EXACT:
                    key = self._exact_distinct_key(entity, entity_id, dimension)
                    for window in PLAN.exact_distinct[(entity, dimension)]:
                        lower = as_of_ms - window.seconds * 1000
                        pipe.zcount(key, f"({lower}", as_of_ms)
                        plan.append(("distinct", (entity, entity_id, dimension, window)))
                else:
                    for window in PLAN.approx_distinct[(entity, dimension)]:
                        keys = self._hll_bucket_keys(entity, entity_id, dimension, as_of_ms, window)
                        pipe.pfcount(*keys)
                        plan.append(("distinct", (entity, entity_id, dimension, window)))

        if Entity.ACCOUNT in PLAN.profiles:
            pipe.hgetall(self._profile_key(Entity.ACCOUNT, account_id, currency))
            plan.append(("profile", (account_id, currency)))
            pipe.zrange(self._amounts_key(Entity.ACCOUNT, account_id, currency), 0, -1)
            plan.append(("amounts", (account_id, currency)))

        return self._assemble(
            plan, pipe.execute(), as_of=as_of, as_of_ms=as_of_ms, account_id=account_id
        )

    def _hll_bucket_keys(
        self, entity: Entity, entity_id: str, dimension: Dimension, as_of_ms: int, window: Window
    ) -> list[str]:
        """Every bucket overlapping the window, inclusive at both ends.

        Inclusive: the partial bucket at each edge may hold values inside the
        window, and omitting it would under-count deterministically -- a worse
        error than HLL's, because it is one-sided.
        """
        first = (as_of_ms - window.seconds * 1000) // HLL_BUCKET_MS
        last = as_of_ms // HLL_BUCKET_MS
        return [
            self._hll_key(entity, entity_id, dimension, bucket) for bucket in range(first, last + 1)
        ]

    def _assemble(
        self,
        plan: list[tuple[str, Any]],
        responses: list[Any],
        *,
        as_of: EventTime,
        as_of_ms: int,
        account_id: str,
    ) -> FeatureContext:
        windows: dict[tuple[Entity, str, Stream, str], WindowState] = {}
        distinct: dict[tuple[Entity, str, str], dict[Dimension, int]] = {}
        buckets: dict[tuple[Entity, str], dict[str, int]] = {}
        previous: dict[tuple[Entity, str, Stream], Observation] = {}
        profiles: dict[tuple[Entity, str], Profile] = {}
        raw_profile: dict[str, str] = {}
        amounts: list[int] = []
        complete_since: EventTime | None = None

        for (kind, target), response in zip(plan, responses, strict=True):
            match kind:
                case "epoch":
                    if response is not None:
                        complete_since = EventTime(from_millis(int(_text(response))))
                case "count":
                    entity, entity_id, stream, window = target
                    if response:
                        windows[(entity, entity_id, stream, window.label)] = WindowState(
                            count=int(response)
                        )
                case "distinct":
                    entity, entity_id, dimension, window = target
                    if response:
                        distinct.setdefault((entity, entity_id, window.label), {})[dimension] = int(
                            response
                        )
                case "buckets":
                    entity, entity_id, _currency = target
                    if response:
                        buckets[(entity, entity_id)] = {
                            _text(k): int(v) for k, v in response.items()
                        }
                case "previous":
                    entity, entity_id, stream = target
                    if response:
                        observation = _observation(response)
                        if observation is not None and observation.occurred_at < as_of:
                            previous[(entity, entity_id, stream)] = observation
                case "profile":
                    raw_profile = {_text(k): _text(v) for k, v in (response or {}).items()}
                case "amounts":
                    amounts = [_amount_of(_text(m)) for m in (response or [])]

        self._merge_bucket_aggregates(windows, buckets, as_of_ms)
        self._merge_distinct(windows, distinct)
        # The account is passed in, not recovered from whichever window happened
        # to come back non-zero. Inferring it meant that an account whose every
        # window count was 0 -- a new account, the case where a profile matters
        # most -- silently lost the profile that had just been fetched for it.
        if raw_profile:
            profiles[(Entity.ACCOUNT, account_id)] = _profile(raw_profile, amounts)

        return FeatureContext(
            as_of=as_of,
            source=FeatureSource.ONLINE_ONLY,
            windows=windows,
            profiles=profiles,
            previous=previous,
            complete_since=complete_since,
            distinct_dimensions={e: PLAN.distinct_dimensions(e) for e in Entity},
        )

    def _merge_bucket_aggregates(
        self,
        windows: dict[tuple[Entity, str, Stream, str], WindowState],
        buckets: dict[tuple[Entity, str], dict[str, int]],
        as_of_ms: int,
    ) -> None:
        """Fold minute buckets into the window states the features read.

        Only buckets strictly inside `(as_of - W, as_of]` are summed, matching
        the half-open window the reference implementation uses. Agreeing at the
        boundary is what makes the parity comparison an equality.
        """
        for (entity, entity_id), fields in buckets.items():
            for window in PLAN.buckets.get(entity, ()):
                lower = (as_of_ms - window.seconds * 1000) // MINUTE_MS
                upper = as_of_ms // MINUTE_MS
                totals = {"c": 0, "s": 0, "q": 0, "d": 0, "k": 0}
                for field, value in fields.items():
                    bucket_text, _, kind = field.partition(":")
                    bucket = int(bucket_text)
                    if lower <= bucket <= upper and kind in totals:
                        totals[kind] += value
                if not totals["c"]:
                    continue
                key = (entity, entity_id, Stream.TRANSACTION, window.label)
                existing = windows.get(key)
                windows[key] = WindowState(
                    count=existing.count if existing else totals["c"],
                    amount_sum_minor=totals["s"],
                    amount_sum_squares=totals["q"],
                    declined_count=totals["d"],
                    outcome_known_count=totals["k"],
                    distinct=existing.distinct if existing else {},
                )

    def _merge_distinct(
        self,
        windows: dict[tuple[Entity, str, Stream, str], WindowState],
        distinct: dict[tuple[Entity, str, str], dict[Dimension, int]],
    ) -> None:
        for (entity, entity_id, label), values in distinct.items():
            key = (entity, entity_id, Stream.TRANSACTION, label)
            # An entity with only distinct-count features has no velocity set
            # and so no state yet; the distinct values are the whole state. Its
            # `count` is then a placeholder nothing reads -- by construction:
            # a COUNT declaration for that entity would put a velocity set in
            # the plan, and the count would be real.
            existing = windows.get(key, WindowState())
            windows[key] = WindowState(
                count=existing.count,
                amount_sum_minor=existing.amount_sum_minor,
                amount_sum_squares=existing.amount_sum_squares,
                declined_count=existing.declined_count,
                outcome_known_count=existing.outcome_known_count,
                distinct=values,
            )


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _amount_of(member: str) -> int:
    """Recover the amount from a sample member `"{occurred_ms}:{dedup}:{amount}"`."""
    parts = member.rsplit(":", 1)
    try:
        return int(parts[-1])
    except ValueError:
        return 0


def _observation(raw: dict[Any, Any]) -> Observation | None:
    fields = {_text(k): _text(v) for k, v in raw.items()}
    try:
        occurred_ms = int(fields["occurred_ms"])
    except (KeyError, ValueError):
        return None
    latitude = float(fields["latitude"]) if fields.get("latitude") else None
    longitude = float(fields["longitude"]) if fields.get("longitude") else None
    return Observation(
        occurred_at=EventTime(from_millis(occurred_ms)),
        latitude=latitude,
        longitude=longitude,
        card_present=fields.get("card_present") == "1",
    )


def _profile(fields: dict[str, str], amounts: list[int]) -> Profile:
    median = mad = None
    if len(amounts) >= MIN_OBSERVATIONS_FOR_ROBUST_Z:
        median = float(statistics.median(amounts))
        mad = float(statistics.median([abs(a - median) for a in amounts]))
    first_seen = fields.get("first_seen_ms")
    return Profile(
        first_seen_at=EventTime(from_millis(int(first_seen))) if first_seen else None,
        observation_count=len(amounts),
        amount_median_minor=median,
        amount_mad_minor=mad,
        habitual_merchants=_habitual(fields, "m:"),
        habitual_mccs=_habitual(fields, "c:"),
        known_devices=frozenset(k[2:] for k in fields if k.startswith("d:")),
        home_latitude=float(fields["lat"]) if fields.get("lat") else None,
        home_longitude=float(fields["lon"]) if fields.get("lon") else None,
    )


def _habitual(fields: dict[str, str], prefix: str) -> frozenset[str]:
    return frozenset(
        key[len(prefix) :]
        for key, value in fields.items()
        if key.startswith(prefix) and int(value) >= HABITUAL_MIN_VISITS
    )


__all__ = ["BUCKET_TRIM_SWEEP", "HLL_BUCKET_MS", "PROFILE_SAMPLE_SIZE", "RedisOnlineFeatureStore"]
