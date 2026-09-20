"""Rebuild the online store from history, and claim completeness only with evidence (ADR-0057).

**What this is.** `docs/PHASE3_PLAN.md` §2 B1: reconstruction rebuilds primitives, never values. A
hydrator replays every observation history holds through `RedisOnlineFeatureStore.observe`, the same
idempotent path the gateway writes through, and then claims completeness by compare-and-set on the
epoch -- only from a time no lost or unreplayed observation can reach.

**The fence.** Hydration runs as the one online-state writer (plan §4.1 point 2). It holds the
writer lock through its own `WriterSupervisor`, waits out the takeover grace and two clock margins
before reading any evidence, checks the fence before every batch, and verifies the store's own
observation counter after every write. A foreign write is therefore detected, not overwritten.

**The epoch.** Before replaying anything, hydration withdraws completeness to
`B = now + 24 h + margin`: past the event time of anything a writer accepted before the fence, so a
crash leaves the store claiming exactly what a withdrawn store claims. The claim then moves the
epoch from `B` to the earlier `T`, in one script, only while the epoch is still `B` and the position
is still hydration's own.

**What bounds `T`** (§4 of the ADR):
- every coverage gap's end plus the future-skew bound, because a write lost inside a gap can carry
  an event time that far ahead;
- the ledger's first session, since writes made before it are invisible to the coverage rule;
- a quarantined gateway record's arrival, since Bronze counted it and Silver holds no canonical row;
- an identity collision's own event time.
An open gap, an open hole, an anomaly, a lost fence or a foreign write make no claim at all.

**Outcomes** come from `app.authorization_outcomes`, the system of record (ADR-0049 §4). The store
applies an outcome only after its row committed, so replaying every row holds a superset of what the
lost store applied, with no delivery path to certify.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Final

from trace_core.contracts.api.transaction import MAX_BACKDATE_S, MAX_CLOCK_SKEW_FUTURE_S
from trace_core.contracts.topics import IDENTITY_EVENTS_V1
from trace_core.domain.enums import AuthorizationOutcome, TransactionChannel
from trace_core.domain.errors import TraceXError
from trace_core.domain.time import EventTime, from_millis, to_millis
from trace_core.features.completeness import CompletenessGuard, HoleLedger
from trace_core.features.observation import Event, IdentityNamespace, authorization_observation
from trace_core.features.semantics import LATE_ARRIVAL_MARGIN_S, Stream
from trace_core.observability import get_logger
from trace_core.observation.coverage import Coverage, Gap, SessionRow
from trace_core.observation.supervisor import WriterSupervisor
from trace_core.repositories.redis_features import LAYOUT, RedisOnlineFeatureStore
from trace_core.repositories.triage_event import PRODUCER_NAME as GATEWAY_PRODUCER
from trace_core.stream.bronze import bronze_topic
from trace_core.stream.bronze_coverage import (
    COVERED_TOPICS,
    BronzeCoverage,
    LedgerConnection,
    assess_bronze_coverage,
    read_ledger,
)
from trace_core.stream.gold import pin_sources
from trace_core.stream.gold_plan import IDENTITY_EVENTS, BuildPlan
from trace_core.stream.lake import LakeConfig
from trace_core.stream.silver_conservation import check_silver_conservation, consumed_bronze
from trace_core.stream.silver_rules import QUARANTINE, silver_topic
from trace_core.stream.tables import snapshot_facts

if TYPE_CHECKING:  # pragma: no cover - typing only
    from pyspark.sql import DataFrame, SparkSession

    from trace_core.stream import checkpoints as _checkpoints  # noqa: F401

_log = get_logger(__name__)

FUTURE_SKEW: Final = dt.timedelta(seconds=MAX_CLOCK_SKEW_FUTURE_S)
"""How far ahead of its write an accepted observation may be dated."""
BACKDATE: Final = dt.timedelta(seconds=MAX_BACKDATE_S)
"""How far behind its write an accepted observation may be dated."""
QUIESCENCE_MARGIN_S: Final = 0.1
"""Added to two clock margins, so the rule's tail bound is strictly past (ADR-0051 §5)."""
BATCH_OBSERVATIONS: Final = 1_000
"""How many observations one marker update covers, subject to the event-time span bound."""
HYDRATION_PRODUCER: Final = "trace-hydrator"
"""Never the gateway's producer: coverage reads the gateway's sessions, and hydration produces
nothing."""
STATE_REPLAYING: Final = "replaying"
STATE_DISCARDING: Final = "discarding"
STATE_CLAIMED: Final = "claimed"

_ROW_COLUMNS: Final = (
    "stream",
    "identity_namespace",
    "event_id",
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
)


class HydrationRefusedError(TraceXError):
    """A precondition does not hold, and nothing was written."""


class HydrationFailedError(TraceXError):
    """The run cannot continue: the fence was lost, or another writer recorded an observation."""


# ------------------------------------------------------------------- pure ---


def after_ms(moment: dt.datetime) -> int:
    """The first millisecond strictly after `moment`."""
    return to_millis(moment) + 1


def event_time_image(gap: Gap) -> tuple[dt.datetime, dt.datetime | None]:
    """The event times a write lost inside `gap` could carry (plan §4.1 point 6).

    Only the upper edge can cross a claim, which vouches for `[T, infinity)`; the lower edge is
    computed and logged so the whole mapping is visible rather than implied.
    """
    return gap.start - BACKDATE, None if gap.end is None else gap.end + FUTURE_SKEW


def replay_span_bound_ms() -> int:
    """How wide one batch's event-time span may be, so nothing it wrote can be folded or dropped.

    A transaction is folded, and an identity event dropped, only by a later observation of the same
    account more than the raw horizon ahead of it. A batch narrower than that horizon, less the
    late-arrival margin, therefore re-delivers only observations the store still recognises, which
    is what makes a resume after a crash a no-op.
    """
    bound = min(LAYOUT.raw_tx_ms, LAYOUT.raw_ie_ms) - LATE_ARRIVAL_MARGIN_S * 1_000
    if bound <= 0:  # pragma: no cover - a declaration change would have to invert the horizons
        raise HydrationRefusedError(
            f"the raw horizons ({LAYOUT.raw_tx_ms} ms, {LAYOUT.raw_ie_ms} ms) leave no batch span "
            f"above the late-arrival margin; resuming could no longer be idempotent"
        )
    return bound


def digest_of(previous: str, identity: str) -> str:
    """The running digest over replayed identities: what a resume compares its prefix against."""
    return hashlib.sha256(f"{previous}\n{identity}".encode()).hexdigest()


def cursor_text(order_key: tuple[int, str, str] | None) -> str:
    return "" if order_key is None else json.dumps(list(order_key), separators=(",", ":"))


def parse_conflicts(text: str) -> list[int]:
    """The identity conflicts a crashed run had already found (ADR-0057 §4(b)).

    Unreadable content is refused, never read as "no conflicts": each one raises `T`, so losing one
    claims completeness over a window a lost observation could fall in -- silently, and in the
    direction that costs data rather than the direction that costs a re-run.
    """
    if not text:
        return []
    try:
        return [int(part) for part in text.split(",")]
    except ValueError as exc:
        raise HydrationRefusedError(
            f"the hydration marker holds unreadable identity conflicts: {exc}"
        ) from exc


def parse_cursor(text: str) -> tuple[int, str, str] | None:
    if not text:
        return None
    try:
        occurred, namespace, event_id = json.loads(text)
        return int(occurred), str(namespace), str(event_id)
    except (ValueError, TypeError) as exc:
        raise HydrationRefusedError(
            f"the hydration marker holds an unreadable cursor: {exc}"
        ) from exc


@dataclass(frozen=True, slots=True)
class LostRecord:
    """A record Bronze counted as an observation that Silver holds no canonical row for."""

    topic: str
    arrival: dt.datetime
    """Its Kafka LogAppendTime. Its write preceded it, within the clock margin."""
    detail: str


@dataclass(frozen=True, slots=True)
class ClaimInputs:
    coverage: Coverage
    sessions: tuple[SessionRow, ...]
    ledger_read_at: dt.datetime
    clock_margin_s: float
    begin_epoch_ms: int
    quarantined: tuple[LostRecord, ...] = ()
    conflicts: tuple[int, ...] = ()
    open_holes: int = 0
    fence_held: bool = True
    foreign_sessions: tuple[str, ...] = ()
    outcomes_changed: bool = False


@dataclass(frozen=True, slots=True)
class Claim:
    """The earliest completeness a rebuilt store may claim, or why it may claim none."""

    since_ms: int | None
    reasons: tuple[str, ...]
    components: Mapping[str, int]

    @property
    def claimable(self) -> bool:
        return self.since_ms is not None

    def summary(self) -> dict[str, Any]:
        return {
            "since_ms": self.since_ms,
            "since": None if self.since_ms is None else from_millis(self.since_ms).isoformat(),
            "reasons": list(self.reasons),
            "components": dict(sorted(self.components.items())),
        }


def compute_claim(inputs: ClaimInputs) -> Claim:
    """The claim `T`, from evidence alone (ADR-0057 §4)."""
    margin = dt.timedelta(seconds=inputs.clock_margin_s)
    reasons: list[str] = []
    if not inputs.fence_held:
        reasons.append("the writer fence is not held")
    if inputs.open_holes:
        reasons.append(f"{inputs.open_holes} completeness hole(s) are open in the ledger")
    if inputs.foreign_sessions:
        reasons.append(
            f"gateway session(s) opened during hydration: {list(inputs.foreign_sessions)}"
        )
    if inputs.outcomes_changed:
        reasons.append("an authorization outcome was recorded during hydration")
    anomalies = [
        *inputs.coverage.anomalies,
        *(
            anomaly
            for session in inputs.coverage.sessions.values()
            for anomaly in session.anomalies
        ),
    ]
    reasons.extend(f"coverage anomaly: {anomaly}" for anomaly in anomalies)

    components: dict[str, int] = {}
    origin = min((row.started_at for row in inputs.sessions), default=inputs.ledger_read_at)
    components["ledger_origin"] = after_ms(origin + FUTURE_SKEW + margin)
    for gap in inputs.coverage.gaps:
        _, upper = event_time_image(gap)
        if upper is None:
            reasons.append(
                f"an open gap in session {gap.session_id} from {gap.start.isoformat()}: "
                f"nothing bounds what it may still hold"
            )
            continue
        components["coverage_gaps"] = max(components.get("coverage_gaps", 0), after_ms(upper))
    for record in inputs.quarantined:
        components["quarantined"] = max(
            components.get("quarantined", 0), after_ms(record.arrival + margin + FUTURE_SKEW)
        )
    for occurred_ms in inputs.conflicts:
        components["identity_conflicts"] = max(
            components.get("identity_conflicts", 0), occurred_ms + 1
        )
    since = max(components.values())
    if reasons:
        return Claim(None, tuple(reasons), components)
    if since >= inputs.begin_epoch_ms:
        return Claim(
            None,
            (
                f"nothing is claimable: the evidence reaches only "
                f"{from_millis(since).isoformat()}, at or after the withdrawal this run began "
                f"with",
            ),
            components,
        )
    return Claim(since, (), components)


def event_from_row(row: Mapping[str, Any]) -> Event:
    """One Silver observation row as the online store's `Event`."""
    channel = row["channel"]
    latitude, longitude = row["latitude"], row["longitude"]
    return Event(
        stream=Stream(str(row["stream"])),
        occurred_at=EventTime(from_millis(int(row["occurred_ms"]))),
        account_id=str(row["account_id"]),
        event_id=str(row["event_id"]),
        currency=str(row["currency"] or ""),
        amount_minor=int(row["amount_minor"] or 0),
        card_id=_optional(row["card_id"]),
        device_id=_optional(row["device_id"]),
        merchant_id=_optional(row["merchant_id"]),
        ip_id=_optional(row["ip_id"]),
        merchant_mcc=_optional(row["merchant_mcc"]),
        merchant_country=_optional(row["merchant_country"]),
        latitude=None if latitude is None else float(latitude),
        longitude=None if longitude is None else float(longitude),
        channel=None if channel is None else TransactionChannel(str(channel)),
        authorization_outcome=None,
    )


def _optional(value: Any) -> str | None:
    return None if value is None else str(value)


def merge_ordered(rows: Iterable[Event], outcomes: Sequence[Event]) -> Iterator[Event]:
    """Both sides in the declared total order, merged into one (ADR-0046 §1)."""
    pending = deque(sorted(outcomes, key=lambda event: event.order_key))
    for event in rows:
        while pending and pending[0].order_key < event.order_key:
            yield pending.popleft()
        yield event
    while pending:
        yield pending.popleft()


# --------------------------------------------------------------- evidence ---


@dataclass(frozen=True, slots=True)
class Evidence:
    """One read of everything the claim rests on, in the order ADR-0057 §2 fixes."""

    coverage: BronzeCoverage
    sessions: tuple[SessionRow, ...]
    ledger_read_at: dt.datetime
    plan: BuildPlan
    consumed: Mapping[str, int]
    quarantined: tuple[LostRecord, ...]
    outcomes: tuple[Event, ...]
    outcome_count: int
    quarantine_version: int

    def summary(self) -> dict[str, Any]:
        return {
            "bronze_versions": dict(self.coverage.table_versions),
            "silver_consumed": dict(self.consumed),
            "silver_pins": {pin.table: pin.version for pin in self.plan.sources},
            "quarantine_version": self.quarantine_version,
            "sessions": len(self.sessions),
            "gaps": len(self.coverage.coverage.gaps),
            "quarantined": len(self.quarantined),
            "outcomes": self.outcome_count,
        }


def read_outcomes(connection: LedgerConnection) -> tuple[tuple[Event, ...], int]:
    """Every recorded authorization outcome as an observation (ADR-0049 §4)."""
    rows = connection.execute(
        "SELECT transaction_id, account_id, authorization_outcome, decided_at "
        "FROM app.authorization_outcomes ORDER BY decided_at, transaction_id",
        (),
    ).fetchall()
    events = tuple(
        authorization_observation(
            transaction_id=str(transaction_id),
            account_id=str(account_id),
            authorization_outcome=AuthorizationOutcome(str(outcome)),
            decided_at=EventTime(decided_at),
        )
        for transaction_id, account_id, outcome, decided_at in rows
    )
    return events, len(events)


def count_outcomes(connection: LedgerConnection) -> int:
    row = connection.execute("SELECT count(*) FROM app.authorization_outcomes", ()).fetchall()
    return int(row[0][0])


def quarantined_records(
    spark: SparkSession, lake: LakeConfig, version: int
) -> tuple[LostRecord, ...]:
    """Covered-topic records Silver did not admit: each one bounds the claim (ADR-0057 §4).

    A `tx.scored.v1` record has no producer but the gateway. An `identity.events.v1` record counts
    only when it carries the session headers, because the generator produces that topic too.
    """
    from pyspark.sql import functions as F  # noqa: N812

    session_header = F.expr("exists(kafka_headers, header -> header.key = 'tracex-session-id')")
    rows = (
        spark.read.format("delta")
        .option("versionAsOf", str(version))
        .load(str(QUARANTINE.local_path(lake)))
        .filter(F.col("silver_topic").isin(list(COVERED_TOPICS)))
        .filter((F.col("silver_topic") != F.lit(IDENTITY_EVENTS_V1)) | session_header)
        .select("silver_topic", "kafka_timestamp", "reason")
        .collect()
    )
    return tuple(
        LostRecord(
            topic=str(row["silver_topic"]),
            arrival=row["kafka_timestamp"].replace(tzinfo=dt.UTC)
            if row["kafka_timestamp"].tzinfo is None
            else row["kafka_timestamp"],
            detail=str(row["reason"]),
        )
        for row in rows
    )


def read_evidence(
    spark: SparkSession,
    lake: LakeConfig,
    *,
    ledger: LedgerConnection,
    clock_margin_s: float,
    now: dt.datetime,
) -> Evidence:
    """Coverage, then Silver's consumption of it, then the pins, in that order (ADR-0057 §2).

    Raises `HydrationRefusedError` when Silver has not consumed the Bronze the coverage was read
    at, or when its conservation is not clean: the store would then be rebuilt from less than the
    evidence vouches for.
    """
    from trace_core.stream import checkpoints

    sessions, ledger_read_at = read_ledger(ledger)
    coverage = assess_bronze_coverage(spark, lake, ledger, clock_margin_s=clock_margin_s)
    consumed: dict[str, int] = {}
    for topic in COVERED_TOPICS:
        spec = silver_topic(topic)
        state = checkpoints.read_state(lake, spec.query)
        if state.identity is None:
            raise HydrationRefusedError(
                f"{spec.query} has no checkpoint: Silver has not read {topic}, so Silver cannot "
                f"hold what Bronze coverage vouches for"
            )
        directory = checkpoints.query_directory(lake, spec.query) / f"v{state.identity.version}"
        source = bronze_topic(topic).table
        read_through = consumed_bronze(
            directory, state.identity, f"delta:{source}", source.local_path(lake)
        )
        if read_through.problems or read_through.through_version is None:
            raise HydrationRefusedError(
                f"{spec.query}: {'; '.join(read_through.problems) or 'nothing committed'}"
            )
        wanted = coverage.table_versions[topic]
        if read_through.through_version < wanted:
            raise HydrationRefusedError(
                f"{spec.query} has read {source} through version {read_through.through_version}, "
                f"below the version {wanted} the coverage was read at: run Silver and retry"
            )
        report = check_silver_conservation(spark, lake, topic)
        if not report.conserved:
            raise HydrationRefusedError(
                f"{topic}: Bronze is not conserved into Silver ({report.summary()})"
            )
        consumed[topic] = read_through.through_version
    plan = pin_sources(spark, lake, 0, now)
    facts = snapshot_facts(spark, QUARANTINE.local_path(lake))
    if facts is None:
        raise HydrationRefusedError(f"{QUARANTINE} does not exist, so Silver has never run")
    quarantined = quarantined_records(spark, lake, facts.version)
    outcomes, outcome_count = read_outcomes(ledger)
    return Evidence(
        coverage=coverage,
        sessions=sessions,
        ledger_read_at=ledger_read_at,
        plan=plan,
        consumed=consumed,
        quarantined=quarantined,
        outcomes=outcomes,
        outcome_count=outcome_count,
        quarantine_version=facts.version,
    )


def observation_frame(spark: SparkSession, lake: LakeConfig, plan: BuildPlan) -> DataFrame:
    """Every observation the online store would have recorded, in the declared total order.

    Gold's own projection of Silver (ADR-0055 §1) supplies the rows, with two corrections the
    online store's identities need:
    - only the gateway's identity events are observations. The generator publishes the same topic,
      and the store never saw those;
    - a gateway identity event is keyed by the `idev_` id it carries in `correlation_id`, not by
      the Silver envelope `event_id` (ADR-0055 §9).
    Authorization outcomes are dropped here: they come from the system of record instead.
    """
    from pyspark.sql import functions as F  # noqa: N812

    from trace_core.stream.gold_features import observations, read_pinned

    frame = observations(spark, lake, plan)
    transactions = frame.filter(
        F.col("identity_namespace") == F.lit(IdentityNamespace.TRANSACTION.value)
    )
    identity = frame.filter(
        F.col("identity_namespace") == F.lit(IdentityNamespace.IDENTITY_EVENT.value)
    )
    producers = read_pinned(spark, lake, plan, IDENTITY_EVENTS).select(
        F.col("event_id").alias("_silver_event_id"), F.col("producer").alias("_producer")
    )
    gateway = (
        identity.join(producers, F.col("event_id") == F.col("_silver_event_id"), "inner")
        .filter(F.split(F.col("_producer"), "@").getItem(0) == F.lit(GATEWAY_PRODUCER))
        .withColumn("event_id", F.col("correlation_id"))
    )
    unkeyed = gateway.filter(
        F.col("event_id").isNull() | (F.length(F.trim(F.col("event_id"))) == 0)
    ).count()
    if unkeyed:
        raise HydrationRefusedError(
            f"{unkeyed} gateway identity event(s) carry no correlation_id, so the identity the "
            f"online store recorded them under is unknown"
        )
    columns = [F.col(name) for name in _ROW_COLUMNS]
    return (
        transactions.select(*columns)
        .unionByName(gateway.select(*columns))
        .orderBy("occurred_ms", "identity_namespace", "event_id")
    )


# -------------------------------------------------------------- the run ---


@dataclass(frozen=True, slots=True)
class ReplayProgress:
    replayed: int
    recorded: int
    redeliveries: int
    conflicts: tuple[int, ...]
    position: int
    cursor: tuple[int, str, str] | None
    digest: str
    resumed_from: int

    def summary(self) -> dict[str, Any]:
        return {
            "replayed": self.replayed,
            "recorded": self.recorded,
            "redeliveries": self.redeliveries,
            "conflicts": len(self.conflicts),
            "position": self.position,
            "resumed_from": self.resumed_from,
        }


@dataclass(frozen=True, slots=True)
class HydrationReport:
    run_id: str
    begin_epoch_ms: int
    claim: Claim
    claimed: bool
    outcome: str
    progress: ReplayProgress
    evidence: Mapping[str, Any]

    def summary(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "begin_epoch_ms": self.begin_epoch_ms,
            "claimed": self.claimed,
            "outcome": self.outcome,
            "claim": self.claim.summary(),
            "replay": self.progress.summary(),
            "evidence": dict(self.evidence),
        }


@dataclass
class Hydrator:
    """One reconstruction run: fence, marker, replay, claim (ADR-0057)."""

    store: RedisOnlineFeatureStore
    spark: SparkSession
    lake: LakeConfig
    writer: WriterSupervisor
    holes: HoleLedger
    ledger: LedgerConnection
    clock_margin_s: float
    run_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    instance_id: str = ""
    batch_size: int = BATCH_OBSERVATIONS
    now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.UTC)
    sleep: Callable[[float], None] = time.sleep
    discard_unfinished: bool = False
    _session_id: str | None = field(default=None, init=False)
    _begin_ms: int = field(default=0, init=False)
    _begun_ms: int = field(default=0, init=False)
    _resumed: bool = field(default=False, init=False)

    def __post_init__(self) -> None:
        if not self.instance_id:
            self.instance_id = f"hydrate:{self.run_id[:8]}"

    # -- phases ------------------------------------------------------------

    def prepare(self) -> str:
        """Take the fence's quiescence, then create or adopt the marker and withdraw to `B`."""
        if not self.writer.ready:
            raise HydrationRefusedError(f"not the fenced writer: {self.writer.status}")
        session = self.writer.session
        self._session_id = None if session is None else session.session_id
        self.sleep(2 * self.clock_margin_s + QUIESCENCE_MARGIN_S)
        if not self.writer.ready_as(self._session_id):
            raise HydrationRefusedError(
                f"the writer fence was lost while waiting: {self.writer.status}"
            )
        now = self.now()
        begin = to_millis(now) + int(FUTURE_SKEW.total_seconds() * 1_000)
        begin += int(self.clock_margin_s * 1_000) + 1
        fields = {
            "run_id": self.run_id,
            "state": STATE_REPLAYING,
            "begun_ms": str(to_millis(now)),
            "begin_epoch_ms": str(begin),
            "cursor": "",
            "count": "0",
            "position": "0",
            "digest": "",
            "conflicts": "",
            "batch_size": str(self.batch_size),
        }
        kind, marker = self.store.start_hydration(fields)
        if kind == "occupied":
            raise HydrationRefusedError(
                f"the store already holds an epoch ({marker['epoch'] or 'none'}) or observations "
                f"(position {marker['position'] or '0'}): hydration claims only on a store it "
                f"started (ADR-0057 §1)"
            )
        if kind == "marker":
            return self._adopt(marker)
        self._begin_ms = begin
        self._begun_ms = to_millis(now)
        self._withdraw_to_begin()
        _log.info("hydration_started", run_id=self.run_id, begin_epoch_ms=begin, resumed=False)
        return "created"

    def _adopt(self, marker: Mapping[str, str]) -> str:
        state = marker.get("state", "")
        if state == STATE_CLAIMED:
            self._begin_ms = int(marker["begin_epoch_ms"])
            self._begun_ms = int(marker["begun_ms"])
            self._resumed = True
            return STATE_CLAIMED
        if state not in {STATE_REPLAYING, STATE_DISCARDING}:
            raise HydrationRefusedError(f"the hydration marker holds an unknown state {state!r}")
        begin = int(marker["begin_epoch_ms"])
        stored = self.store.stored_epoch_ms()
        # No epoch at all is ADR-0057 §6's "after the marker, before `B`" row: `prepare` writes the
        # marker and only then withdraws, so a crash between those two round trips leaves exactly
        # this. Nothing is vouched for and nothing was replayed, so the run adopts the marker and
        # withdraws to a fresh `B'`, which only ever moves the epoch later. Refusing it instead
        # wedged the namespace: the discard branch below was unreachable, and recovery meant
        # deleting keys by hand -- the "wipe and restart" this ADR's alternatives reject.
        if stored is not None and stored != begin:
            raise HydrationRefusedError(
                f"the store's epoch is {stored}, not the {begin} an unfinished hydration "
                f"withdrew to: something else has moved it, so this run may claim nothing"
            )
        self._begin_ms = begin
        self._begun_ms = int(marker["begun_ms"])
        self._refuse_foreign_writes(marker)
        if stored is None:
            # The crash above left a marker and no epoch, so this store vouches for nothing and
            # protects nothing: `observe`'s NX would date the epoch from the first replayed
            # observation (ADR-0057 §5.2). Withdraw to a fresh `B'` computed from this run's clock
            # -- later than the marker's `B`, and the epoch only ever moves later.
            now = self.now()
            begin = to_millis(now) + int(FUTURE_SKEW.total_seconds() * 1_000)
            begin += int(self.clock_margin_s * 1_000) + 1
            self._begin_ms = begin
            self._begun_ms = to_millis(now)
            if not self.store.update_hydration(
                run_id=str(marker["run_id"]),
                state=STATE_REPLAYING,
                fields={"begin_epoch_ms": str(begin), "begun_ms": str(to_millis(now))},
            ):
                raise HydrationRefusedError("another run adopted the hydration marker first")
            self._withdraw_to_begin()
            _log.info("hydration_withdrew_after_marker_only_crash", begin_epoch_ms=begin)
        if state == STATE_DISCARDING or self.discard_unfinished:
            deleted = self.store.discard_unfinished_hydration(
                run_id=str(marker["run_id"]), begin_epoch_ms=begin
            )
            if deleted < 0:
                raise HydrationRefusedError("the unfinished hydration could not be discarded")
            _log.warning("hydration_discarded", run_id=self.run_id, keys=deleted)
        if not self.store.update_hydration(
            run_id=str(marker["run_id"]), state=STATE_REPLAYING, fields={"run_id": self.run_id}
        ):
            raise HydrationRefusedError("another run adopted the hydration marker first")
        self._resumed = True
        _log.info("hydration_resumed", run_id=self.run_id, begin_epoch_ms=begin)
        return "resumed"

    def _refuse_foreign_writes(self, marker: Mapping[str, str]) -> None:
        """A gateway that wrote while this hydration was not running invalidates the resume."""
        recorded = int(marker.get("position") or 0)
        stored = self.store.stored_position()
        # The crashed run's batch size, not this one's: a run killed at --batch-size 2 and resumed
        # at the 1,000 default would otherwise tolerate 1,000 observations written by someone else.
        tolerance = int(marker.get("batch_size") or self.batch_size)
        if stored > recorded + tolerance:
            raise HydrationRefusedError(
                f"the store's position is {stored}, more than one batch ({tolerance}) beyond the "
                f"{recorded} the marker recorded: another writer has recorded observations"
            )
        foreign = self._foreign_sessions()
        if foreign:
            raise HydrationRefusedError(
                f"gateway session(s) {foreign} opened after this hydration began: another writer "
                f"may have recorded observations"
            )

    def _foreign_sessions(self) -> tuple[str, ...]:
        """Gateway sessions that opened after this run began, on the database clock.

        Widened by the clock margin towards reporting one: a session this run cannot rule out is
        treated as a writer, and the store's own counter catches what the clocks miss.
        """
        sessions, _ = read_ledger(self.ledger)
        begun = from_millis(self._begun_ms) - dt.timedelta(seconds=self.clock_margin_s)
        return tuple(row.session_id for row in sessions if row.started_at > begun)

    def _withdraw_to_begin(self) -> None:
        guard = CompletenessGuard(
            self.store, self.holes, instance_id=self.instance_id, clock=self.now
        )
        guard.resume()
        guard.reconcile()
        self.store.withdraw_completeness(resume_at=EventTime(from_millis(self._begin_ms)))
        stored = self.store.stored_epoch_ms()
        if stored != self._begin_ms:
            raise HydrationRefusedError(
                f"the store's epoch is {stored}, later than the {self._begin_ms} hydration "
                f"withdrew to: it vouches for less than this run could claim"
            )

    def replay(self, evidence: Evidence) -> ReplayProgress:
        """Write every observation, in event-time order, through the store's own path."""
        marker = self.store.hydration_marker()
        cursor = parse_cursor(marker.get("cursor", ""))
        count = int(marker.get("count") or 0)
        digest = marker.get("digest", "")
        # The counter comes from the store, not from the marker: a crash inside a batch leaves the
        # store ahead of the last marker update, and those writes are this run's own. `prepare`
        # has already refused a resume whose store moved further than one batch, or whose ledger
        # shows a gateway session since. From here the per-write check below is what detects a
        # writer racing this run.
        position = self.store.stored_position()
        bound = replay_span_bound_ms()
        frame = observation_frame(self.spark, self.lake, evidence.plan)
        rows = (event_from_row(row.asDict()) for row in frame.toLocalIterator())
        state = _ReplayState(
            count=count,
            position=position,
            digest=digest,
            resumed_from=count,
            conflicts=parse_conflicts(marker.get("conflicts", "")),
        )
        prefix_seen, prefix_digest, checked = 0, "", cursor is None
        batch: list[Event] = []
        for event in merge_ordered(rows, evidence.outcomes):
            if cursor is not None and event.order_key <= cursor:
                prefix_seen += 1
                prefix_digest = digest_of(prefix_digest, event.identity)
                continue
            if not checked:
                self._check_prefix(prefix_seen, prefix_digest, count, digest)
                checked = True
            if batch and (
                len(batch) >= self.batch_size or event.occurred_ms - batch[0].occurred_ms >= bound
            ):
                self._flush(batch, state)
                batch = []
            batch.append(event)
        if not checked:
            self._check_prefix(prefix_seen, prefix_digest, count, digest)
        if batch:
            self._flush(batch, state)
        progress = ReplayProgress(
            replayed=state.count - state.resumed_from,
            recorded=state.recorded,
            redeliveries=state.redeliveries,
            conflicts=tuple(state.conflicts),
            position=state.position,
            cursor=state.cursor,
            digest=state.digest,
            resumed_from=state.resumed_from,
        )
        _log.info("hydration_replayed", run_id=self.run_id, **progress.summary())
        return progress

    def _check_prefix(self, seen: int, seen_digest: str, count: int, digest: str) -> None:
        if seen == count and seen_digest == digest:
            return
        raise HydrationRefusedError(
            f"history below the cursor has changed since the unfinished run: it now holds {seen} "
            f"observation(s) where the marker recorded {count}. Resuming would replay them out of "
            f"order; re-run with --discard-unfinished to rebuild from an empty store"
        )

    def _flush(self, batch: Sequence[Event], state: _ReplayState) -> None:
        if not self.writer.ready_as(self._session_id):
            raise HydrationFailedError(f"the writer fence was lost: {self.writer.status}")
        for event in batch:
            receipt = self.store.observe(event)
            expected = state.position + (1 if receipt.recorded else 0)
            if receipt.position != expected:
                raise HydrationFailedError(
                    f"the store's observation counter moved to {receipt.position}, not the "
                    f"{expected} this run wrote: another writer recorded an observation"
                )
            state.position = receipt.position
            state.count += 1
            if receipt.recorded:
                state.recorded += 1
            else:
                state.redeliveries += 1
            if receipt.conflicting:
                state.conflicts.append(event.occurred_ms)
            state.digest = digest_of(state.digest, event.identity)
        state.cursor = batch[-1].order_key
        if not self.store.update_hydration(
            run_id=self.run_id,
            state=STATE_REPLAYING,
            fields={
                "cursor": cursor_text(state.cursor),
                "count": str(state.count),
                "position": str(state.position),
                "digest": state.digest,
                # An identity conflict is evidence the claim needs (ADR-0057 §4(b)): the lost store
                # may hold either row. It is found once, while replaying, so a resume that did not
                # carry it forward would drop the `identity_conflicts` component and claim `T`
                # EARLIER than the evidence allows -- the over-claiming direction.
                "conflicts": ",".join(str(ms) for ms in state.conflicts),
            },
        ):
            raise HydrationFailedError("the hydration marker is no longer this run's")

    def claim(self, evidence: Evidence, progress: ReplayProgress) -> HydrationReport:
        """Re-check the refusals, then move the epoch out of `B` in one script."""
        inputs = ClaimInputs(
            coverage=evidence.coverage.coverage,
            sessions=evidence.sessions,
            ledger_read_at=evidence.ledger_read_at,
            clock_margin_s=self.clock_margin_s,
            begin_epoch_ms=self._begin_ms,
            quarantined=evidence.quarantined,
            conflicts=progress.conflicts,
            open_holes=self.holes.open_holes(),
            fence_held=self.writer.ready_as(self._session_id),
            foreign_sessions=self._foreign_sessions(),
            outcomes_changed=count_outcomes(self.ledger) != evidence.outcome_count,
        )
        claim = compute_claim(inputs)
        if claim.since_ms is None:
            _log.warning("hydration_not_claimed", run_id=self.run_id, **claim.summary())
            return HydrationReport(
                run_id=self.run_id,
                begin_epoch_ms=self._begin_ms,
                claim=claim,
                claimed=False,
                outcome="not_claimable",
                progress=progress,
                evidence=evidence.summary(),
            )
        outcome = self.store.claim_completeness(
            run_id=self.run_id,
            begin_epoch_ms=self._begin_ms,
            expected_position=progress.position,
            since_ms=claim.since_ms,
        )
        claimed = outcome == "claimed"
        log = _log.info if claimed else _log.warning
        log(
            "hydration_claimed" if claimed else "hydration_not_claimed",
            run_id=self.run_id,
            outcome=outcome,
            **claim.summary(),
        )
        return HydrationReport(
            run_id=self.run_id,
            begin_epoch_ms=self._begin_ms,
            claim=claim,
            claimed=claimed,
            outcome=outcome,
            progress=progress,
            evidence=evidence.summary(),
        )

    def run(self) -> HydrationReport:
        """Prepare, read the evidence, replay, claim. Idempotent: a claimed run writes nothing."""
        state = self.prepare()
        if state == STATE_CLAIMED:
            marker = self.store.hydration_marker()
            claimed_ms = int(marker.get("claimed_ms") or 0)
            claim = Claim(claimed_ms, (), {"already_claimed": claimed_ms})
            progress = ReplayProgress(0, 0, 0, (), self.store.stored_position(), None, "", 0)
            if self.store.stored_epoch_ms() != claimed_ms:
                raise HydrationRefusedError(
                    f"the marker says this store was hydrated to {claimed_ms}, but its epoch is "
                    f"{self.store.stored_epoch_ms()}: a live store is never re-hydrated"
                )
            _log.info("hydration_already_claimed", run_id=self.run_id, since_ms=claimed_ms)
            return HydrationReport(
                run_id=str(marker.get("run_id", self.run_id)),
                begin_epoch_ms=int(marker["begin_epoch_ms"]),
                claim=claim,
                claimed=True,
                outcome="already_claimed",
                progress=progress,
                evidence={},
            )
        evidence = read_evidence(
            self.spark,
            self.lake,
            ledger=self.ledger,
            clock_margin_s=self.clock_margin_s,
            now=self.now(),
        )
        progress = self.replay(evidence)
        return self.claim(evidence, progress)


@dataclass
class _ReplayState:
    count: int
    position: int
    digest: str
    resumed_from: int
    recorded: int = 0
    redeliveries: int = 0
    conflicts: list[int] = field(default_factory=list)
    cursor: tuple[int, str, str] | None = None


__all__ = [
    "BATCH_OBSERVATIONS",
    "HYDRATION_PRODUCER",
    "Claim",
    "ClaimInputs",
    "Evidence",
    "HydrationFailedError",
    "HydrationRefusedError",
    "HydrationReport",
    "Hydrator",
    "LostRecord",
    "ReplayProgress",
    "after_ms",
    "compute_claim",
    "cursor_text",
    "digest_of",
    "event_from_row",
    "event_time_image",
    "merge_ordered",
    "observation_frame",
    "parse_cursor",
    "read_evidence",
    "read_outcomes",
    "replay_span_bound_ms",
]
