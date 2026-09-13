"""The hot-path metric set, declared once (docs/ARCHITECTURE.md §13).

Names live here rather than at each call site for the same reason the partition
keys do: two spellings of one metric produce two series, and the one an alert
watches is whichever the author of the alert happened to see first. §13 names
them, `tests/unit/test_metrics.py` diffs this module against that section, and
neither can drift without the other.

**What is counted matters as much as what is measured.** Every degraded decision
increments `degraded_mode_total{reason}`: CLAUDE.md §3.7 permits failing open and
requires that every fail-open be tagged and counted, and a fail-open nobody counts
is indistinguishable from a healthy system. The same applies to rule abstention —
a pack that has quietly gone blind looks exactly like a quiet day in
`tx_scored_total` alone.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Final

from opentelemetry.metrics import CallbackOptions, Counter, Histogram, Observation

from trace_core.observability.telemetry import get_meter

# Latency buckets chosen around the budget rather than around round numbers:
# the 100 ms p99 target and the 20 ms p50 target both need a boundary near them,
# or the histogram cannot answer the only two questions anyone will ask of it.
LATENCY_BUCKETS_S: Final = (
    0.001,
    0.0025,
    0.005,
    0.010,
    0.020,
    0.035,
    0.050,
    0.075,
    0.100,
    0.250,
    0.500,
    1.0,
)

TX_SCORE_LATENCY: Final = "tx_score_latency_seconds"
"""SCORING ONLY: canonical mapping, feature read, feature evaluation, rules,
decision assembly. It is the `latency_ms` the caller is told.

Explicitly not the request. It used to also span triage and the observe-write,
which made it a number that described neither one thing nor the other -- and on
a workload where most requests open an investigation, "scoring latency" that
silently included a Postgres transaction is the kind of plausible-but-wrong
telemetry that gets trusted."""

REQUEST_LATENCY: Final = "gateway_request_latency_seconds"
"""THE WHOLE server-side request: authentication, rate limiting, the replay
lookup, scoring, triage, the observe-write, the replay store and serialisation.

The one to compare against a p99 budget. Measured from the first thing the
handler does to the last, so the only latency it excludes is what happens
outside the process -- which is exactly the part a server cannot fix and must
not take credit for hiding."""
FEATURE_READ_LATENCY: Final = "feature_read_latency_seconds"
TX_SCORED_TOTAL: Final = "tx_scored_total"
DEGRADED_MODE_TOTAL: Final = "degraded_mode_total"
RULE_FIRED_TOTAL: Final = "rule_fired_total"
RULE_ABSTAINED_TOTAL: Final = "rule_abstained_total"
FEATURE_UNAVAILABLE_TOTAL: Final = "feature_unavailable_total"
TRIAGE_ENQUEUED_TOTAL: Final = "triage_enqueued_total"
IDEMPOTENT_REPLAY_TOTAL: Final = "idempotent_replay_total"
RATE_LIMITED_TOTAL: Final = "rate_limited_total"
UNAUTHENTICATED_TOTAL: Final = "unauthenticated_total"
RULE_PACK_RELOAD_FAILED_TOTAL: Final = "rule_pack_reload_failed_total"

ONLINE_STORE_EVICTED_KEYS: Final = "online_store_evicted_keys_total"
"""Keys Redis has discarded under its own memory pressure.

**The only signal that distinguishes two indistinguishable things.** An evicted
feature key reads back empty, and an empty read is what an account with no
history looks like -- so eviction silently converts `INSUFFICIENT_HISTORY` from
"this account is new" into "we lost what we knew about this account". The rules
tier then abstains, the score falls, and a transaction that should have been
CRITICAL is scored LOW with `degraded=false`. Nothing in the decision says so.

The remedies are opposite (wait, versus provision memory) and the consequences
are opposite (a correct abstention, versus a missed detection), which is exactly
why ADR-0032 refused to fold the two feature states together. This counter is
what lets an operator tell which one is happening. A measured run reached
**393,176 evicted keys** with a 63% keyspace-miss rate and reported nothing."""

ONLINE_STORE_MEMORY_BYTES: Final = "online_store_memory_bytes"
"""Redis `used_memory`, so the approach to the cap is visible before the cliff."""

HOT_PATH_METRICS: Final[frozenset[str]] = frozenset(
    {
        TX_SCORE_LATENCY,
        REQUEST_LATENCY,
        FEATURE_READ_LATENCY,
        TX_SCORED_TOTAL,
        DEGRADED_MODE_TOTAL,
        RULE_FIRED_TOTAL,
        RULE_ABSTAINED_TOTAL,
        FEATURE_UNAVAILABLE_TOTAL,
        TRIAGE_ENQUEUED_TOTAL,
        IDEMPOTENT_REPLAY_TOTAL,
        RATE_LIMITED_TOTAL,
        UNAUTHENTICATED_TOTAL,
        RULE_PACK_RELOAD_FAILED_TOTAL,
        ONLINE_STORE_EVICTED_KEYS,
        ONLINE_STORE_MEMORY_BYTES,
    }
)

ARCHITECTURE_DECLARED: Final[frozenset[str]] = frozenset(
    {TX_SCORE_LATENCY, REQUEST_LATENCY, TX_SCORED_TOTAL, DEGRADED_MODE_TOTAL}
)
"""The subset `docs/ARCHITECTURE.md` §13 names verbatim for the hot path.

Separate from the full set because §13 lists the metrics the DESIGN requires; the
others are ones the implementation found it needed. A test asserts this subset
still appears in §13, so removing one from the document without removing it from
the code fails the build.
"""


class HotPathMetrics:
    """Instruments for one process. Constructed once, at start-up.

    Creating an instrument per request is both slow and wrong -- OpenTelemetry
    identifies an instrument by name, so repeated creation either re-registers or
    silently returns the first one, and which of those happens is a detail nobody
    should have to know.
    """

    __slots__ = (
        "abstained",
        "degraded",
        "feature_read_latency",
        "feature_unavailable",
        "latency",
        "rate_limited",
        "reload_failed",
        "replays",
        "request_latency",
        "rules_fired",
        "scored",
        "triaged",
        "unauthenticated",
    )

    def __init__(self, meter_name: str = "trace_core.gateway") -> None:
        meter = get_meter(meter_name)
        self.latency: Histogram = meter.create_histogram(
            TX_SCORE_LATENCY,
            unit="s",
            description="Scoring only: features, rules, banding. Excludes triage and later I/O.",
        )
        self.request_latency: Histogram = meter.create_histogram(
            REQUEST_LATENCY,
            unit="s",
            description="Whole server-side request, including triage and the observe-write.",
        )
        self.feature_read_latency: Histogram = meter.create_histogram(
            FEATURE_READ_LATENCY,
            unit="s",
            description="Time spent reading the online feature store, within the scoring budget.",
        )
        self.scored: Counter = meter.create_counter(
            TX_SCORED_TOTAL, description="Transactions scored, by risk band."
        )
        self.degraded: Counter = meter.create_counter(
            DEGRADED_MODE_TOTAL,
            description="Decisions made with a dependency unavailable, by reason.",
        )
        self.rules_fired: Counter = meter.create_counter(
            RULE_FIRED_TOTAL, description="Rule firings, by rule id."
        )
        self.abstained: Counter = meter.create_counter(
            RULE_ABSTAINED_TOTAL,
            description="Rules that could not reach a verdict because an input was absent.",
        )
        self.feature_unavailable: Counter = meter.create_counter(
            FEATURE_UNAVAILABLE_TOTAL,
            description="Feature evaluations with no value, by feature and reason.",
        )
        self.triaged: Counter = meter.create_counter(
            TRIAGE_ENQUEUED_TOTAL, description="Cases opened and enqueued for investigation."
        )
        self.replays: Counter = meter.create_counter(
            IDEMPOTENT_REPLAY_TOTAL, description="Requests answered from the replay cache."
        )
        self.rate_limited: Counter = meter.create_counter(
            RATE_LIMITED_TOTAL, description="Requests refused for exceeding a token's budget."
        )
        self.unauthenticated: Counter = meter.create_counter(
            UNAUTHENTICATED_TOTAL, description="Requests refused for a missing or invalid token."
        )
        self.reload_failed: Counter = meter.create_counter(
            RULE_PACK_RELOAD_FAILED_TOTAL,
            description="Rule-pack reloads refused; the previous pack stayed in force.",
        )


def observe_reading(values: dict[str, int], key: str) -> list[Observation]:
    """One observation, or none at all. Never a zero.

    Absent is not zero. A gauge that published 0 when it could not read the store
    would assert "nothing has been evicted" on precisely the occasions it does not
    know -- the same error as imputing a missing feature to zero, which
    docs/DATA_ENGINEERING.md §5 prohibits for the same reason: a fabricated value
    is indistinguishable from a measured one downstream.
    """
    if key not in values:
        return []
    return [Observation(values[key])]


def register_online_store_gauges(
    meter_name: str, stores: Mapping[str, Callable[[], dict[str, int]]]
) -> None:
    """Publish each Redis instance's memory and eviction counters as gauges.

    One series per store, distinguished by the `store` attribute, because the
    two instances have opposite contracts (ADR-0044): `features` runs
    `noeviction` and its eviction counter must read ZERO forever -- any rise is
    a configuration fault, not load -- while `cache` runs `allkeys-lru` and is
    expected to evict under load. A single unlabelled series would hide the
    one that matters inside the one that is normal.

    Read through callbacks rather than recorded on the hot path: these are
    properties of the store, not of a request, and an `INFO` per scored
    transaction would spend hot-path budget on a question whose answer changes
    on a timescale of seconds. A callback that raises would take the whole
    `/metrics` scrape down with it, so each is expected to swallow and return
    an empty mapping; an absent reading is a visible gap, a failed scrape hides
    every other metric too.
    """
    meter = get_meter(meter_name)

    def _evicted(options: CallbackOptions) -> Iterable[Observation]:
        del options
        for name, info in stores.items():
            for observation in observe_reading(info(), "evicted_keys"):
                yield Observation(observation.value, {"store": name})

    def _memory(options: CallbackOptions) -> Iterable[Observation]:
        del options
        for name, info in stores.items():
            for observation in observe_reading(info(), "used_memory"):
                yield Observation(observation.value, {"store": name})

    meter.create_observable_gauge(
        ONLINE_STORE_EVICTED_KEYS,
        callbacks=[_evicted],
        description="Keys Redis discarded under memory pressure; feature state lost silently.",
    )
    meter.create_observable_gauge(
        ONLINE_STORE_MEMORY_BYTES,
        callbacks=[_memory],
        unit="By",
        description="Redis used_memory, so approaching the cap is visible before eviction starts.",
    )
