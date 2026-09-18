"""The frozen targets and the declared run configuration.

**Targets** are copied from their sources, once, here, so a change is a visible one-line diff:

- `docs/ROADMAP.md` Phase 3 TARGETS: "Sustain >= 5k events/s locally; consumer lag recovers to
  < 10 s after a 2-minute outage, with the outage test offering the 5,000 events/s target rate. The
  rate is fixed before measurement and never lowered afterwards; insufficient recovery capacity is
  recorded as a failure."
- `docs/PHASE3_PLAN.md` §3 Q7 (the same rate rule) and §4.3 (consumer lag and Gold freshness).

**What "outage" means.** The metric the target names is *consumer* lag (Kafka -> Silver commit,
§4.3), and the outage test must keep *offering* 5,000 events/s to Kafka (ROADMAP, Q7), which a
broker outage would make impossible. So the outage is a consumer-side outage: the process running
the Bronze and Silver queries is stopped and restarted after exactly `OUTAGE_S`, while the producers
keep publishing to the live broker. Recovery includes the restart from the checkpoints -- JVM start
and all -- because that is what a consumer returning from an outage does.

**Choices the specification leaves open**, each declared here before any run:

- `THROUGHPUT_QUANTISATION_TOLERANCE`: with exactly the target offered, a pipeline that keeps up
  commits the target rate give or take where micro-batch commits fall, so a bare `>=` would be a
  coin flip. Sustained means the achieved Kafka -> Silver rate is within 1% of the target **and**
  every consumer-lag sample in the measured window is below the 10 s recovery target (a pipeline
  falling behind by more than about 1.5% accumulates more than 10 s of backlog over the window).
- `RECOVERY_HOLD_S`: recovered means lag below 10 s at a Silver commit and staying below it at
  every commit for the following 60 s. A single dip is not recovery.
- `RECOVERY_BOUND_S`: the target gives no time limit, so the observation after the restart is
  bounded; not recovering within it is a recorded FAIL.
- `CLOCK_OFFSET_TOLERANCE_MS`: lag subtracts a broker clock (LogAppendTime) from a host clock
  (Delta commit time). If the measured offset interval is wider than 5% of the 10 s target, the
  run cannot say which side of the target it is on, and it is INVALID rather than a pass or fail.
- `MAX_SCHEDULE_LAG_S` and `OFFERED_RATE_TOLERANCE`: the harness itself must offer the rate. A
  producer more than a second behind its schedule, or a window offered below 99% of the rate, makes
  the run INVALID -- never a pipeline failure and never a pass.
"""

from __future__ import annotations

from typing import Annotated, Final

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from trace_core.contracts.topics import IDENTITY_EVENTS_V1, TX_AUTHORIZATION_V1, TX_SCORED_V1

HARNESS_VERSION: Final = "1.0.0"
"""Recorded as the tool version. Bump it when anything that decides a verdict changes."""

TOOL: Final = "trace-x-load-stream"
SUBJECT: Final = "stream-kafka-bronze-silver-throughput-outage-and-gold-freshness"

# --------------------------------------------------------------- frozen targets ---

TARGET_RATE_EVENTS_PER_S: Final = 5_000
"""ROADMAP Phase 3 and PHASE3_PLAN Q7: offered in both phases, never lowered."""
LAG_TARGET_MS: Final = 10_000
"""Consumer lag recovers to below this (strictly)."""
OUTAGE_S: Final = 120
"""The 2-minute outage. Not configurable: it is the target's own number."""
GOLD_P95_LAG_MS: Final = 15 * 60 * 1000
GOLD_MAX_LAG_MS: Final = 60 * 60 * 1000
GOLD_STALL_MS: Final = 30 * 60 * 1000
"""PHASE3_PLAN §4.3 Gold freshness: p95 within 15 min, no build over 60 min, no 30-min stall."""

# ----------------------------------------------------------- declared choices ---

THROUGHPUT_QUANTISATION_TOLERANCE: Final = 0.01
OFFERED_RATE_TOLERANCE: Final = 0.01
MAX_SCHEDULE_LAG_S: Final = 1.0
CLOCK_OFFSET_TOLERANCE_MS: Final = 500.0
MIN_SAMPLES_PER_WINDOW: Final = 10
RECOVERY_HOLD_S: Final = 60
RECOVERY_BOUND_S: Final = 900

PRODUCER_NAME: Final = f"trace-load-stream@{HARNESS_VERSION}"
"""Not the gateway's name, on purpose: a record in the benchmark lake never claims the gateway's
provenance. Consequence, declared: Gold admits only gateway-produced identity events, so the mix's
identity events reach Silver but not Gold."""

DEFAULT_MIX: Final[dict[str, int]] = {
    TX_SCORED_V1: 14,
    TX_AUTHORIZATION_V1: 5,
    IDENTITY_EVENTS_V1: 1,
}
"""Out of every 20 events: 14 scored transactions (the gateway's observation log, and Gold's main
input), 5 authorization outcomes for transactions scored earlier, 1 identity event. The topics
Gold reads, in the proportions the hot path produces them; `tx.raw.v1` and device events have no
runtime producer in Phase 3."""


class RunConfig(BaseModel):
    """Everything that shapes the measurement, fixed before it starts and recorded whole."""

    model_config = ConfigDict(strict=True, frozen=True, extra="forbid")

    rate_events_per_s: Annotated[int, Field(gt=0)] = TARGET_RATE_EVENTS_PER_S
    mix: dict[str, Annotated[int, Field(gt=0)]] = Field(default_factory=lambda: dict(DEFAULT_MIX))
    producer_workers: Annotated[int, Field(ge=1, le=32)] = 4
    account_pool: Annotated[int, Field(ge=1_000)] = 100_000
    scored_templates: Annotated[int, Field(ge=1, le=1_000)] = 64
    seed: int = 20260918

    warmup_min_s: Annotated[int, Field(ge=0)] = 120
    warmup_max_s: Annotated[int, Field(ge=0)] = 600
    measured_s: Annotated[int, Field(ge=60)] = 600
    pre_outage_s: Annotated[int, Field(ge=0)] = 60
    recovery_bound_s: Annotated[int, Field(ge=1)] = RECOVERY_BOUND_S
    recovery_hold_s: Annotated[int, Field(ge=1)] = RECOVERY_HOLD_S

    bronze_trigger_s: Annotated[float, Field(gt=0)] = 2.0
    silver_trigger_s: Annotated[float, Field(gt=0)] = 2.0
    max_offsets_per_trigger: Annotated[int, Field(gt=0)] | None = None
    master: Annotated[str, Field(pattern=r"^local\[(\d+|\*)\]$")] = "local[4]"
    driver_memory: Annotated[str, Field(pattern=r"^\d+[mg]$")] = "2g"
    shuffle_partitions: Annotated[int, Field(ge=1)] = 2

    gold: bool = True
    gold_driver_memory: Annotated[str, Field(pattern=r"^\d+[mg]$")] = "2g"
    gold_min_interval_s: Annotated[int, Field(ge=0)] = 0

    consumer_stop_timeout_s: Annotated[int, Field(ge=1)] = 90
    poll_interval_s: Annotated[float, Field(gt=0)] = 2.0

    @field_validator("rate_events_per_s")
    @classmethod
    def _never_below_target(cls, value: int) -> int:
        if value < TARGET_RATE_EVENTS_PER_S:
            raise ValueError(
                f"the offered rate {value} events/s is below the {TARGET_RATE_EVENTS_PER_S} "
                f"events/s target; the rate is fixed before measurement and never lowered "
                f"(ROADMAP Phase 3, PHASE3_PLAN Q7)"
            )
        return value

    @field_validator("mix")
    @classmethod
    def _released_topics_only(cls, value: dict[str, int]) -> dict[str, int]:
        unknown = sorted(set(value) - set(DEFAULT_MIX))
        if unknown or not value:
            raise ValueError(
                f"the mix may name only {sorted(DEFAULT_MIX)} (the events the builders make); "
                f"got {sorted(value)}"
            )
        if TX_AUTHORIZATION_V1 in value and TX_SCORED_V1 not in value:
            raise ValueError("an authorization outcome needs a scored transaction to refer to")
        return value

    @model_validator(mode="after")
    def _windows(self) -> RunConfig:
        if self.warmup_max_s < self.warmup_min_s:
            raise ValueError("warmup_max_s must be at least warmup_min_s")
        return self

    @property
    def per_worker_rate(self) -> float:
        return self.rate_events_per_s / self.producer_workers


def targets() -> dict[str, float | int]:
    """The frozen targets and declared choices, as recorded in every manifest."""
    return {
        "rate_events_per_s": TARGET_RATE_EVENTS_PER_S,
        "lag_target_ms": LAG_TARGET_MS,
        "outage_s": OUTAGE_S,
        "gold_p95_lag_ms": GOLD_P95_LAG_MS,
        "gold_max_lag_ms": GOLD_MAX_LAG_MS,
        "gold_stall_ms": GOLD_STALL_MS,
        "throughput_quantisation_tolerance": THROUGHPUT_QUANTISATION_TOLERANCE,
        "offered_rate_tolerance": OFFERED_RATE_TOLERANCE,
        "max_schedule_lag_s": MAX_SCHEDULE_LAG_S,
        "clock_offset_tolerance_ms": CLOCK_OFFSET_TOLERANCE_MS,
        "min_samples_per_window": MIN_SAMPLES_PER_WINDOW,
    }
