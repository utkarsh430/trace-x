"""The frozen parity partitions, lateness model, strata and bounds (ADR-0056 §4, §6).

Everything here is declared before the first measured run and never tuned after one. Each value is
part of a digest the PARITY record carries, so a record can always say which declaration it was
measured under, and a changed declaration cannot masquerade as the old one: `tests/unit/
test_parity_partition.py` pins the digests.

**Chosen, not measured.** No production lateness data exists. The representative model is the
eval-v2 dataset's own recorded ingestion lag plus a small, declared overlay of the arrival faults an
HTTP ingress can actually receive. The adversarial model is the same classes at higher rates.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from eval.replay.faults import (
    HELD_BACK_CLASSES,
    INVALID_CLASSES,
    OVERLAY_VERSION,
    FaultClass,
    FaultPlan,
    Timing,
)

PARITY_SEMANTICS_VERSION: Final = 1

APPROXIMATE_RMS_BOUND: Final = 0.01
"""RMS relative error per approximate feature and cardinality stratum (PHASE3_PLAN §4.3)."""
MIN_COMPARISONS_PER_STRATUM: Final = 200
MIN_COMPARISONS_OVERALL: Final = 1_000
"""Per approximate feature, over comparisons whose true cardinality is at least one."""

MAX_NOT_VOUCHED_FRACTION_PER_FEATURE: Final = 0.5
"""Per feature and pairing: the share of its comparisons that may be excluded as unvouched.

ADR-0046 §5 excludes an absence the store stopped vouching for, and §3 F1 expects that to be
material on the representative partition's late band -- so exclusions are not themselves a fault.
But `ParityTally.add` subtracts each one from `compared`, no verdict reads `not_vouched_fraction`,
and the volume floors above bind only the approximate features. Without this bound a run could
exclude nearly every comparison of an EXACT feature, report zero divergences, and pass.

Chosen, not derived, and frozen before any measured run: a feature more than half of whose offered
comparisons were excluded was not meaningfully compared, whatever the divergence count says. It is
not part of the partition declaration and so changes no frozen digest."""

ZERO_STRATUM: Final = "0"
"""True cardinality zero: both sides must be exactly zero, never judged by the RMS bound."""
STRATA: Final[tuple[tuple[str, int, int | None], ...]] = (
    ("1-9", 1, 9),
    ("10-99", 10, 99),
    ("100-999", 100, 999),
    ("1000+", 1_000, None),
)
"""Decades of true cardinality. Every stratum a run exercises at all is gated at its minimum; a
stratum it never exercises is recorded as unexercised, and the bound is not claimed there."""

ARRIVAL_SKEW_BOUND: Final = 0.01
"""Arrival skew must be strictly below this on the representative partition (PHASE3_PLAN §4.3)."""

BASIS_POINTS: Final = 10_000

EXCLUDED_CLASSES: Final = frozenset({*INVALID_CLASSES, FaultClass.CONFLICTING_DUPLICATE})
"""Classes a parity overlay may not request.

- The three invalid classes are transport faults with no HTTP analogue: the gateway's request
  contract carries no schema version, and a truncated body is rejected before any state changes.
  Silver's handling of them is `P3.event-time`'s evidence.
- `conflicting_duplicate` cannot be built for a `tx.authorization.v1` base event: the overlay's
  `_conflict` treats every topic other than transactions, identity and device events as an
  investigation request and reads `payload.score`, which an outcome does not have. Recorded as a
  defect in `eval/replay/faults.py`, which this step does not own."""


def stratum_of(true_cardinality: float) -> str:
    """The stratum a true cardinality falls in."""
    if true_cardinality < 0:
        raise ValueError(f"a cardinality cannot be negative: {true_cardinality}")
    if true_cardinality == 0:
        return ZERO_STRATUM
    for name, low, high in STRATA:
        if true_cardinality >= low and (high is None or true_cardinality <= high):
            return name
    raise ValueError(f"cardinality {true_cardinality} falls between declared strata")


def _digest(content: Mapping[str, Any]) -> str:
    text = json.dumps(content, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(text.encode()).hexdigest()


def _parse(text: str) -> dt.datetime:
    moment = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        raise ValueError(f"{text!r} has no timezone")
    return moment.astimezone(dt.UTC)


@dataclass(frozen=True)
class LatenessModel:
    """How a partition's arrivals depart from event time, beyond the dataset's own ingestion lag.

    Rates are basis points of the slice's base events. `reordered` counts pairs, so it displaces
    twice its count of events. Counts round half up, so a declared rate always yields the same
    count for the same slice."""

    name: str
    seed: int
    timing: Timing
    basis_points: Mapping[FaultClass, int]
    late_arrival_delay_s: tuple[int, int] = (1_800, 7_200)
    future_within_24h_lead_s: tuple[int, int] = (3_600, 43_200)
    future_beyond_24h_lead_s: tuple[int, int] = (93_600, 259_200)
    duplicate_delay_s: tuple[int, int] = (1, 120)
    reorder_max_gap_s: float = 120.0
    overlay_version: str = OVERLAY_VERSION

    def __post_init__(self) -> None:
        for fault, rate in self.basis_points.items():
            if not isinstance(fault, FaultClass):
                raise ValueError(f"unknown fault class {fault!r}")
            if fault in EXCLUDED_CLASSES and rate:
                raise ValueError(f"{fault} cannot be part of a parity overlay (EXCLUDED_CLASSES)")
            if isinstance(rate, bool) or not isinstance(rate, int) or not 0 <= rate <= BASIS_POINTS:
                raise ValueError(f"{fault}: rate must be whole basis points in [0, 10000]")

    def counts(self, base_events: int) -> dict[FaultClass, int]:
        if base_events < 0:
            raise ValueError("base_events cannot be negative")
        return {
            fault: (base_events * rate + BASIS_POINTS // 2) // BASIS_POINTS
            for fault, rate in sorted(self.basis_points.items())
            if rate
        }

    def plan(self, base_events: int) -> FaultPlan:
        return FaultPlan(
            seed=self.seed,
            counts=self.counts(base_events),
            timing=self.timing,
            late_arrival_delay_s=self.late_arrival_delay_s,
            future_within_24h_lead_s=self.future_within_24h_lead_s,
            future_beyond_24h_lead_s=self.future_beyond_24h_lead_s,
            duplicate_delay_s=self.duplicate_delay_s,
            reorder_max_gap_s=self.reorder_max_gap_s,
        )

    @property
    def holds_back(self) -> bool:
        """Whether any class costs real wall-clock waits beyond the slice's own span."""
        return any(self.basis_points.get(fault, 0) for fault in HELD_BACK_CLASSES)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "overlay_version": self.overlay_version,
            "seed": self.seed,
            "timing": self.timing.value,
            "basis_points": {f.value: r for f, r in sorted(self.basis_points.items()) if r},
            "late_arrival_delay_s": list(self.late_arrival_delay_s),
            "future_within_24h_lead_s": list(self.future_within_24h_lead_s),
            "future_beyond_24h_lead_s": list(self.future_beyond_24h_lead_s),
            "duplicate_delay_s": list(self.duplicate_delay_s),
            "reorder_max_gap_s": self.reorder_max_gap_s,
            "inherits": "the base dataset's recorded ingestion lag (envelope ingested_at)",
        }

    @property
    def digest(self) -> str:
        return _digest(self.as_dict())


@dataclass(frozen=True)
class Partition:
    """One slice of a frozen dataset, replayed through the gateway with a lateness model.

    The warm-up is the `warmup_s` of the dataset before the slice, posted compressed and fault-free
    with the slice's uniform shift, so the slice's windows and profiles read realistic history. As
    PHASE3_PLAN §4.3 excludes a compressed replay from arrival skew, warm-up transactions count for
    implementation parity only."""

    name: str
    gated: bool
    dataset_version: str
    dataset_manifest: str
    dataset_digest: str
    streams: tuple[str, ...]
    slice_start: str
    slice_end: str
    warmup_s: int
    lateness: LatenessModel

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise ValueError(f"{self.name}: the slice is empty")
        if self.warmup_s < 0:
            raise ValueError(f"{self.name}: warmup_s cannot be negative")

    @property
    def start(self) -> dt.datetime:
        return _parse(self.slice_start)

    @property
    def end(self) -> dt.datetime:
        return _parse(self.slice_end)

    @property
    def warmup_start(self) -> dt.datetime:
        return self.start - dt.timedelta(seconds=self.warmup_s)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "gated": self.gated,
            "dataset_version": self.dataset_version,
            "dataset_manifest": self.dataset_manifest,
            "dataset_digest": self.dataset_digest,
            "streams": list(self.streams),
            "slice_start": self.slice_start,
            "slice_end": self.slice_end,
            "warmup_s": self.warmup_s,
            "lateness_model_digest": self.lateness.digest,
        }

    @property
    def digest(self) -> str:
        return _digest(self.as_dict())


EVAL_V2_MANIFEST: Final = "eval/track_a/eval-v2.manifest.json"
EVAL_V2_DATASET_DIGEST: Final = (
    "sha256:88f2142a266100a00ed2533086a0d65753cbfe8656cd2cb1dd54126fc0485185"
)
PARTITION_STREAMS: Final = ("tx.raw.v1", "identity.events.v1", "tx.authorization.v1")
"""`device.events.v1` is not replayed: no released feature reads it, and the gateway records no
online state for it (ADR-0046 §4)."""

REPRESENTATIVE_LATENESS: Final = LatenessModel(
    name="representative-v1",
    seed=20260915,
    timing=Timing.PACED,
    basis_points={
        FaultClass.REORDERED: 50,
        FaultClass.EXACT_DUPLICATE: 50,
        FaultClass.RETRY_NEW_EVENT_ID: 20,
        FaultClass.LATE: 20,
        FaultClass.FUTURE_WITHIN_24H: 5,
    },
)
ADVERSARIAL_LATENESS: Final = LatenessModel(
    name="adversarial-ingress-v1",
    seed=20260916,
    timing=Timing.PACED,
    basis_points={
        FaultClass.REORDERED: 500,
        FaultClass.EXACT_DUPLICATE: 300,
        FaultClass.RETRY_NEW_EVENT_ID: 300,
        FaultClass.LATE: 300,
        FaultClass.FUTURE_WITHIN_24H: 100,
        FaultClass.FUTURE_BEYOND_24H: 50,
    },
)

REPRESENTATIVE: Final = Partition(
    name="representative",
    gated=True,
    dataset_version="eval-v2",
    dataset_manifest=EVAL_V2_MANIFEST,
    dataset_digest=EVAL_V2_DATASET_DIGEST,
    streams=PARTITION_STREAMS,
    slice_start="2026-02-04T12:00:00.000Z",
    slice_end="2026-02-04T13:00:00.000Z",
    warmup_s=86_400,
    lateness=REPRESENTATIVE_LATENESS,
)
ADVERSARIAL: Final = Partition(
    name="adversarial-ingress",
    gated=False,
    dataset_version="eval-v2",
    dataset_manifest=EVAL_V2_MANIFEST,
    dataset_digest=EVAL_V2_DATASET_DIGEST,
    streams=PARTITION_STREAMS,
    slice_start="2026-02-11T12:00:00.000Z",
    slice_end="2026-02-11T13:00:00.000Z",
    warmup_s=86_400,
    lateness=ADVERSARIAL_LATENESS,
)
PARTITIONS: Final[Mapping[str, Partition]] = {p.name: p for p in (REPRESENTATIVE, ADVERSARIAL)}
