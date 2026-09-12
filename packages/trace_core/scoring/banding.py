"""Combining fired rules into a score and a band.

**Why noisy-OR rather than a normalised sum.** Two properties decide this, and a
plain weighted average has neither.

*Monotonicity.* Another rule firing must never lower the score. With
`sum(w) / sum(all_w)` the denominator is fixed, so that holds — but adding a new
rule to the pack lowers the score of every transaction that does not fire it,
which silently re-tunes the whole system. With noisy-OR the score depends only on
the rules that actually fired, so the pack can grow without moving decisions that
had nothing to do with the addition.

*A single strong signal must be able to stand alone.* Under a normalised sum, one
rule with weight 0.9 in a pack of eighteen contributes 0.9/12.2 ≈ 0.07 and is
indistinguishable from noise. Impossible travel is not a seventh of a signal.

So `score = 1 - prod(1 - w_i)` over fired rules: bounded in [0, 1] by construction,
monotone in both the number and the weight of fired rules, and each weight reads
directly as "how far this rule alone moves the score". It treats rules as
independent evidence, which they are not — correlated rules therefore
over-contribute, and that is a real limitation recorded in ADR-0033's risks
rather than hidden here.

**A band floor overrides the score, in one direction only.** It can raise a band,
never lower it. A rule that could *reduce* risk would be a way for an attacker to
buy safety by triggering it, and there is no such thing as exculpatory evidence on
a hot path that sees one transaction.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Any, Final, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from trace_core.contracts.canonical_json import content_hash
from trace_core.domain.enums import RiskBand
from trace_core.domain.errors import ContractError
from trace_core.rules.engine import Evaluation

DEFAULT_CONFIG: Final = Path(__file__).resolve().parent / "config" / "core.v1.yaml"

_BAND_ORDER: Final[tuple[RiskBand, ...]] = (
    RiskBand.LOW,
    RiskBand.MEDIUM,
    RiskBand.HIGH,
    RiskBand.CRITICAL,
)


class BandsModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    medium: Annotated[float, Field(gt=0.0, lt=1.0)]
    high: Annotated[float, Field(gt=0.0, lt=1.0)]
    critical: Annotated[float, Field(gt=0.0, le=1.0)]

    @model_validator(mode="after")
    def _strictly_increasing(self) -> Self:
        if not self.medium < self.high < self.critical:
            raise ValueError(
                f"band thresholds must strictly increase, got medium={self.medium}, "
                f"high={self.high}, critical={self.critical}. Out of order, a band "
                f"becomes unreachable and nothing about the output would look wrong."
            )
        return self


class ThresholdConfigModel(BaseModel):
    """The declared operating point."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    config_id: Annotated[str, Field(min_length=1, max_length=64)]
    version: Annotated[str, Field(pattern=r"^\d+\.\d+\.\d+$")]
    description: Annotated[str, Field(min_length=10, max_length=512)]
    bands: BandsModel
    triage_at: RiskBand

    @model_validator(mode="after")
    def _triage_band_is_reachable(self) -> Self:
        if self.triage_at is RiskBand.LOW:
            raise ValueError(
                "triage_at=LOW would open an investigation for every transaction, "
                "which is not triage"
            )
        return self


@dataclass(frozen=True, slots=True)
class ThresholdConfig:
    """A loaded, digest-pinned banding configuration."""

    config_id: str
    version: str
    medium: float
    high: float
    critical: float
    triage_at: RiskBand
    digest: str

    @classmethod
    def from_model(cls, model: ThresholdConfigModel) -> ThresholdConfig:
        return cls(
            config_id=model.config_id,
            version=model.version,
            medium=model.bands.medium,
            high=model.bands.high,
            critical=model.bands.critical,
            triage_at=model.triage_at,
            digest=content_hash(model.model_dump(mode="json")),
        )

    def band_for(self, score: float) -> RiskBand:
        """The band a score alone earns, before any rule's floor is applied."""
        if score >= self.critical:
            return RiskBand.CRITICAL
        if score >= self.high:
            return RiskBand.HIGH
        if score >= self.medium:
            return RiskBand.MEDIUM
        return RiskBand.LOW

    def opens_investigation(self, band: RiskBand) -> bool:
        return _BAND_ORDER.index(band) >= _BAND_ORDER.index(self.triage_at)


def load_thresholds(path: Path | None = None) -> ThresholdConfig:
    """Load and validate the banding configuration, or raise.

    Fatal on failure, like the rule pack's first load: a gateway with no
    thresholds cannot band anything, and inventing a default would be an
    unrecorded operating point in a system whose whole point is that the
    operating point is recorded.
    """
    target = path or DEFAULT_CONFIG
    try:
        raw: Any = yaml.safe_load(target.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ContractError(f"threshold config could not be read: {exc}") from exc
    if not isinstance(raw, dict):
        raise ContractError("threshold config must be a mapping at the top level")
    try:
        model = ThresholdConfigModel.model_validate(raw)
    except Exception as exc:
        raise ContractError(f"threshold config failed validation: {exc}") from exc
    return ThresholdConfig.from_model(model)


def combine(evaluation: Evaluation) -> float:
    """The combined score from the rules that fired. Bounded in [0, 1].

    Noisy-OR. An abstaining rule contributes nothing -- which is the honest
    treatment, since it reached no verdict -- and the caller reports coverage
    separately so a decision made with a blind pack is distinguishable from one
    made with a quiet one.
    """
    survival = 1.0
    for outcome in evaluation.fired:
        survival *= 1.0 - outcome.weight
    # Clamped rather than trusted: a weight of exactly 1.0 makes survival 0.0,
    # and floating error on a long product can leave a hair outside [0, 1].
    return max(0.0, min(1.0, 1.0 - survival))


def band(evaluation: Evaluation, config: ThresholdConfig) -> tuple[float, RiskBand]:
    """The score and the band, with any fired rule's floor applied.

    The floor raises, never lowers. A rule able to *reduce* a band would be a way
    for an attacker to buy safety by triggering it.
    """
    score = combine(evaluation)
    scored_band = config.band_for(score)
    floor = evaluation.band_floor
    if floor is None:
        return score, scored_band
    if _BAND_ORDER.index(floor) > _BAND_ORDER.index(scored_band):
        return score, floor
    return score, scored_band
