"""Generator configuration and its digest (ADR-0029).

`GeneratorConfig` is the complete description of a dataset: change anything here
and you have a different dataset, which must carry a different digest. Its
canonical digest is the `fraud_scenario_config_digest` field that ADR-0017
requires in every run manifest, so a metric that moved because the scenario mix
changed is attributable rather than mysterious.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from collections.abc import Mapping
from typing import Annotated, Final, Self

from pydantic import Field, model_validator

from trace_core.contracts.base import StrictModel

GENERATOR_VERSION: Final = "1.0.0"
"""Semantic version of the GENERATION LOGIC.

Bumped whenever a change would alter the output for an unchanged config. It is
recorded beside the digest, so "same config, different bytes" is explainable
rather than alarming.
"""

PRODUCER: Final = f"trace-generator@{GENERATOR_VERSION}"


class GeneratorConfig(StrictModel):
    """Everything that determines a dataset."""

    seed: Annotated[int, Field(ge=0, le=2**63 - 1)] = 42
    row_count: Annotated[int, Field(ge=1, le=100_000_000)] = 1000

    start_at: dt.datetime = dt.datetime(2026, 1, 1, tzinfo=dt.UTC)
    end_at: dt.datetime = dt.datetime(2026, 3, 1, tzinfo=dt.UTC)

    account_count: Annotated[int, Field(ge=1, le=10_000_000)] = 1000
    merchant_count: Annotated[int, Field(ge=1, le=1_000_000)] = 200
    device_count: Annotated[int, Field(ge=1, le=10_000_000)] = 1200
    ip_count: Annotated[int, Field(ge=1, le=10_000_000)] = 800
    cards_per_account: Annotated[int, Field(ge=1, le=8)] = 1

    currency: Annotated[str, Field(pattern=r"^[A-Z]{3}$")] = "GBP"

    merchant_zipf_exponent: Annotated[float, Field(gt=0.0, le=3.0)] = 1.1
    """Merchant popularity follows a power law: a few merchants take most of the
    volume. A uniform choice would make merchant-risk aggregates meaningless
    because every merchant would have a similar transaction count."""

    fraud_rate: Annotated[float, Field(ge=0.0, le=0.5)] = 0.005
    """Target share of transactions that are fraudulent.

    ~0.5% matches the order of magnitude `docs/ARCHITECTURE.md` §7 assumes
    ("roughly one in two hundred"), which is why PR-AUC rather than ROC-AUC is
    the primary metric.

    **This is a target, and there is a floor beneath it.** Ten mandatory
    instances -- one per pattern -- are planted before the weighted mix runs, so
    that no dataset is missing a pattern. On a dataset small enough that those
    ten already exceed `row_count * fraud_rate`, the realised rate is set by the
    floor and this value has no effect. Coverage matters more than hitting a rate
    on a toy dataset, but the consequence is real, so the realised rate is
    measured and recorded rather than assumed equal to the target.
    """

    habitual_merchant_ratio: Annotated[float, Field(ge=0.0, le=1.0)] = 0.75
    """Share of an account's legitimate spend going to merchants it already
    uses. Without this there is no 'usual behaviour' for fraud to depart from."""

    geo_jitter_km: Annotated[float, Field(ge=0.0, le=500.0)] = 12.0
    """Spread of legitimate transactions around an account's home location."""

    @model_validator(mode="after")
    def _check_window(self) -> Self:
        if self.start_at.tzinfo is None or self.end_at.tzinfo is None:
            raise ValueError("start_at and end_at must be timezone-aware UTC")
        if self.end_at <= self.start_at:
            raise ValueError(f"end_at {self.end_at} must be after start_at {self.start_at}")
        if self.device_count < self.account_count // 4:
            raise ValueError(
                "device_count is implausibly low relative to account_count: nearly every "
                "account would share a device, which would swamp the device-farm signal"
            )
        return self

    @classmethod
    def from_mapping(cls, data: Mapping[str, object]) -> GeneratorConfig:
        """Build from a plain mapping, e.g. one parsed out of JSON.

        Goes through JSON-mode validation deliberately. The models are strict,
        and strictness differs between modes: in Python mode an ISO 8601 string
        is not a datetime, so `GeneratorConfig(**parsed_json)` fails on
        `start_at` and `end_at`. This exists so that trap is encountered once,
        here, rather than at every call site that reads a committed config.
        """
        return cls.model_validate_json(json.dumps(dict(data), default=str))

    def canonical_json(self) -> str:
        """Stable serialisation: sorted keys, no incidental whitespace."""
        payload = json.loads(self.model_dump_json())
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def digest(self) -> str:
        """`fraud_scenario_config_digest` for the run manifest (ADR-0017)."""
        return "sha256:" + hashlib.sha256(self.canonical_json().encode()).hexdigest()

    @property
    def window_seconds(self) -> int:
        return int((self.end_at - self.start_at).total_seconds())
