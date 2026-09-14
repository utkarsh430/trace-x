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

CORRECTIONS: Final[tuple[str, ...]] = (
    "T1",
    "T2",
    "T3",
    "M1",
    "M2",
    "M3",
    "M4",
    "M5",
    "M6",
    "N1",
    "N2",
    "N3",
    "N4",
    "N5",
    "N6",
    "N7",
    "N8",
    "N9",
    "N10",
    "N11",
    "N12",
)
"""Every eval-v2 correction a `LPC-5` §14.3 ablation can switch off, in declaration order."""

_Share = Annotated[float, Field(ge=0.0, le=1.0)]
_DailyRate = Annotated[float, Field(ge=0.0, le=50.0)]
_YearlyRate = Annotated[float, Field(ge=0.0, le=1000.0)]
_Weight = Annotated[float, Field(ge=0.0)]


class BaselineIdentityConfig(StrictModel):
    """The legitimate baseline eval-v1 lacks: the eval-v2 gate block.

    It covers legitimate identity and device activity and, from stage 1b, the
    legitimate transaction behaviour whose absence made transaction fields label
    proxies: payments from non-home devices (T1) and legitimate declines (T3).
    Scenario sub-second timing (T2) has no rate; it applies whenever the block is
    present.

    **Absent in eval-v1.** `GeneratorConfig.baseline_identity` defaults to None,
    which reproduces eval-v1 exactly and is omitted from the canonical config
    JSON, so eval-v1's config digest does not move.

    **Every value is chosen, not measured.** No field is a statistic about real
    customers, and none may be quoted as one. Each field's rationale is below and
    in the eval-v2 ADR (draft: eval/track_a/drafts/eval-v2-adr-draft.md §4, §4b). Where a value was
    chosen knowing how it interacts with a label-proxy criterion, the ADR says so.
    """

    login_rate_per_account_day: _DailyRate = 0.4
    """Mean successful logins per account per day: of the order of a few
    digital-banking sessions a week."""

    login_rate_dispersion_sigma: Annotated[float, Field(ge=0.0, le=3.0)] = 0.8
    """Lognormal sigma of a mean-one per-account multiplier on the login and
    abandoned-burst rates. Some accounts rarely log in and some do daily; the
    multiplier is mean-preserving, so the rate above stays the population mean."""

    login_away_ip_share: _Share = 0.05
    """Share of logins from outside the account's home IPs (mobile networks,
    travel, work, VPNs), drawn from the whole universe, datacenter ranges included."""

    typo_burst_share_per_login: _Share = 0.06
    """Share of successful logins preceded by a burst of mistyped attempts."""

    typo_burst_size_weights: tuple[_Weight, _Weight, _Weight, _Weight, _Weight] = (
        0.70,
        0.20,
        0.07,
        0.02,
        0.01,
    )
    """Relative weights of burst sizes 1 to 5: mostly one typo, long bursts rare."""

    reset_after_burst_share: _Share = 0.5
    """Share of bursts of three or more that end in a forgotten-password reset
    before the successful login."""

    abandoned_burst_rate_per_account_day: _DailyRate = 0.004
    """Bursts of failures with no success -- the user gives up or is locked out."""

    password_change_rate_per_account_year: _YearlyRate = 0.8
    """Voluntary and forced password changes, roughly one a year."""

    email_change_rate_per_account_year: _YearlyRate = 0.1
    """Contact details change rarely."""

    phone_change_rate_per_account_year: _YearlyRate = 0.1
    """Contact details change rarely."""

    address_change_rate_per_account_year: _YearlyRate = 0.12
    """Moving home, roughly once a decade."""

    mfa_reset_rate_per_account_year: _YearlyRate = 0.1
    """Second-factor loss outside a device replacement."""

    mfa_enrolled_rate_per_account_year: _YearlyRate = 0.1
    """Second-factor enrolment outside a device replacement."""

    new_device_rate_per_account_year: _YearlyRate = 1.0
    """New devices: phone replacement, additional devices, and app reinstalls or
    browser resets that surface as a new device id. The ADR discloses that
    `DEVICE_FIRST_SEEN_24H` precision falls as this rises."""

    mfa_reset_on_new_device_share: _Share = 0.25
    """Share of new devices accompanied by an MFA reset."""

    mfa_enrolled_on_new_device_share: _Share = 0.3
    """Share of new devices accompanied by an MFA enrolment instead."""

    change_before_first_seen_share: _Share = 0.5
    """Share of those MFA changes recorded before the device's FIRST_SEEN, from a
    device already known (the reset went through another channel first)."""

    attribute_change_rate_per_device_year: _YearlyRate = 2.0
    """OS and app updates change a device's reported attributes a few times a year."""

    fingerprint_change_rate_per_device_year: _YearlyRate = 0.3
    """Occasional fingerprint resets on a known device."""

    # ---- T1: legitimate payments from non-home devices (stage 1b) ----------
    new_device_payment_share: _Share = 0.5
    """After a legitimate FIRST_SEEN, the share of the account's later legitimate
    transactions paid on its most recently enrolled device: a replacement phone
    takes over, an extra device takes a share."""

    secondary_device_account_share: _Share = 0.3
    """Share of accounts that also pay from a secondary device -- a household or
    work device known from before the window, outside the account's home devices."""

    secondary_device_transaction_share: _Share = 0.1
    """For such an account, the share of its legitimate transactions (those not
    already moved to a newly enrolled device) paid from the secondary device."""

    # ---- T3: legitimate declines (stage 1b) --------------------------------
    decline_share_per_transaction: _Share = 0.02
    """Standalone legitimate declines -- insufficient funds, an expired card,
    issuer risk rules -- before the account's multiplier and a cap."""

    decline_retry_share_per_transaction: _Share = 0.01
    """Legitimate purchases preceded by a declined attempt the customer retries
    within minutes, before the account's multiplier and a cap."""

    decline_propensity_sigma: Annotated[float, Field(ge=0.0, le=3.0)] = 1.0
    """Lognormal sigma of a mean-one per-account multiplier on both decline
    shares: declines concentrate on a minority of accounts."""

    # ---- G2 and the ablation controls (Stage 2) -----------------------------
    coverage_floor_instances: Annotated[int, Field(ge=1, le=1000)] = 20
    """G2 (`LPC-5` §14.4): after the weighted mix, a pattern with fewer instances is topped up to
    this many. A topped-up mix is a coverage floor, not natural prevalence, and every report on the
    dataset says so. Acceptance needs 20; smaller values exist for small in-memory tests, and
    `LPC-5` S8 fails them."""

    disabled_corrections: tuple[str, ...] = ()
    """`LPC-5` §14.3 ablations: corrections switched off, each falling back to eval-v1's behaviour
    for its own aspect only. Sorted and unique, so one ablation has one digest. Empty for a
    candidate."""

    def applies(self, correction: str) -> bool:
        """Whether a correction is on. Refuses names outside `CORRECTIONS`."""
        if correction not in CORRECTIONS:
            raise ValueError(f"unknown correction {correction!r}")
        return correction not in self.disabled_corrections

    @model_validator(mode="after")
    def _check_shares(self) -> Self:
        if sum(self.typo_burst_size_weights) <= 0.0:
            raise ValueError("typo_burst_size_weights must have a positive total")
        unknown = sorted(set(self.disabled_corrections) - set(CORRECTIONS))
        if unknown:
            raise ValueError(f"disabled_corrections names unknown corrections {unknown}")
        if list(self.disabled_corrections) != sorted(
            set(self.disabled_corrections), key=CORRECTIONS.index
        ):
            raise ValueError(
                "disabled_corrections must be unique and in CORRECTIONS order, so that one "
                "ablation has exactly one config digest"
            )
        coupled = self.mfa_reset_on_new_device_share + self.mfa_enrolled_on_new_device_share
        if coupled > 1.0:
            raise ValueError(
                "mfa_reset_on_new_device_share + mfa_enrolled_on_new_device_share must not "
                f"exceed 1, got {coupled}: they are exclusive outcomes of one draw"
            )
        return self


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

    baseline_identity: BaselineIdentityConfig | None = None
    """The legitimate baseline (eval-v2 gate).

    None -- the default, and eval-v1's value -- reproduces eval-v1 exactly, and
    `canonical_json` omits the key so eval-v1's config digest is unchanged. When
    set, legitimate identity and device activity, non-home payment devices and
    legitimate declines are woven in, and planted events get sub-second timing:
    transaction rows change, while the transaction count, every label and every
    planted override do not.
    """

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
        """Stable serialisation: sorted keys, no incidental whitespace.

        An absent `baseline_identity` is omitted rather than written as null.
        eval-v1's recorded config has no such key, and its recorded
        `fraud_scenario_config_digest` must keep resolving; an additive field
        whose absence means "off" is the only encoding that allows both.
        """
        payload = json.loads(self.model_dump_json())
        if payload.get("baseline_identity") is None:
            payload.pop("baseline_identity", None)
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def digest(self) -> str:
        """`fraud_scenario_config_digest` for the run manifest (ADR-0017)."""
        return "sha256:" + hashlib.sha256(self.canonical_json().encode()).hexdigest()

    @property
    def window_seconds(self) -> int:
        return int((self.end_at - self.start_at).total_seconds())
