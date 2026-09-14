"""Legitimate identity and device activity for ordinary accounts (eval-v2).

**Why this exists.** In `eval-v1`, identity and device events are emitted only
inside fraud scenarios, so any identity signal marks fraud and every identity
feature is a label proxy (docs/PHASE3_PLAN.md §3 Q5). This module plans the
activity a real customer base produces anyway -- logins, typos, credential and
contact changes, new devices, device updates -- so that presence stops
determining the label and only the scenarios' *patterns* stay distinctive. It
also fixes which non-home devices each account legitimately pays from (T1); the
engine applies that to legitimate transactions.

**Gated, and invisible when off.** Nothing here runs unless
`GeneratorConfig.baseline_identity` is set. `eval-v1` does not set it, and its
output stays byte-identical to the generator before this module existed.

**Determinism (ADR-0029).** Every draw comes from a substream keyed by account
index and activity, and none touches a substream the eval-v1 transaction path
uses.

**Coherence.** Logins use the account's own devices and home IPs. A device is
*known* to an account either from before the window (a home device, or the
account's secondary payment device) or from its in-window `FIRST_SEEN`; every
event references only devices known at its own time. So no event references a
device on an account before that account's legitimate `FIRST_SEEN` of it, and the
engine applies the same rule to transactions.

**Every rate is chosen, not measured.** See `BaselineIdentityConfig` and the
eval-v2 ADR (draft: eval/track_a/drafts/eval-v2-adr-draft.md §4, §4b). None may be quoted as a
statistic about real customers.
"""

from __future__ import annotations

import random
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Final

from data.generator.behavior import sample_occurred_at
from data.generator.config import BaselineIdentityConfig, GeneratorConfig
from data.generator.population import Universe
from data.generator.rng import derive
from trace_core.domain.time import to_millis

IDENTITY_TOPIC: Final = "identity.events.v1"
DEVICE_TOPIC: Final = "device.events.v1"

SECOND_MS: Final = 1_000
MINUTE_MS: Final = 60_000
DAY_SECONDS: Final = 86_400
DAYS_PER_YEAR: Final = 365.25

# ---- timing inside an episode ---------------------------------------------
# Chosen, not measured. Module constants rather than config fields: they shape
# an episode rather than set a rate, and they are covered by the git SHA that
# every run record carries.
TYPO_SPACING_MS: Final = (5 * SECOND_MS, 40 * SECOND_MS)
"""Between consecutive mistyped attempts."""
TYPO_TO_LOGIN_MS: Final = (10 * SECOND_MS, 90 * SECOND_MS)
"""From the last mistyped attempt to the successful login."""
FAILURE_TO_RESET_MS: Final = (MINUTE_MS, 15 * MINUTE_MS)
"""From the last failure to a forgotten-password reset."""
RESET_TO_LOGIN_MS: Final = (30 * SECOND_MS, 5 * MINUTE_MS)
"""From the reset to the successful login that follows it."""
MFA_COUPLING_MS: Final = (MINUTE_MS, 30 * MINUTE_MS)
"""Between a new device's FIRST_SEEN and the MFA change that accompanies it."""
RESET_MIN_BURST: Final = 3
"""Bursts at least this long may end in a password reset."""

_NOVEL_DEVICE_ATTEMPTS: Final = 32

# Stream names: each names an ordinal sequence per account, and the pair
# (stream, ordinal) keys the event's own envelope substream at emission.
STREAM_DEVICES: Final = "devices"
STREAM_DEVICE_ATTRS: Final = "device-attrs"
STREAM_LOGINS: Final = "logins"
STREAM_ABANDONED: Final = "abandoned"
STREAM_CHANGES: Final = "changes"


@dataclass(frozen=True, slots=True)
class BaselineEvent:
    """One planned legitimate identity or device event.

    Carries no label because there is none to carry: legitimate activity is not
    part of any scenario, and side events are never labelled (labels are per
    transaction).
    """

    occurred_ms: int
    account_index: int
    topic: str
    event_type: str
    device_id: str
    ip_id: str | None
    stream: str
    ordinal: int

    @property
    def key(self) -> str:
        """Unique within a dataset; keys the event's envelope substream."""
        return f"{self.account_index}:{self.stream}:{self.ordinal}"


@dataclass(frozen=True, slots=True)
class DeviceReferences:
    """What the scenarios already say about each account's devices."""

    scenario_devices: Mapping[int, Collection[str]] = field(default_factory=dict)
    """account index -> devices a scenario names explicitly for that account.
    Legitimate activity never adopts one, as a new device or a secondary one:
    doing so could make a scenario's DEVICE_NOVELTY key false."""


@dataclass(frozen=True, slots=True)
class PaymentDevices:
    """The non-home devices an account may legitimately pay from (T1)."""

    secondary: str | None
    """A household or work device known from before the window, if any."""
    enrolled: tuple[tuple[int, str], ...]
    """(legitimate FIRST_SEEN millisecond, device), in time order."""

    def latest_enrolled_before(self, at_ms: int) -> str | None:
        """The most recently enrolled device with FIRST_SEEN strictly before `at_ms`."""
        latest: str | None = None
        for tau, device in self.enrolled:
            if tau >= at_ms:
                break
            latest = device
        return latest


@dataclass(frozen=True, slots=True)
class BaselinePlan:
    """Every legitimate identity and device event, and each account's payment devices."""

    events: list[BaselineEvent]
    payment_devices: Mapping[int, PaymentDevices]
    """Only accounts with a secondary device or at least one enrolment appear."""


@dataclass(slots=True)
class _DeviceTimeline:
    pre_window: list[str]
    """Devices known from before the window: home devices, then the secondary one."""
    enrolled: list[tuple[int, str]]
    """(FIRST_SEEN millisecond, device) in time order."""

    def known_at(self, at_ms: int) -> list[str]:
        return self.pre_window + [device for tau, device in self.enrolled if tau <= at_ms]


class _Plan:
    """Collects one account's events, assigning ordinals per stream."""

    __slots__ = ("_end_ms", "_ordinals", "_start_ms", "account_index", "events")

    def __init__(self, account_index: int, start_ms: int, end_ms: int) -> None:
        self.account_index = account_index
        self.events: list[BaselineEvent] = []
        self._ordinals: dict[str, int] = {}
        self._start_ms = start_ms
        self._end_ms = end_ms

    def add(
        self,
        stream: str,
        occurred_ms: int,
        topic: str,
        event_type: str,
        device_id: str,
        ip_id: str | None = None,
    ) -> None:
        # Episodes near the edges can place events outside the window; those are
        # dropped rather than clamped, because clamping would pile events onto
        # the boundary millisecond -- an artefact with no counterpart in fraud.
        if not self._start_ms <= occurred_ms < self._end_ms:
            return
        ordinal = self._ordinals.get(stream, 0)
        self._ordinals[stream] = ordinal + 1
        self.events.append(
            BaselineEvent(
                occurred_ms=occurred_ms,
                account_index=self.account_index,
                topic=topic,
                event_type=event_type,
                device_id=device_id,
                ip_id=ip_id,
                stream=stream,
                ordinal=ordinal,
            )
        )


def poisson(rng: random.Random, mean: float) -> int:
    """A Poisson(mean) count, by counting unit-rate arrivals in `[0, mean)`.

    The standard library has no Poisson sampler and ADR-0029 rules out NumPy's.
    Exact for any mean, and it costs one draw per event plus one -- proportional
    to the output, so no mean is expensive relative to what it produces.
    """
    if mean <= 0.0:
        return 0
    count = 0
    elapsed = rng.expovariate(1.0)
    while elapsed < mean:
        count += 1
        elapsed += rng.expovariate(1.0)
    return count


def _weighted_index(value: float, weights: Sequence[float]) -> int:
    """Index chosen by weight from a uniform draw; a zero weight is never chosen."""
    target = value * sum(weights)
    running = 0.0
    for index, weight in enumerate(weights):
        running += weight
        if target < running:
            return index
    return max(i for i, w in enumerate(weights) if w > 0)


def _sample_ms(rng: random.Random, config: GeneratorConfig) -> int:
    """A time with the generator's diurnal and weekly shape, in milliseconds."""
    return to_millis(sample_occurred_at(rng, config.start_at, config.end_at))


def _pick[T](rng: random.Random, items: Sequence[T]) -> T:
    return items[rng.randrange(len(items))]


def _pick_ip(
    rng: random.Random,
    settings: BaselineIdentityConfig,
    home_ips: Sequence[str],
    universe: Universe,
) -> str:
    if rng.random() < settings.login_away_ip_share:
        # Any universe IP, datacenter ranges included: VPN users log in from
        # them too, so a datacenter login is enriched for stuffing but not
        # exclusive to it.
        return universe.ips[rng.randrange(len(universe.ips))].ip_id
    return _pick(rng, home_ips)


def _novel_device(rng: random.Random, universe: Universe, excluded: Collection[str]) -> str | None:
    """A universe device outside `excluded`, or None if none was found.

    None drops the adoption rather than risking a device the account already
    knows -- a FIRST_SEEN that was not first would break the module's guarantee.
    """
    for _ in range(_NOVEL_DEVICE_ATTEMPTS):
        candidate = universe.devices[rng.randrange(len(universe.devices))].device_id
        if candidate not in excluded:
            return candidate
    return None


def plan_account(
    config: GeneratorConfig,
    universe: Universe,
    account_index: int,
    references: DeviceReferences,
) -> tuple[list[BaselineEvent], PaymentDevices]:
    """One account's legitimate identity and device events, and its payment devices."""
    settings = config.baseline_identity
    if settings is None:
        return [], PaymentDevices(secondary=None, enrolled=())

    seed = config.seed
    key = str(account_index)
    start_ms = to_millis(config.start_at)
    end_ms = to_millis(config.end_at)
    days = config.window_seconds / DAY_SECONDS
    years = days / DAYS_PER_YEAR

    profile = universe.profiles[account_index]
    home_devices = list(dict.fromkeys(profile.home_devices))
    home_ips = list(dict.fromkeys(profile.home_ips))
    scenario_devices = references.scenario_devices.get(account_index, ())

    plan = _Plan(account_index, start_ms, end_ms)
    secondary = _plan_secondary(config, universe, key, home_devices, scenario_devices)
    pre_window = home_devices + ([secondary] if secondary is not None else [])
    timeline = _plan_devices(config, universe, plan, key, years, pre_window, scenario_devices)
    _plan_device_attributes(config, plan, key, years, start_ms, timeline)

    engagement = derive(seed, "baseline-engagement", key)
    sigma = settings.login_rate_dispersion_sigma
    # Mean-one lognormal: E[exp(N(mu, sigma))] = exp(mu + sigma^2 / 2) = 1.
    multiplier = engagement.lognormvariate(-(sigma**2) / 2.0, sigma) if sigma > 0 else 1.0

    _plan_logins(config, universe, plan, key, days * multiplier, home_ips, timeline)
    _plan_abandoned_bursts(config, universe, plan, key, days * multiplier, home_ips, timeline)
    _plan_changes(config, plan, key, years, timeline)
    return plan.events, PaymentDevices(secondary=secondary, enrolled=tuple(timeline.enrolled))


def plan_baseline(
    config: GeneratorConfig, universe: Universe, references: DeviceReferences
) -> BaselinePlan:
    """Legitimate activity for every account. Empty when the gate is off."""
    if config.baseline_identity is None:
        return BaselinePlan(events=[], payment_devices={})
    events: list[BaselineEvent] = []
    payment_devices: dict[int, PaymentDevices] = {}
    for account_index in range(len(universe.profiles)):
        account_events, devices = plan_account(config, universe, account_index, references)
        events.extend(account_events)
        if devices.secondary is not None or devices.enrolled:
            payment_devices[account_index] = devices
    return BaselinePlan(events=events, payment_devices=payment_devices)


# ------------------------------------------------------------- activities ----


def _plan_secondary(
    config: GeneratorConfig,
    universe: Universe,
    key: str,
    home_devices: list[str],
    scenario_devices: Collection[str],
) -> str | None:
    """Whether the account also pays from a household or work device, and which (T1)."""
    settings = config.baseline_identity
    assert settings is not None  # guarded by plan_account
    rng = derive(config.seed, "baseline-secondary", key)
    if rng.random() >= settings.secondary_device_account_share:
        return None
    return _novel_device(rng, universe, set(home_devices) | set(scenario_devices))


def _plan_devices(
    config: GeneratorConfig,
    universe: Universe,
    plan: _Plan,
    key: str,
    years: float,
    pre_window: list[str],
    scenario_devices: Collection[str],
) -> _DeviceTimeline:
    """New-device enrolments, each a FIRST_SEEN plus an optional MFA change."""
    settings = config.baseline_identity
    assert settings is not None
    rng = derive(config.seed, "baseline-devices", key)

    count = poisson(rng, settings.new_device_rate_per_account_year * years)
    taus = sorted(_sample_ms(rng, config) for _ in range(count))

    enrolled: list[tuple[int, str]] = []
    pending: list[tuple[int, str, str | None]] = []

    for tau in taus:
        # Always a device new to the account: never pre-window, never already
        # enrolled, never one a scenario names for this account.
        excluded = set(pre_window) | {d for _, d in enrolled} | set(scenario_devices)
        device = _novel_device(rng, universe, excluded)
        if device is None:
            continue
        enrolled.append((tau, device))
        plan.add(STREAM_DEVICES, tau, DEVICE_TOPIC, "FIRST_SEEN", device)

        draw = rng.random()
        coupled: str | None = None
        if draw < settings.mfa_reset_on_new_device_share:
            coupled = "MFA_RESET"
        elif (
            draw
            < settings.mfa_reset_on_new_device_share + settings.mfa_enrolled_on_new_device_share
        ):
            coupled = "MFA_ENROLLED"
        if coupled is not None:
            before = rng.random() < settings.change_before_first_seen_share
            offset = rng.randrange(*MFA_COUPLING_MS)
            if before:
                # Through another channel, from a device already known -- never
                # the new one, which is not yet seen at that moment.
                pending.append((tau - offset, coupled, None))
            else:
                pending.append((tau + offset, coupled, device))

    timeline = _DeviceTimeline(pre_window=list(pre_window), enrolled=enrolled)
    # Resolved only once the timeline is final, so a device enrolled later can
    # never be chosen for an event that precedes its FIRST_SEEN.
    for at_ms, event_type, device_id in pending:
        chosen = device_id if device_id is not None else _pick(rng, timeline.known_at(at_ms))
        plan.add(STREAM_DEVICES, at_ms, IDENTITY_TOPIC, event_type, chosen)
    return timeline


def _plan_device_attributes(
    config: GeneratorConfig,
    plan: _Plan,
    key: str,
    years: float,
    start_ms: int,
    timeline: _DeviceTimeline,
) -> None:
    """ATTRIBUTE_CHANGED and FINGERPRINT_CHANGED on each device while it is known.

    Poisson thinning: a whole-window draw keeps only the times the device is
    known, which is exact for a process with the window's time shape.
    """
    settings = config.baseline_identity
    assert settings is not None
    rng = derive(config.seed, "baseline-device-attrs", key)
    lifetimes = [(device, start_ms) for device in timeline.pre_window] + [
        (device, tau + 1) for tau, device in timeline.enrolled
    ]
    rates = (
        ("ATTRIBUTE_CHANGED", settings.attribute_change_rate_per_device_year),
        ("FINGERPRINT_CHANGED", settings.fingerprint_change_rate_per_device_year),
    )
    for device, known_from in lifetimes:
        for event_type, rate in rates:
            for _ in range(poisson(rng, rate * years)):
                at_ms = _sample_ms(rng, config)
                if at_ms >= known_from:
                    plan.add(STREAM_DEVICE_ATTRS, at_ms, DEVICE_TOPIC, event_type, device)


def _plan_logins(
    config: GeneratorConfig,
    universe: Universe,
    plan: _Plan,
    key: str,
    engaged_days: float,
    home_ips: Sequence[str],
    timeline: _DeviceTimeline,
) -> None:
    """Successful logins, some preceded by a typo burst, some bursts by a reset."""
    settings = config.baseline_identity
    assert settings is not None
    rng = derive(config.seed, "baseline-logins", key)
    weights = settings.typo_burst_size_weights

    for _ in range(poisson(rng, settings.login_rate_per_account_day * engaged_days)):
        login_ms = _sample_ms(rng, config)
        failures: list[int] = []
        reset_ms: int | None = None
        if rng.random() < settings.typo_burst_share_per_login:
            size = 1 + _weighted_index(rng.random(), weights)
            if size >= RESET_MIN_BURST and rng.random() < settings.reset_after_burst_share:
                reset_ms = login_ms - rng.randrange(*RESET_TO_LOGIN_MS)
                last_failure = reset_ms - rng.randrange(*FAILURE_TO_RESET_MS)
            else:
                last_failure = login_ms - rng.randrange(*TYPO_TO_LOGIN_MS)
            failures.append(last_failure)
            for _ in range(size - 1):
                failures.append(failures[-1] - rng.randrange(*TYPO_SPACING_MS))

        # The device must be known when the episode STARTS, not merely when it
        # succeeds: a failure on a not-yet-seen device would precede FIRST_SEEN.
        episode_start = min([login_ms, *failures])
        device = _pick(rng, timeline.known_at(episode_start))
        ip = _pick_ip(rng, settings, home_ips, universe)

        for at_ms in reversed(failures):
            plan.add(STREAM_LOGINS, at_ms, IDENTITY_TOPIC, "LOGIN_FAILED", device, ip)
        if reset_ms is not None:
            # A credential change carries no ip_id, matching the scenario shape
            # for changes; a different key set would betray the label.
            plan.add(STREAM_LOGINS, reset_ms, IDENTITY_TOPIC, "PASSWORD_CHANGE", device)
        plan.add(STREAM_LOGINS, login_ms, IDENTITY_TOPIC, "LOGIN_SUCCEEDED", device, ip)


def _plan_abandoned_bursts(
    config: GeneratorConfig,
    universe: Universe,
    plan: _Plan,
    key: str,
    engaged_days: float,
    home_ips: Sequence[str],
    timeline: _DeviceTimeline,
) -> None:
    """Failures with no success: the user gives up or is locked out."""
    settings = config.baseline_identity
    assert settings is not None
    rng = derive(config.seed, "baseline-abandoned", key)
    weights = settings.typo_burst_size_weights

    for _ in range(poisson(rng, settings.abandoned_burst_rate_per_account_day * engaged_days)):
        first_ms = _sample_ms(rng, config)
        size = 1 + _weighted_index(rng.random(), weights)
        times = [first_ms]
        for _ in range(size - 1):
            times.append(times[-1] + rng.randrange(*TYPO_SPACING_MS))
        device = _pick(rng, timeline.known_at(first_ms))
        ip = _pick_ip(rng, settings, home_ips, universe)
        for at_ms in times:
            plan.add(STREAM_ABANDONED, at_ms, IDENTITY_TOPIC, "LOGIN_FAILED", device, ip)


def _plan_changes(
    config: GeneratorConfig,
    plan: _Plan,
    key: str,
    years: float,
    timeline: _DeviceTimeline,
) -> None:
    """Standalone credential, contact and MFA changes, each its own Poisson process."""
    settings = config.baseline_identity
    assert settings is not None
    rng = derive(config.seed, "baseline-changes", key)
    for event_type, rate in standalone_change_rates(settings):
        for _ in range(poisson(rng, rate * years)):
            at_ms = _sample_ms(rng, config)
            plan.add(
                STREAM_CHANGES,
                at_ms,
                IDENTITY_TOPIC,
                event_type,
                _pick(rng, timeline.known_at(at_ms)),
            )


def standalone_change_rates(settings: BaselineIdentityConfig) -> tuple[tuple[str, float], ...]:
    """(identity_event_type, per-account-year rate), in a fixed draw order."""
    return (
        ("PASSWORD_CHANGE", settings.password_change_rate_per_account_year),
        ("EMAIL_CHANGE", settings.email_change_rate_per_account_year),
        ("PHONE_CHANGE", settings.phone_change_rate_per_account_year),
        ("ADDRESS_CHANGE", settings.address_change_rate_per_account_year),
        ("MFA_RESET", settings.mfa_reset_rate_per_account_year),
        ("MFA_ENROLLED", settings.mfa_enrolled_rate_per_account_year),
    )
