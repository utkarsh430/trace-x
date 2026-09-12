"""Each fraud scenario produces its documented signature (ADR-0030).

ROADMAP Phase 1 automated test: "each scenario produces its documented
signature". A scenario whose injection does not create the shape it claims makes
its `causal_evidence_keys` a lie, and those keys are the basis of evidence
precision and recall in Phase 9 -- so the metric would be measured against
evidence that was never actually planted.

These tests therefore check the *shape of the injected events*, not the label.
"""

from __future__ import annotations

import collections
import re
from pathlib import Path

import pytest
from data.generator.config import GeneratorConfig
from data.generator.population import build_universe
from data.generator.rng import derive
from data.generator.scenarios import (
    ALL_SCENARIOS,
    SCENARIOS_BY_PATTERN,
    AnomalousHighValue,
    ImpossibleTravel,
    ScenarioInstance,
    default_mix,
)

from trace_core.domain.enums import EvidenceKind, FraudPattern
from trace_core.domain.geo import GeoPoint, implied_speed_kmh

pytestmark = pytest.mark.unit

CONFIG = GeneratorConfig(
    row_count=4000, account_count=400, merchant_count=150, device_count=600, ip_count=300
)
UNIVERSE = build_universe(CONFIG)


def _inject(pattern: FraudPattern, salt: str = "a") -> ScenarioInstance:
    scenario = SCENARIOS_BY_PATTERN[pattern]
    rng = derive(CONFIG.seed, "scenario-test", f"{pattern.value}{salt}")
    return scenario.inject(rng, UNIVERSE, f"fi_{pattern.value}", 1_767_225_600_000)


def _samples(pattern: FraudPattern, n: int = 12) -> list[ScenarioInstance]:
    return [_inject(pattern, salt=str(i)) for i in range(n)]


def _tx(instance: ScenarioInstance) -> list:
    return [e for e in instance.events if e.topic == "tx.raw.v1"]


# ---------------------------------------------------------- registration ----


def test_exactly_ten_scenarios() -> None:
    """ROADMAP Phase 1 exit condition."""
    assert len(ALL_SCENARIOS) == 10
    assert len({s.pattern for s in ALL_SCENARIOS}) == 10


def test_every_fraud_pattern_has_a_scenario() -> None:
    assert {s.pattern for s in ALL_SCENARIOS} == set(FraudPattern)


@pytest.mark.parametrize("scenario", ALL_SCENARIOS, ids=lambda s: s.pattern.value)
def test_every_scenario_declares_causal_keys(scenario: object) -> None:
    """Empty keys would make the instance unscoreable in Phase 9."""
    keys = scenario.causal_evidence_keys()  # type: ignore[attr-defined]
    assert keys
    assert keys <= set(EvidenceKind)


@pytest.mark.parametrize("scenario", ALL_SCENARIOS, ids=lambda s: s.pattern.value)
def test_every_scenario_documents_its_signature(scenario: object) -> None:
    signature = scenario.signature()  # type: ignore[attr-defined]
    assert len(signature) > 40, "a signature nobody can check is not a signature"


@pytest.mark.parametrize("scenario", ALL_SCENARIOS, ids=lambda s: s.pattern.value)
def test_every_scenario_produces_at_least_one_transaction(scenario: object) -> None:
    """An episode with no fraudulent transaction contributes no positive label."""
    instance = _inject(scenario.pattern)  # type: ignore[attr-defined]
    assert instance.transaction_count >= 1
    assert instance.participants


@pytest.mark.parametrize("scenario", ALL_SCENARIOS, ids=lambda s: s.pattern.value)
def test_injection_is_deterministic(scenario: object) -> None:
    first = _inject(scenario.pattern)  # type: ignore[attr-defined]
    second = _inject(scenario.pattern)  # type: ignore[attr-defined]
    assert first == second


def test_the_mix_covers_every_scenario_and_is_positive() -> None:
    mix = default_mix(CONFIG)
    assert {s.pattern for s, _ in mix} == set(FraudPattern)
    assert all(weight > 0 for _, weight in mix)


def test_the_mix_is_not_uniform() -> None:
    """Single-transaction fraud is common and many-account fraud is rare; a
    uniform mix would give rare patterns an implausible share of all rows."""
    weights = [w for _, w in default_mix(CONFIG)]
    assert max(weights) > min(weights) * 3


# ----------------------------------------------------------- signatures -----


def test_account_takeover_starts_with_an_identity_change_then_a_novel_device() -> None:
    """IDENTITY_CHANGE and DEVICE_NOVELTY are only causal if actually planted."""
    for instance in _samples(FraudPattern.ACCOUNT_TAKEOVER, 8):
        events = instance.events
        identity = [e for e in events if e.topic == "identity.events.v1"]
        assert identity, "no identity event: IDENTITY_CHANGE would be an uncausal key"
        assert identity[0].identity_event_type in {
            "PASSWORD_CHANGE",
            "EMAIL_CHANGE",
            "MFA_RESET",
        }
        transactions = _tx(instance)
        assert identity[0].occurred_ms < min(e.occurred_ms for e in transactions)

        account_index = transactions[0].account_index
        home_devices = set(UNIVERSE.profiles[account_index].home_devices)
        used = {e.overrides["device_id"] for e in transactions}
        assert used and not (used & home_devices), "device is not novel to this account"


def test_account_takeover_spends_above_profile_and_away_from_home() -> None:
    import math

    for instance in _samples(FraudPattern.ACCOUNT_TAKEOVER, 8):
        transactions = _tx(instance)
        profile = UNIVERSE.profiles[transactions[0].account_index]
        typical = math.exp(profile.amount_mu)
        assert all(e.overrides["amount_minor"] > typical * 2.5 for e in transactions)
        far = GeoPoint(
            transactions[0].overrides["latitude"], transactions[0].overrides["longitude"]
        )
        from trace_core.domain.geo import haversine_km

        assert haversine_km(profile.account.home, far) > 100


def test_card_testing_is_many_tiny_probes_across_many_merchants() -> None:
    for instance in _samples(FraudPattern.CARD_TESTING, 8):
        transactions = _tx(instance)
        probes = [e for e in transactions if e.overrides["amount_minor"] < 250]
        assert len(probes) >= 9, "too few probes to read as card testing"
        merchants = {e.overrides["merchant_id"] for e in probes}
        assert len(merchants) >= 5, "probes must span many merchants"
        span_s = (max(e.occurred_ms for e in probes) - min(e.occurred_ms for e in probes)) / 1000
        assert span_s < 30 * 60, "a burst, not a trickle"
        assert max(e.overrides["amount_minor"] for e in transactions) > 1000, "no payoff charge"


def test_card_testing_probes_include_declines() -> None:
    """Testing a stolen card produces declines; all-approved would be implausible."""
    outcomes = collections.Counter(
        e.overrides.get("authorization_outcome")
        for instance in _samples(FraudPattern.CARD_TESTING, 8)
        for e in _tx(instance)
    )
    assert outcomes["DECLINED"] > 0


def test_impossible_travel_implies_an_infeasible_speed() -> None:
    threshold = ImpossibleTravel().min_speed_kmh
    for instance in _samples(FraudPattern.IMPOSSIBLE_TRAVEL, 12):
        first, second = _tx(instance)
        speed = implied_speed_kmh(
            GeoPoint(first.overrides["latitude"], first.overrides["longitude"]),
            GeoPoint(second.overrides["latitude"], second.overrides["longitude"]),
            (second.occurred_ms - first.occurred_ms) / 1000,
        )
        assert speed > threshold, f"implied {speed:.0f} km/h is not impossible"


def test_impossible_travel_is_card_present_at_both_ends() -> None:
    """A card-not-present leg has an innocent explanation, so the signature needs
    both legs physical."""
    for instance in _samples(FraudPattern.IMPOSSIBLE_TRAVEL, 8):
        assert all(e.overrides["channel"] == "CARD_PRESENT" for e in _tx(instance))


def test_velocity_attack_is_a_burst_on_one_account() -> None:
    for instance in _samples(FraudPattern.VELOCITY_ATTACK, 8):
        transactions = _tx(instance)
        assert len(transactions) >= 16
        assert len({e.account_index for e in transactions}) == 1
        span_s = (
            max(e.occurred_ms for e in transactions) - min(e.occurred_ms for e in transactions)
        ) / 1000
        assert span_s < 40 * 60


def test_velocity_attack_amounts_stay_ordinary() -> None:
    """Velocity must be detectable on its own, not as a side effect of an amount
    anomaly -- otherwise the two scenarios are not distinguishable."""
    import math

    for instance in _samples(FraudPattern.VELOCITY_ATTACK, 8):
        transactions = _tx(instance)
        typical = math.exp(UNIVERSE.profiles[transactions[0].account_index].amount_mu)
        assert all(e.overrides["amount_minor"] < typical * 3 for e in transactions)


def test_device_farm_shares_one_device_across_many_accounts() -> None:
    minimum = SCENARIOS_BY_PATTERN[FraudPattern.DEVICE_FARM].min_accounts  # type: ignore[attr-defined]
    for instance in _samples(FraudPattern.DEVICE_FARM, 8):
        transactions = _tx(instance)
        devices = {e.overrides["device_id"] for e in transactions}
        assert len(devices) == 1, "a farm is one device, many accounts"
        assert len({e.account_index for e in transactions}) >= minimum


def test_fraud_ring_shares_devices_and_ips_across_members() -> None:
    for instance in _samples(FraudPattern.FRAUD_RING, 8):
        transactions = _tx(instance)
        accounts = {e.account_index for e in transactions}
        devices = {e.overrides["device_id"] for e in transactions}
        merchants = {e.overrides["merchant_id"] for e in transactions}
        assert len(accounts) >= 3, "a ring needs members"
        assert len(devices) < len(accounts), "members must share devices, not own them"
        assert len(merchants) <= 3, "a ring converges on a shared merchant set"


def test_merchant_collusion_concentrates_uniform_high_amounts_on_one_merchant() -> None:
    for instance in _samples(FraudPattern.MERCHANT_COLLUSION, 8):
        transactions = _tx(instance)
        assert len({e.overrides["merchant_id"] for e in transactions}) == 1
        assert len({e.account_index for e in transactions}) >= 10
        amounts = [e.overrides["amount_minor"] for e in transactions]
        spread = (max(amounts) - min(amounts)) / max(1, sum(amounts) / len(amounts))
        assert spread < 0.2, "real spend at one merchant is dispersed; laundering is not"


def test_credential_stuffing_is_failed_logins_from_a_small_ip_pool() -> None:
    for instance in _samples(FraudPattern.CREDENTIAL_STUFFING, 8):
        identity = [e for e in instance.events if e.topic == "identity.events.v1"]
        assert len(identity) >= 16, "too few attempts to read as stuffing"
        assert len({e.account_index for e in identity}) >= 10, "must span many accounts"
        pool = {e.overrides["ip_id"] for e in identity}
        assert len(pool) <= 3, "a small origin pool is the signal"
        kinds = collections.Counter(e.identity_event_type for e in identity)
        assert kinds["LOGIN_FAILED"] > kinds.get("LOGIN_SUCCEEDED", 0), "mostly failures"


def test_credential_stuffing_originates_from_datacenter_ips() -> None:
    """IP_REPUTATION is a causal key only if the origin is actually reputable-bad."""
    datacenter = {ip.ip_id for ip in UNIVERSE.ips if ip.is_datacenter}
    for instance in _samples(FraudPattern.CREDENTIAL_STUFFING, 6):
        used = {e.overrides["ip_id"] for e in instance.events if e.topic == "identity.events.v1"}
        assert used <= datacenter


def test_anomalous_high_value_is_a_single_extreme_charge() -> None:
    import math

    multiple = AnomalousHighValue().min_multiple
    for instance in _samples(FraudPattern.ANOMALOUS_HIGH_VALUE, 12):
        transactions = _tx(instance)
        assert len(transactions) == 1
        typical = math.exp(UNIVERSE.profiles[transactions[0].account_index].amount_mu)
        assert transactions[0].overrides["amount_minor"] >= typical * multiple


def test_unusual_location_device_is_deliberately_weak() -> None:
    """It must NOT also carry an amount anomaly.

    This scenario overlaps with a customer travelling with a new phone, on
    purpose: it is what the Phase 7 Skeptic should challenge and what the Phase 9
    Arm F ablation is measured on. Strengthening it would make the ablation easy
    and destroy what it measures (ADR-0030).
    """
    import math

    for instance in _samples(FraudPattern.UNUSUAL_LOCATION_DEVICE, 12):
        transactions = _tx(instance)
        assert len(transactions) == 1
        event = transactions[0]
        profile = UNIVERSE.profiles[event.account_index]
        typical = math.exp(profile.amount_mu)
        amount = event.overrides["amount_minor"]
        assert 0.5 * typical < amount < 2.0 * typical, "amount must stay ordinary"
        assert event.overrides["device_id"] not in set(profile.home_devices)


def test_unusual_location_device_claims_only_two_causal_keys() -> None:
    """Its ambiguity is the point; claiming more would inflate evidence recall."""
    keys = SCENARIOS_BY_PATTERN[FraudPattern.UNUSUAL_LOCATION_DEVICE].causal_evidence_keys()
    assert keys == {EvidenceKind.DEVICE_NOVELTY, EvidenceKind.GEO_DISPERSION}


# ------------------------------------------------- causal key discipline ----


def test_causal_keys_are_not_a_wish_list() -> None:
    """No scenario claims more than half the vocabulary.

    Listing every plausible signal would inflate evidence recall for free and
    make the metric meaningless.
    """
    for scenario in ALL_SCENARIOS:
        assert len(scenario.causal_evidence_keys()) <= len(EvidenceKind) // 2


def test_causal_keys_exclude_terminal_kinds() -> None:
    """DECISION and PROPOSED_ACTION are agent outputs, not facts about a
    transaction. Claiming them as causal would score an agent for citing its own
    conclusion."""
    forbidden = {EvidenceKind.DECISION, EvidenceKind.PROPOSED_ACTION, EvidenceKind.CHALLENGE}
    for scenario in ALL_SCENARIOS:
        assert not scenario.causal_evidence_keys() & forbidden


def test_scenarios_do_not_all_claim_the_same_keys() -> None:
    """Identical key sets would make per-pattern evidence metrics uninformative."""
    sets = {frozenset(s.causal_evidence_keys()) for s in ALL_SCENARIOS}
    assert len(sets) >= 8


# ------------------------------------------------- doc <-> code agreement ----

ROOT = Path(__file__).resolve().parents[2]


def _catalogue() -> str:
    return (ROOT / "docs" / "FRAUD_SCENARIOS.md").read_text()


@pytest.mark.parametrize("scenario", ALL_SCENARIOS, ids=lambda s: s.pattern.value)
def test_the_catalogue_documents_every_scenario(scenario: object) -> None:
    """ROADMAP Phase 1 exit condition: ten scenarios implemented AND documented."""
    assert f"`{scenario.pattern.value}`" in _catalogue()  # type: ignore[attr-defined]


def _catalogue_sections() -> dict[str, str]:
    """Map pattern name -> its §3 subsection, anchored on the heading.

    Anchored rather than split on the first mention: patterns are named again in
    §4's rules table, and splitting there silently parsed the wrong text.
    """
    text = _catalogue()
    sections: dict[str, str] = {}
    for match in re.finditer(r"^### \d+\.\d+ `([A-Z_]+)`$", text, re.M):
        body = text[match.end() :]
        nxt = re.search(r"^#{2,3} ", body, re.M)
        sections[match.group(1)] = body[: nxt.start()] if nxt else body
    return sections


@pytest.mark.parametrize("scenario", ALL_SCENARIOS, ids=lambda s: s.pattern.value)
def test_the_catalogue_lists_the_same_causal_keys_as_the_code(scenario: object) -> None:
    """CLAUDE.md §14: code and a control-plane document disagreeing is a bug in
    one of them. These keys are what evidence precision and recall are computed
    against, so a catalogue that overstates them would mislead anyone reading the
    metric definition.
    """
    pattern = scenario.pattern.value  # type: ignore[attr-defined]
    sections = _catalogue_sections()
    assert pattern in sections, f"{pattern} has no §3 subsection in the catalogue"
    line = next(ln for ln in sections[pattern].splitlines() if ln.startswith("**Causal keys.**"))
    documented = set(re.findall(r"`([A-Z][A-Z_]{2,})`", line))
    declared = {k.value for k in scenario.causal_evidence_keys()}  # type: ignore[attr-defined]
    assert documented == declared, (
        f"{pattern}: catalogue says {sorted(documented)}, code declares {sorted(declared)}"
    )


def test_the_catalogue_parser_actually_found_the_sections() -> None:
    """Guards against the comparison above becoming a no-op."""
    assert len(_catalogue_sections()) == 10
