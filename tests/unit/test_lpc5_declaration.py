"""`LPC-5` §16.1-§16.2 and §16.5: the declaration is pinned, cited and isolated.

The criterion is frozen as a document (`eval/track_a/criteria/lpc-5.md`). These tests keep the
code's copy of it honest:
- the document's digest is pinned, so editing it without a numbered revision fails;
- the declaration's thresholds are pinned as literals, so loosening one is a visible diff;
- every allowlist citation is checked verbatim against the scenario catalogue;
- the allowlist's structure is checked against the rules the criterion states for it.
"""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

import pytest
from data.generator.lpc5 import declaration as d
from data.generator.lpc5 import stats

from trace_core.domain.enums import FraudPattern
from trace_core.features.definitions import ONLINE_FEATURES

ROOT = Path(__file__).resolve().parents[2]


def test_the_frozen_criterion_digest_is_pinned() -> None:
    body = (ROOT / d.CRITERION_PATH).read_bytes()
    assert hashlib.sha256(body).hexdigest() == d.CRITERION_SHA256, (
        "eval/track_a/criteria/lpc-5.md changed. A change is a numbered revision (its §20): update "
        "the revision log, the declaration and this digest together."
    )
    assert f"FROZEN, revision {d.REVISION}" in body.decode("utf-8")


def test_the_statistical_constants_are_pinned() -> None:
    assert (d.Z, d.ENRICHMENT_BOUND, d.MIN_EXCESS, d.LEGIT_SHARE_FLOOR) == (1.645, 2.0, 0.02, 0.001)
    assert (d.S1_TRIGGER, d.PRECISION_BOUND) == (0.02, 0.25)
    assert (d.SUPPORT_MIN_ROWS, d.SUPPORT_MIN_ACCOUNTS, d.SUPPORT_MIN_SHARE) == (30, 20, 0.001)
    assert (d.STRATUM_MIN_ROWS, d.STRATUM_MIN_ACCOUNTS) == (30, 20)
    assert d.S2_TOLERANCE == 0.01
    assert (d.S3_SLICES, d.S3_MIN_LEGIT_SLICE_SHARE, d.S3_LOW_FACTOR, d.S3_HIGH_FACTOR) == (
        20,
        0.01,
        0.25,
        4.0,
    )
    assert d.S3_SCENARIO_PARTS == 3
    assert (d.S4_MAX_OFFSET_MS, d.S4_MIN_CLUSTERS, d.S4_TRIGGER, d.S4_CONCENTRATION) == (
        86_400_000,
        5,
        0.02,
        4.0,
    )
    assert (d.S4_WINDOW_HALF_BINS, d.S4_BACKGROUND_NEAR, d.S4_BACKGROUND_FAR) == (1, 2, 31)
    assert (d.S5_TOLERANCE, d.S7_LIFT, d.S7_MARGIN, d.MIN_INSTANCES) == (0.02, 2.0, 0.25, 20)
    assert (d.R9_MIN_LEGIT_ROWS, d.R9_MIN_LEGIT_CLUSTERS, d.R9_MIN_PLANTED_TX_CLUSTERS) == (
        30,
        30,
        30,
    )


def test_the_generator_rules_are_pinned() -> None:
    assert d.G1_MAX_DRAWS == 10_000
    assert d.G1_PROBE_RANGE == (1, 250)
    assert d.G1_HIGH_VALUE_MULTIPLE == (20.0, 60.0)
    assert d.G1_RULES[FraudPattern.VELOCITY_ATTACK].upper_multiple == 3.0
    uld = d.G1_RULES[FraudPattern.UNUSUAL_LOCATION_DEVICE]
    assert (uld.lower_multiple, uld.lower_exclusive, uld.upper_multiple) == (0.5, True, 2.0)
    assert d.G1_RULES[FraudPattern.ACCOUNT_TAKEOVER].lower_multiple == 3.0
    assert d.G1_CARD_TESTING_PAYOFF.lower_multiple == 3.0
    assert set(d.G1_RULES) == set(FraudPattern)
    assert (d.G6_PRICE_LOG_RANGE, d.G6_PRICE_MULTIPLIER) == ((8.3, 8.9), (0.99, 1.01))
    assert (d.G6_PAYER_TOLERANCE_SIGMAS, d.G6_S7A_LOG_SLACK) == (1.0, 0.011)
    assert d.G3_MAX_SPEED_KMH == 900.0
    assert {FraudPattern.ACCOUNT_TAKEOVER, FraudPattern.UNUSUAL_LOCATION_DEVICE} == d.G3_SCENARIOS
    assert d.G5_TAKEOVER_WINDOW_MS == (1_200_000, 21_600_000)
    assert d.G5_RING_SPAN_MS == (172_800_000, 604_800_000)
    assert d.G5_FARM_SPAN_MS == (3_600_000, 86_400_000)
    assert (d.DM1_NAMESPACE, d.DM1_FLOOR_MS, d.DM1_MEAN_MS, d.DM1_SD_MS) == (
        "authorization-latency",
        40,
        300.0,
        150.0,
    )
    assert d.G7_GATED_CAUSAL_KEYS[FraudPattern.CREDENTIAL_STUFFING] == {
        "AUTHENTICATION_ANOMALY",
        "IP_REPUTATION",
        "DEVICE_SHARING",
    }
    assert d.G7_GATED_CAUSAL_KEYS[FraudPattern.CARD_TESTING] == {
        "VELOCITY",
        "AMOUNT_ANOMALY",
        "MCC_ANOMALY",
    }


def test_the_allowlist_has_the_revision_4_rows() -> None:
    ids = [row.row_id for row in d.ALLOWLIST]
    assert len(ids) == len(set(ids)) == 55
    assert "MC-3" not in ids
    assert "CS-8" not in ids
    assert {row.scenario for row in d.ALLOWLIST} == set(FraudPattern)
    by_id = {row.row_id: row for row in d.ALLOWLIST}
    assert by_id["MC-1"].attribute == "merchant_same_amount_accounts_24h"
    assert by_id["MC-1"].e_min == 0.50
    assert by_id["CT-9"].attribute == "authorization_outcome"
    assert by_id["IT-2"].none == {"≥1000"}
    assert by_id["ATO-4"].populations == (d.Population.TX, d.Population.ID)
    # Revision 3: no minimum effect for MC-5 and DF-5; value-specific support exemptions.
    assert {row.row_id for row in d.ALLOWLIST if row.e_min is None} == {"MC-5", "DF-5"}
    assert by_id["ATO-3"].rare == {"ADDRESS_CHANGE", "EMAIL_CHANGE", "PHONE_CHANGE"}  # revision 4
    assert not by_id["CT-1"].rare
    assert by_id["CT-1"].none == {"3-4", "5-9", "10-19", "20+"}
    assert by_id["CT-2"].rare == {"3-4"}
    assert by_id["CT-2"].none == {"5-9", "10-19", "20+"}
    assert by_id["CT-4"].rare == by_id["CT-2"].rare and by_id["CT-4"].none == by_id["CT-2"].none
    assert not by_id["CT-3"].rare
    assert by_id["CT-3"].none == {"5-9", "10-19", "20+"}
    assert dict(by_id["CT-3"].consequence_rare) == {"tx_count_24h": {"10-19"}}
    assert by_id["CT-6"].none == by_id["CT-10"].none == by_id["VA-6"].none == {"5-9", "10+"}
    assert by_id["CT-11"].rare == {"(0,0.4)"}
    assert by_id["VA-1"].none == {"3-4", "5-9", "10-19", "20+"}
    assert by_id["VA-2"].none == by_id["VA-4"].none == {"5-9", "10-19", "20+"}
    assert by_id["VA-2"].rare == by_id["VA-4"].rare == {"3-4"}  # revision 4
    assert by_id["VA-3"].none == {"5-9", "10-19", "20+"}
    assert dict(by_id["VA-3"].consequence_rare) == {"tx_count_24h": {"10-19"}}
    assert dict(by_id["VA-3"].consequence_none) == {"tx_count_24h": {"20+"}}
    assert not by_id["CS-6"].rare
    assert by_id["CS-6"].none == {"5+"}
    assert dict(by_id["FR-1"].consequence_rare) == {"device_accounts_24h": {"5+"}}


def test_every_allowlist_row_is_structurally_sound() -> None:
    for row in d.ALLOWLIST:
        where = row.row_id
        assert row.e_min is None or 0.0 < row.e_min <= 1.0, where
        for name, exempt in (*row.consequence_rare.items(), *row.consequence_none.items()):
            owners = [pop for pop in row.populations if name in row.consequences_in(pop)]
            assert owners, f"{where}: {name} carries exemptions but is not a consequence"
            for population in owners:
                spec = d.attribute(population, name)
                assert spec.values is None or exempt <= set(spec.values), (where, name)
        for name in set(row.consequence_rare) & set(row.consequence_none):
            assert not row.consequence_rare[name] & row.consequence_none[name], (where, name)
        assert row.rare <= row.values and row.none <= row.values, where
        assert not row.rare & row.none, where
        assert (row.composition is None) == (len(row.values) == 1), where
        assert row.sources, where
        for population in row.populations:
            spec = d.attribute(population, row.attribute)
            assert spec.klass is d.Klass.BEHAVIOUR, f"{where}: allowlisted attribute must be B"
            if spec.values is not None:
                present = row.values & set(spec.values)
                assert present, f"{where}: none of {sorted(row.values)} exist in {population}"
        assert set(row.consequences) <= set(row.populations), where
        for population, names in row.consequences.items():
            for name in names:
                assert d.attribute(population, name).klass is d.Klass.BEHAVIOUR, (where, name)


def test_every_allowlist_value_exists_in_some_named_population() -> None:
    for row in d.ALLOWLIST:
        vocab: set[str] = set()
        open_vocab = False
        for population in row.populations:
            spec = d.attribute(population, row.attribute)
            if spec.values is None:
                open_vocab = True
            else:
                vocab |= set(spec.values)
        assert open_vocab or row.values <= vocab, (row.row_id, sorted(row.values - vocab))


def test_no_attribute_is_both_named_and_a_consequence_for_one_scenario() -> None:
    for scenario in FraudPattern:
        rows = [row for row in d.ALLOWLIST if row.scenario is scenario]
        for population in d.Population:
            named = {row.attribute for row in rows if population in row.populations}
            consequence_owners: dict[str, list[str]] = {}
            for row in rows:
                for name in row.consequences_in(population):
                    consequence_owners.setdefault(name, []).append(row.row_id)
            assert not named & set(consequence_owners), (scenario, population)
            ambiguous = {n: o for n, o in consequence_owners.items() if len(o) > 1}
            assert not ambiguous, (scenario, population, ambiguous)


def test_episode_consequences_are_declared_once_and_never_named_or_row_consequences() -> None:
    by_id = {episode.row_id: episode for episode in d.EPISODE_CONSEQUENCES}
    assert list(by_id) == ["ATO-E1", "ATO-E2", "IT-E1"]
    assert by_id["ATO-E1"].attributes == {
        "tx_count_1h",
        "tx_count_24h",
        "gap_prev",
        "distinct_merchants_1h",
        "prior_decisions_1h",
        "prior_declined_share_1h",
    }
    assert by_id["ATO-E2"].attributes == by_id["IT-E1"].attributes == {"hour", "daypart"}
    incidental = {e.row_id for e in d.EPISODE_CONSEQUENCES if e.kind is d.EpisodeKind.INCIDENTAL}
    assert incidental == {"ATO-E2", "IT-E1"}
    for episode in d.EPISODE_CONSEQUENCES:
        rows = [row for row in d.ALLOWLIST if row.scenario is episode.scenario]
        for population in episode.populations:
            named = {row.attribute for row in rows if population in row.populations}
            consequences = {name for row in rows for name in row.consequences_in(population)}
            for name in episode.attributes:
                spec = d.attribute(population, name)
                assert spec.klass is d.Klass.BEHAVIOUR, (episode.row_id, population, name)
                assert name not in named | consequences, (episode.row_id, population, name)


def test_documented_consequences_are_value_scoped_and_never_named() -> None:
    """Revision 4 §6.7: specific values of attributes no row of the scenario names, never every
    value of an attribute; `within` entries stay inside rows of the same scenario, unexempt."""
    entries = d.DOCUMENTED_CONSEQUENCES
    ids = [entry.row_id for entry in entries]
    assert len(ids) == len(set(ids)) == 36
    other_ids = {row.row_id for row in d.ALLOWLIST} | {e.row_id for e in d.EPISODE_CONSEQUENCES}
    assert not set(ids) & other_ids
    by_row = {row.row_id: row for row in d.ALLOWLIST}
    by_id = {entry.row_id: entry for entry in entries}
    exempt = {entry.row_id for entry in entries if entry.rare or entry.none}
    assert exempt == {"VA-C1", "VA-C2", "ATO-C3"}
    assert by_id["VA-C1"].none == {"5-9", "10+"}
    assert by_id["VA-C2"].rare == {"3-4"} and by_id["VA-C2"].none == {"5+"}
    assert by_id["ATO-C3"].none == {"<1h"}
    assert by_id["DF-C3"].within == {"DF-1", "DF-2"}
    scopes: set[tuple[FraudPattern, d.Population, str, str]] = set()
    for entry in entries:
        where = entry.row_id
        rows = [row for row in d.ALLOWLIST if row.scenario is entry.scenario]
        named = {row.attribute for row in rows if entry.population in row.populations}
        consequences = {name for row in rows for name in row.consequences_in(entry.population)}
        episodes = {
            name
            for episode in d.EPISODE_CONSEQUENCES
            if episode.scenario is entry.scenario and entry.population in episode.populations
            for name in episode.attributes
        }
        assert entry.values and entry.attributes, where
        assert entry.rare <= entry.values and entry.none <= entry.values, where
        assert not entry.rare & entry.none, where
        assert entry.source.kind is not d.SourceKind.KEY, where
        if entry.within:
            assert not entry.rare and not entry.none, f"{where}: exemptions need scenario scope"
        else:
            assert not entry.attributes & (named | consequences | episodes), where
        for row_id in entry.within:
            row = by_row[row_id]
            assert row.scenario is entry.scenario, (where, row_id)
            assert entry.population in row.populations, (where, row_id)
            assert row.attribute not in entry.attributes, (where, row_id)
        for name in entry.attributes:
            spec = d.attribute(entry.population, name)
            assert spec.klass in (d.Klass.BEHAVIOUR, d.Klass.AVAILABILITY), (where, name)
            if spec.values is not None:
                assert entry.values < set(spec.values), f"{where}: {name} must stay value-scoped"
            for scope in sorted(entry.within) or [""]:
                key = (entry.scenario, entry.population, name, scope)
                assert key not in scopes, (where, key)
                scopes.add(key)


def test_optional_field_attributes_exist_and_are_behavioural() -> None:
    assert {
        population: dict(fields) for population, fields in d.OPTIONAL_FIELD_ATTRIBUTES.items()
    } == {
        d.Population.ID: {
            "ip": ("ip_datacenter", "ip_login_accounts"),
            "device": ("device_home", "device_age", "device_login_accounts"),
            "user_agent": ("user_agent",),
        }
    }
    for population, fields in d.OPTIONAL_FIELD_ATTRIBUTES.items():
        for names in fields.values():
            for name in names:
                assert d.attribute(population, name).klass is d.Klass.BEHAVIOUR, name


def _catalogue_sections() -> dict[str, str]:
    text = (ROOT / "docs" / "FRAUD_SCENARIOS.md").read_text(encoding="utf-8")
    sections: dict[str, str] = {}
    matches = list(re.finditer(r"^### (3\.\d+) `([A-Z_]+)`$", text, flags=re.M))
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else text.index("\n## 4.")
        sections[match.group(1)] = text[match.end() : end]
    return sections


def _normalised(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("**", "")).strip()


def test_every_citation_is_verbatim_in_its_scenario_subsection() -> None:
    sections = _catalogue_sections()
    assert len(sections) == 10
    for row in d.ALLOWLIST:
        body = sections[d.CATALOGUE_SECTIONS[row.scenario]]
        for source in row.sources:
            if source.kind is d.SourceKind.KEY:
                (key,) = source.fragments
                keys_line = next(
                    line for line in body.splitlines() if line.startswith("**Causal keys.**")
                )
                assert f"`{key}`" in keys_line, (row.row_id, key)
                admissible = {
                    (population, name, values) for population, name, values in d.KEY_MAP[key]
                }
                assert any(
                    population in row.populations and name == row.attribute and row.values <= values
                    for population, name, values in admissible
                ), (row.row_id, key)
            else:
                for fragment in source.fragments:
                    assert _normalised(fragment) in _normalised(body), (row.row_id, fragment)
    for episode in d.EPISODE_CONSEQUENCES:
        body = sections[d.CATALOGUE_SECTIONS[episode.scenario]]
        assert episode.source.kind is not d.SourceKind.KEY, episode.row_id
        for fragment in episode.source.fragments:
            assert _normalised(fragment) in _normalised(body), (episode.row_id, fragment)
    for entry in d.DOCUMENTED_CONSEQUENCES:
        body = sections[d.CATALOGUE_SECTIONS[entry.scenario]]
        for fragment in entry.source.fragments:
            assert _normalised(fragment) in _normalised(body), (entry.row_id, fragment)


def test_key_citations_admit_only_values_the_map_allows() -> None:
    for row in d.ALLOWLIST:
        for source in row.sources:
            if source.kind is not d.SourceKind.KEY:
                continue
            (key,) = source.fragments
            allowed: set[str] = set()
            for population, name, values in d.KEY_MAP[key]:
                if population in row.populations and name == row.attribute:
                    allowed |= values
            if len(row.sources) == 1:
                assert row.values <= allowed, (row.row_id, sorted(row.values - allowed))


def test_the_released_features_are_the_registry_and_every_one_has_a_dependency() -> None:
    assert set(d.RELEASED_FEATURES) == set(ONLINE_FEATURES.ids)
    assert len(d.RELEASED_FEATURES) == 26
    assert set(d.DEPENDS) == set(d.RELEASED_FEATURES)
    for feature, dependency in d.DEPENDS.items():
        assert d.attribute(d.Population.TX, dependency).klass is d.Klass.BEHAVIOUR, feature
        assert (
            d.attribute(d.Population.TX, f"{d.AVAIL_PREFIX}{feature}").klass is d.Klass.AVAILABILITY
        )


def test_attribute_names_are_unique_and_strata_exist() -> None:
    for population, specs in d.ATTRIBUTES.items():
        names = [spec.name for spec in specs]
        assert len(names) == len(set(names)), population
        for spec in specs:
            if spec.stratum is not None:
                assert spec.stratum in names, (population, spec.name)


def test_out_rows_are_never_judged_on_the_window() -> None:
    names = {spec.name for spec in d.ATTRIBUTES[d.Population.OUT]}
    assert "in_window" not in names


def test_every_ablation_names_a_correction_of_the_stage_2_plan() -> None:
    assert set(d.ABLATIONS) == {
        "T1", "T2", "T3", "M1", "M2", "M3", "M4", "M5", "M6",
        *(f"N{n}" for n in range(1, 13)),
    }  # fmt: skip
    assert "G2" not in d.ABLATIONS
    assert len(d.NEGATIVE_CONTROL) == 14


def test_no_documented_offsets_are_declared() -> None:
    assert d.DOCUMENTED_OFFSETS == ()
    assert set(d.PAIR_KINDS) == {f"K{n}" for n in range(1, 9)}


@pytest.mark.parametrize(
    ("x", "r", "k", "lo", "hi"),
    [(0, 0, 0, 0.0, 1.0), (10, 10, 10, 0.7870, 1.0), (5, 10, 10, 0.2693, 0.7307)],
)
def test_the_share_bounds_are_the_family_wilson_interval(
    x: int, r: int, k: int, lo: float, hi: float
) -> None:
    share = stats.Share(x, r, k)
    assert share.lo == pytest.approx(lo, abs=1e-4)
    assert share.hi == pytest.approx(hi, abs=1e-4)


def test_named_tests_at_their_boundaries() -> None:
    legit = stats.Share(0, 10_000, 5_000)
    assert stats.legit_hi_floored(legit) == pytest.approx(max(legit.hi, 0.001))
    assert stats.enriched(stats.Share(40, 40, 40), legit)
    assert not stats.enriched(stats.Share(1, 1000, 1000), legit)
    assert stats.supported(30, 20, 30_000)
    assert not stats.supported(29, 20, 1_000)
    assert not stats.supported(30, 19, 1_000)
    assert not stats.supported(30, 20, 30_001)
    assert stats.supported(30, 20, 30_001, share_condition=False)
    assert stats.differs(stats.Share(100, 100, 100), stats.Share(0, 10_000, 5_000), 0.01)
    assert not stats.differs(stats.Share(1, 100, 100), stats.Share(100, 10_000, 5_000), 0.01)
    half = stats.Share(5_000, 10_000, 5_000)
    assert stats.s7b_threshold(half) == pytest.approx(min(2 * half.hi, half.hi + 0.25))


def test_no_runtime_module_imports_lpc5() -> None:
    offenders: list[str] = []
    scanned = 0
    for root in ("packages", "services", "mcp_servers"):
        base = ROOT / root
        if not base.is_dir():
            continue
        for path in base.rglob("*.py"):
            scanned += 1
            if "lpc5" in path.read_text(encoding="utf-8"):
                offenders.append(str(path.relative_to(ROOT)))
    for name in ("engine.py", "baseline.py", "emit.py", "cli.py", "scenarios.py"):
        scanned += 1
        if "lpc5" in (ROOT / "data" / "generator" / name).read_text(encoding="utf-8"):
            offenders.append(f"data/generator/{name}")
    assert scanned > 20
    assert not offenders, offenders
