"""`LPC-5` §16.3: each statistical check, passing and failing, on hand-built tables and inputs."""

from __future__ import annotations

from collections.abc import Sequence

from data.generator.lpc5 import declaration as d
from data.generator.lpc5.attributes import Column, Table
from data.generator.lpc5.fixtures import account
from data.generator.lpc5.judge import (
    Allowlist,
    CheckResult,
    Pair,
    r7_r8,
    r9,
    s1_conditional,
    s1_unconditional,
    s2_representation,
    s3_calendar,
    s4_offsets,
    s7_effects,
)

from trace_core.domain.enums import FraudPattern

TX = d.Population.TX
ATO = FraudPattern.ACCOUNT_TAKEOVER.value
CT = FraudPattern.CARD_TESTING.value
IT = FraudPattern.IMPOSSIBLE_TRAVEL.value
ALLOW = Allowlist()


class Build:
    """A TX table: legitimate rows first, then each scenario's rows, one cluster per planted row."""

    def __init__(
        self,
        legit_rows: int = 2_000,
        legit_accounts: int = 400,
        planted: Sequence[tuple[str, int]] = ((ATO, 40),),
    ) -> None:
        self.groups: list[str] = []
        self.clusters: list[str] = []
        self.accounts: list[str] = []
        for n in range(legit_rows):
            name = account(n % legit_accounts)
            self.groups.append(d.LEGIT)
            self.clusters.append(name)
            self.accounts.append(name)
        for scenario, count in planted:
            for k in range(count):
                self.groups.append(scenario)
                self.clusters.append(f"{scenario}:{k}")
                self.accounts.append(account(900_000 + k))
        self.legit_rows = legit_rows
        self.columns: dict[str, list[str | None]] = {}

    def column(self, name: str, legit: Sequence[str | None], planted: Sequence[str | None]) -> None:
        assert len(legit) == self.legit_rows
        assert len(legit) + len(planted) == len(self.groups)
        self.columns[name] = [*legit, *planted]

    def table(self, population: d.Population = TX) -> Table:
        n = len(self.groups)
        columns: dict[str, Column] = {}
        for name, values in self.columns.items():
            column = Column(n)
            for i, value in enumerate(values):
                if value is not None:
                    column.put(i, value)
            columns[name] = column
        return Table(population, n, self.groups, self.clusters, self.accounts, columns)


def _cells(result: CheckResult, check: str) -> set[tuple[str | None, str | None, str | None]]:
    return {(f.attribute, f.value, f.group) for f in result.findings if f.check == check}


def test_a_planted_only_value_fails_support_precision_and_enrichment() -> None:
    build = Build()
    build.column("hour", ["3"] * 2_000, ["4"] * 40)
    tables = {TX: build.table()}
    r7, r8 = r7_r8(tables, ALLOW)
    s1u = s1_unconditional(tables, ALLOW)
    assert ("hour", "4", ATO) in _cells(r7, "R7")
    assert ("hour", "4", d.POOLED) in _cells(r8, "R8")
    assert ("hour", "4", ATO) in _cells(s1u, "S1-U(a)")
    assert ("hour", "4", d.POOLED) in _cells(s1u, "S1-U(a)")
    assert ("hour", "4", ATO) in _cells(s1u, "S1-U(b)")


def test_an_attribute_distributed_like_legitimate_rows_passes() -> None:
    build = Build()
    build.column("hour", ["3", "4"] * 1_000, ["3", "4"] * 20)
    tables = {TX: build.table()}
    r7, r8 = r7_r8(tables, ALLOW)
    s1u = s1_unconditional(tables, ALLOW)
    assert r7.passed and r8.passed and s1u.passed
    assert r7.judged > 0 and s1u.judged > 0


def test_an_allowlisted_value_keeps_its_support_check_but_skips_precision_and_enrichment() -> None:
    build = Build()
    legit = ["not"] * 50 + ["home"] * 1_950
    build.column("device_home", legit, ["not"] * 40)
    build.column("ip_home", legit, ["not"] * 40)  # the same shape, but ORDINARY for takeovers
    tables = {TX: build.table()}
    r7, _ = r7_r8(tables, ALLOW)
    s1u = s1_unconditional(tables, ALLOW)
    assert ("device_home", "not", ATO) not in _cells(r7, "R7")
    assert ("device_home", "not", ATO) not in _cells(s1u, "S1-U(a)")
    assert ("device_home", "not", ATO) not in _cells(s1u, "S1-U(b)")
    assert ("ip_home", "not", ATO) in _cells(r7, "R7")
    assert ("ip_home", "not", ATO) in _cells(s1u, "S1-U(b)")
    assert ("ip_home", "not", ATO) not in _cells(s1u, "S1-U(a)")


def test_support_needs_thirty_rows_twenty_accounts_and_the_share_unless_exempt() -> None:
    def judged(legit_with_value: int, value: str) -> CheckResult:
        build = Build(legit_rows=40_000, legit_accounts=4_000, planted=((CT, 40),))
        legit = ["1"] * 40_000
        for k in range(legit_with_value):
            legit[k * 10] = value  # one row per account: accounts 0..k-1
        build.column("tx_count_1m", legit, [value] * 40)
        return s1_unconditional({TX: build.table()}, ALLOW)

    # "3-4" is a `rare` value of CT-1: 30 rows from 30 accounts suffice below the 0.1 % share.
    assert ("tx_count_1m", "3-4", CT) not in _cells(judged(30, "3-4"), "S1-U(a)")
    assert ("tx_count_1m", "3-4", CT) in _cells(judged(29, "3-4"), "S1-U(a)")
    # "5-9" is a `none` value: no legitimate row is needed.
    assert ("tx_count_1m", "5-9", CT) not in _cells(judged(0, "5-9"), "S1-U(a)")
    # "2" is allowlisted without exemption: the share condition applies.
    assert ("tx_count_1m", "2", CT) in _cells(judged(30, "2"), "S1-U(a)")


def test_the_trigger_leaves_a_small_planted_share_unjudged() -> None:
    build = Build()
    build.column("hour", ["3"] * 2_000, ["9"] + ["3"] * 39)
    result = s1_unconditional({TX: build.table()}, ALLOW)
    assert ("hour", "9", ATO) not in _cells(result, "S1-U(a)")


def test_s1b_judges_ordinary_attributes_and_consequences_inside_the_stratum() -> None:
    build = Build()
    legit_home = ["not"] * 100 + ["home"] * 1_900
    build.column("device_home", legit_home, ["not"] * 40)
    build.column("hour", ["3"] * 2_000, ["4"] * 40)
    build.column("device_accounts", ["2"] * 100 + ["1"] * 1_900, ["2"] * 40)
    tables = {TX: build.table()}
    result = s1_conditional(tables, ALLOW)
    assert ("hour", "4", ATO) in _cells(result, "S1-B(i)")
    assert ("device_accounts", "2", ATO) not in _cells(result, "S1-B(i)")
    r7, _ = r7_r8(tables, ALLOW)
    assert ("device_accounts", "2", ATO) not in _cells(r7, "R7")  # a consequence: S1-B only


def test_s1b_falls_back_to_every_legitimate_row_when_the_stratum_is_thin() -> None:
    build = Build()
    build.column("device_home", ["not"] * 10 + ["home"] * 1_990, ["not"] * 40)
    build.column("device_accounts", ["2"] * 10 + ["1"] * 1_990, ["2"] * 40)
    result = s1_conditional({TX: build.table()}, ALLOW)
    assert ("device_accounts", "2", ATO) in _cells(result, "S1-B(i)")


def test_judged_composition_needs_a_supported_stratum_and_matching_shares() -> None:
    def composition(legit: list[str]) -> CheckResult:
        build = Build()
        build.column("device_age", legit, ["first-use"] * 20 + ["<1h"] * 20)
        return s1_conditional({TX: build.table()}, ALLOW)

    thin = composition(["first-use"] * 5 + ["<1h"] * 5 + ["≥7d"] * 1_990)
    assert any(f.check == "S1-B(iii)" and f.value is None for f in thin.findings)
    even = composition(["first-use"] * 50 + ["<1h"] * 50 + ["≥7d"] * 1_900)
    assert not [f for f in even.findings if f.check == "S1-B(iii)"]
    skewed = composition(["first-use"] * 5 + ["<1h"] * 95 + ["≥7d"] * 1_900)
    assert ("device_age", "first-use", ATO) in _cells(skewed, "S1-B(iii)")


def test_representation_values_may_not_be_planted_only_or_differ() -> None:
    build = Build()
    build.column("subsecond", ["fractional"] * 2_000, ["whole"] + ["fractional"] * 39)
    assert ("subsecond", "whole", ATO) in _cells(s2_representation({TX: build.table()}), "S2a")
    shifted = Build()
    shifted.column("subsecond", ["fractional"] * 2_000, ["whole"] * 40)
    result = s2_representation({TX: shifted.table()})
    assert ("subsecond", "fractional", ATO) in _cells(result, "S2b")
    same = Build()
    same.column("subsecond", ["fractional"] * 2_000, ["fractional"] * 40)
    assert s2_representation({TX: same.table()}).passed


def test_pooled_enrichment_is_exempt_only_through_a_scenario_above_ten_percent() -> None:
    build = Build(planted=((ATO, 90), (CT, 10)))
    build.column("device_home", ["home"] * 2_000, ["not"] * 100)
    build.column("tx_count_1m", ["1"] * 2_000, ["1"] * 90 + ["5-9"] * 10)
    _, r8 = r7_r8({TX: build.table()}, ALLOW)
    assert ("device_home", "not", d.POOLED) not in _cells(r8, "R8")
    assert ("tx_count_1m", "5-9", d.POOLED) in _cells(r8, "R8")


def test_the_pooled_calendar_rule_sees_concentration_and_absence() -> None:
    legit = list(range(0, 20_000, 10))
    crowded, _ = s3_calendar({ATO: [500] * 40}, legit, 0, 20_000)
    assert {f.value for f in crowded.findings} == {"slice 1"}
    starts = list(range(0, 19_000, 50))[:400]
    empty_last, _ = s3_calendar({ATO: starts}, legit, 0, 20_000)
    assert "slice 20" in {f.value for f in empty_last.findings}
    spread = list(range(25, 20_000, 50))
    passed, _ = s3_calendar({ATO: spread}, legit, 0, 20_000)
    assert passed.passed and passed.judged == 20


def test_the_per_scenario_thirds_rule_and_its_boundaries() -> None:
    def thirds(starts: list[int]) -> CheckResult:
        return s3_calendar({CT: starts}, list(range(0, 30_000, 10)), 0, 30_000)[1]

    assert thirds([5_000] * 7 + [15_000] * 7 + [25_000] * 6).passed
    assert not thirds([15_000] * 10 + [25_000] * 10).passed  # an empty third
    assert thirds([5_000] * 14 + [15_000] * 6 + [25_000] * 1).passed  # exactly two thirds
    assert not thirds([5_000] * 15 + [15_000] * 5 + [25_000] * 1).passed
    # A start exactly on the first boundary belongs to the second third.
    assert thirds([9_999] * 7 + [10_000] * 7 + [20_000] * 7).passed
    assert not thirds([9_999] * 14 + [10_000] * 7).passed


def test_a_point_mass_offset_is_a_spike_and_a_documented_range_is_not() -> None:
    spike = [Pair(ATO, f"i{k}", 120_000) for k in range(40)]
    legit = [Pair(d.LEGIT, account(k), k * 1_000) for k in range(400)]
    result = s4_offsets({"K2": [*spike, *legit]})
    assert {f.value for f in result.findings} >= {"120s"}
    ranged = [Pair(CT, f"i{k}", 8_000 + (k * 617) % 37_000) for k in range(60)]
    assert s4_offsets({"K1": [*ranged, *legit]}).passed


def test_a_legitimate_spike_at_the_same_offset_forgives_a_planted_one() -> None:
    spike = [Pair(ATO, f"i{k}", 60_000) for k in range(40)]
    legit = [Pair(d.LEGIT, account(k), 60_000 + (k % 3) * 400) for k in range(400)]
    assert s4_offsets({"K1": [*spike, *legit]}).passed


def test_a_documented_effect_must_stand_out_even_when_legitimate_rows_share_the_value() -> None:
    common = Build(planted=((IT, 40),))
    common.column(
        "channel", ["CARD_PRESENT"] * 1_900 + ["CARD_NOT_PRESENT"] * 100, ["CARD_PRESENT"] * 40
    )
    result = s7_effects({TX: common.table()}, ALLOW)
    assert [f.check for f in result.findings] == ["S7b/IT-1"]
    usual = Build(planted=((IT, 40),))
    usual.column(
        "channel", ["CARD_PRESENT"] * 920 + ["CARD_NOT_PRESENT"] * 1_080, ["CARD_PRESENT"] * 40
    )
    assert s7_effects({TX: usual.table()}, ALLOW).passed


def test_non_vacuity_needs_legitimate_and_planted_clusters() -> None:
    small = Build(legit_rows=10, legit_accounts=10, planted=((ATO, 5),))
    result = r9({TX: small.table()})
    assert {f.detail.split(" ")[0] for f in result.findings} == {"legitimate", "planted"}
    assert r9({TX: Build().table()}).passed
