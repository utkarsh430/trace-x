"""`LPC-5` §16.3: each statistical check, passing and failing, on hand-built tables and inputs."""

from __future__ import annotations

from collections.abc import Sequence

from data.generator.lpc5 import declaration as d
from data.generator.lpc5 import stats
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
VA = FraudPattern.VELOCITY_ATTACK.value
ULD = FraudPattern.UNUSUAL_LOCATION_DEVICE.value
FR = FraudPattern.FRAUD_RING.value
DF = FraudPattern.DEVICE_FARM.value
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
    build.column("weekday", ["Mon"] * 2_000, ["Tue"] * 40)
    tables = {TX: build.table()}
    r7, r8 = r7_r8(tables, ALLOW)
    s1u = s1_unconditional(tables, ALLOW)
    assert ("weekday", "Tue", ATO) in _cells(r7, "R7")
    assert ("weekday", "Tue", d.POOLED) in _cells(r8, "R8")
    assert ("weekday", "Tue", ATO) in _cells(s1u, "S1-U(a)")
    assert ("weekday", "Tue", d.POOLED) in _cells(s1u, "S1-U(a)")
    assert ("weekday", "Tue", ATO) in _cells(s1u, "S1-U(b)")


def test_an_attribute_distributed_like_legitimate_rows_passes() -> None:
    build = Build()
    build.column("weekday", ["Mon", "Tue"] * 1_000, ["Mon", "Tue"] * 20)
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
    def judged(legit_with_value: int, value: str, name: str = "tx_count_1m") -> CheckResult:
        build = Build(legit_rows=40_000, legit_accounts=4_000, planted=((CT, 40),))
        legit = ["1"] * 40_000
        for k in range(legit_with_value):
            legit[k * 10] = value  # one row per account: accounts 0..k-1
        build.column(name, legit, [value] * 40)
        return s1_unconditional({TX: build.table()}, ALLOW)

    # "3-4" is a `rare` value of CT-2: 30 rows from 30 accounts suffice below the 0.1 % share.
    assert ("tx_count_5m", "3-4", CT) not in _cells(judged(30, "3-4", "tx_count_5m"), "S1-U(a)")
    assert ("tx_count_5m", "3-4", CT) in _cells(judged(29, "3-4", "tx_count_5m"), "S1-U(a)")
    # "3-4" and "5-9" are `none` values of CT-1 (revision 3): no legitimate row is needed.
    assert ("tx_count_1m", "3-4", CT) not in _cells(judged(0, "3-4"), "S1-U(a)")
    assert ("tx_count_1m", "5-9", CT) not in _cells(judged(0, "5-9"), "S1-U(a)")
    # "2" is allowlisted without exemption: the share condition applies.
    assert ("tx_count_1m", "2", CT) in _cells(judged(30, "2"), "S1-U(a)")


def test_the_trigger_leaves_a_small_planted_share_unjudged() -> None:
    build = Build()
    build.column("weekday", ["Mon"] * 2_000, ["Wed"] + ["Mon"] * 39)
    result = s1_unconditional({TX: build.table()}, ALLOW)
    assert ("weekday", "Wed", ATO) not in _cells(result, "S1-U(a)")


def test_s1b_judges_ordinary_attributes_and_consequences_inside_the_stratum() -> None:
    build = Build()
    legit_home = ["not"] * 100 + ["home"] * 1_900
    build.column("device_home", legit_home, ["not"] * 40)
    build.column("weekday", ["Mon"] * 2_000, ["Tue"] * 40)
    build.column("device_accounts", ["2"] * 100 + ["1"] * 1_900, ["2"] * 40)
    tables = {TX: build.table()}
    result = s1_conditional(tables, ALLOW)
    assert ("weekday", "Tue", ATO) in _cells(result, "S1-B(i)")
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


Cell = tuple[str | None, str | None, str | None]
NOT_POOLED = ("device_home", "not", d.POOLED)


def _device_home_not(
    planted: Sequence[tuple[str, int, int]], legit_not: int = 10
) -> tuple[set[Cell], set[Cell]]:
    """`device_home = not` on `hits` of each scenario's `rows`: R7's cells, and every pooled cell R8
    or S1-U reports. Takeovers (ATO-4), unusual-location transactions (ULD-1) and rings (FR-1's
    consequence) admit the value; card testing does not."""
    build = Build(planted=tuple((scenario, rows) for scenario, rows, _ in planted))
    values: list[str | None] = []
    for _, rows, hits in planted:
        values += ["not"] * hits + ["home"] * (rows - hits)
    build.column("device_home", ["not"] * legit_not + ["home"] * (2_000 - legit_not), values)
    tables = {TX: build.table()}
    r7, r8 = r7_r8(tables, ALLOW)
    s1u = s1_unconditional(tables, ALLOW)
    pooled = _cells(r8, "R8") | {(f.attribute, f.value, f.group) for f in s1u.findings}
    return _cells(r7, "R7"), {cell for cell in pooled if cell[2] == d.POOLED}


def test_r8_trivial_non_enriched_support_does_not_block_an_enriched_contributor() -> None:
    """Revision 4: card testing's single row exceeds the legitimate upper bound (revision 3 counted
    that) but is not itself ENRICHED, so only the takeovers contribute, and they admit the value."""
    legit = stats.Share(10, 2_000, 400)
    assert legit.hi < 1 / 40
    assert not stats.enriched(stats.Share(1, 40, 40), legit)
    r7, pooled = _device_home_not([(ATO, 40, 40), (CT, 40, 1)])
    assert NOT_POOLED not in pooled
    assert ("device_home", "not", CT) not in r7


def test_r8_two_enriched_allowlisted_contributors_are_exempt() -> None:
    r7, pooled = _device_home_not([(ATO, 40, 40), (ULD, 40, 40)])
    assert NOT_POOLED not in pooled
    assert not r7


def test_r8_an_enriched_contributor_that_is_not_allowlisted_fails() -> None:
    r7, pooled = _device_home_not([(CT, 40, 40)])
    assert NOT_POOLED in pooled
    assert ("device_home", "not", CT) in r7


def test_r8_mixed_allowlisted_and_non_allowlisted_enriched_contributors_fail() -> None:
    r7, pooled = _device_home_not([(ATO, 40, 40), (CT, 40, 40)])
    assert NOT_POOLED in pooled
    assert r7 == {("device_home", "not", CT)}


def test_r8_a_pooled_enrichment_with_no_individually_enriched_contributor_is_judged() -> None:
    """Every scenario admits the value, but none is ENRICHED on its own: nothing contributes
    materially, so the pooled enrichment is judged rather than exempt."""
    legit = stats.Share(0, 2_000, 400)
    assert not stats.enriched(stats.Share(5, 100, 100), legit)
    assert stats.enriched(stats.Share(15, 300, 300), legit)
    r7, pooled = _device_home_not([(ATO, 100, 5), (ULD, 100, 5), (FR, 100, 5)], legit_not=0)
    assert NOT_POOLED in pooled
    assert not r7


def test_a_documented_consequence_value_is_admitted_and_keeps_its_support_check() -> None:
    """Revision 4 §6.7: VA-C1 lists `distinct_merchants_1h` `3-4`, `5-9` and `10+` for velocity
    attacks, `5-9` with no support check. Listed values skip R7 and precision and admit R8; `3-4`
    keeps its support check. An unlisted value, and the same value for another scenario, stay
    judged."""
    build = Build(planted=((VA, 40), (IT, 40)))
    build.column(
        "distinct_merchants_1h",
        ["1"] * 2_000,
        ["3-4"] * 15 + ["5-9"] * 15 + ["2"] * 10 + ["3-4"] * 20 + ["1"] * 20,
    )
    tables = {TX: build.table()}
    r7, r8 = r7_r8(tables, ALLOW)
    s1u = s1_unconditional(tables, ALLOW)
    name = "distinct_merchants_1h"
    assert (name, "3-4", VA) not in _cells(r7, "R7")
    assert (name, "5-9", VA) not in _cells(r7, "R7")
    assert (name, "2", VA) in _cells(r7, "R7")
    assert (name, "3-4", IT) in _cells(r7, "R7")
    assert (name, "3-4", VA) in _cells(s1u, "S1-U(a)")
    assert (name, "5-9", VA) not in _cells(s1u, "S1-U(a)")
    assert (name, "3-4", VA) not in _cells(s1u, "S1-U(b)")
    assert (name, "2", VA) in _cells(s1u, "S1-U(b)")
    assert (name, "5-9", d.POOLED) not in _cells(r8, "R8")
    assert (name, "3-4", d.POOLED) in _cells(r8, "R8")


def test_a_documented_value_within_a_row_skips_only_that_rows_sweep() -> None:
    """Revision 4 §6.7: DF-C1 lists `avail:account_tenure_days = AVAILABLE` inside DF-5's stratum
    (a first-use device) only. The same value inside DF-1's stratum stays judged."""
    build = Build(planted=((DF, 40),))
    build.column("device_age", ["first-use"] * 1_000 + ["≥7d"] * 1_000, ["first-use"] * 40)
    build.column("device_accounts", ["3-5"] * 1_000 + ["1"] * 1_000, ["3-5"] * 40)
    build.column(
        "avail:account_tenure_days",
        ["INSUFFICIENT_HISTORY"] * 1_000 + ["AVAILABLE"] * 1_000,
        ["AVAILABLE"] * 40,
    )
    s1b = s1_conditional({TX: build.table()}, ALLOW)
    strata = {
        row_id
        for f in s1b.findings
        if f.check == "S1-B(i)" and f.attribute == "avail:account_tenure_days"
        for row_id in ("DF-1", "DF-5")
        if f"within {row_id} (" in f.detail
    }
    assert strata == {"DF-1"}


def test_an_episode_consequence_skips_enrichment_but_keeps_its_support_check() -> None:
    """Revision 3 §6.6: a takeover's hourly count is its documented episode, judged for support
    only; the same shape stays judged for a scenario whose episode it is not."""
    build = Build(planted=((ATO, 40), (IT, 40)))
    build.column("tx_count_1h", ["1"] * 1_960 + ["3-4"] * 40, ["3-4"] * 80)
    build.column("device_home", ["not"] * 100 + ["home"] * 1_900, ["not"] * 40 + ["home"] * 40)
    tables = {TX: build.table()}
    r7, _ = r7_r8(tables, ALLOW)
    s1u = s1_unconditional(tables, ALLOW)
    s1b = s1_conditional(tables, ALLOW)
    assert ("tx_count_1h", "3-4", ATO) not in _cells(r7, "R7")
    assert ("tx_count_1h", "3-4", ATO) not in _cells(s1u, "S1-U(b)")
    assert ("tx_count_1h", "3-4", ATO) not in _cells(s1b, "S1-B(i)")
    assert ("tx_count_1h", "3-4", IT) in _cells(r7, "R7")
    unsupported = Build()
    unsupported.column("tx_count_1h", ["1"] * 2_000, ["3-4"] * 40)
    result = s1_unconditional({TX: unsupported.table()}, ALLOW)
    assert ("tx_count_1h", "3-4", ATO) in _cells(result, "S1-U(a)")
    hours = Build(planted=((ATO, 40), (CT, 40)))
    hours.column("hour", ["3"] * 2_000, ["4"] * 80)
    r7_hours, _ = r7_r8({TX: hours.table()}, ALLOW)
    assert ("hour", "4", ATO) not in _cells(r7_hours, "R7")  # incidental for takeovers
    assert ("hour", "4", CT) in _cells(r7_hours, "R7")


def test_a_consequence_value_can_carry_its_own_support_exemption() -> None:
    """Revision 3: `tx_count_24h = 10-19` is `rare` for CT-3's consequence; `5-9` is not exempt."""

    def support(value: str, legit_rows: int) -> CheckResult:
        build = Build(legit_rows=40_000, legit_accounts=4_000, planted=((CT, 40),))
        legit = ["1"] * 40_000
        for k in range(legit_rows):
            legit[k * 10] = value
        build.column("tx_count_24h", legit, [value] * 40)
        return s1_unconditional({TX: build.table()}, ALLOW)

    assert ("tx_count_24h", "10-19", CT) not in _cells(support("10-19", 30), "S1-U(a)")
    assert ("tx_count_24h", "10-19", CT) in _cells(support("10-19", 29), "S1-U(a)")
    assert ("tx_count_24h", "5-9", CT) in _cells(support("5-9", 30), "S1-U(a)")


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


def test_an_allowed_effect_without_a_minimum_is_not_judged_by_s7b() -> None:
    """Revision 3: MC-5 keeps its allowlisted status and has no E_min, so S7b neither judges it
    nor reports it unjudged."""
    collusion = FraudPattern.MERCHANT_COLLUSION.value
    build = Build(planted=((collusion, 40),))
    build.column(
        "mcc_habitual",
        ["unhabitual"] * 200 + ["habitual"] * 1_800,
        ["unhabitual"] * 5 + ["habitual"] * 35,
    )
    result = s7_effects({TX: build.table()}, ALLOW)
    assert "S7b/MC-5" not in {f.check for f in result.findings}
    assert "MC-5" not in result.unjudged and "DF-5" not in result.unjudged


def test_non_vacuity_needs_legitimate_and_planted_clusters() -> None:
    small = Build(legit_rows=10, legit_accounts=10, planted=((ATO, 5),))
    result = r9({TX: small.table()})
    assert {f.detail.split(" ")[0] for f in result.findings} == {"legitimate", "planted"}
    assert r9({TX: Build().table()}).passed
