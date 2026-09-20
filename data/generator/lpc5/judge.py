"""`LPC-5`'s statistical checks: §5.4 R7-R9, §7 S1, §8 S2a-S2b, §9 S3, §10 S4 and §13 S7b.

Each check is a pure function returning a `CheckResult`: how many cells it judged and every failing
cell. A check with no findings passes. Non-vacuity — whether a check judged anything — is §14.5's
concern and is reported alongside.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from data.generator.lpc5 import declaration as d
from data.generator.lpc5 import stats
from data.generator.lpc5.attributes import Table


@dataclass(frozen=True, slots=True)
class Finding:
    check: str
    population: d.Population | None = None
    attribute: str | None = None
    value: str | None = None
    group: str | None = None
    stratum: str | None = None
    detail: str = ""


@dataclass(frozen=True, slots=True)
class CheckResult:
    check: str
    judged: int
    findings: tuple[Finding, ...] = ()
    unjudged: tuple[str, ...] = ()
    """What §14.5 requires to have been judged and was not: allowlist rows, pair kinds, groups."""

    @property
    def passed(self) -> bool:
        return not self.findings


# ---------------------------------------------------------------------------- tallies ---------
@dataclass(slots=True)
class Tally:
    """Row and cluster counts for one attribute, by (group, stratum, value)."""

    rows: Counter[tuple[str, str, str]] = field(default_factory=Counter)
    clusters: defaultdict[tuple[str, str, str], set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )
    stratum_rows: Counter[tuple[str, str]] = field(default_factory=Counter)
    stratum_clusters: defaultdict[tuple[str, str], set[str]] = field(
        default_factory=lambda: defaultdict(set)
    )

    def share(self, group: str, stratum: str, values: Iterable[str]) -> stats.Share:
        x = sum(self.rows[(group, stratum, value)] for value in values)
        return stats.Share(
            x,
            self.stratum_rows[(group, stratum)],
            len(self.stratum_clusters[(group, stratum)]),
        )

    def value_rows(self, group: str, stratum: str, value: str) -> int:
        return self.rows[(group, stratum, value)]

    def value_clusters(self, group: str, stratum: str, value: str) -> int:
        return len(self.clusters.get((group, stratum, value), ()))

    def cells(self) -> list[tuple[str, str, str]]:
        return list(self.rows)

    def values_in(self, stratum: str) -> set[str]:
        return {value for (_, s, value) in self.rows if s == stratum}

    def strata_of(self, group: str) -> set[str]:
        return {s for (g, s) in self.stratum_rows if g == group}


def tally(
    table: Table,
    name: str,
    rows: Iterable[int] | None = None,
    *,
    pooled: bool = True,
) -> Tally:
    """Count one attribute over a table, optionally over a subset of its rows.

    Rows whose attribute (or stratum) is not applicable take no part. `pooled` also counts every
    planted row under `POOLED`."""
    spec = d.attribute(table.population, name)
    column = table.column(name)
    codes = column.codes
    vocab = column.vocab
    stratum_column = table.column(spec.stratum) if spec.stratum is not None else None
    result = Tally()
    indices = range(table.size) if rows is None else rows
    for i in indices:
        code = codes[i]
        if code == 0:
            continue
        value = vocab[code]
        assert value is not None
        if stratum_column is None:
            stratum = ""
        else:
            stratum_value = stratum_column.get(i)
            if stratum_value is None:
                continue
            stratum = stratum_value
        group = table.groups[i]
        cluster = table.clusters[i]
        groups = (group, d.POOLED) if pooled and group != d.LEGIT else (group,)
        for g in groups:
            result.rows[(g, stratum, value)] += 1
            result.clusters[(g, stratum, value)].add(cluster)
            result.stratum_rows[(g, stratum)] += 1
            result.stratum_clusters[(g, stratum)].add(cluster)
    return result


# ---------------------------------------------------------------------------- allowlist -------
class Allowlist:
    """§6.3 statuses, read from the declaration."""

    def __init__(
        self,
        rows: Sequence[d.AllowRow] = d.ALLOWLIST,
        episodes: Sequence[d.EpisodeConsequence] = d.EPISODE_CONSEQUENCES,
        documented: Sequence[d.DocumentedConsequence] = d.DOCUMENTED_CONSEQUENCES,
    ) -> None:
        self.rows = tuple(rows)
        self._by: dict[tuple[str, d.Population], list[d.AllowRow]] = defaultdict(list)
        for row in self.rows:
            for population in row.populations:
                self._by[(row.scenario.value, population)].append(row)
        self._episode: dict[tuple[str, d.Population], set[str]] = defaultdict(set)
        for episode in episodes:
            for population in episode.populations:
                self._episode[(episode.scenario.value, population)] |= episode.attributes
        self._documented: dict[tuple[str, d.Population, str], set[str]] = defaultdict(set)
        self._documented_exempt: dict[tuple[str, d.Population, str, str], str] = {}
        self._within: dict[tuple[str, d.Population, str], set[str]] = defaultdict(set)
        for entry in documented:
            for name in entry.attributes:
                if entry.within:
                    for row_id in entry.within:
                        self._within[(row_id, entry.population, name)] |= entry.values
                    continue
                key = (entry.scenario.value, entry.population, name)
                self._documented[key] |= entry.values
                for value in entry.none:
                    self._documented_exempt[(*key, value)] = "none"
                for value in entry.rare:
                    self._documented_exempt[(*key, value)] = "rare"

    def rows_of(self, scenario: str, population: d.Population) -> list[d.AllowRow]:
        return self._by.get((scenario, population), [])

    def named(self, scenario: str, population: d.Population) -> frozenset[str]:
        return frozenset(row.attribute for row in self.rows_of(scenario, population))

    def allowlisted(self, scenario: str, population: d.Population, name: str, value: str) -> bool:
        return any(
            row.attribute == name and value in row.values
            for row in self.rows_of(scenario, population)
        )

    def exemption(
        self, scenario: str, population: d.Population, name: str, value: str
    ) -> str | None:
        for row in self.rows_of(scenario, population):
            if row.attribute == name:
                if value in row.none:
                    return "none"
                if value in row.rare:
                    return "rare"
            elif name in row.consequences_in(population):
                if value in row.consequence_none.get(name, frozenset()):
                    return "none"
                if value in row.consequence_rare.get(name, frozenset()):
                    return "rare"
        return self._documented_exempt.get((scenario, population, name, value))

    def consequence_of(
        self, scenario: str, population: d.Population, name: str
    ) -> d.AllowRow | None:
        rows = self.rows_of(scenario, population)
        for row in rows:
            if name in row.consequences_in(population):
                return row
        if name.startswith(d.AVAIL_PREFIX):
            dependency = d.DEPENDS[name.removeprefix(d.AVAIL_PREFIX)]
            for row in rows:
                if row.attribute == dependency or dependency in row.consequences_in(population):
                    return row
        return None

    def episode_consequence(self, scenario: str, population: d.Population, name: str) -> bool:
        """§6.6: an attribute an episode-consequence entry of the scenario lists, or via §4.7."""
        names = self._episode.get((scenario, population), set())
        if name in names:
            return True
        if name.startswith(d.AVAIL_PREFIX):
            return d.DEPENDS[name.removeprefix(d.AVAIL_PREFIX)] in names
        return False

    def documented(self, scenario: str, population: d.Population, name: str, value: str) -> bool:
        """§6.7: a value a scenario-wide documented-consequence entry of the scenario lists."""
        return value in self._documented.get((scenario, population, name), frozenset())

    def documented_within(
        self, row_id: str, population: d.Population, name: str, value: str
    ) -> bool:
        """§6.7: a value a documented-consequence entry lists inside the stratum of `row_id`."""
        return value in self._within.get((row_id, population, name), frozenset())

    def is_ordinary(self, scenario: str, population: d.Population, name: str, value: str) -> bool:
        return (
            not self.allowlisted(scenario, population, name, value)
            and self.consequence_of(scenario, population, name) is None
            and not self.episode_consequence(scenario, population, name)
            and not self.documented(scenario, population, name, value)
        )

    def pooled_exempt(
        self, population: d.Population, name: str, stratum: str, value: str, counts: Tally
    ) -> bool:
        """R8's exemption (revision 4): every materially contributing scenario admits `value`.

        A scenario contributes materially only when it is itself `ENRICHED` for the value within the
        stratum, by §3's test. With no such scenario, or one that does not admit the value, the cell
        is judged. Scenarios at trivial, non-enriched shares neither block nor grant the exemption;
        R7 still judges each scenario on its own."""
        legit = counts.share(d.LEGIT, stratum, (value,))
        contributors = [
            group
            for group, cell_stratum, cell_value in counts.cells()
            if group not in (d.LEGIT, d.POOLED)
            and cell_stratum == stratum
            and cell_value == value
            and stats.enriched(counts.share(group, stratum, (value,)), legit)
        ]
        return bool(contributors) and all(
            not self.is_ordinary(scenario, population, name, value) for scenario in contributors
        )


def _scenarios(table: Table) -> list[str]:
    return sorted({group for group in table.groups if group != d.LEGIT})


def _specs(table: Table, klasses: set[d.Klass]) -> list[d.AttributeSpec]:
    return [
        spec
        for spec in d.ATTRIBUTES[table.population]
        if spec.klass in klasses and spec.name in table.columns
    ]


# ---------------------------------------------------------------------------- R7, R8, R9 ------
def r7_r8(
    tables: Mapping[d.Population, Table], allow: Allowlist
) -> tuple[CheckResult, CheckResult]:
    r7: list[Finding] = []
    r8: list[Finding] = []
    judged7 = judged8 = 0
    for population, table in tables.items():
        for spec in _specs(
            table, {d.Klass.BEHAVIOUR, d.Klass.REPRESENTATION, d.Klass.AVAILABILITY}
        ):
            counts = tally(table, spec.name)
            for group, stratum, value in counts.cells():
                if group == d.LEGIT:
                    continue
                legit = counts.share(d.LEGIT, stratum, (value,))
                share = counts.share(group, stratum, (value,))
                if group == d.POOLED:
                    if allow.pooled_exempt(population, spec.name, stratum, value, counts):
                        continue
                    judged8 += 1
                    if stats.enriched(share, legit):
                        r8.append(
                            _finding("R8", population, spec, value, group, stratum, share, legit)
                        )
                    continue
                ordinary = spec.klass is d.Klass.REPRESENTATION or allow.is_ordinary(
                    group, population, spec.name, value
                )
                if not ordinary:
                    continue
                judged7 += 1
                if stats.enriched(share, legit):
                    r7.append(_finding("R7", population, spec, value, group, stratum, share, legit))
    return CheckResult("R7", judged7, tuple(r7)), CheckResult("R8", judged8, tuple(r8))


def _finding(
    check: str,
    population: d.Population,
    spec: d.AttributeSpec,
    value: str,
    group: str,
    stratum: str,
    share: stats.Share,
    legit: stats.Share,
    note: str = "",
) -> Finding:
    detail = (
        f"{group} {share.x}/{share.r} "
        f"(clusters {share.k}, lo {share.lo:.4f}, hi {share.hi:.4f}) vs "
        f"LEGIT {legit.x}/{legit.r} (clusters {legit.k}, lo {legit.lo:.4f}, hi {legit.hi:.4f})"
    )
    return Finding(
        check,
        population,
        spec.name,
        value,
        group,
        stratum or None,
        f"{detail}{'; ' + note if note else ''}",
    )


def r9(tables: Mapping[d.Population, Table]) -> CheckResult:
    findings: list[Finding] = []
    for population, table in tables.items():
        legit_rows = sum(1 for g in table.groups if g == d.LEGIT)
        legit_clusters = len(
            {c for g, c in zip(table.groups, table.clusters, strict=True) if g == d.LEGIT}
        )
        if legit_rows < d.R9_MIN_LEGIT_ROWS or legit_clusters < d.R9_MIN_LEGIT_CLUSTERS:
            findings.append(
                Finding(
                    "R9",
                    population,
                    detail=f"legitimate rows {legit_rows}, clusters {legit_clusters}",
                )
            )
    tx = tables.get(d.Population.TX)
    planted = (
        0
        if tx is None
        else len({c for g, c in zip(tx.groups, tx.clusters, strict=True) if g != d.LEGIT})
    )
    if planted < d.R9_MIN_PLANTED_TX_CLUSTERS:
        findings.append(Finding("R9", d.Population.TX, detail=f"planted TX clusters {planted}"))
    return CheckResult("R9", len(tables) + 1, tuple(findings))


# ---------------------------------------------------------------------------- S1-U -----------
def s1_unconditional(tables: Mapping[d.Population, Table], allow: Allowlist) -> CheckResult:
    findings: list[Finding] = []
    judged = 0
    for population, table in tables.items():
        for spec in _specs(table, {d.Klass.BEHAVIOUR, d.Klass.AVAILABILITY}):
            counts = tally(table, spec.name)
            for group, stratum, value in counts.cells():
                if group == d.LEGIT or (
                    group == d.POOLED
                    and allow.pooled_exempt(population, spec.name, stratum, value, counts)
                ):
                    continue
                share = counts.share(group, stratum, (value,))
                if share.lo <= d.S1_TRIGGER:
                    continue
                judged += 1
                legit = counts.share(d.LEGIT, stratum, (value,))
                exemption = (
                    None
                    if group == d.POOLED
                    else allow.exemption(group, population, spec.name, value)
                )
                if exemption != "none":
                    supported = stats.supported(
                        counts.value_rows(d.LEGIT, stratum, value),
                        counts.value_clusters(d.LEGIT, stratum, value),
                        legit.r,
                        share_condition=exemption != "rare",
                    )
                    if not supported:
                        findings.append(
                            _finding(
                                "S1-U(a)",
                                population,
                                spec,
                                value,
                                group,
                                stratum,
                                share,
                                legit,
                                f"exemption {exemption}" if exemption else "",
                            )
                        )
                ordinary = group == d.POOLED or allow.is_ordinary(
                    group, population, spec.name, value
                )
                if ordinary:
                    bound = stats.precision_hi(
                        share.x,
                        legit.x,
                        counts.value_clusters(group, stratum, value)
                        + counts.value_clusters(d.LEGIT, stratum, value),
                    )
                    if bound > d.PRECISION_BOUND:
                        findings.append(
                            _finding(
                                "S1-U(b)",
                                population,
                                spec,
                                value,
                                group,
                                stratum,
                                share,
                                legit,
                                f"precision upper bound {bound:.4f}",
                            )
                        )
    return CheckResult("S1-U", judged, tuple(findings))


# ---------------------------------------------------------------------------- S1-B -----------
def s1_conditional(tables: Mapping[d.Population, Table], allow: Allowlist) -> CheckResult:
    """Within each allowlisted stratum, only the documented behaviour may differ (§7 S1-B)."""
    findings: list[Finding] = []
    judged = 0
    row_judged: Counter[str] = Counter()
    for row in allow.rows:
        scenario = row.scenario.value
        for population in row.populations:
            table = tables.get(population)
            if table is None or row.attribute not in table.columns:
                continue
            column = table.column(row.attribute)
            sigma = [
                i
                for i in range(table.size)
                if column.get(i) in row.values and table.groups[i] in (scenario, d.LEGIT)
            ]
            scenario_rows = [i for i in sigma if table.groups[i] == scenario]
            legit_sigma = [i for i in sigma if table.groups[i] == d.LEGIT]
            legit_accounts = len({table.accounts[i] for i in legit_sigma})
            stratum_supported = (
                len(legit_sigma) >= d.STRATUM_MIN_ROWS and legit_accounts >= d.STRATUM_MIN_ACCOUNTS
            )
            reference = (
                legit_sigma
                if stratum_supported
                else [i for i in range(table.size) if table.groups[i] == d.LEGIT]
            )
            if not scenario_rows:
                continue
            named = allow.named(scenario, population)
            for spec in _specs(table, {d.Klass.BEHAVIOUR, d.Klass.AVAILABILITY}):
                if spec.name in named or allow.episode_consequence(scenario, population, spec.name):
                    continue
                owner = allow.consequence_of(scenario, population, spec.name)
                if owner is not None and owner is not row:
                    continue
                counts = tally(table, spec.name, [*scenario_rows, *reference], pooled=False)
                for group, stratum, value in counts.cells():
                    if (
                        group != scenario
                        or allow.documented(scenario, population, spec.name, value)
                        or allow.documented_within(row.row_id, population, spec.name, value)
                    ):
                        continue
                    judged += 1
                    row_judged[row.row_id] += 1
                    share = counts.share(scenario, stratum, (value,))
                    legit = counts.share(d.LEGIT, stratum, (value,))
                    if stats.enriched(share, legit):
                        findings.append(
                            _finding(
                                "S1-B(i)",
                                population,
                                spec,
                                value,
                                scenario,
                                stratum,
                                share,
                                legit,
                                f"within {row.row_id} ({row.attribute} in {sorted(row.values)}); "
                                f"reference "
                                f"{'stratum' if stratum_supported else 'all legitimate rows'}",
                            )
                        )
            if row.composition is d.Composition.JUDGED and len(row.values) >= 2:
                judged += 1
                row_judged[row.row_id] += 1
                if not stratum_supported:
                    findings.append(
                        Finding(
                            "S1-B(iii)",
                            population,
                            row.attribute,
                            None,
                            scenario,
                            None,
                            f"{row.row_id}: legitimate rows in the stratum {len(legit_sigma)} from "
                            f"{legit_accounts} accounts; composition cannot be judged",
                        )
                    )
                    continue
                spec = d.attribute(population, row.attribute)
                counts = tally(table, row.attribute, [*scenario_rows, *legit_sigma], pooled=False)
                for stratum in counts.strata_of(scenario):
                    for value in sorted(row.values & counts.values_in(stratum)):
                        share = counts.share(scenario, stratum, (value,))
                        legit = counts.share(d.LEGIT, stratum, (value,))
                        if stats.enriched(share, legit):
                            findings.append(
                                _finding(
                                    "S1-B(iii)",
                                    population,
                                    spec,
                                    value,
                                    scenario,
                                    stratum,
                                    share,
                                    legit,
                                    f"composition within {row.row_id}",
                                )
                            )
    unjudged = tuple(row.row_id for row in allow.rows if row_judged[row.row_id] == 0)
    return CheckResult("S1-B", judged, tuple(findings), unjudged)


# ---------------------------------------------------------------------------- S2a, S2b --------
def s2_representation(tables: Mapping[d.Population, Table]) -> CheckResult:
    findings: list[Finding] = []
    judged = 0
    for population, table in tables.items():
        for spec in _specs(table, {d.Klass.REPRESENTATION}):
            counts = tally(table, spec.name)
            groups = {g for (g, _) in counts.stratum_rows if g != d.LEGIT}
            for group in sorted(groups):
                for stratum in counts.strata_of(group):
                    legit_values = {v for (g, s, v) in counts.rows if g == d.LEGIT and s == stratum}
                    for value in sorted(counts.values_in(stratum)):
                        judged += 1
                        share = counts.share(group, stratum, (value,))
                        legit = counts.share(d.LEGIT, stratum, (value,))
                        if share.x >= 1 and value not in legit_values:
                            findings.append(
                                _finding(
                                    "S2a", population, spec, value, group, stratum, share, legit
                                )
                            )
                        elif stats.differs(share, legit, d.S2_TOLERANCE):
                            findings.append(
                                _finding(
                                    "S2b", population, spec, value, group, stratum, share, legit
                                )
                            )
    return CheckResult("S2", judged, tuple(findings))


# ---------------------------------------------------------------------------- S3 -------------
def s3_calendar(
    starts: Mapping[str, Sequence[int]],
    legit_times: Sequence[int],
    start_ms: int,
    end_ms: int,
) -> tuple[CheckResult, CheckResult]:
    """`starts`: per scenario, each instance's start (least TX time). Returns (pooled, thirds)."""
    span = end_ms - start_ms
    slices = d.S3_SLICES

    def slice_of(t: int, parts: int) -> int | None:
        if not start_ms <= t < end_ms:
            return None
        return min((t - start_ms) * parts // span, parts - 1)

    legit_counts = Counter(s for t in legit_times if (s := slice_of(t, slices)) is not None)
    legit_total = len(legit_times)
    pooled_starts = [t for series in starts.values() for t in series]
    pooled_counts = Counter(s for t in pooled_starts if (s := slice_of(t, slices)) is not None)
    n = len(pooled_starts)
    pooled_findings: list[Finding] = []
    pooled_judged = 0
    if legit_total and n:
        for j in range(slices):
            share = legit_counts[j] / legit_total
            if share < d.S3_MIN_LEGIT_SLICE_SHARE:
                continue
            pooled_judged += 1
            k = pooled_counts[j]
            lo, hi = stats.Share(k, n, n).bounds
            if hi < d.S3_LOW_FACTOR * share or lo > d.S3_HIGH_FACTOR * share:
                pooled_findings.append(
                    Finding(
                        "S3/pooled",
                        d.Population.TX,
                        group=d.POOLED,
                        value=f"slice {j + 1}",
                        detail=(
                            f"{k}/{n} starts (lo {lo:.4f}, hi {hi:.4f}); "
                            f"legitimate share {share:.4f}"
                        ),
                    )
                )
    thirds_findings: list[Finding] = []
    for scenario, series in sorted(starts.items()):
        total = len(series)
        parts = Counter(s for t in series if (s := slice_of(t, d.S3_SCENARIO_PARTS)) is not None)
        for part in range(d.S3_SCENARIO_PARTS):
            k = parts[part]
            if k == 0 or 3 * k > 2 * total:
                thirds_findings.append(
                    Finding(
                        "S3/thirds",
                        d.Population.TX,
                        group=scenario,
                        value=f"third {part + 1}",
                        detail=f"{k} of {total} instance starts",
                    )
                )
    return (
        CheckResult("S3/pooled", pooled_judged, tuple(pooled_findings)),
        CheckResult("S3/thirds", len(starts) * d.S3_SCENARIO_PARTS, tuple(thirds_findings)),
    )


# ---------------------------------------------------------------------------- S4 -------------
@dataclass(frozen=True, slots=True)
class Pair:
    group: str
    cluster: str
    offset_ms: int


@dataclass(slots=True)
class _Bins:
    counts: Counter[int] = field(default_factory=Counter)
    clusters: defaultdict[int, set[str]] = field(default_factory=lambda: defaultdict(set))
    total: int = 0
    all_clusters: set[str] = field(default_factory=set)

    def window(self, low: int, high: int) -> int:
        return sum(self.counts.get(b, 0) for b in range(low, high + 1))

    def spike(self, b: int) -> bool:
        half = d.S4_WINDOW_HALF_BINS
        members: set[str] = set()
        for bin_ in range(b - half, b + half + 1):
            members |= self.clusters.get(bin_, set())
        if len(members) < d.S4_MIN_CLUSTERS:
            return False
        k = len(self.all_clusters)
        lo3 = stats.Share(self.window(b - half, b + half), self.total, k).lo
        if lo3 <= d.S4_TRIGGER:
            return False
        near, far = d.S4_BACKGROUND_NEAR, d.S4_BACKGROUND_FAR
        background = self.window(b - far, b - near) + self.window(b + near, b + far)
        hi_c = stats.Share(background, self.total, k).hi
        width = 2 * half + 1
        return lo3 / width > d.S4_CONCENTRATION * hi_c / (2 * (far - near + 1))


def s4_offsets(pairs: Mapping[str, Sequence[Pair]]) -> CheckResult:
    """`pairs`: per pair kind, every planted and legitimate pair (§10)."""
    findings: list[Finding] = []
    judged = 0
    for kind, series in sorted(pairs.items()):
        bins: dict[str, _Bins] = defaultdict(_Bins)
        for pair in series:
            if pair.offset_ms < 0 or pair.offset_ms > d.S4_MAX_OFFSET_MS:
                continue
            b = pair.offset_ms // 1000
            entry = bins[pair.group]
            entry.counts[b] += 1
            entry.clusters[b].add(pair.cluster)
            entry.total += 1
            entry.all_clusters.add(pair.cluster)
        legit = bins.get(d.LEGIT)
        for group, entry in sorted(bins.items()):
            if group == d.LEGIT:
                continue
            candidates = {b + delta for b in entry.counts for delta in (-1, 0, 1) if b + delta >= 0}
            for b in sorted(candidates):
                judged += 1
                if not entry.spike(b):
                    continue
                if legit is not None and any(legit.spike(c) for c in (b - 1, b, b + 1)):
                    continue
                findings.append(
                    Finding(
                        f"S4/{kind}",
                        group=group,
                        value=f"{b}s",
                        detail=(
                            f"{entry.window(b - 1, b + 1)} of {entry.total} pairs "
                            f"in bins {b - 1}..{b + 1}"
                        ),
                    )
                )
    planted_kinds = {
        kind for kind, series in pairs.items() if any(p.group != d.LEGIT for p in series)
    }
    unjudged = tuple(k for k in ("K1", "K2", "K7") if k not in planted_kinds)
    return CheckResult("S4", judged, tuple(findings), unjudged)


# ---------------------------------------------------------------------------- S7b ------------
def s7_effects(tables: Mapping[d.Population, Table], allow: Allowlist) -> CheckResult:
    findings: list[Finding] = []
    judged = 0
    judged_rows: set[str] = set()
    for row in allow.rows:
        if row.e_min is None:
            continue  # an allowed effect with no minimum (revision 3)
        for population in row.populations:
            table = tables.get(population)
            if table is None or row.attribute not in table.columns:
                continue
            judged += 1
            judged_rows.add(row.row_id)
            counts = tally(table, row.attribute, pooled=False)
            scenario = row.scenario.value
            strata = counts.strata_of(scenario) | counts.strata_of(d.LEGIT) or {""}
            (stratum,) = strata if len(strata) == 1 else ("",)
            share = counts.share(scenario, stratum, row.values)
            legit = counts.share(d.LEGIT, stratum, row.values)
            threshold = stats.s7b_threshold(legit)
            if share.lo < row.e_min or share.lo < threshold:
                findings.append(
                    Finding(
                        f"S7b/{row.row_id}",
                        population,
                        row.attribute,
                        ",".join(sorted(row.values)),
                        scenario,
                        None,
                        f"lo {share.lo:.4f} ({share.x}/{share.r}, clusters {share.k}); need >= "
                        f"E_min {row.e_min} and >= {threshold:.4f} (legitimate hi {legit.hi:.4f})",
                    )
                )
    unjudged = tuple(
        row.row_id for row in allow.rows if row.e_min is not None and row.row_id not in judged_rows
    )
    return CheckResult("S7b", judged, tuple(findings), unjudged)
