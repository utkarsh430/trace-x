"""`LPC-5` §3: the interval and the named tests every check is written in.

One Wilson bound for the whole criterion family: the point estimate uses rows and the width uses
clusters. It is `label_proxy.wilson_interval`, so LPC-1 to LPC-5 cannot drift apart.
"""

from __future__ import annotations

from dataclasses import dataclass

from data.generator.label_proxy import wilson_interval
from data.generator.lpc5 import declaration as d


@dataclass(frozen=True, slots=True)
class Share:
    """`x` of `r` rows have the value; the rows fall in `k` distinct clusters."""

    x: int
    r: int
    k: int

    @property
    def point(self) -> float:
        return self.x / self.r if self.r else 0.0

    @property
    def bounds(self) -> tuple[float, float]:
        return wilson_interval(self.x, self.r, self.k, d.Z)

    @property
    def lo(self) -> float:
        return self.bounds[0]

    @property
    def hi(self) -> float:
        return self.bounds[1]


def legit_hi_floored(legit: Share) -> float:
    """`hiF`: the legitimate upper bound, never read below the share floor."""
    return max(legit.hi, d.LEGIT_SHARE_FLOOR)


def enriched(group: Share, legit: Share) -> bool:
    """`ENRICHED(g)`: evidence that the group holds the value more than twice as often."""
    lo = group.lo
    hi_f = legit_hi_floored(legit)
    return lo > d.ENRICHMENT_BOUND * hi_f and lo - hi_f > d.MIN_EXCESS


def differs(group: Share, legit: Share, delta: float) -> bool:
    """`DIFFERS(g, delta)`: evidence of a difference either way, beyond `delta`."""
    return group.lo > legit.hi + delta or group.hi < legit.lo - delta


def supported(
    legit_rows_with_value: int,
    legit_accounts_with_value: int,
    legit_rows: int,
    *,
    share_condition: bool = True,
) -> bool:
    """`SUPPORTED(V)`: enough legitimate rows, from enough accounts, and a large enough share.

    `share_condition=False` is a `rare` exemption (§6.3): it drops only the share condition."""
    if legit_rows_with_value < d.SUPPORT_MIN_ROWS:
        return False
    if legit_accounts_with_value < d.SUPPORT_MIN_ACCOUNTS:
        return False
    if not share_condition:
        return True
    return legit_rows > 0 and legit_rows_with_value / legit_rows >= d.SUPPORT_MIN_SHARE


def precision_hi(group_rows: int, legit_rows: int, clusters: int) -> float:
    """`PRECISION_HI`: the upper bound on the group's share of the rows that have the value."""
    return wilson_interval(group_rows, group_rows + legit_rows, clusters, d.Z)[1]


def s7b_threshold(legit: Share) -> float:
    """The documented effect's lower bound must reach `min(2 * hiF, hiF + 0.25)` (§13 S7b)."""
    hi_f = legit_hi_floored(legit)
    return min(d.S7_LIFT * hi_f, hi_f + d.S7_MARGIN)
