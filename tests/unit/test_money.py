"""Money is integer minor units, never a float (CLAUDE.md §6).

Float money is the classic silent defect: every individual operation looks
right, and the error only appears as an unexplainable reconciliation gap after
enough rows. These tests exist so the type refuses the mistake at construction
rather than accumulating it.
"""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from trace_core.domain.errors import CurrencyMismatchError, InvalidMoneyError
from trace_core.domain.money import Money, total, zero

pytestmark = pytest.mark.unit

AMOUNTS = st.integers(min_value=-(10**12), max_value=10**12)
CURRENCIES = st.sampled_from(["USD", "EUR", "GBP", "JPY"])


# ------------------------------------------------------------ construction --


@pytest.mark.parametrize(
    "bad",
    [19.99, 0.1, -5.0, True, False, "500", None, complex(1, 0)],
    ids=["float", "small-float", "neg-float", "true", "false", "str", "none", "complex"],
)
def test_non_integer_amounts_are_refused(bad: object) -> None:
    with pytest.raises(InvalidMoneyError):
        Money(bad, "USD")  # type: ignore[arg-type]


def test_bool_is_refused_even_though_it_is_an_int_subclass() -> None:
    """`isinstance(True, int)` is True, so a naive guard lets Money(True) mean 1 cent.

    Note there is no `type: ignore` on the call below, and that is the point:
    `bool` IS a subtype of `int`, so mypy accepts `Money(True, "USD")` happily.
    Static typing cannot catch this one, which is why the runtime guard in
    `__post_init__` has to.
    """
    with pytest.raises(InvalidMoneyError):
        Money(True, "USD")


@pytest.mark.parametrize("bad", ["usd", "US", "USDD", "", "12A", "US$"])
def test_currency_must_be_iso_4217_alphabetic(bad: str) -> None:
    with pytest.raises(InvalidMoneyError):
        Money(100, bad)


def test_money_is_immutable() -> None:
    m = Money(100, "USD")
    with pytest.raises(AttributeError):
        m.amount_minor = 200  # type: ignore[misc]


# -------------------------------------------------------------- currency ----


@pytest.mark.parametrize("op", ["add", "sub", "lt", "le", "gt", "ge"])
def test_cross_currency_operations_raise(op: str) -> None:
    """The domain holds no exchange rate, so combining currencies is meaningless."""
    usd, eur = Money(100, "USD"), Money(100, "EUR")
    ops = {
        "add": lambda: usd + eur,
        "sub": lambda: usd - eur,
        "lt": lambda: usd < eur,
        "le": lambda: usd <= eur,
        "gt": lambda: usd > eur,
        "ge": lambda: usd >= eur,
    }
    with pytest.raises(CurrencyMismatchError):
        ops[op]()


def test_equality_across_currencies_is_false_not_an_error() -> None:
    """Dataclass equality must stay total so Money works in sets and dict keys."""
    assert Money(100, "USD") != Money(100, "EUR")


# ------------------------------------------------------------- arithmetic --


@given(a=AMOUNTS, b=AMOUNTS, c=AMOUNTS, cur=CURRENCIES)
@pytest.mark.property
def test_addition_is_associative_and_exact(a: int, b: int, c: int, cur: str) -> None:
    """Exactly the property float money fails."""
    x, y, z = Money(a, cur), Money(b, cur), Money(c, cur)
    assert (x + y) + z == x + (y + z)
    assert (x + y).amount_minor == a + b


@given(a=AMOUNTS, cur=CURRENCIES)
@pytest.mark.property
def test_zero_is_the_additive_identity(a: int, cur: str) -> None:
    m = Money(a, cur)
    assert m + zero(cur) == m
    assert m - m == zero(cur)


@given(amounts=st.lists(AMOUNTS, max_size=50), cur=CURRENCIES)
@pytest.mark.property
def test_total_matches_a_plain_integer_sum(amounts: list[int], cur: str) -> None:
    assert total([Money(a, cur) for a in amounts], cur).amount_minor == sum(amounts)


def test_total_of_an_empty_list_is_well_defined() -> None:
    """The currency is a parameter precisely so this does not raise."""
    assert total([], "GBP") == zero("GBP")


# ---------------------------------------------------------------- scaling --


@pytest.mark.parametrize(
    ("minor", "num", "den", "expected"),
    [
        (5, 1, 2, 2),  # 2.5 -> 2  (half to even)
        (7, 1, 2, 4),  # 3.5 -> 4  (half to even)
        (10, 1, 2, 5),  # exact
        (100, 3, 4, 75),  # exact
        (-5, 1, 2, -2),  # -2.5 -> -2 (half to even, symmetric)
        (1, 1, 3, 0),  # 0.333 -> 0
        (2, 1, 3, 1),  # 0.667 -> 1
    ],
)
def test_scaled_rounds_half_to_even(minor: int, num: int, den: int, expected: int) -> None:
    """Banker's rounding: round-half-up biases a large sum of fees upward."""
    assert Money(minor, "USD").scaled(num, den).amount_minor == expected


def test_scaled_refuses_a_float_ratio() -> None:
    """A float ratio would reintroduce exactly the drift this type prevents."""
    with pytest.raises(InvalidMoneyError):
        Money(100, "USD").scaled(0.5, 1)  # type: ignore[arg-type]


def test_scaled_refuses_a_zero_denominator() -> None:
    with pytest.raises(InvalidMoneyError):
        Money(100, "USD").scaled(1, 0)


@given(a=AMOUNTS, cur=CURRENCIES)
@pytest.mark.property
def test_no_float_ever_appears_in_a_money_value(a: int, cur: str) -> None:
    m = Money(a, cur).scaled(7, 3)
    assert isinstance(m.amount_minor, int) and not isinstance(m.amount_minor, bool)
