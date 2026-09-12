"""Money as integer minor units.

CLAUDE.md §6: **money is never a float.** Binary floating point cannot represent
most decimal fractions, so `0.1 + 0.2 != 0.3` and a sum of a million amounts
drifts. In a fraud system that drift shows up as a reconciliation mismatch
nobody can explain, months later.

`amount_minor` is the integer count of the currency's minor unit — cents for
USD, pence for GBP. Currencies without a minor unit (JPY) simply have a minor
unit equal to the major unit; this type does not need to know, because it never
formats for display or converts between currencies.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

from trace_core.domain.errors import CurrencyMismatchError, InvalidMoneyError

_ISO_4217: Final = re.compile(r"^[A-Z]{3}$")


@dataclass(frozen=True, slots=True, order=False)
class Money:
    """An exact amount in one currency.

    Comparison and arithmetic are defined only within a single currency: there
    is no exchange rate in the domain, and silently treating USD and EUR as
    interchangeable is precisely the bug this type exists to make impossible.
    """

    amount_minor: int
    currency: str

    def __post_init__(self) -> None:
        # bool is a subclass of int, so `isinstance(True, int)` is True. Without
        # this guard `Money(True, "USD")` would silently mean one cent.
        if isinstance(self.amount_minor, bool) or not isinstance(self.amount_minor, int):
            raise InvalidMoneyError(
                f"amount_minor must be an int of minor units, got "
                f"{type(self.amount_minor).__name__}={self.amount_minor!r}. "
                f"Money is never a float (CLAUDE.md §6)."
            )
        if not isinstance(self.currency, str) or not _ISO_4217.match(self.currency):
            raise InvalidMoneyError(
                f"currency must be an uppercase ISO-4217 alphabetic code, got {self.currency!r}"
            )

    # ------------------------------------------------------------ helpers --

    def _same_currency(self, other: Money) -> None:
        if self.currency != other.currency:
            raise CurrencyMismatchError(
                f"cannot combine {self.currency} with {other.currency}; "
                f"the domain holds no exchange rate"
            )

    # --------------------------------------------------------- arithmetic --

    def __add__(self, other: Money) -> Money:
        self._same_currency(other)
        return Money(self.amount_minor + other.amount_minor, self.currency)

    def __sub__(self, other: Money) -> Money:
        self._same_currency(other)
        return Money(self.amount_minor - other.amount_minor, self.currency)

    def __neg__(self) -> Money:
        return Money(-self.amount_minor, self.currency)

    def __abs__(self) -> Money:
        return Money(abs(self.amount_minor), self.currency)

    def scaled(self, numerator: int, denominator: int) -> Money:
        """Multiply by an exact rational, rounding half to even.

        Deliberately not `__mul__` by a float: a rational keeps the operation
        exact and auditable, and banker's rounding avoids the upward bias that
        round-half-up accumulates across many rows.
        """
        if not isinstance(numerator, int) or not isinstance(denominator, int):
            raise InvalidMoneyError("scaled() takes integers; a float ratio reintroduces drift")
        if denominator == 0:
            raise InvalidMoneyError("denominator must be non-zero")
        numer, denom = self.amount_minor * numerator, denominator
        if denom < 0:
            numer, denom = -numer, -denom
        # Python's divmod floors toward negative infinity, so `whole` is already
        # the floor and `rest` satisfies 0 <= rest < denom for negatives too.
        # Reasoning in terms of truncation instead rounds -2.5 to -4.
        whole, rest = divmod(numer, denom)
        twice = rest * 2
        if twice > denom or (twice == denom and whole % 2 != 0):
            whole += 1
        return Money(whole, self.currency)

    # --------------------------------------------------------- comparison --

    def __lt__(self, other: Money) -> bool:
        self._same_currency(other)
        return self.amount_minor < other.amount_minor

    def __le__(self, other: Money) -> bool:
        self._same_currency(other)
        return self.amount_minor <= other.amount_minor

    def __gt__(self, other: Money) -> bool:
        self._same_currency(other)
        return self.amount_minor > other.amount_minor

    def __ge__(self, other: Money) -> bool:
        self._same_currency(other)
        return self.amount_minor >= other.amount_minor

    def __str__(self) -> str:
        return f"{self.amount_minor} {self.currency}"


def zero(currency: str) -> Money:
    """The additive identity for a currency."""
    return Money(0, currency)


def total(amounts: list[Money], currency: str) -> Money:
    """Sum amounts, requiring the currency up front.

    The currency is a parameter rather than inferred from the first element so
    that summing an empty list is well defined instead of raising.
    """
    result = zero(currency)
    for amount in amounts:
        result = result + amount
    return result
