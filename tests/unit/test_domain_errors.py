"""The typed error hierarchy.

CLAUDE.md §6 forbids `except Exception: pass`. That rule only works if the
project's own failures are distinguishable from genuine bugs, which is what a
single root class buys: a boundary can catch `TraceXError` and let a TypeError
crash the process, where it belongs.
"""

from __future__ import annotations

import inspect

import pytest

from trace_core.domain import errors
from trace_core.domain.errors import (
    IllegalTransitionError,
    MissingDependencyError,
    TraceXError,
)

pytestmark = pytest.mark.unit

ALL_ERRORS = [
    obj
    for _, obj in inspect.getmembers(errors, inspect.isclass)
    if issubclass(obj, BaseException) and obj.__module__ == errors.__name__
]


def test_the_module_actually_defines_errors() -> None:
    assert len(ALL_ERRORS) >= 15


@pytest.mark.parametrize("err", ALL_ERRORS, ids=lambda e: e.__name__)
def test_every_error_descends_from_the_single_root(err: type[BaseException]) -> None:
    assert issubclass(err, TraceXError)


@pytest.mark.parametrize("err", ALL_ERRORS, ids=lambda e: e.__name__)
def test_every_error_explains_itself(err: type[BaseException]) -> None:
    """An exception with no docstring gives an on-call engineer nothing."""
    assert err.__doc__ and len(err.__doc__.strip()) > 30, f"{err.__name__} is undocumented"


def test_catching_the_root_does_not_swallow_real_bugs() -> None:
    with pytest.raises(TypeError):
        try:
            raise TypeError("a genuine programming error")
        except TraceXError:  # pragma: no cover - must not catch
            pytest.fail("TraceXError caught a TypeError; the hierarchy is too broad")


def test_illegal_transition_names_the_legal_alternatives() -> None:
    """The message has to be actionable without opening the transition table."""
    err = IllegalTransitionError("case", "CLOSED", "INVESTIGATE", frozenset({"REOPEN"}))
    text = str(err)
    assert "case" in text and "CLOSED" in text and "INVESTIGATE" in text and "REOPEN" in text
    assert err.machine == "case" and err.state == "CLOSED"


def test_illegal_transition_from_a_terminal_state_says_so() -> None:
    err = IllegalTransitionError("case", "CLOSED", "ANYTHING", frozenset())
    assert "terminal state" in str(err)


def test_missing_dependency_tells_you_the_install_command() -> None:
    """Degrading silently is the failure this replaces, so the message must act."""
    err = MissingDependencyError("confluent_kafka", "stream", "Publishing to Kafka")
    text = str(err)
    assert "confluent_kafka" in text
    assert 'pip install -e ".[stream]"' in text
    assert err.extra == "stream"
