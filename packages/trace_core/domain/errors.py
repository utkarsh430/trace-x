"""Typed domain exceptions.

CLAUDE.md §6: errors are typed exceptions from this module. `except Exception:
pass` is never acceptable — a swallowed error in a fraud system is a silent
wrong answer, which is worse than a loud failure.

Everything derives from `TraceXError`, so a process boundary can catch exactly
the project's own failures and let genuine bugs (TypeError, AttributeError)
propagate to a crash where they belong.
"""

from __future__ import annotations


class TraceXError(Exception):
    """Root of every error TRACE-X raises deliberately."""


# --------------------------------------------------------------- domain ----


class DomainError(TraceXError):
    """A domain invariant was violated."""


class IllegalTransitionError(DomainError):
    """A state machine was asked for a transition its table does not allow.

    Raised, never warned and never silently ignored: a case that reaches an
    impossible state has lost its audit meaning, and the audit trail is the
    product (docs/SECURITY.md §8).
    """

    def __init__(self, machine: str, state: str, event: str, allowed: frozenset[str]) -> None:
        self.machine = machine
        self.state = state
        self.event = event
        self.allowed = allowed
        super().__init__(
            f"{machine}: {event!r} is not legal in state {state!r}; "
            f"legal events here: {sorted(allowed) or '<none — terminal state>'}"
        )


class CurrencyMismatchError(DomainError):
    """Arithmetic was attempted across two currencies."""


class InvalidMoneyError(DomainError):
    """Money was constructed from something other than integer minor units.

    CLAUDE.md §6: money is never a float. A float amount is a rounding bug that
    surfaces months later in a reconciliation mismatch.
    """


class NaiveDatetimeError(DomainError):
    """A timezone-naive datetime reached the domain.

    Time is always timezone-aware UTC. A naive datetime is ambiguous, and event
    time versus processing time is the distinction the whole warm path rests on
    (ADR-0026).
    """


# ------------------------------------------------------------ contracts ----


class ContractError(TraceXError):
    """An event or API contract was violated."""


class SchemaValidationError(ContractError):
    """A payload failed validation against its committed JSON Schema."""


class UnreleasedTopicError(ContractError):
    """Code referenced a topic whose schema has not been released.

    docs/EVENT_CONTRACTS.md: a schema file is immutable once merged, so a topic
    is released only in the phase that gains a real producer for it. Publishing
    to an unreleased topic would freeze a contract nobody has exercised.
    """


# ------------------------------------------------- ingestion / features ----


class AdapterCoverageError(TraceXError):
    """A SourceAdapter declared field coverage it does not actually produce.

    ADR-0022: declared coverage is the basis of every UNAVAILABLE decision
    downstream. An adapter that lies about coverage fabricates signal.
    """


class FeatureUnavailableError(TraceXError):
    """An UNAVAILABLE feature value was read as though it were a number.

    ADR-0022's central failure mode: coercing UNAVAILABLE to 0 turns "this
    source never provides it" into "the value is zero", which looks like a weak
    signal and is actually a fabricated one.
    """


# ---------------------------------------------------------- generation ----


class DeterminismError(TraceXError):
    """A generation run did not reproduce its recorded digest.

    Same seed must give the same dataset. A digest that moved silently
    invalidates every benchmark number that cited it (ADR-0017).
    """


class GroundTruthAccessError(TraceXError):
    """Ground truth was reached from somewhere that must not reach it.

    The real control is a PostgreSQL grant (ADR-0004), not this exception. This
    exists so the application-side violation fails with an explanation rather
    than a raw psycopg permission error.
    """


class MissingDependencyError(TraceXError):
    """An optional phase dependency is required by the requested operation.

    Raised instead of silently degrading. `make seed --sink kafka` without the
    `stream` extra must say so, never quietly write a file instead.
    """

    def __init__(self, package: str, extra: str, purpose: str) -> None:
        self.package = package
        self.extra = extra
        super().__init__(
            f"{purpose} needs '{package}', which ships in the '{extra}' extra. "
            f'Install it with: pip install -e ".[{extra}]"'
        )


# ------------------------------------------- declared now, used later ------
# Declared here so the taxonomy is complete and downstream phases extend it
# rather than inventing a parallel hierarchy.


class ToolAuthorizationError(TraceXError):
    """An agent called a tool outside its allowed_tools (Phase 5).

    CLAUDE.md §10.3: enforced by the runtime, never by the prompt. The call is
    refused, audited, and counted as a failure.
    """


class BudgetExhaustedError(TraceXError):
    """An investigation hit one of its four termination bounds (Phase 6).

    CLAUDE.md §10.4: this produces INSUFFICIENT_EVIDENCE and a human-queue
    entry. It is a valid recorded outcome, never a hang and never a crash.
    """
