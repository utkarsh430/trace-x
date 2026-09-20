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


class FeatureWriteFailedError(TraceXError):
    """The online store is reachable but refused to record an observation.

    Distinct from an unreachable store on purpose. Unreachable is an outage the
    circuit breaker should learn about; refused-because-full is a capacity
    condition the breaker must NOT open on, because the store is still
    answering reads and opening the circuit would blind scoring to punish a
    write. Under `noeviction` this is the ONLY way memory pressure can present
    (ADR-0044): loudly, as a counted degradation, rather than as feature state
    quietly disappearing.

    Raising it also invalidates the store's completeness epoch: an observation
    that was not recorded is a hole in the history, and every window that
    spans the hole is no longer complete.
    """


class ToolchainMismatchError(TraceXError):
    """The Phase 3 JVM toolchain does not match its pins, so no Spark session starts.

    Raised by `trace_core.stream.session` BEFORE a JVM is launched, and again if the
    JVM that did launch reports different versions than the files promised. The
    alternative is Spark failing much later with `UnsupportedClassVersionError`
    or a `NoSuchMethodError` from inside a query -- errors that name neither the
    component nor the fix. The message lists every failed check with its remedy,
    so one run shows the whole problem rather than its first symptom.
    """


class EventPublishError(TraceXError):
    """Events handed to the Kafka producer could not be confirmed as delivered.

    Raised by `trace_core.contracts.publish` when a flush leaves messages still
    queued, when any delivery report came back failed, when an event was shed or
    refused, or when the idempotent producer reported a fatal error. It replaces a
    close that flushed for a fixed time and let whatever was still queued vanish
    with the process, so a seed run could report success over a topic missing its
    tail. Unconfirmed is reported as unconfirmed, never as delivered.
    """

    def __init__(
        self,
        message: str,
        *,
        outstanding: int = 0,
        failed: dict[str, int] | None = None,
        shed: dict[str, int] | None = None,
        refused: dict[str, int] | None = None,
        fatal: str | None = None,
    ) -> None:
        self.outstanding = outstanding
        self.failed = dict(failed or {})
        self.shed = dict(shed or {})
        self.refused = dict(refused or {})
        self.fatal = fatal
        super().__init__(message)


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


class NonConformantFeatureSetError(TraceXError):
    """A run was asked of an online path that does not serve the declared feature set.

    Raised before any work, so a load test or replay can never produce a record whose
    `feature_set_version` names semantics the values were not computed with.
    """


# ---------------------------------------------------- the Delta lake (Phase 3) ---


class LakeContractError(TraceXError):
    """A lake convention was violated, so the operation that depended on it did not run.

    The conventions cover naming, table declarations, drift, checkpoints, streaming
    sources and scan measurement (`trace_core.stream.lake`, `tables`, `checkpoints`).
    Every subclass is raised BEFORE the unsafe action -- a write, a query start, a
    published measurement -- because in each case the alternative is a Delta or Spark
    behaviour that succeeds with a wrong result.
    """


class LakeConfigError(LakeContractError):
    """`TRACE_DELTA_ROOT` cannot be used as a local lake root.

    Raised for a set-but-blank value, a URI, a path a `delta.`<path>`` identifier cannot
    quote, or a relative path with no source checkout to anchor it to. A blank value is
    refused rather than defaulted: a deployment that meant to point somewhere and wrote
    nothing would otherwise put its lake wherever it happened to start.
    """


class LakeNameError(LakeContractError):
    """A table name, query name or transaction app id does not follow the lake's naming rules.

    The name becomes a directory, a Unity Catalog identifier and part of a Delta
    transaction app id; a name valid for one of those and not the others would make one
    logical table resolve differently locally and on Databricks.
    """


class TableDeclarationError(LakeContractError):
    """A table declaration contradicts itself or the lake contract, so no table is created.

    Examples: clustering and partitioning together, a property that is not on the
    allow-list or that adds a protocol feature without an opt-in, or session settings that
    Delta would add to every new table.
    """


class TableDriftError(LakeContractError):
    """A live table no longer matches its declaration, so the job refuses to write to it.

    The message lists every difference at once -- schema, properties, constraints, layout,
    protocol versions and features -- because fixing drift one symptom per restart is how
    an operator learns to switch the check off.
    """


class ProvenanceError(LakeContractError):
    """A commit's `userMetadata` claims to be TRACE-X provenance but cannot be trusted as such.

    Foreign or absent metadata is simply not ours. Metadata that carries our marker and is
    malformed is an error, because the checkpoint guard attributes commits from it, and a
    guess would attribute a commit to the wrong query.
    """


class StreamingSourceRetentionError(LakeContractError):
    """A streaming source cannot be read without risking silently skipped data.

    Raised before a query starts: its checkpoint needs Delta log versions the source no
    longer retains, the source was replaced, or the reader or session carries a setting
    whose purpose is to tolerate loss (`failOnDataLoss=false`, `ignoreMissingFiles`, ...).
    """


class CheckpointRefusedError(LakeContractError):
    """A streaming query was refused a start, a reset or a write, because its checkpoint
    cannot be trusted to pair with the tables it reads and writes.

    Every refusal names its evidence -- table ids, transaction app ids, batch ids, start
    positions -- and its remedy. The remedy is never "delete the checkpoint": that is the
    action that creates the silent-skip and silent-duplication states this error prevents.
    """


class ScanMeasurementError(LakeContractError):
    """A query's file scans could not be measured completely, so no measurement is returned.

    Raised when adaptive execution dropped a scan that ran, when a scan's reads are absent
    from Spark's status store, when the plan's root executes differently from the path the
    measurement uses, or when Spark's metric names change. A partial measurement published
    as a benchmark number would be a plausible, wrong one.
    """
