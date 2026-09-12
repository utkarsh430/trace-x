"""The `SourceAdapter` port (ADR-0022).

Every inbound dataset implements this. Two implementations must exist before the
abstraction may be called portable (CLAUDE.md §3.4): `GeneratorAdapter`
(Track A, full coverage) in Phase 1, and `IeeeCisAdapter` (Track B, partial
coverage) in Phase 4B. **Both pass the same conformance suite**, and Phase 4B's
strongest claim — that IEEE-CIS flows through an unmodified medallion — rests on
this port being the only thing that had to change.

**On the signature.** ADR-0022 writes `to_canonical(b) -> DataFrame[CanonicalTransaction]`.
Phase 1 has no Spark (that is Phase 3), so the port is defined row-wise over an
iterable and Phase 3 binds it by mapping over partitions. That is a faithful
implementation of the ADR rather than a departure from it: the unit of the
contract is the mapping from a source row to a canonical row, and a DataFrame is
one way to apply it. Defining it row-wise also means the conformance suite runs
in `test-fast` without a JVM.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import Annotated, Any, Protocol, runtime_checkable

from pydantic import Field

from trace_core.contracts.base import StrictModel
from trace_core.contracts.canonical import CanonicalField, CanonicalTransaction


class SourceProfile(StrictModel):
    """What an adapter declares about its dataset.

    Every field here appears in a run manifest (ADR-0017): `name` and `version`
    as `source_adapter_id` / `source_adapter_version`, and `digest` as part of
    the dataset identity. A metric whose source cannot be identified is not
    reproducible.
    """

    name: Annotated[str, Field(min_length=1, max_length=64)]
    version: Annotated[str, Field(pattern=r"^\d+\.\d+\.\d+$")]
    """Semantic version of the ADAPTER, not of the dataset. Changing how a field
    is mapped changes this, and therefore changes the run manifest."""

    field_coverage: frozenset[CanonicalField]
    """Which canonical fields this source supplies. Every row it emits carries
    the identical set, and the conformance suite checks the declaration against
    what is actually produced."""

    row_count: Annotated[int, Field(ge=0)]
    digest: Annotated[str, Field(pattern=r"^sha256:[0-9a-f]{64}$")]
    """Content digest of the underlying data. Two runs citing the same digest
    read the same bytes."""

    @property
    def uncovered(self) -> frozenset[CanonicalField]:
        """Canonical fields this source does NOT supply.

        Reported alongside every transfer metric, so a thin overlap visibly
        invalidates the metric rather than quietly weakening it (ADR-0021).
        """
        return frozenset(CanonicalField) - self.field_coverage


@runtime_checkable
class SourceAdapter(Protocol):
    """Maps a foreign schema onto `CanonicalTransaction`.

    A Protocol, not a base class: the domain depends on the shape, never on an
    implementation, and an adapter can live in `data/adapters/` without
    `trace_core` importing it (CLAUDE.md §6, ports and adapters).
    """

    def describe(self) -> SourceProfile:
        """Declare the dataset and, critically, its field coverage."""
        ...

    def to_canonical(self, batch: Iterable[Any]) -> Iterator[CanonicalTransaction]:
        """Map source rows to canonical rows.

        An iterator rather than a list: a 1 M-row dataset should stream, and
        Phase 3 applies this per Spark partition.
        """
        ...

    def label_column(self) -> str:
        """Name of the ground-truth label column **in the source dataset**.

        This is deliberately only a *name*. The adapter never reads labels and
        never attaches them to a `CanonicalTransaction`: labels live in the
        `groundtruth` schema, which the application role cannot read at all
        (ADR-0004). The evaluation harness, connecting as `trace_eval`, is the
        only thing that resolves this name to values.
        """
        ...
