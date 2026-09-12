"""Dataset digest.

The digest is the dataset's identity. Every run manifest cites one (ADR-0017),
and `eval-v1` is referenced by digest rather than by path so it can never be
regenerated in place (docs/EVALUATION.md §2).

It is computed over **canonical JSON of each row, in emission order**, not over
the output file's bytes. A file digest would change with the container format,
the compression level or the pyarrow version, none of which change the data. A
row digest changes exactly when the data changes, which is the property that
makes "same seed, same digest" a meaningful claim.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable, Iterator
from typing import Any

from trace_core.contracts.canonical_json import canonical_bytes


class DatasetDigest:
    """Streaming sha256 over canonical row JSON.

    Streaming because the digest must be computable while writing a 1 M-row
    dataset, without holding it in memory.
    """

    __slots__ = ("_hasher", "_rows")

    def __init__(self) -> None:
        self._hasher = hashlib.sha256()
        self._rows = 0

    def update(self, row: Any) -> None:
        """Digest a structured row, encoding it here."""
        self.update_bytes(canonical_bytes(row))

    def update_bytes(self, payload: bytes) -> None:
        """Digest an ALREADY-encoded row.

        The writer encodes every row anyway -- to validate it and to write it --
        so re-encoding here would double the serialisation cost of the whole
        pipeline. The docstring of `emit` claims one serialisation; this is what
        makes that true rather than aspirational.
        """
        self._hasher.update(payload)
        self._hasher.update(b"\n")
        self._rows += 1

    @property
    def row_count(self) -> int:
        return self._rows

    def hexdigest(self) -> str:
        return "sha256:" + self._hasher.hexdigest()


# Re-exported, not redefined. The envelope's `idempotency_key` hashes the same
# way this digest does, and two implementations of a hash's input drift silently
# -- so the encoding lives in `trace_core.contracts.canonical_json` and both
# callers import it. Kept importable from here because `eval-v1`'s manifest and
# several tests already reference this name.
__all__ = ["DatasetDigest", "canonical_bytes", "digest_of", "tee_digest"]


def digest_of(rows: Iterable[Any]) -> tuple[str, int]:
    """Digest an iterable, returning `(digest, row_count)`."""
    digest = DatasetDigest()
    for row in rows:
        digest.update(row)
    return digest.hexdigest(), digest.row_count


def tee_digest(rows: Iterable[Any], digest: DatasetDigest) -> Iterator[Any]:
    """Yield rows while digesting them, so writing and digesting are one pass."""
    for row in rows:
        digest.update(row)
        yield row
