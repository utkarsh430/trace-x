"""Ground-truth labels produced alongside events.

**This module defines the shape; it never persists anything.** Labels reach
PostgreSQL only through `data.generator.groundtruth`, into a schema the
application role has no grant on at all (ADR-0004). Keeping the two separate is
what makes "the generator can write ground truth and nothing else can read it" a
structural statement rather than a convention.

Every transaction gets a label, fraudulent or not. Omitting the negatives would
make the evaluation harness unable to compute a false-positive rate, which is
half of what a fraud metric is.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from trace_core.domain.enums import EvidenceKind, FraudPattern


@dataclass(frozen=True, slots=True)
class TransactionLabel:
    """The truth about one transaction."""

    transaction_id: str
    is_fraud: bool
    fraud_pattern: FraudPattern | None = None
    scenario_instance_id: str | None = None
    causal_evidence_keys: frozenset[EvidenceKind] = field(default_factory=frozenset)
    """The evidence kinds that actually explain THIS transaction.

    The basis of evidence precision and recall as set operations
    (`docs/EVALUATION.md` §5) -- which is the only reason those metrics need no
    LLM judge. Empty for legitimate transactions.
    """

    def __post_init__(self) -> None:
        if self.is_fraud:
            if self.fraud_pattern is None:
                raise ValueError(f"{self.transaction_id}: fraud label with no pattern")
            if not self.causal_evidence_keys:
                raise ValueError(
                    f"{self.transaction_id}: fraud label with no causal_evidence_keys. "
                    f"Evidence precision and recall are computed against this set, so an "
                    f"empty one silently makes the instance unscoreable."
                )
        elif self.fraud_pattern is not None or self.causal_evidence_keys:
            raise ValueError(
                f"{self.transaction_id}: a legitimate label must carry no pattern and no "
                f"causal keys -- otherwise a negative row looks partially explained"
            )
