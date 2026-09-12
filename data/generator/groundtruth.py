"""The only module that writes ground truth (ADR-0004, ADR-0031).

**Everything about this file is a containment boundary.** `tests/unit/
test_groundtruth_not_referenced.py` asserts that no other module under
`trace_core/` or `data/` so much as names the `groundtruth` schema, so the set of
places that could leak labels is one file rather than a codebase.

It connects as `trace_generator`, which may INSERT labels and SELECT only the
dataset registry. **It cannot read back the labels it writes.** That means no
credential used in ordinary development can read ground truth -- not the
application's, and not the generator's either.

Writes are one transaction. A half-written dataset would be worse than none: the
evaluation harness would compute metrics against a subset while believing it had
the whole, and nothing would look wrong.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from data.generator.labels import TransactionLabel
from trace_core.domain.errors import GroundTruthAccessError, MissingDependencyError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from data.generator.scenarios import ScenarioInstance

SCHEMA: Final = "groundtruth"
ROLE: Final = "trace_generator"
# The NAME of an environment variable, not a credential. Suppressed inline
# rather than by ignoring S105 here -- this is the one module that touches
# ground truth, and a blanket ignore would hide a real secret added later.
PASSWORD_ENV: Final = "TRACE_GENERATOR_DB_PASSWORD"  # noqa: S105


@dataclass(frozen=True, slots=True)
class DatasetRecord:
    """The registry row identifying one generated dataset."""

    dataset_version: str
    seed: int
    generator_version: str
    fraud_scenario_config_digest: str
    dataset_digest: str
    row_count: int


def _connect(dsn: str | None = None) -> Any:
    """Open a connection as `trace_generator`.

    psycopg is imported lazily and its absence is a loud, actionable error rather
    than a silent skip: a seed run that quietly failed to write ground truth
    would leave a dataset nobody could evaluate.
    """
    try:
        import psycopg
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised by the failure drill
        raise MissingDependencyError("psycopg", "db", "Writing ground truth to PostgreSQL") from exc

    if dsn:
        return psycopg.connect(dsn)

    password = os.getenv(PASSWORD_ENV)
    if not password:
        raise GroundTruthAccessError(
            f"{PASSWORD_ENV} is not set, so the generator cannot authenticate as "
            f"{ROLE!r}. Ground truth is never written as the application role or as the "
            f"migration owner (ADR-0031)."
        )
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "127.0.0.1"),
        port=int(os.getenv("POSTGRES_PORT", "5442")),
        dbname=os.getenv("POSTGRES_DB", "tracex"),
        user=ROLE,
        password=password,
    )


def write_dataset(
    record: DatasetRecord,
    labels: Iterable[TransactionLabel],
    instances: Iterable[ScenarioInstance] = (),
    *,
    dsn: str | None = None,
    replace: bool = False,
) -> int:
    """Write one dataset's ground truth. Returns the `dataset_id`.

    All or nothing. A partially-written dataset would let the harness compute
    metrics over a subset while believing it had the whole, which is a silently
    wrong number -- the category this project treats as worse than a crash.

    Re-seeding an existing `dataset_version` is refused by a UNIQUE constraint
    unless `replace=True`, which deletes the prior rows by FK cascade. The refusal
    is the default because `eval-v1` must never be regenerated in place
    (`docs/EVALUATION.md` §2).
    """
    connection = _connect(dsn)
    try:
        with connection, connection.cursor() as cur:
            if replace:
                cur.execute(
                    "DELETE FROM groundtruth.datasets WHERE dataset_version = %s",
                    (record.dataset_version,),
                )
            cur.execute(
                """
                INSERT INTO groundtruth.datasets
                    (dataset_version, seed, generator_version,
                     fraud_scenario_config_digest, dataset_digest, row_count)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING dataset_id
                """,
                (
                    record.dataset_version,
                    record.seed,
                    record.generator_version,
                    record.fraud_scenario_config_digest,
                    record.dataset_digest,
                    record.row_count,
                ),
            )
            row = cur.fetchone()
            if row is None:  # pragma: no cover - RETURNING always yields a row
                raise GroundTruthAccessError("dataset insert returned no id")
            dataset_id = int(row[0])

            label_rows: list[tuple[Any, ...]] = []
            evidence_rows: list[tuple[Any, ...]] = []
            for label in labels:
                label_rows.append(
                    (
                        dataset_id,
                        label.transaction_id,
                        label.is_fraud,
                        label.fraud_pattern.value if label.fraud_pattern else None,
                        label.scenario_instance_id,
                    )
                )
                evidence_rows.extend(
                    (dataset_id, label.transaction_id, kind.value)
                    for kind in label.causal_evidence_keys
                )

            if label_rows:
                cur.executemany(
                    "INSERT INTO groundtruth.transaction_labels "
                    "(dataset_id, transaction_id, is_fraud, fraud_pattern, scenario_instance_id) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    label_rows,
                )
            if evidence_rows:
                cur.executemany(
                    "INSERT INTO groundtruth.causal_evidence "
                    "(dataset_id, transaction_id, evidence_kind) VALUES (%s, %s, %s)",
                    evidence_rows,
                )

            instance_rows = [
                (
                    dataset_id,
                    instance.instance_id,
                    instance.pattern.value,
                    json.dumps(instance.participants, sort_keys=True),
                    instance.transaction_count,
                )
                for instance in instances
            ]
            if instance_rows:
                cur.executemany(
                    "INSERT INTO groundtruth.scenario_instances "
                    "(dataset_id, instance_id, fraud_pattern, participants, transaction_count) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    instance_rows,
                )
        return dataset_id
    finally:
        connection.close()


def dataset_exists(dataset_version: str, *, dsn: str | None = None) -> bool:
    """Whether a dataset version is already registered.

    Reads `datasets` -- the one ground-truth table `trace_generator` may SELECT.
    A permission error on any other table is the control working, not a bug.
    """
    connection = _connect(dsn)
    try:
        with connection, connection.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM groundtruth.datasets WHERE dataset_version = %s",
                (dataset_version,),
            )
            return cur.fetchone() is not None
    finally:
        connection.close()
