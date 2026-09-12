"""Sinks: where generated events are written.

**One serialisation.** A row is encoded to canonical JSON exactly once, and those
same bytes are validated, digested and written. The obvious implementation
encodes three times -- once to validate, once to digest, once to write -- and
profiling showed encoding is a material share of the pipeline, so the obvious
implementation was not kept.

**Kafka fails loudly.** Requesting the Kafka sink without the `stream` extra
raises `MissingDependencyError` naming the extra. It never falls back to a file:
a seed run that silently wrote somewhere else would be discovered much later, by
someone wondering why a topic is empty.
"""

from __future__ import annotations

import gzip
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, Protocol

from data.generator.digest import DatasetDigest, canonical_bytes
from trace_core.domain.errors import MissingDependencyError, SchemaValidationError

TOPIC_SUFFIX: Final = ".jsonl"


class ValidationPolicy:
    """How much produce-time validation a run performs.

    `docs/EVENT_CONTRACTS.md` §6.1 requires every message to validate before
    publication, so ALL is the default. The other modes exist because the
    trade-off is real and measured, and whichever is used is recorded in the run
    record -- never left for a reader to guess.
    """

    ALL: Final = "all"
    SAMPLE: Final = "sample"
    NONE: Final = "none"

    CHOICES: Final = (ALL, SAMPLE, NONE)
    SAMPLE_EVERY: Final = 97
    """Prime, so sampling does not align with any periodic structure in the data."""


_VALIDATORS: dict[str, Any] = {}


def _validator_for(topic: str) -> Any:
    """The generated Pydantic model for a topic, loaded once."""
    if topic not in _VALIDATORS:
        from trace_core.contracts.events import (
            device_events_v1,
            identity_events_v1,
            tx_raw_v1,
        )

        _VALIDATORS.update(
            {
                "tx.raw.v1": tx_raw_v1.TxRawV1,
                "identity.events.v1": identity_events_v1.IdentityEventV1,
                "device.events.v1": device_events_v1.DeviceEventV1,
            }
        )
    try:
        return _VALIDATORS[topic]
    except KeyError:
        raise SchemaValidationError(
            f"no released schema for topic {topic!r}; a topic is released only in the "
            f"phase that gains a producer for it (ADR-0028)"
        ) from None


def encode_and_validate(topic: str, event: dict[str, Any], policy: str, position: int) -> bytes:
    """Encode once, validate those exact bytes, return them for writing."""
    payload = canonical_bytes(event)
    should_check = policy == ValidationPolicy.ALL or (
        policy == ValidationPolicy.SAMPLE and position % ValidationPolicy.SAMPLE_EVERY == 0
    )
    if should_check:
        try:
            _validator_for(topic).model_validate_json(payload)
        except Exception as exc:
            raise SchemaValidationError(
                f"event {position} on {topic} does not satisfy its released schema; "
                f"an invalid message is never published (EVENT_CONTRACTS.md §6.1): {exc}"
            ) from exc
    return payload


class Sink(Protocol):
    """Where encoded events go."""

    def write(self, topic: str, payload: bytes) -> None: ...

    def close(self) -> None: ...


@dataclass
class JsonlSink:
    """One newline-delimited JSON file per topic, optionally gzipped.

    Per topic rather than one mixed file: Phase 3 reads each topic separately,
    and a mixed file would force every consumer to filter.
    """

    directory: Path
    compress: bool = False
    _handles: dict[str, Any] = field(default_factory=dict)
    bytes_written: int = 0

    def _handle(self, topic: str) -> Any:
        if topic not in self._handles:
            self.directory.mkdir(parents=True, exist_ok=True)
            suffix = TOPIC_SUFFIX + (".gz" if self.compress else "")
            path = self.directory / f"{topic}{suffix}"
            # Deliberately not a context manager at this call site: a streaming
            # sink holds one handle open across millions of writes and closes it
            # in close(). The sink itself IS a context manager, so the lifetime
            # is still scoped -- just to the sink rather than to a single write.
            opened = (
                gzip.open(path, "wb")  # noqa: SIM115
                if self.compress
                else path.open("wb")
            )
            self._handles[topic] = opened
        return self._handles[topic]

    def write(self, topic: str, payload: bytes) -> None:
        handle = self._handle(topic)
        handle.write(payload)
        handle.write(b"\n")
        self.bytes_written += len(payload) + 1

    def close(self) -> None:
        for handle in self._handles.values():
            handle.close()
        self._handles.clear()

    def __enter__(self) -> JsonlSink:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def paths(self) -> list[Path]:
        suffix = TOPIC_SUFFIX + (".gz" if self.compress else "")
        return sorted(self.directory.glob(f"*{suffix}"))


@dataclass
class ParquetSink:
    """One Parquet file per topic, written in batches.

    Parquet because the ROADMAP sizes the frozen dataset in it, and because Phase
    3 reads the medallion from columnar storage -- JSONL is the wire format, not
    the storage format.

    Batched rather than accumulated: holding a million nested dicts to build one
    table would cost more memory than generating them did. `row_group_size`
    trades compression ratio against memory, and the default is deliberately
    modest so a laptop can produce the full dataset.
    """

    directory: Path
    batch_rows: int = 50_000
    compression: str = "zstd"
    _writers: dict[str, Any] = field(default_factory=dict)
    _buffers: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    bytes_written: int = 0

    def write_row(self, topic: str, event: dict[str, Any]) -> None:
        """Parquet needs the structured row, not the encoded bytes."""
        buffer = self._buffers.setdefault(topic, [])
        buffer.append(event)
        if len(buffer) >= self.batch_rows:
            self._flush(topic)

    def write(self, topic: str, payload: bytes) -> None:
        # Satisfies the Sink protocol; the byte payload is not what Parquet
        # stores, so `write_rows` calls `write_row` when the sink supports it.
        del topic, payload

    def _flush(self, topic: str) -> None:
        rows = self._buffers.get(topic)
        if not rows:
            return
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pylist(rows)
        if topic not in self._writers:
            self.directory.mkdir(parents=True, exist_ok=True)
            self._writers[topic] = pq.ParquetWriter(
                self.directory / f"{topic}.parquet",
                table.schema,
                compression=self.compression,
            )
        self._writers[topic].write_table(table)
        rows.clear()

    def close(self) -> None:
        for topic in list(self._buffers):
            self._flush(topic)
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()
        self.bytes_written = sum(p.stat().st_size for p in self.paths())

    def paths(self) -> list[Path]:
        return sorted(self.directory.glob("*.parquet"))

    def __enter__(self) -> ParquetSink:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


@dataclass
class KafkaSink:
    """Publish to Kafka. Requires the `stream` extra and a running broker."""

    bootstrap_servers: str
    _producer: Any = None

    def __post_init__(self) -> None:
        try:
            from confluent_kafka import Producer
        except ModuleNotFoundError as exc:
            raise MissingDependencyError(
                "confluent_kafka", "stream", "Publishing generated events to Kafka"
            ) from exc
        self._producer = Producer({"bootstrap.servers": self.bootstrap_servers})

    def write(self, topic: str, payload: bytes) -> None:
        self._producer.produce(topic, value=payload)
        self._producer.poll(0)

    def close(self) -> None:
        self._producer.flush(30)


@dataclass
class NullSink:
    """Discards output. For measuring generation cost without I/O."""

    count: int = 0

    def write(self, topic: str, payload: bytes) -> None:
        del topic, payload
        self.count += 1

    def close(self) -> None:
        return None


def write_rows(
    rows: Iterable[Any], sink: Sink, policy: str, digest: DatasetDigest
) -> Iterator[Any]:
    """Encode, validate, digest and write in one pass, yielding rows as it goes.

    Yielded so the caller can collect labels without a second traversal: a 1M-row
    dataset is generated once, not once per consumer.
    """
    for position, row in enumerate(rows):
        payload = encode_and_validate(row.topic, row.event, policy, position)
        # The digest covers transactions only. Identity and device events are
        # scenario colour: including them would make the dataset digest depend on
        # how many episodes happened to need a login event, which is not what
        # "this dataset" means.
        if row.topic == "tx.raw.v1":
            # The bytes encoded just above, not a second encoding of the same row.
            digest.update_bytes(payload)
        # A columnar sink stores the structured row, not the encoded bytes. The
        # bytes are still produced above because that is what validation and the
        # digest read -- one encoding, several consumers.
        writer = getattr(sink, "write_row", None)
        if writer is not None:
            writer(row.topic, row.event)
        else:
            sink.write(row.topic, payload)
        yield row
