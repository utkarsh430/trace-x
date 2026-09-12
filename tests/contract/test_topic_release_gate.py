"""No code may publish to a topic whose schema has not been released.

A schema file is immutable once merged (ADR-0026), so releasing one before a
real producer exists freezes a contract nobody has exercised -- and the only way
to correct it afterwards is a `.v2` topic plus a dual-write window. The gate
therefore works in both directions:

* code referencing an unreleased topic fails here;
* a topic listed as PLANNED that has quietly acquired a producer also fails,
  because the ledger has stopped describing reality.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.contract

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "docs" / "contracts" / "RELEASED.json"
SCHEMA_DIR = ROOT / "docs" / "contracts" / "events"

# Directories holding code that could produce or consume events.
SOURCE_ROOTS = ("packages", "data", "services", "mcp_servers", "eval")

# A dotted topic name ending in a version: `tx.raw.v1`, `action.executed.v1`.
TOPIC_LITERAL = re.compile(r"""["']([a-z][a-z0-9]*(?:\.[a-z][a-z0-9]*)+\.v\d+)["']""")


@pytest.fixture(scope="module")
def manifest() -> dict[str, list[dict[str, str]]]:
    return json.loads(MANIFEST.read_text())


@pytest.fixture(scope="module")
def released(manifest: dict[str, list[dict[str, str]]]) -> set[str]:
    return {entry["topic"] for entry in manifest["released"]}


@pytest.fixture(scope="module")
def planned(manifest: dict[str, list[dict[str, str]]]) -> set[str]:
    return {entry["topic"] for entry in manifest["planned"]}


def _source_files() -> list[Path]:
    files: list[Path] = []
    for root in SOURCE_ROOTS:
        base = ROOT / root
        if base.is_dir():
            files.extend(p for p in base.rglob("*.py") if "__pycache__" not in p.parts)
    return sorted(files)


def _referenced_topics() -> dict[str, list[str]]:
    """topic -> the files that mention it."""
    hits: dict[str, list[str]] = {}
    for path in _source_files():
        for topic in set(TOPIC_LITERAL.findall(path.read_text())):
            hits.setdefault(topic, []).append(str(path.relative_to(ROOT)))
    return hits


def test_every_released_topic_has_a_committed_schema(
    manifest: dict[str, list[dict[str, str]]],
) -> None:
    for entry in manifest["released"]:
        assert (SCHEMA_DIR / entry["schema"]).is_file(), (
            f"{entry['topic']} is marked released but {entry['schema']} does not exist"
        )


def test_no_code_references_an_unreleased_topic(released: set[str], planned: set[str]) -> None:
    """The gate. Publishing to a PLANNED topic means shipping against a contract
    that has not been reviewed or frozen."""
    violations = {
        topic: files for topic, files in _referenced_topics().items() if topic not in released
    }
    assert not violations, (
        "code references topics that are not RELEASED in docs/contracts/RELEASED.json:\n"
        + "\n".join(
            f"  {topic} ({'PLANNED' if topic in planned else 'UNKNOWN'}) in {files}"
            for topic, files in sorted(violations.items())
        )
        + "\n  Release the schema in the phase that introduces its producer."
    )


def test_the_scan_actually_looks_at_source(released: set[str]) -> None:
    """Guards against the gate becoming a no-op if the source layout changes."""
    files = _source_files()
    assert len(files) > 20, f"topic scan found only {len(files)} source files; check SOURCE_ROOTS"


def test_the_topic_pattern_matches_a_known_topic_shape(released: set[str]) -> None:
    """If the regex stopped matching, every test above would pass vacuously."""
    assert TOPIC_LITERAL.findall('publish("tx.raw.v1", payload)') == ["tx.raw.v1"]
    assert TOPIC_LITERAL.findall('x = "action.executed.v1"') == ["action.executed.v1"]
    assert TOPIC_LITERAL.findall('"not-a-topic"') == []
    assert released, "the released set is empty; the gate would pass vacuously"
