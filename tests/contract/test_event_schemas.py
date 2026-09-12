"""Event JSON Schemas: the source of truth for events (ADR-0026, ADR-0028).

These are contract tests, not unit tests. They assert properties of the
*committed schema files*, which outlive any particular implementation and are
read by Python producers, Spark consumers and a possible future Go gateway.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

pytestmark = pytest.mark.contract

ROOT = Path(__file__).resolve().parents[2]
SCHEMA_DIR = ROOT / "docs" / "contracts" / "events"
MANIFEST = ROOT / "docs" / "contracts" / "RELEASED.json"

ENVELOPE_FIELDS = (
    "event_id",
    "event_type",
    "schema_version",
    "occurred_at",
    "ingested_at",
    "producer",
    "trace_id",
    "correlation_id",
    "idempotency_key",
)

# docs/SECURITY.md §5.1 names these exactly. They are attacker-controlled and
# must carry both a declared trust tier and a length bound.
ATTACKER_CONTROLLED = frozenset({"merchant_name", "user_agent", "memo", "device_label"})


def _schemas() -> list[Path]:
    return sorted(SCHEMA_DIR.glob("*.json"))


def _event_schemas() -> list[Path]:
    return [p for p in _schemas() if p.name != "envelope.v1.json"]


@pytest.fixture(scope="module")
def manifest() -> dict[str, Any]:
    return json.loads(MANIFEST.read_text())


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _walk_properties(node: Any) -> list[tuple[str, dict[str, Any]]]:
    """Every (name, subschema) pair under any `properties` block, recursively."""
    found: list[tuple[str, dict[str, Any]]] = []
    if isinstance(node, dict):
        for name, sub in (node.get("properties") or {}).items():
            if isinstance(sub, dict):
                found.append((name, sub))
        for value in node.values():
            found.extend(_walk_properties(value))
    elif isinstance(node, list):
        for item in node:
            found.extend(_walk_properties(item))
    return found


# ------------------------------------------------------------ structure ----


def test_schemas_exist() -> None:
    assert _event_schemas(), "no event schemas committed"


@pytest.mark.parametrize("path", _schemas(), ids=lambda p: p.name)
def test_schema_is_valid_json_schema(path: Path) -> None:
    """A malformed schema silently validates nothing."""
    Draft202012Validator.check_schema(_load(path))


@pytest.mark.parametrize("path", _schemas(), ids=lambda p: p.name)
def test_schema_declares_its_dialect_and_id(path: Path) -> None:
    doc = _load(path)
    assert doc.get("$schema") == "https://json-schema.org/draft/2020-12/schema"
    assert doc.get("$id", "").endswith(path.name)
    assert doc.get("title")
    assert len(doc.get("description", "")) > 40, "a schema with no description is not a contract"


@pytest.mark.parametrize("path", _schemas(), ids=lambda p: p.name)
def test_no_object_accepts_unknown_fields(path: Path) -> None:
    """extra=forbid is load-bearing: a silently-ignored field is silent data loss."""

    def check(node: Any, where: str) -> None:
        if isinstance(node, dict):
            if node.get("type") == "object" and "properties" in node:
                assert node.get("additionalProperties") is False, (
                    f"{path.name}:{where} accepts unknown fields"
                )
            for key, value in node.items():
                check(value, f"{where}.{key}")
        elif isinstance(node, list):
            for i, item in enumerate(node):
                check(item, f"{where}[{i}]")

    check(_load(path), "$")


# ------------------------------------------------------------- envelope ----


def test_envelope_declares_every_mandatory_field() -> None:
    """docs/EVENT_CONTRACTS.md §2 lists these; all are required."""
    doc = _load(SCHEMA_DIR / "envelope.v1.json")
    assert set(doc["properties"]) == set(ENVELOPE_FIELDS)
    assert set(doc["required"]) == set(ENVELOPE_FIELDS)


@pytest.mark.parametrize("path", _event_schemas(), ids=lambda p: p.name)
def test_every_event_embeds_the_envelope_by_reference(path: Path) -> None:
    doc = _load(path)
    assert set(doc["required"]) == {"envelope", "payload"}
    assert doc["properties"]["envelope"] == {"$ref": "envelope.v1.json"}


@pytest.mark.parametrize("path", _event_schemas(), ids=lambda p: p.name)
def test_payload_never_redefines_an_envelope_field(path: Path) -> None:
    """docs/EVENT_CONTRACTS.md §2: envelope fields are never nested in payload."""
    payload = _load(path)["properties"]["payload"]
    clash = set(payload.get("properties", {})) & set(ENVELOPE_FIELDS)
    assert not clash, f"{path.name} payload redefines envelope field(s): {sorted(clash)}"


def test_event_time_and_processing_time_are_both_present_and_distinct() -> None:
    """ADR-0026 calls conflating them the most damaging shortcut available here."""
    props = _load(SCHEMA_DIR / "envelope.v1.json")["properties"]
    assert "EVENT TIME" in props["occurred_at"]["description"].upper()
    assert "PROCESSING TIME" in props["ingested_at"]["description"].upper()
    assert props["occurred_at"]["format"] == props["ingested_at"]["format"] == "date-time"


def test_event_id_pattern_pins_uuid_version_7() -> None:
    """Time-ordering is the property the dedup key depends on; `format: uuid`
    alone would accept a v4 and lose it."""
    pattern = _load(SCHEMA_DIR / "envelope.v1.json")["properties"]["event_id"]["pattern"]
    import re

    compiled = re.compile(pattern)
    assert compiled.match("01a094da-017c-7064-9c80-317fa3b1799d"), "a real v7 must match"
    assert not compiled.match("00000000-0000-4000-8000-000000000000"), "a v4 must not match"


# -------------------------------------------------------- trust tiering ----


@pytest.mark.parametrize("path", _event_schemas(), ids=lambda p: p.name)
def test_attacker_controlled_fields_declare_their_tier_and_a_length_bound(path: Path) -> None:
    """docs/SECURITY.md §5.1 and §6.5 of EVENT_CONTRACTS.md.

    An unbounded attacker-controlled string is both a prompt-injection surface
    and a memory-exhaustion one.
    """
    for name, sub in _walk_properties(_load(path)):
        if name not in ATTACKER_CONTROLLED:
            continue
        assert sub.get("x-trust-tier") == "UNTRUSTED", f"{path.name}:{name} declares no trust tier"
        assert isinstance(sub.get("maxLength"), int), f"{path.name}:{name} has no maxLength"


def test_the_attacker_controlled_set_is_actually_exercised() -> None:
    """Guards against the test above silently matching nothing."""
    seen = {
        name
        for path in _event_schemas()
        for name, _ in _walk_properties(_load(path))
        if name in ATTACKER_CONTROLLED
    }
    assert seen == ATTACKER_CONTROLLED, f"unexercised: {sorted(ATTACKER_CONTROLLED - seen)}"


@pytest.mark.parametrize("path", _event_schemas(), ids=lambda p: p.name)
def test_only_declared_attacker_controlled_fields_claim_untrusted(path: Path) -> None:
    """The tier is stamped at ingestion and never removed; a field claiming it
    outside the documented set means SECURITY.md §5.1 needs updating too."""
    tagged = {
        name
        for name, sub in _walk_properties(_load(path))
        if sub.get("x-trust-tier") == "UNTRUSTED"
    }
    assert tagged <= ATTACKER_CONTROLLED, (
        f"undocumented untrusted field(s): {sorted(tagged - ATTACKER_CONTROLLED)}"
    )


# ------------------------------------------------------------ money -------


def test_money_is_an_integer_everywhere_it_appears() -> None:
    """CLAUDE.md §6: money is integer minor units. A float amount on the wire
    would defeat the Money value object the moment it is parsed."""
    found = 0
    for path in _event_schemas():
        for name, sub in _walk_properties(_load(path)):
            if not name.endswith("_minor"):
                continue
            found += 1
            assert sub["type"] == "integer", f"{path.name}:{name} is not an integer"
    assert found, "no monetary field found; this test is not exercising anything"


# ------------------------------------------------------ release ledger ----


def test_manifest_lists_every_committed_schema(manifest: dict[str, Any]) -> None:
    listed = {e["schema"] for e in manifest["released"]} | {
        c["schema"] for c in manifest["components"]
    }
    on_disk = {p.name for p in _schemas()}
    assert listed == on_disk, (
        f"release ledger and docs/contracts/events/ disagree: "
        f"only in ledger {sorted(listed - on_disk)}, only on disk {sorted(on_disk - listed)}"
    )


def test_released_schemas_match_their_recorded_digests(manifest: dict[str, Any]) -> None:
    """A released schema file is immutable (ADR-0026).

    Editing one changes its digest and fails here. The digest can be updated
    deliberately, which is the point: it turns an invisible edit into a
    reviewable diff that says 'a released contract changed'.
    """
    for entry in [*manifest["released"], *manifest["components"]]:
        actual = hashlib.sha256((SCHEMA_DIR / entry["schema"]).read_bytes()).hexdigest()
        assert actual == entry["sha256"], (
            f"{entry['schema']} changed but docs/contracts/RELEASED.json was not updated. "
            f"A released schema is immutable -- a real change needs a new .v2 file and a "
            f"dual-write window (docs/EVENT_CONTRACTS.md §4)."
        )


def test_released_and_planned_topics_are_disjoint(manifest: dict[str, Any]) -> None:
    released = {e["topic"] for e in manifest["released"]}
    planned = {e["topic"] for e in manifest["planned"]}
    assert not released & planned


def test_every_planned_topic_is_documented_in_event_contracts(manifest: dict[str, Any]) -> None:
    text = (ROOT / "docs" / "EVENT_CONTRACTS.md").read_text()
    for entry in [*manifest["released"], *manifest["planned"]]:
        assert entry["topic"] in text, f"{entry['topic']} is not in docs/EVENT_CONTRACTS.md §3"


def test_topic_keys_match_the_documented_keys(manifest: dict[str, Any]) -> None:
    """The key is the entity whose ordering matters, never load balancing."""
    expected = {
        "tx.raw.v1": "account_id",
        "identity.events.v1": "account_id",
        "device.events.v1": "device_id",
    }
    for entry in manifest["released"]:
        assert entry["key"] == expected[entry["topic"]]
        assert len(entry["key_reason"]) > 20, (
            "a key with no stated reason is a load-balancing choice"
        )
