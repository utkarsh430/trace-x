"""Only one module may touch the ground-truth schema.

The database grant is the real control (ADR-0004), and it is verified against a
live PostgreSQL in `tests/integration/`. This test is the cheap structural
companion: it bounds the set of code that could *attempt* a leak to a single
file, so a reviewer knows exactly where to look and an accidental reference
fails immediately rather than at the next integration run.

CLAUDE.md §11: "Agents are never given ground truth in any form, including
indirectly through a feature."
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit, pytest.mark.security]

ROOT = Path(__file__).resolve().parents[2]

# The single module permitted to reference the schema, plus the migration that
# creates it. Both are deliberate, reviewed exceptions.
ALLOWED = {
    Path("data/generator/groundtruth.py"),
    Path("migrations/versions/0002_groundtruth_tables.py"),
    Path("migrations/versions/0001_schemas_roles_grants.py"),
}

SCANNED_ROOTS = ("packages", "data", "services", "mcp_servers")
# No word boundaries: `groundtruth_labels = ...` must be caught too, and the
# right-hand \b would not match it because `_` is a word character. The
# self-test below plants exactly that case.
GROUNDTRUTH = re.compile(r"groundtruth", re.IGNORECASE)

# Names that would indicate a label travelling through application code.
LABEL_TOKENS = re.compile(r"\b(is_fraud|fraud_pattern|causal_evidence_keys)\b")


def _sources() -> list[Path]:
    files: list[Path] = []
    for root in SCANNED_ROOTS:
        base = ROOT / root
        if base.is_dir():
            files.extend(p for p in base.rglob("*.py") if "__pycache__" not in p.parts)
    return sorted(files)


def _executable_source(path: Path) -> str:
    """The module with comments and string literals removed.

    Tokenised rather than pattern-matched. A first attempt used a heuristic on
    line prefixes and flagged docstring *continuation* lines that merely explain
    the control -- documentation about the boundary is exactly what should be
    encouraged. Only executable references matter here, and the database grant
    remains the real control either way.
    """
    import io
    import tokenize

    kept: list[str] = []
    try:
        for token in tokenize.generate_tokens(io.StringIO(path.read_text()).readline):
            if token.type in (tokenize.COMMENT, tokenize.STRING):
                continue
            kept.append(f"{token.start[0]}:{token.string}")
    except (tokenize.TokenError, IndentationError):  # pragma: no cover
        return path.read_text()
    return "\n".join(kept)


# Importing the writer module is the intended way to write ground truth; it is
# the only reference that does not widen the boundary.
_IMPORT_OF_WRITER = re.compile(r"data\.generator\.groundtruth|from data\.generator import")

# `GroundTruthAccessError` is a member of the typed error taxonomy. Naming an
# exception after the control it guards is not a reference to the schema, and
# exempting the one identifier is tighter than allow-listing the whole module.
EXEMPT_IDENTIFIER = re.compile(r"GroundTruthAccessError")


def test_the_scan_covers_real_source() -> None:
    """Guards against the checks below passing vacuously."""
    assert len(_sources()) > 20


def test_only_the_writer_references_the_groundtruth_schema() -> None:
    """No module outside the allow-list may name the schema in executable code."""
    offenders: list[str] = []
    for path in _sources():
        relative = path.relative_to(ROOT)
        if relative in ALLOWED:
            continue
        source = _executable_source(path)
        if _IMPORT_OF_WRITER.search(source):
            # Importing the writer is fine; referencing the schema alongside it
            # is not, so the import tokens are removed before the check.
            source = _IMPORT_OF_WRITER.sub("", source)
        hits = [
            line
            for line in source.splitlines()
            if GROUNDTRUTH.search(line) and not EXEMPT_IDENTIFIER.search(line)
        ]
        offenders.extend(f"{relative}: {h[:90]}" for h in hits)
    assert not offenders, (
        "modules outside the ground-truth writer reference the schema in code:\n  "
        + "\n  ".join(offenders)
    )


def test_the_scan_would_catch_a_real_reference(tmp_path: Path) -> None:
    """A containment test that cannot fail is not a containment test."""
    planted = tmp_path / "leaky.py"
    planted.write_text("groundtruth_labels = 1\n")
    assert GROUNDTRUTH.search(_executable_source(planted))


def test_docstrings_about_the_control_are_not_flagged(tmp_path: Path) -> None:
    """Explaining the boundary must stay encouraged."""
    documented = tmp_path / "documented.py"
    documented.write_text('"""Never reads the groundtruth schema."""\nx = 1\n')
    assert not GROUNDTRUTH.search(_executable_source(documented))


def test_the_allowed_modules_exist() -> None:
    """A stale allow-list would silently widen the boundary."""
    for relative in ALLOWED:
        assert (ROOT / relative).is_file(), f"{relative} is allow-listed but missing"


def test_label_fields_never_appear_in_the_canonical_contract() -> None:
    """A label reaching CanonicalTransaction would route ground truth straight
    into the feature path."""
    text = (ROOT / "packages/trace_core/contracts/canonical.py").read_text()
    code = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    body = code.split('"""', 2)[-1] if '"""' in code else code
    assert not LABEL_TOKENS.search(body.split("class CanonicalTransaction")[-1].split("def ")[0])


def test_the_generator_engine_keeps_labels_beside_events_not_inside_them() -> None:
    """`GeneratedRow` carries the label as a sibling of the event, so a sink can
    write events without ever touching ground truth."""
    from data.generator.engine import GeneratedRow

    fields = set(GeneratedRow.__dataclass_fields__)
    assert {"event", "label"} <= fields
    assert "label" not in str(GeneratedRow.__dataclass_fields__["event"].type)
