"""The control plane must be real, not placeholder (CLAUDE.md §14).

A future Claude Code session recovers full state from these documents plus git
history. A doc that exists but says nothing is worse than an absent one, because
it looks like coverage.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]

CONTROL_PLANE = {
    "CLAUDE.md": "project constitution",
    "docs/ARCHITECTURE.md": "architecture specification",
    "docs/ROADMAP.md": "implementation roadmap",
    "docs/PROGRESS.md": "progress tracker",
    "docs/TESTING.md": "testing specification",
    "docs/EVALUATION.md": "evaluation specification",
    "docs/SECURITY.md": "security model",
    "docs/API_CONTRACTS.md": "API contract policy",
    "docs/EVENT_CONTRACTS.md": "event contract policy",
    "docs/DATA_ENGINEERING.md": "data engineering specification",
    "docs/FRAUD_SCENARIOS.md": "fraud scenario catalogue",
    "docs/LOCAL_DEVELOPMENT.md": "local development guide",
    "docs/OPERATIONS.md": "operations runbook",
}

PLACEHOLDER = re.compile(r"\b(TODO|TBD|FIXME|coming soon|to be written|lorem ipsum|XXX)\b", re.I)


@pytest.mark.parametrize("path", sorted(CONTROL_PLANE))
def test_control_plane_document_exists(path: str) -> None:
    assert (ROOT / path).is_file(), f"missing {CONTROL_PLANE[path]}: {path}"


@pytest.mark.parametrize("path", sorted(CONTROL_PLANE))
def test_control_plane_document_has_substance(path: str) -> None:
    """Guards against the doc-shaped-hole failure mode."""
    text = (ROOT / path).read_text()
    assert len(text.splitlines()) >= 40, f"{path} is too thin to be a real {CONTROL_PLANE[path]}"
    assert text.count("#") >= 5, f"{path} has almost no structure"


@pytest.mark.parametrize("path", sorted(CONTROL_PLANE))
def test_control_plane_document_has_no_placeholders(path: str) -> None:
    text = (ROOT / path).read_text()
    hits = PLACEHOLDER.findall(text)
    assert not hits, f"{path} contains placeholder markers: {sorted(set(hits))}"


def test_constitution_carries_no_progress_information() -> None:
    """CLAUDE.md holds rules only; status lives in PROGRESS.md (CLAUDE.md preamble)."""
    text = (ROOT / "CLAUDE.md").read_text().lower()
    for leaked in (
        "current phase:",
        "last verified commit",
        "work in progress:",
        "currently failing",
    ):
        assert leaked not in text, f"CLAUDE.md must not carry progress state: found {leaked!r}"


# --------------------------------------------------------------------- ADRs --

ADR_DIR = ROOT / "docs" / "adr"
ADR_FILES = sorted(
    p for p in ADR_DIR.glob("*.md") if p.name[0].isdigit() and p.name != "0000-template.md"
)
REQUIRED_SECTIONS = (
    "## Context",
    "## Decision",
    "## Alternatives Considered",
    "## Consequences",
    "## Status",
)


def test_adrs_exist() -> None:
    assert len(ADR_FILES) >= 20, f"expected the full decision set, found {len(ADR_FILES)}"


@pytest.mark.parametrize("adr", ADR_FILES, ids=lambda p: p.stem)
def test_adr_has_all_required_sections(adr: Path) -> None:
    text = adr.read_text()
    for section in REQUIRED_SECTIONS:
        assert section in text, f"{adr.name} is missing '{section}'"


@pytest.mark.parametrize("adr", ADR_FILES, ids=lambda p: p.stem)
def test_adr_records_a_real_alternative(adr: Path) -> None:
    """An ADR with no genuine alternative has not made a decision."""
    body = adr.read_text().split("## Alternatives Considered", 1)[1].split("## Consequences", 1)[0]
    rows = [ln for ln in body.splitlines() if ln.strip().startswith("|") and "---" not in ln]
    assert len(rows) >= 3, f"{adr.name} lists fewer than two alternatives"


@pytest.mark.parametrize("adr", ADR_FILES, ids=lambda p: p.stem)
def test_adr_states_negative_consequences(adr: Path) -> None:
    """An ADR with only upsides has not been thought through."""
    body = adr.read_text().split("## Consequences", 1)[1].split("## Status", 1)[0]
    assert "**Negative" in body, f"{adr.name} states no negative consequences"


@pytest.mark.parametrize("adr", ADR_FILES, ids=lambda p: p.stem)
def test_adr_has_a_valid_status(adr: Path) -> None:
    tail = adr.read_text().rsplit("## Status", 1)[1]
    assert re.search(r"\b(Accepted|Proposed|Rejected|Superseded)\b", tail), (
        f"{adr.name} has no recognisable status"
    )


def test_adr_numbers_are_unique_and_contiguous() -> None:
    nums = sorted(int(p.name[:4]) for p in ADR_FILES)
    assert len(nums) == len(set(nums)), "duplicate ADR numbers"
    assert nums == list(range(1, len(nums) + 1)), f"ADR numbering has gaps: {nums}"


def test_adr_index_lists_every_adr() -> None:
    index = (ADR_DIR / "README.md").read_text()
    for adr in ADR_FILES:
        assert adr.name in index, f"{adr.name} is missing from the ADR index"


def test_delta_layout_adr_is_not_accepted_before_its_benchmark() -> None:
    """ADR-0015 exists precisely to prevent writing the conventional answer first."""
    adr = next(p for p in ADR_FILES if p.name.startswith("0015"))
    text = adr.read_text()
    # Evidence is a committed report citing a publishable run of at least ten million rows, not
    # merely a non-empty file: the benchmark's own source lives in the same directory, so "any
    # file" would have let the ADR be accepted the moment the harness was written.
    report = ROOT / "benchmarks" / "delta_layout" / "REPORT.md"
    manifests = ROOT / "eval" / "manifest"
    cited = re.compile(r"run_id: (bench-[\w.-]+-delta-layout-[0-9a-f]{8})")
    runs = set(cited.findall(report.read_text())) if report.is_file() else set()
    evidence = set()
    for run_id in runs:
        path = manifests / f"{run_id}.json"
        if not path.is_file():
            continue
        record = json.loads(path.read_text())
        if (
            record.get("subject") == "delta-layout"
            and int(record.get("row_count", 0)) >= 10_000_000
            and record.get("publishable") is True
            and record.get("dirty_worktree") is False
        ):
            evidence.add(run_id)
    if "Status:** **Proposed**" in text or "Status:** Proposed" in text:
        return
    assert evidence, (
        "ADR-0015 must remain Proposed until benchmarks/delta_layout/REPORT.md cites a publishable "
        "run of at least ten million rows whose manifest is committed"
    )
    assert any(run_id in text for run_id in evidence), (
        f"ADR-0015 is no longer Proposed but cites none of the benchmark's runs {sorted(evidence)}"
    )


# ------------------------------------------------------------- referential --


def test_every_adr_referenced_in_docs_exists() -> None:
    """A dangling ADR reference sends a future session looking for nothing."""
    known = {f"ADR-{p.name[:4]}" for p in ADR_FILES}
    docs = [ROOT / "CLAUDE.md", *sorted((ROOT / "docs").rglob("*.md"))]
    dangling: set[str] = set()
    for d in docs:
        if d.parent.name == "adr" and d.name[0].isdigit():
            continue
        for ref in re.findall(r"ADR-\d{4}", d.read_text()):
            if ref not in known and ref != "ADR-0000":
                dangling.add(f"{d.relative_to(ROOT)} -> {ref}")
    assert not dangling, f"dangling ADR references: {sorted(dangling)}"
