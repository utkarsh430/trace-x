"""`GeneratorRunRecord` — provenance for a generation run.

CLAUDE.md §13 forbids any number in `README.md` or `docs/**` that does not cite a
`run_id` resolving to a recorded run. Phase 1 produces real measurements --
generation rate, dataset size, realised fraud rate -- and the full `RunManifest`
of ADR-0017 is a Phase 9 deliverable. Without something in between, Phase 1's
numbers would either go unpublished or be published in violation of the rule.

This is that something: a **machine-assembled** record, never hand-written, in
the same `eval/manifest/` directory `make check-claims` already resolves against.
It is deliberately a strict subset of the eventual manifest -- same discipline,
smaller surface -- and it carries `record_type: GENERATOR` so the claim linter can
refuse to let it back a *quality* claim. A generation run has no model and no LLM
tier, so it can substantiate throughput and size and nothing else.

`dirty_worktree` is recorded honestly. A run from an uncommitted tree is not
publishable (`docs/EVALUATION.md` §8 rule 4), and the record says so rather than
omitting the field.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import platform
import shutil
import subprocess
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Final

ROOT: Final = Path(__file__).resolve().parents[2]
MANIFEST_DIR: Final = ROOT / "eval" / "manifest"
RECORD_TYPE: Final = "GENERATOR"

REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "run_id",
    "record_type",
    "track",
    "git_commit_sha",
    "dirty_worktree",
    "generator_version",
    "seed",
    "fraud_scenario_config_digest",
    "dataset_version",
    "dataset_digest",
    "row_count",
    "env_lock_digest",
    "python_version",
    "started_at",
    "finished_at",
    "validation_policy",
    "measured",
)
"""Every field must be present for the record to resolve.

A record missing one is rejected rather than partially trusted -- the same rule
ADR-0017 applies to the full manifest, for the same reason: a number whose
provenance is incomplete is indistinguishable from one that was invented.
"""


def _git(*args: str) -> str:
    binary = shutil.which("git") or "git"
    result = subprocess.run(  # noqa: S603
        [binary, *args], cwd=ROOT, capture_output=True, text=True, timeout=30
    )
    return result.stdout.strip() if result.returncode == 0 else ""


def git_commit_sha() -> str:
    return _git("rev-parse", "HEAD") or "unknown"


def is_dirty() -> bool:
    """Whether the worktree has uncommitted changes.

    Reported, never suppressed: a run recorded with `dirty_worktree: true` is not
    publishable, and hiding it would make an unpublishable number look publishable.
    """
    return bool(_git("status", "--porcelain"))


def env_lock_digest() -> str:
    lock = ROOT / "requirements.lock"
    if not lock.is_file():
        return "sha256:" + "0" * 64
    return "sha256:" + hashlib.sha256(lock.read_bytes()).hexdigest()


@dataclass(frozen=True, slots=True)
class GeneratorRunRecord:
    """One generation run, recorded completely enough to be cited."""

    run_id: str
    generator_version: str
    seed: int
    fraud_scenario_config_digest: str
    dataset_version: str
    dataset_digest: str
    row_count: int
    started_at: str
    finished_at: str
    validation_policy: str
    measured: dict[str, Any] = field(default_factory=dict)
    record_type: str = RECORD_TYPE
    track: str = "SYNTHETIC"
    git_commit_sha: str = field(default_factory=git_commit_sha)
    dirty_worktree: bool = field(default_factory=is_dirty)
    env_lock_digest: str = field(default_factory=env_lock_digest)
    python_version: str = field(default_factory=platform.python_version)

    def validate(self) -> list[str]:
        """Problems that would stop this record resolving a citation."""
        data = asdict(self)
        problems = [
            f"missing field: {name}" for name in REQUIRED_FIELDS if not _present(data, name)
        ]
        if self.record_type != RECORD_TYPE:
            problems.append(f"record_type must be {RECORD_TYPE!r}")
        if not self.measured:
            problems.append("measured is empty; the record would substantiate nothing")
        return problems

    @property
    def publishable(self) -> bool:
        """Whether a number from this run may appear in docs.

        False on a dirty worktree: the tree that produced the number cannot be
        reconstructed, so the number is unreproducible (EVALUATION.md §8 rule 4).
        """
        return not self.validate() and not self.dirty_worktree

    def write(self, directory: Path | None = None) -> Path:
        target = (directory or MANIFEST_DIR) / f"{self.run_id}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(asdict(self), indent=2, sort_keys=True) + "\n")
        return target


def _present(data: dict[str, Any], name: str) -> bool:
    """A field counts as present if it exists and is not None.

    `False` and `0` are legitimate values -- `dirty_worktree: false` is the good
    case -- so emptiness alone is not the test.
    """
    if name not in data:
        return False
    value = data[name]
    return value is not None and value != ""


def new_run_id(dataset_version: str, started: dt.datetime) -> str:
    """A readable, sortable, collision-resistant id.

    Date-prefixed so a directory listing is chronological, and suffixed with a
    digest of the exact commit and timestamp so two runs of the same dataset on
    the same day are distinguishable.
    """
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    entropy = hashlib.sha256(f"{dataset_version}{stamp}{git_commit_sha()}".encode()).hexdigest()
    return f"gen-{started.strftime('%Y%m%d')}-{dataset_version}-{entropy[:8]}"
