#!/usr/bin/env python3
"""Breaking-change gate for the committed OpenAPI document (ADR-0036, ADR-0037).

`docs/API_CONTRACTS.md` §2: *"every PR diffs `openapi.yaml` against `main`. A
breaking change without a major-version bump fails the build."*

**The comparison is `oasdiff`, run as a PINNED container image.** Not a
hand-rolled rule engine: the compatibility table in §2 has about a dozen rows,
and the interesting cases are the ones nobody writes down -- a narrowed enum
nested three schemas deep, a `required` added inside a `oneOf`, a widened type
that is compatible one way and not the other. A bespoke implementation would be
wrong in exactly those places and confidently green.

Pinned by tag AND digest, from `[tool.trace_x.tools]`, and run from an image
rather than a binary on `PATH`, so no developer's locally-installed version can
disagree with CI's. A gate whose verdict depends on who ran it is not a gate.

**A self-test proves it still rejects.** `--self-test` feeds it a pair of fixture
documents containing a change §2 classifies as breaking and requires a non-zero
exit. A compatibility checker that has never rejected anything is not known to
work, and this one sits in front of a contract nobody can un-break.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import Final

ROOT: Final = Path(__file__).resolve().parent.parent
SPEC: Final = ROOT / "docs" / "contracts" / "openapi.yaml"
FIXTURES: Final = ROOT / "tests" / "contract" / "fixtures" / "openapi"


def tool_image() -> str:
    """The pinned `oasdiff` reference from the pin matrix."""
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    image = str(data["tool"]["trace_x"].get("tools", {}).get("oasdiff", ""))
    if "@sha256:" not in image:
        raise SystemExit(
            "oasdiff is not pinned by digest in [tool.trace_x.tools]. A moved tag "
            "would silently change what this gate rejects (ADR-0036)."
        )
    return image


def _docker() -> str:
    binary = shutil.which("docker")
    if binary is None:
        raise SystemExit(
            "docker is required to run the pinned oasdiff image. It is pinned rather "
            "than installed on PATH so no local version can disagree with CI's."
        )
    return binary


def compare(base: Path, revision: Path) -> tuple[int, str]:
    """Run `oasdiff breaking base revision`. Returns `(exit code, output)`.

    `--fail-on ERR` makes a breaking change a non-zero exit; warnings do not
    fail the build, because §2 already classifies which changes are breaking and
    a gate that fails on advisories gets disabled.
    """
    with tempfile.TemporaryDirectory() as tmp:
        workspace = Path(tmp)
        shutil.copy(base, workspace / "base.yaml")
        shutil.copy(revision, workspace / "revision.yaml")
        result = subprocess.run(  # noqa: S603
            [
                _docker(),
                "run",
                "--rm",
                "--network=none",
                "-v",
                f"{workspace}:/specs:ro",
                tool_image(),
                "breaking",
                "/specs/base.yaml",
                "/specs/revision.yaml",
                "--fail-on",
                "ERR",
                "--format",
                "text",
            ],
            capture_output=True,
            text=True,
            timeout=180,
        )
    return result.returncode, (result.stdout + result.stderr).strip()


def self_test() -> int:
    """Prove the gate still rejects a change §2 classifies as breaking.

    Two pairs, and both matter. The breaking pair must fail: a checker that
    accepts everything is worse than no checker, because it is believed. The
    compatible pair must pass: a checker that rejects everything gets bypassed
    within a week, and then nothing is checked at all.
    """
    base = FIXTURES / "base.yaml"
    breaking = FIXTURES / "breaking.yaml"
    compatible = FIXTURES / "compatible.yaml"
    for path in (base, breaking, compatible):
        if not path.is_file():
            raise SystemExit(f"missing fixture {path.relative_to(ROOT)}")

    code, output = compare(base, breaking)
    if code == 0:
        print(
            "SELF-TEST FAILED: oasdiff accepted a change docs/API_CONTRACTS.md §2 "
            "classifies as breaking (a required request field added, and an enum "
            f"value removed from a response).\n{output}",
            file=sys.stderr,
        )
        return 1
    print(f"self-test: breaking change correctly rejected (exit {code})")

    code, output = compare(base, compatible)
    if code != 0:
        print(
            "SELF-TEST FAILED: oasdiff rejected a change §2 classifies as compatible "
            f"(an optional field and a new endpoint added). A gate that rejects "
            f"everything is bypassed within a week.\n{output}",
            file=sys.stderr,
        )
        return 1
    print("self-test: compatible change correctly accepted")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, help="the previous spec (e.g. main's)")
    parser.add_argument("--revision", type=Path, default=SPEC, help="the proposed spec")
    parser.add_argument("--self-test", action="store_true", help="prove the gate still rejects")
    args = parser.parse_args()

    if args.self_test:
        return self_test()
    if args.base is None:
        parser.error("--base is required unless --self-test is given")

    code, output = compare(args.base, args.revision)
    if code == 0:
        print("PASS - no breaking change against the base specification")
        return 0
    print(
        "BREAKING CHANGE detected against the base specification.\n"
        "  docs/API_CONTRACTS.md §2: a breaking change requires a NEW MAJOR VERSION,\n"
        "  which is a parallel surface (/v2/...), never a mutation of /v1.\n"
        f"\n{output}",
        file=sys.stderr,
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
