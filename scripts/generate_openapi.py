#!/usr/bin/env python3
"""Generate `docs/contracts/openapi.yaml` FROM the Pydantic models.

`docs/API_CONTRACTS.md` §1 fixes the direction of truth for APIs: Pydantic models
-> OpenAPI -> TypeScript client, and *"never edit the generated spec or client by
hand."* That is the opposite of events (ADR-0028), where JSON Schema is the source
because a schema is a cross-language contract and deriving it from one runtime's
type system would privilege that runtime. An API has exactly one server, so the
server's types are the honest source.

Committed rather than generated on demand, for two reasons. A CI job diffs it
against `main` to catch a breaking change without a version bump (§2), and that
diff needs a stored previous version. And a client generator needs a file it can
fetch without booting the service.

`--check` regenerates into memory and compares, hermetically: the same answer on
a dirty worktree, in CI and in a test. One definition with three callers -- the
Phase 0 secret scan went red in CI and green locally precisely because it had two.

**The version is pinned to the URL major.** `/v1/...` is a parallel surface, not
a mutation (§2), so the document's `info.version` tracks the major and NOT the
service's build version: a spec whose version moved on every deploy would make
the CI diff meaningless, because every comparison would look like a version bump.
"""

from __future__ import annotations

import argparse
import difflib
import os
import sys
from pathlib import Path
from typing import Any, Final

ROOT: Final = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "packages"))
sys.path.insert(0, str(ROOT))

OUTPUT: Final = ROOT / "docs" / "contracts" / "openapi.yaml"

API_MAJOR_VERSION: Final = "1"
"""Tracks the URL major version, never the build.

A spec whose `info.version` changed on every deploy would make the breaking-change
diff useless: every run would show a version bump and nothing would ever be
flagged.
"""

HEADER: Final = """# GENERATED FROM packages/trace_core/contracts/api/ -- DO NOT EDIT BY HAND.
#
# Pydantic models are the source of truth for the API (docs/API_CONTRACTS.md §1).
# Change a model, then run `make codegen-openapi`. A gate in `make verify` and in
# CI regenerates and fails on any diff, so an edit made here is reverted rather
# than merged.
#
# Committed on purpose: the CI breaking-change diff needs a stored previous
# version to compare against, and a client generator needs a file it can fetch
# without booting the service.
"""


def _minimum_environment() -> dict[str, str]:
    """Enough configuration to build the app, and nothing real.

    Generation must not require a database, a Redis or a deployment's
    credentials: the shape of the API is a property of the code, and a document
    that could only be produced on a configured machine would drift on every
    other one. The token is a placeholder that satisfies the length rule and
    authenticates nothing -- it never leaves this process.
    """
    return {
        **os.environ,
        "TRACE_SERVICE_TOKEN_OPENAPI": "x" * 32,
    }


def render() -> str:
    """The committed document, as text."""
    import yaml

    os.environ.update(_minimum_environment())

    from fastapi.openapi.utils import get_openapi
    from services.gateway.app import create_app

    app = create_app()
    document: dict[str, Any] = get_openapi(
        title=app.title,
        version=API_MAJOR_VERSION,
        openapi_version=app.openapi_version,
        summary=app.summary,
        description=app.description,
        routes=app.routes,
    )
    _register_problem_schema(document)
    # sort_keys so the file is stable: an unordered dump would diff on every
    # regeneration and train everyone to ignore the drift gate.
    body = yaml.safe_dump(document, sort_keys=True, default_flow_style=False, width=100)
    return HEADER + body


def _register_problem_schema(document: dict[str, Any]) -> None:
    """Put `Problem` in `components.schemas`, once.

    Error responses reference it by `$ref` and advertise only
    `application/problem+json`, because passing FastAPI a response `model` also
    registers an `application/json` variant -- and the spec would then promise a
    media type this service never serves, which a generated client would be built
    to parse.

    The consequence is that FastAPI does not register the schema itself, so the
    `$ref` would dangle. A spec with a dangling reference is worse than one with a
    spurious media type: a client generator fails on it, or silently emits `any`.
    `tests/contract/test_openapi_contract.py` asserts no reference in the document
    dangles, which is what keeps this function honest.
    """
    from trace_core.contracts.api.problem import Problem

    components = document.setdefault("components", {})
    schemas = components.setdefault("schemas", {})
    schemas["Problem"] = Problem.model_json_schema(ref_template="#/components/schemas/{model}")


def generate() -> int:
    rendered = render()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(rendered, encoding="utf-8")
    import yaml as _yaml

    paths = len(_yaml.safe_load(rendered).get("paths", {}))
    print(f"wrote {OUTPUT.relative_to(ROOT)} ({len(rendered.splitlines())} lines, {paths} paths)")
    return 0


def check() -> int:
    """Fail if the committed document differs from a fresh render."""
    if not OUTPUT.is_file():
        print(
            f"{OUTPUT.relative_to(ROOT)} does not exist. Run `make codegen-openapi` and "
            f"commit the result: the CI breaking-change diff needs a stored version.",
            file=sys.stderr,
        )
        return 1
    fresh = render()
    committed = OUTPUT.read_text(encoding="utf-8")
    if fresh == committed:
        print(f"PASS - {OUTPUT.relative_to(ROOT)} matches the Pydantic models")
        return 0
    diff = "\n".join(
        difflib.unified_diff(
            committed.splitlines(),
            fresh.splitlines(),
            fromfile="committed",
            tofile="regenerated",
            lineterm="",
            n=2,
        )
    )
    print(
        "the committed OpenAPI document is out of date with the API models.\n"
        "  Run `make codegen-openapi` and commit the result. Do not hand-edit the spec:\n"
        "  Pydantic models are the source of truth for APIs (docs/API_CONTRACTS.md §1).\n"
        f"\n{diff[:4000]}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if the committed spec differs from a fresh render (CI gate)",
    )
    raise SystemExit(check() if parser.parse_args().check else generate())
