"""The committed OpenAPI document, and the gate that guards it.

`docs/API_CONTRACTS.md` §1 makes the Pydantic models the source of truth and the
spec a generated artifact; §2 makes a breaking change without a major-version
bump a build failure. Three things have to hold for that to be more than an
intention, and each is asserted here:

1. the committed document matches the models it is generated from;
2. it describes what the service actually serves -- the right paths, the right
   media types, and no reference that dangles;
3. the breaking-change gate still rejects a breaking change.

(3) is the one that decays silently. A compatibility checker that has stopped
rejecting anything looks exactly like a codebase that has stopped making
breaking changes.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.contract

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "docs" / "contracts" / "openapi.yaml"
FIXTURES = ROOT / "tests" / "contract" / "fixtures" / "openapi"


@pytest.fixture(scope="module")
def spec() -> dict[str, Any]:
    assert SPEC.is_file(), (
        "docs/contracts/openapi.yaml is missing. It is committed on purpose: the CI "
        "breaking-change diff needs a stored previous version to compare against."
    )
    document: dict[str, Any] = yaml.safe_load(SPEC.read_text())
    return document


# --- (1) generated, not hand-written --------------------------------------------


def test_the_committed_spec_matches_the_models() -> None:
    """Runs the same `--check` the verify gate and CI run.

    Hermetic on purpose: regenerating into memory gives the same answer on a
    dirty worktree, in CI and here. One definition with three callers -- the
    Phase 0 secret scan went red in CI and green locally because it had two.
    """
    result = subprocess.run(  # noqa: S603
        [sys.executable, str(ROOT / "scripts" / "generate_openapi.py"), "--check"],
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=180,
    )
    assert result.returncode == 0, (
        f"the committed spec is out of date with the API models. Run "
        f"`make codegen-openapi` and commit the result.\n{result.stdout}\n{result.stderr}"
    )


def test_the_spec_says_it_is_generated(spec: dict[str, Any]) -> None:
    """A generated file that does not say so invites a hand edit that the drift
    gate then reverts, which wastes someone's afternoon."""
    header = SPEC.read_text().splitlines()[0]
    assert "GENERATED" in header and "DO NOT EDIT" in header


def test_the_version_tracks_the_url_major_not_the_build(spec: dict[str, Any]) -> None:
    """A spec whose `info.version` moved on every deploy would make the
    breaking-change diff meaningless: every comparison would show a bump."""
    assert spec["info"]["version"] == "1"
    assert all(path.startswith("/v1/") for path in spec["paths"])


# --- (2) it describes what is actually served -------------------------------------


def test_every_documented_surface_is_present(spec: dict[str, Any]) -> None:
    """API_CONTRACTS §3 lists the gateway's surface. A documented endpoint that
    does not exist is a client built against nothing."""
    assert set(spec["paths"]) == {
        "/v1/transactions",
        "/v1/events/identity",
        "/v1/events/device",
        "/v1/events/authorization",
    }


def test_errors_are_documented_as_rfc_9457_problem_documents(spec: dict[str, Any]) -> None:
    """§4: RFC 9457 *always*. A spec that described FastAPI's default
    `HTTPValidationError` would describe a shape this service never returns, and
    a generated client would be built to parse it."""
    responses = spec["paths"]["/v1/transactions"]["post"]["responses"]
    for status in ("400", "401", "409", "422", "429", "503"):
        assert status in responses, f"{status} is not documented on POST /v1/transactions"
        media = responses[status]["content"]
        assert list(media) == ["application/problem+json"], (
            f"{status} advertises {list(media)}; the service serves problem+json only, "
            f"and a spec promising application/json would be promising a shape it "
            f"never sends"
        )
    assert "Problem" in spec["components"]["schemas"]
    assert "HTTPValidationError" not in spec["components"]["schemas"]


def test_no_reference_in_the_spec_dangles(spec: dict[str, Any]) -> None:
    """A dangling `$ref` fails a client generator, or silently becomes `any`.

    This is what keeps `_register_problem_schema` honest: error responses use a
    `$ref` rather than a response `model` so only `problem+json` is advertised,
    and FastAPI therefore does not register the schema itself.
    """
    referenced = set(re.findall(r"#/components/schemas/([A-Za-z0-9_]+)", json.dumps(spec)))
    defined = set(spec["components"]["schemas"])
    assert not (referenced - defined), f"dangling $ref(s): {sorted(referenced - defined)}"


def test_the_problem_schema_enumerates_nothing_it_should_not(spec: dict[str, Any]) -> None:
    """The problem document carries a trace id and a request id, because §4 says
    every error path must let an operator pivot to the trace it belongs to."""
    problem = spec["components"]["schemas"]["Problem"]
    assert {"type", "title", "status", "detail", "instance", "trace_id", "request_id"} <= set(
        problem["properties"]
    )


def test_money_is_an_integer_in_the_published_contract(spec: dict[str, Any]) -> None:
    """CLAUDE.md §6, as a client would read it. A spec that typed `amount_minor`
    as a number would have generated clients sending floats."""
    request = spec["components"]["schemas"]["TransactionRequest"]["properties"]
    assert request["amount_minor"]["type"] == "integer"


def test_processing_time_is_absent_from_the_request_contract(spec: dict[str, Any]) -> None:
    """`ingested_at` is stamped by the gateway. A client able to set it would be
    setting our lag metric (ADR-0026)."""
    assert "ingested_at" not in spec["components"]["schemas"]["TransactionRequest"]["properties"]


def test_no_ground_truth_field_appears_anywhere_in_the_spec(spec: dict[str, Any]) -> None:
    """The published contract is the most visible surface in the system. A label
    reaching it would be the leak nobody had to look for (CLAUDE.md §11)."""
    rendered = json.dumps(spec).lower()
    for forbidden in ("is_fraud", "fraud_pattern", "causal_evidence", "groundtruth"):
        assert forbidden not in rendered, f"{forbidden!r} appears in the published API spec"


# --- (3) the gate still rejects -----------------------------------------------------


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    probe = subprocess.run(  # noqa: S603
        [shutil.which("docker") or "docker", "info", "--format", "{{.ServerVersion}}"],
        capture_output=True,
        timeout=15,
    )
    return probe.returncode == 0


def test_the_breaking_change_gate_still_rejects() -> None:
    """The self-test, run as a test.

    A compatibility checker that has stopped rejecting anything looks exactly
    like a codebase that has stopped making breaking changes, and only one of
    those is good news.
    """
    if not _docker_available():
        pytest.skip(
            "SKIPPED (NOT PASSED): docker is unavailable, so the pinned oasdiff image "
            "cannot run and the breaking-change gate is NOT exercised. The tool is "
            "pinned by digest rather than installed on PATH so no local version can "
            "disagree with CI's (ADR-0036)."
        )
    result = subprocess.run(  # noqa: S603
        [sys.executable, str(ROOT / "scripts" / "openapi_diff.py"), "--self-test"],
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=300,
    )
    assert result.returncode == 0, (
        f"the breaking-change gate's self-test failed.\n{result.stdout}\n{result.stderr}"
    )
    assert "correctly rejected" in result.stdout
    assert "correctly accepted" in result.stdout


def test_the_fixtures_encode_the_documented_compatibility_rules() -> None:
    """The fixtures are the gate's own test data; if they stopped containing a
    breaking change, the self-test would pass on nothing."""
    base = yaml.safe_load((FIXTURES / "base.yaml").read_text())
    breaking = yaml.safe_load((FIXTURES / "breaking.yaml").read_text())
    compatible = yaml.safe_load((FIXTURES / "compatible.yaml").read_text())

    base_required = set(base["components"]["schemas"]["ThingRequest"]["required"])
    breaking_required = set(breaking["components"]["schemas"]["ThingRequest"]["required"])
    assert breaking_required > base_required, (
        "the breaking fixture no longer makes an optional request field required, "
        "which §2 classifies as breaking"
    )

    base_enum = set(base["components"]["schemas"]["ThingResponse"]["properties"]["status"]["enum"])
    breaking_enum = set(
        breaking["components"]["schemas"]["ThingResponse"]["properties"]["status"]["enum"]
    )
    assert breaking_enum < base_enum, "the breaking fixture no longer narrows a response enum"

    assert set(compatible["paths"]) > set(base["paths"]), (
        "the compatible fixture no longer adds an endpoint"
    )
