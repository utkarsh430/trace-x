#!/usr/bin/env python3
"""Load-test runner, record writer and report renderer for `trace-gateway`.

ROADMAP Phase 2 states the targets this exists to measure: **p99 < 100 ms,
p50 < 20 ms at 500 TPS sustained**, and **zero 5xx over a 10-minute run**. Its
exit condition is *"load report committed with real measured numbers"*, and the
three words that do the work there are *real*, *measured* and *numbers*.

So the design follows from CLAUDE.md §13 rather than from convenience:

* **Every number comes out of the k6 summary.** There is no code path that
  writes a latency, a count or a rate that k6 did not report. A metric the
  summary does not carry raises instead of defaulting -- "zero 5xx" and "5xx
  were never counted" are different facts and only one of them is evidence.
* **The exit conditions are asserted mechanically.** k6's own thresholds fail the
  run early, which is useful, but a phase exit condition that lives only in a
  threshold is one a future commit can relax in the same diff that failed it.
  `evaluate()` is the binding check and it decides this process's exit code.
* **The run's behaviour is pinned to the run.** The rule pack digest, threshold
  config digest and feature set version are read from the *running gateway* --
  never hardcoded, never assumed from the checkout -- because a p99 that cannot
  be attributed to the rules that produced it substantiates nothing. The
  checkout is cross-checked against the live values and a disagreement aborts:
  it would mean the recorded `git_commit_sha` does not describe the behaviour
  measured.
* **`dirty_worktree` is recorded honestly and enforced.** A dirty-tree run is not
  publishable (`docs/EVALUATION.md` §8 rule 4), so the record is still written --
  the run is evidence, not a secret -- but the report is **not**, because writing
  it would put unreproducible numbers into a document `make check-claims` scans.

The instrument is the **pinned k6 container image** from `pyproject.toml`
`[tool.trace_x.tools]` (ADR-0036), never a binary on PATH. See
`tests/load/k6/README.md` for why, and for how to run this.

Run: `make load-gateway` (needs `make up` and a running gateway).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Final

ROOT: Final = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "packages"))
sys.path.insert(0, str(ROOT))

from data.generator.record import (  # noqa: E402
    env_lock_digest,
    git_commit_sha,
    is_dirty,
)

from trace_core.domain.errors import TraceXError  # noqa: E402
from trace_core.security.service_tokens import (  # noqa: E402
    ENV_PREFIX as SERVICE_TOKEN_ENV_PREFIX,
)

MANIFEST_DIR: Final = ROOT / "eval" / "manifest"
REPORT_FOR: Final[dict[str, Path]] = {
    "representative": ROOT / "benchmarks" / "gateway" / "REPORT.md",
    "triage-saturation": ROOT / "benchmarks" / "gateway" / "ADVERSARIAL.md",
}
"""One report per profile, never a shared path.

If both wrote to `REPORT.md` the adversarial run would silently overwrite the
acceptance evidence with numbers from a workload that is not the gate -- and the
file would still look like the gate's report."""
K6_SCRIPT_DIR: Final = ROOT / "tests" / "load" / "k6"
PROFILES: Final[dict[str, str]] = {
    "representative": "representative.js",
    "triage-saturation": "triage_saturation.js",
}
"""The two workload profiles, and why there are two.

`representative` is the **canonical Phase 2 acceptance gate**. Its entity model
is derived from `eval/track_a/eval-v1.manifest.json` -- the project's own frozen
statement of what normal traffic looks like -- and its population is derived from
the offered rate so that per-account velocity stays realistic as the rate changes.

`triage-saturation` is the original profile, preserved unweakened. It
concentrates 80% of load onto 5% of a 20,000-account pool, which at 500 TPS is
1,440 transactions per account per hour against velocity thresholds of 5/minute
and 40/hour. It measured 91.7% of requests opening an investigation where the
frozen dataset produces 0.222%. That makes it a genuine and useful adversarial
benchmark -- it characterises the system when nearly every request triages -- and
a misleading acceptance gate, because it measures the cost of opening
investigations rather than the cost of scoring. Both facts are worth having, so
both profiles exist and the report says which one produced it.
"""

DEFAULT_PROFILE: Final = "representative"
SERVICE: Final = "trace-gateway"
TOOL: Final = "k6"

# docs/ROADMAP.md § Phase 2 TARGETS. Budgets, not measurements: they are what
# the measured values are compared against, and they are stated once here so a
# future edit to the target is a visible one-line diff rather than a number
# quietly changed in a report.
TARGET_P99_MS: Final = 100.0
TARGET_P50_MS: Final = 20.0
DEFAULT_TARGET_TPS: Final = 500
DEFAULT_DURATION_S: Final = 600

# An arrival-rate run that starts fewer iterations than it offered did not
# sustain the target. k6's own `dropped_iterations` catches the usual case; this
# is the independent arithmetic check, because a shortfall can also come from a
# late start or an early stop, which drop nothing.
MIN_RATE_FRACTION: Final = 0.99

TOKEN_ENV: Final = "TRACE_LOAD_TOKEN"  # noqa: S105 - a variable NAME, not a credential
"""The full `<token_id>.<secret>` presented as a Bearer credential.

Passed to the container by NAME (`docker run -e TRACE_LOAD_TOKEN`) rather than as
`-e NAME=value`, so the secret never appears in an argument list that `ps`, a
shell history or a CI log would capture (docs/SECURITY.md §9).
"""

SERVICE_TOKEN_PREFIX: Final = SERVICE_TOKEN_ENV_PREFIX
"""The prefix the gateway itself reads.

Imported from `trace_core.security.service_tokens` rather than restated, so the
harness cannot drift from the variable the service actually looks for -- a
mismatch would surface as "no service token available" on a correctly configured
stack, and the operator would go looking in the wrong place.
"""


class LoadHarnessError(TraceXError):
    """The harness cannot produce a trustworthy measurement.

    Raised rather than warned about, in every case where continuing would
    produce a number that looks like a result and is not one.
    """


# ----------------------------------------------------------- the instrument --


def tool_images() -> dict[str, str]:
    """The pinned non-Python tool images (ADR-0036)."""
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return dict(data["tool"]["trace_x"].get("tools", {}))


def k6_image() -> str:
    """The pinned k6 image reference, or an explanation of why there is none.

    Read from `pyproject.toml` rather than accepted as an argument: the pin is
    the point. A load test run on whatever k6 happened to be installed produces
    numbers that cannot be compared with the next run's, and -- worse -- look
    exactly like numbers that can.
    """
    image = tool_images().get(TOOL)
    if not image:
        raise LoadHarnessError(
            "no pinned k6 image in pyproject.toml [tool.trace_x.tools]. The load "
            "harness deliberately has no PATH fallback: an unpinned instrument "
            "silently changes what a published benchmark number means (ADR-0036)."
        )
    if "@sha256:" not in image:
        raise LoadHarnessError(
            f"the k6 image {image!r} is pinned by tag only. A tag can be moved and a "
            f"digest cannot, so a tag-only pin does not make two runs comparable."
        )
    return image


def docker_binary() -> str:
    binary = shutil.which("docker")
    if binary is None:
        raise LoadHarnessError(
            "docker is required: the load generator runs as the pinned image "
            "(ADR-0036), never as a local binary. Start Docker and retry."
        )
    return binary


def k6_version(image: str) -> str:
    """The version reported by the image itself.

    Read from the container rather than parsed out of the pin, so the recorded
    `tool_version` describes what actually ran. If the two ever disagree, the
    image that ran is the fact.
    """
    result = subprocess.run(  # noqa: S603
        [docker_binary(), "run", "--rm", image, "version"],
        capture_output=True,
        text=True,
        timeout=180,
        check=False,
    )
    if result.returncode != 0:
        raise LoadHarnessError(
            f"could not read the k6 version from {image}: {result.stderr.strip() or 'no output'}"
        )
    match = re.search(r"v\d+\.\d+\.\d+", result.stdout)
    if match is None:
        raise LoadHarnessError(
            f"the k6 image reported an unparseable version: {result.stdout.strip()!r}. "
            f"A run whose instrument version is unknown cannot be compared with another."
        )
    return match.group(0)


# ------------------------------------------------------- gateway provenance --


@dataclass(frozen=True, slots=True)
class GatewayIdentity:
    """What the instance under test says it is, and what it is running.

    Every field is read from the live gateway. Hardcoding any of them would let
    a report attribute a p99 to rules that were never loaded -- which is the
    specific way a load report becomes decorative.
    """

    service: str
    service_version: str
    rule_pack_id: str
    rule_pack_digest: str
    threshold_config_digest: str
    feature_set_version: str
    probe_degraded: bool


def _http_json(
    url: str, *, method: str, token: str | None, body: dict[str, Any] | None, timeout: float
) -> tuple[int, dict[str, Any]]:
    """One JSON request. Returns the status and the decoded body."""
    scheme = urllib.parse.urlparse(url).scheme
    if scheme not in {"http", "https"}:
        # Defence in depth: the base URL is operator-supplied, and without this
        # a `file:` URL would turn a probe into a local file read.
        raise LoadHarnessError(f"refusing a non-HTTP base URL: {url!r}")
    payload = json.dumps(body).encode() if body is not None else None
    headers = {"Accept": "application/json"}
    if payload is not None:
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["X-Idempotency-Key"] = f"probe-{uuid.uuid4().hex}"
    request = urllib.request.Request(url, data=payload, headers=headers, method=method)  # noqa: S310
    try:
        # Suppressed deliberately: the scheme is asserted above, and the URL is
        # built from the operator's own --base-url, never from remote input. A
        # file:/ or custom scheme cannot reach this call.
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 # nosec B310
            return int(response.status), json.loads(response.read() or b"{}")
    except urllib.error.HTTPError as exc:
        try:
            return int(exc.code), json.loads(exc.read() or b"{}")
        except ValueError:
            return int(exc.code), {}
    except (urllib.error.URLError, TimeoutError) as exc:
        raise LoadHarnessError(
            f"the gateway at {url} is not reachable ({exc}). Start it with `make up` "
            f"before measuring it."
        ) from exc


def probe_gateway(base_url: str, token: str) -> GatewayIdentity:
    """Read the live identity and configuration digests from the gateway.

    One real `POST /v1/transactions`, because that response is the only place the
    gateway publishes the digests that produced a decision (`RiskDecision` carries
    `rule_pack_digest`, `threshold_config_digest` and `feature_set_version`
    precisely so "which rules were live at 14:03?" is answerable from the
    response). It costs one transaction and it is sent before the measurement
    window opens, so it does not appear in any figure.
    """
    base = base_url.rstrip("/")
    status, health = _http_json(f"{base}/healthz", method="GET", token=None, body=None, timeout=10)
    if status != 200 or not health.get("service") or not health.get("version"):
        raise LoadHarnessError(
            f"GET {base}/healthz returned {status} {health!r}; the harness cannot record "
            f"which service and version it measured."
        )

    now = dt.datetime.now(dt.UTC).replace(microsecond=0)
    probe_body: dict[str, Any] = {
        "transaction_id": f"tx_probe_{uuid.uuid4().hex[:16]}",
        "account_id": "acct_000001",
        "amount_minor": 1000,
        "currency": "USD",
        "occurred_at": now.isoformat().replace("+00:00", "Z"),
        "merchant_id": "mrch_00001",
        "device_id": "dev_000001",
        "ip_id": "ip_00001",
        "channel": "CARD_NOT_PRESENT",
        "entry_mode": "ECOMMERCE",
    }
    status, decision = _http_json(
        f"{base}/v1/transactions", method="POST", token=token, body=probe_body, timeout=30
    )
    if status != 200:
        raise LoadHarnessError(
            f"the configuration probe returned {status}: {decision!r}. The digests that "
            f"attribute a measurement to the behaviour that produced it are only "
            f"published on a successful decision, so the run is not started."
        )
    missing = [
        key
        for key in ("rule_pack_id", "rule_pack_digest", "threshold_config_digest")
        if not decision.get(key)
    ]
    if missing or not decision.get("feature_set_version"):
        raise LoadHarnessError(
            f"the gateway's decision omits {missing or ['feature_set_version']}; without "
            f"them a recorded p99 cannot be attributed to the rules that produced it "
            f"(check_claims.py LOADTEST_REQUIRED)."
        )
    return GatewayIdentity(
        service=str(health["service"]),
        service_version=str(health["version"]),
        rule_pack_id=str(decision["rule_pack_id"]),
        rule_pack_digest=str(decision["rule_pack_digest"]),
        threshold_config_digest=str(decision["threshold_config_digest"]),
        feature_set_version=str(decision["feature_set_version"]),
        probe_degraded=bool(decision.get("degraded", False)),
    )


def local_configuration() -> dict[str, str] | None:
    """The digests this checkout would produce, or `None` if it cannot say.

    Corroboration, not the source of truth -- the live gateway is that. Its job
    is to catch the case where the deployment is running something other than
    this commit, which would make the recorded `git_commit_sha` describe
    behaviour that never ran.
    """
    try:
        from trace_core.features.definitions import ONLINE_FEATURES
        from trace_core.features.spec import FEATURE_SET_VERSION
        from trace_core.rules.loader import default_loader
        from trace_core.scoring.banding import load_thresholds

        pack = default_loader(frozenset(ONLINE_FEATURES.ids)).load()
        return {
            "rule_pack_id": pack.pack_id,
            "rule_pack_digest": pack.digest,
            "threshold_config_digest": load_thresholds().digest,
            "feature_set_version": FEATURE_SET_VERSION,
        }
    except Exception as exc:
        print(f"  note: the checkout's configuration could not be loaded ({exc}).")
        print("        The live gateway's digests are still authoritative and recorded.")
        return None


def assert_configuration_agrees(live: GatewayIdentity, local: dict[str, str] | None) -> None:
    """Abort when the deployment is not running this commit.

    The record pairs a `git_commit_sha` with the digests that produced the
    numbers. If the gateway is serving a different rule pack from the one in the
    worktree, that pairing is a fiction and nobody reading the report later could
    detect it.
    """
    if local is None:
        return
    disagreements = [
        f"{key}: gateway {getattr(live, key)!r} != checkout {value!r}"
        for key, value in local.items()
        if getattr(live, key) != value
    ]
    if disagreements:
        raise LoadHarnessError(
            "the running gateway is not serving this checkout's configuration:\n  "
            + "\n  ".join(disagreements)
            + "\nThe run is not started: its record would pair this commit's SHA with "
            "digests this commit does not produce. Restart the gateway on this commit."
        )


def resolve_token(explicit: str | None) -> str:
    """The Bearer credential, from the argument, the environment, or a configured pair.

    Never printed, never written to the record, never passed on a command line.
    The last resort -- assembling `<id>.<secret>` from the `TRACE_SERVICE_TOKEN_*`
    variables the gateway itself reads -- exists so a local run works from the
    same `.env` the stack was started with, without a second place to keep a
    credential in sync.
    """
    if explicit:
        return explicit
    if value := os.environ.get(TOKEN_ENV):
        return value
    configured = {
        key[len(SERVICE_TOKEN_PREFIX) :].lower(): value
        for key, value in os.environ.items()
        if key.startswith(SERVICE_TOKEN_PREFIX) and value
    }
    if not configured:
        raise LoadHarnessError(
            f"no service token available. Set {TOKEN_ENV}=<token_id>.<secret>, or export "
            f"the {SERVICE_TOKEN_PREFIX}<ID> variable the gateway was started with. "
            f"Every request is authenticated (docs/SECURITY.md §3, Plane B), so a run "
            f"without one would measure the 401 path."
        )
    token_id, secret = sorted(configured.items())[0]
    return f"{token_id}.{secret}"


# ------------------------------------------------------------- running k6 ---


def container_base_url(base_url: str) -> str:
    """The URL as seen from inside the container.

    `localhost` inside a container is the container. Rewriting it here rather
    than making the operator remember is the difference between a clear run and
    ten minutes of connection refusals reported as a load result.
    """
    parsed = urllib.parse.urlparse(base_url)
    if parsed.hostname in {"localhost", "127.0.0.1", "::1"}:
        port = f":{parsed.port}" if parsed.port else ""
        return f"{parsed.scheme}://host.docker.internal{port}{parsed.path}".rstrip("/")
    return base_url.rstrip("/")


def k6_command(
    *,
    script: str,
    docker: str,
    image: str,
    base_url: str,
    target_tps: int,
    duration_s: int,
    seed: int,
    nonce: str,
    pre_allocated_vus: int,
    max_vus: int,
    out_dir: Path,
) -> list[str]:
    """The exact argument vector, built separately so it can be asserted on.

    The credential is **not** among these arguments, by construction: it is
    passed as a bare `-e TRACE_LOAD_TOKEN`, which tells Docker to inherit the
    value from this process's environment. A `-e NAME=value` form would put the
    secret into an argument list that `ps`, a shell history and a CI log all
    capture (docs/SECURITY.md §9), and `tests/unit/test_load_harness.py` asserts
    it does not appear here.
    """
    command = [
        docker,
        "run",
        "--rm",
        # Linux needs the mapping spelled out; Docker Desktop already provides
        # the name and ignores a duplicate.
        "--add-host=host.docker.internal:host-gateway",
        "-v",
        f"{K6_SCRIPT_DIR}:/scripts:ro",
        "-v",
        f"{out_dir}:/out",
        "-e",
        # By name: the value is inherited from this process's environment so the
        # secret never reaches an argument list.
        TOKEN_ENV,
        "-e",
        f"BASE_URL={container_base_url(base_url)}",
        "-e",
        f"TARGET_TPS={target_tps}",
        "-e",
        f"DURATION={duration_s}s",
        "-e",
        f"SEED={seed}",
        "-e",
        f"RUN_NONCE={nonce}",
        "-e",
        f"PRE_ALLOCATED_VUS={pre_allocated_vus}",
        "-e",
        f"MAX_VUS={max_vus}",
        "-e",
        "SUMMARY_PATH=/out/summary.json",
    ]
    if os.name == "posix":
        # So the summary lands owned by the operator rather than by root.
        command[3:3] = ["--user", f"{os.getuid()}:{os.getgid()}"]
    command += [image, "run", f"/scripts/{script}"]
    return command


def run_k6(
    *,
    script: str,
    image: str,
    base_url: str,
    token: str,
    target_tps: int,
    duration_s: int,
    seed: int,
    nonce: str,
    pre_allocated_vus: int,
    max_vus: int,
    out_dir: Path,
) -> int:
    """Run the load profile. Returns k6's exit code.

    k6's output is not captured: a ten-minute run with no visible progress is
    indistinguishable from a hang, and the operator needs to see the rate holding.
    The machine-readable summary comes back through the mounted directory.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    command = k6_command(
        script=script,
        docker=docker_binary(),
        image=image,
        base_url=base_url,
        target_tps=target_tps,
        duration_s=duration_s,
        seed=seed,
        nonce=nonce,
        pre_allocated_vus=pre_allocated_vus,
        max_vus=max_vus,
        out_dir=out_dir,
    )
    environment = {**os.environ, TOKEN_ENV: token}
    result = subprocess.run(command, env=environment, check=False)  # noqa: S603
    return result.returncode


# ------------------------------------------------------- reading the result --


def load_summary(out_dir: Path) -> dict[str, Any]:
    path = out_dir / "summary.json"
    if not path.is_file():
        raise LoadHarnessError(
            f"k6 wrote no summary to {path}. Without it there is no measurement, and "
            f"the harness will not write a record or a report from a run it cannot read."
        )
    try:
        data: dict[str, Any] = json.loads(path.read_text())
    except ValueError as exc:
        raise LoadHarnessError(f"the k6 summary at {path} is not valid JSON: {exc}") from exc
    if not isinstance(data.get("metrics"), dict):
        raise LoadHarnessError(f"the k6 summary at {path} carries no metrics block.")
    return data


def _metric(summary: dict[str, Any], name: str) -> dict[str, Any]:
    metric = summary["metrics"].get(name)
    if not isinstance(metric, dict) or not isinstance(metric.get("values"), dict):
        raise LoadHarnessError(
            f"the k6 summary has no '{name}' metric. The harness will not substitute a "
            f"default: an absent counter and a counter that read zero are different "
            f"facts, and only one of them is evidence (CLAUDE.md §13)."
        )
    values: dict[str, Any] = metric["values"]
    return values


def counter(summary: dict[str, Any], name: str) -> int:
    values = _metric(summary, name)
    if "count" not in values:
        raise LoadHarnessError(f"the '{name}' counter in the k6 summary has no count.")
    return int(values["count"])


def trend(summary: dict[str, Any], name: str, stat: str) -> float:
    values = _metric(summary, name)
    if stat not in values:
        raise LoadHarnessError(
            f"the '{name}' trend in the k6 summary has no '{stat}'. Check "
            f"`summaryTrendStats` in the profile's k6 script."
        )
    return float(values[stat])


@dataclass(frozen=True, slots=True)
class Measured:
    """Everything the run measured. Nothing here is computed from a default.

    Latency is reported from two vantage points on purpose. `client_*` is
    `http_req_duration` -- what a caller experiences, which is what the ROADMAP
    target is about. `server_*` is the gateway's own `RiskDecision.latency_ms`,
    which is what attributes a miss to the scoring path rather than to the
    network or to the load generator.
    """

    client_p50_ms: float
    client_p90_ms: float
    client_p95_ms: float
    client_p99_ms: float
    client_avg_ms: float
    client_min_ms: float
    client_max_ms: float
    server_p50_ms: float
    server_p99_ms: float
    server_samples: int
    requests: int
    iterations: int
    dropped_iterations: int
    test_run_duration_s: float
    achieved_tps: float
    http_2xx: int
    http_4xx: int
    http_429: int
    http_5xx: int
    unparseable_responses: int
    degraded_responses: int
    band_low: int
    band_medium: int
    band_high: int
    band_critical: int


def extract(summary: dict[str, Any]) -> Measured:
    """Read the measurement out of the k6 summary, or fail saying what is absent."""
    state = summary.get("state")
    if not isinstance(state, dict) or "testRunDurationMs" not in state:
        raise LoadHarnessError(
            "the k6 summary carries no run duration, so the achieved rate cannot be "
            "computed and 'at 500 TPS sustained' cannot be checked."
        )
    duration_s = float(state["testRunDurationMs"]) / 1000.0
    iterations = counter(summary, "iterations")
    if duration_s <= 0:
        raise LoadHarnessError("the k6 summary reports a non-positive run duration.")
    return Measured(
        client_p50_ms=trend(summary, "http_req_duration", "p(50)"),
        client_p90_ms=trend(summary, "http_req_duration", "p(90)"),
        client_p95_ms=trend(summary, "http_req_duration", "p(95)"),
        client_p99_ms=trend(summary, "http_req_duration", "p(99)"),
        client_avg_ms=trend(summary, "http_req_duration", "avg"),
        client_min_ms=trend(summary, "http_req_duration", "min"),
        client_max_ms=trend(summary, "http_req_duration", "max"),
        server_p50_ms=trend(summary, "gateway_server_latency_ms", "p(50)"),
        server_p99_ms=trend(summary, "gateway_server_latency_ms", "p(99)"),
        server_samples=int(trend(summary, "gateway_server_latency_ms", "count")),
        requests=counter(summary, "http_reqs"),
        iterations=iterations,
        dropped_iterations=counter(summary, "dropped_iterations"),
        test_run_duration_s=duration_s,
        achieved_tps=iterations / duration_s,
        http_2xx=counter(summary, "gateway_http_2xx"),
        http_4xx=counter(summary, "gateway_http_4xx"),
        http_429=counter(summary, "gateway_http_429"),
        http_5xx=counter(summary, "gateway_http_5xx"),
        unparseable_responses=counter(summary, "gateway_unparseable_responses"),
        degraded_responses=counter(summary, "gateway_degraded_responses"),
        band_low=counter(summary, "gateway_band_low"),
        band_medium=counter(summary, "gateway_band_medium"),
        band_high=counter(summary, "gateway_band_high"),
        band_critical=counter(summary, "gateway_band_critical"),
    )


# ---------------------------------------------------------------- verdicts ---

INTEGRITY: Final = "INTEGRITY"
"""The measurement itself is not trustworthy. No report is written."""

TARGET: Final = "TARGET"
"""The measurement is sound and the ROADMAP target was or was not met. Either
way it is recorded and published as found (CLAUDE.md §17)."""


@dataclass(frozen=True, slots=True)
class Verdict:
    name: str
    kind: str
    passed: bool
    detail: str


def evaluate(measured: Measured, *, target_tps: int) -> list[Verdict]:
    """The binding assertions. k6's thresholds are an early warning; this decides.

    Split into INTEGRITY and TARGET because they demand different responses. An
    integrity failure means the run measured something other than what it claims
    to -- a rate-limited run measures the limiter, a run with 4xx responses
    measures the validation layer -- and publishing those numbers under a
    latency heading would be a false claim however honestly they were collected.
    A target failure is a real result about a real system, and CLAUDE.md §17
    forbids concealing it.
    """
    required_tps = target_tps * MIN_RATE_FRACTION
    return [
        Verdict(
            "requests_were_made",
            INTEGRITY,
            measured.iterations > 0,
            f"{measured.iterations} iterations completed; a run with no data substantiates nothing",
        ),
        Verdict(
            "target_rate_sustained",
            INTEGRITY,
            measured.achieved_tps >= required_tps,
            f"achieved {measured.achieved_tps:.1f} TPS against a {target_tps} TPS target "
            f"(floor {required_tps:.1f}); below it, the latency describes a smaller test "
            f"than the one claimed",
        ),
        Verdict(
            "no_dropped_iterations",
            INTEGRITY,
            measured.dropped_iterations == 0,
            f"{measured.dropped_iterations} iterations were never started: offered load "
            f"that never left the generator",
        ),
        Verdict(
            "not_rate_limited",
            INTEGRITY,
            measured.http_429 == 0,
            f"{measured.http_429} responses were 429. A rate-limited run measures the "
            f"limiter, not the scoring path: raise TRACE_RATE_LIMIT_PER_MINUTE above "
            f"target_tps * 60 for the load window, or spread the load over more tokens",
        ),
        Verdict(
            "no_client_errors",
            INTEGRITY,
            measured.http_4xx == 0,
            f"{measured.http_4xx} responses were 4xx; a rejected request exercises the "
            f"validation layer, and its latency is not the scoring path's",
        ),
        Verdict(
            "responses_were_readable",
            INTEGRITY,
            measured.unparseable_responses == 0,
            f"{measured.unparseable_responses} successful responses could not be parsed, "
            f"so the band and degraded counts below them are incomplete",
        ),
        Verdict(
            "zero_5xx",
            TARGET,
            measured.http_5xx == 0,
            f"{measured.http_5xx} server errors over {measured.test_run_duration_s:.0f}s "
            f"(ROADMAP Phase 2 exit condition: zero)",
        ),
        Verdict(
            "p99_under_budget",
            TARGET,
            measured.client_p99_ms < TARGET_P99_MS,
            f"p99 {measured.client_p99_ms:.2f} ms against a {TARGET_P99_MS:.0f} ms budget",
        ),
        Verdict(
            "p50_under_budget",
            TARGET,
            measured.client_p50_ms < TARGET_P50_MS,
            f"p50 {measured.client_p50_ms:.2f} ms against a {TARGET_P50_MS:.0f} ms budget",
        ),
    ]


def failures(verdicts: list[Verdict], kind: str) -> list[Verdict]:
    return [v for v in verdicts if v.kind == kind and not v.passed]


# ------------------------------------------------------------- the record ---


@dataclass
class LoadTestRunRecord:
    """Provenance for a load run, in the shape `scripts/check_claims.py` resolves.

    Every field in that file's `LOADTEST_REQUIRED` is present and machine-assembled.
    The three configuration fields come from the running gateway, because a p99
    that cannot be attributed to the rules and thresholds that produced it is a
    number with no subject.
    """

    run_id: str
    service: str
    service_version: str
    tool_version: str
    target_tps: int
    duration_s: int
    rule_pack_id: str
    rule_pack_digest: str
    threshold_config_digest: str
    feature_set_version: str
    degraded_mode: bool
    seed: int
    tool_image: str
    k6_exit_code: int
    verdicts: list[dict[str, Any]]
    measured: dict[str, Any]
    started_at: str
    finished_at: str
    workload_profile: str = DEFAULT_PROFILE
    """Which workload produced these numbers.

    Recorded because the same gateway measured 91.7% triage under one profile and
    0.222% under the frozen dataset's own distribution: a latency figure without
    its workload is as unattributable as one without its rule pack digest."""

    record_type: str = "LOADTEST"
    track: str = "SYNTHETIC"
    tool: str = TOOL
    git_commit_sha: str = field(default_factory=git_commit_sha)
    dirty_worktree: bool = field(default_factory=is_dirty)
    env_lock_digest: str = field(default_factory=env_lock_digest)
    python_version: str = field(default_factory=platform.python_version)
    host_platform: str = field(default_factory=platform.platform)
    """The machine the measurement was taken on.

    Not required by `check_claims.py`, and recorded anyway. A latency number is
    a property of a host as much as of a service: the same gateway measured
    through Docker Desktop's VM on macOS and on a Linux runner produces
    different client-side percentiles, and a reader comparing two `run_id`s with
    no way to tell them apart would attribute the difference to the code.
    """

    @property
    def publishable(self) -> bool:
        """Whether a number from this run may appear in a scanned document.

        False on a dirty worktree: the tree that produced the number cannot be
        reconstructed, so the number is unreproducible (`docs/EVALUATION.md`
        §8 rule 4).
        """
        return not self.dirty_worktree

    def write(self, directory: Path | None = None) -> Path:
        target = (directory or MANIFEST_DIR) / f"{self.run_id}.json"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(asdict(self), indent=2, sort_keys=True) + "\n")
        return target


def new_run_id(started: dt.datetime) -> str:
    """Date-prefixed so a listing is chronological, commit-suffixed so two runs
    of the same day on different commits are distinguishable."""
    return f"load-{started:%Y%m%d}-gateway-{git_commit_sha()[:8]}"


# ------------------------------------------------------------- the report ---


def _profile_preamble(profile: str) -> str:
    """Say plainly, at the top, whether this report is the acceptance gate.

    Both profiles produce a report in the same shape, and the shape is
    persuasive. Without this line a reader has no way to tell the gate from the
    adversarial characterisation, and the adversarial numbers are the alarming
    ones -- so the ambiguity fails in the direction of overstating a problem, or
    of quietly passing a gate that was never run.
    """
    if profile == "representative":
        return (
            "> **Workload: `representative` — this IS the Phase 2 acceptance gate.** Its entity "
            "model is derived from `eval/track_a/eval-v1.manifest.json`, and its population is "
            "derived from the offered rate so per-account velocity stays realistic."
        )
    return (
        "> **Workload: `triage-saturation` — this is NOT the acceptance gate.** It concentrates "
        "80% of load onto 5% of a 20,000-account pool, which drives nearly every request over "
        "the velocity thresholds and into triage. It characterises the system under saturation; "
        "the gate is `benchmarks/gateway/REPORT.md`."
    )


def render_report(record: LoadTestRunRecord, measured: Measured, verdicts: list[Verdict]) -> str:
    """Render `benchmarks/gateway/REPORT.md` from the record and the measurement.

    Every number sits under a heading that declares the `run_id`. That is not
    decoration: `scripts/check_claims.py` resolves a `run_id` **section-scoped**
    and clears it at the next heading, so a results table under an undeclared
    heading is an unbacked claim and the linter says so. Repeating the id per
    section is what keeps the report both readable and checkable.

    The renderer takes a `Measured`, never a summary or a dict of optionals, so
    there is no path by which a value that was not measured reaches the page.
    """
    rid = record.run_id
    lines = [
        "# trace-gateway — hot-path load test",
        "",
        "> Written by `make load-gateway`, never by hand. Every figure below comes from the",
        f"> run recorded as `run_id: {rid}`, which `make check-claims` resolves.",
        ">",
        "> This is a `LOADTEST` record: it exercised the service, not a model. It can",
        "> substantiate latency, throughput and availability, and the claim linter refuses",
        "> to let it back a quality claim (`docs/EVALUATION.md` §8 rule 2).",
        "",
        _profile_preamble(record.workload_profile),
        "",
        f"## Run — `run_id: {rid}`",
        "",
        "| field | value |",
        "|---|---|",
        f"| service | `{record.service}` {record.service_version} |",
        f"| instrument | `{record.tool}` {record.tool_version} |",
        f"| image | `{record.tool_image}` |",
        f"| workload profile | **{record.workload_profile}** |",
        f"| offered rate | {record.target_tps} TPS |",
        f"| window | {record.duration_s} s |",
        f"| traffic seed | {record.seed} |",
        f"| rule pack | `{record.rule_pack_id}` `{record.rule_pack_digest}` |",
        f"| thresholds | `{record.threshold_config_digest}` |",
        f"| feature set | {record.feature_set_version} |",
        f"| degraded mode observed | {str(record.degraded_mode).lower()} |",
        f"| host | {record.host_platform} |",
        f"| commit | `{record.git_commit_sha}` |",
        f"| dirty worktree | {str(record.dirty_worktree).lower()} |",
        f"| started / finished | {record.started_at} / {record.finished_at} |",
        "",
        "The rule-pack and threshold digests were read from the running gateway's own",
        "decision, not from this checkout, and the two were required to agree before the",
        "run started.",
        "",
        f"## Latency — `run_id: {rid}`",
        "",
        "`client` is `http_req_duration`: what a caller experiences, which is what the",
        "ROADMAP budget is about. `server` is the gateway's own `RiskDecision.latency_ms`,",
        "which separates the scoring path from the network and the load generator.",
        "",
        "| statistic | client | server |",
        "|---|---|---|",
        f"| p50 | {measured.client_p50_ms:.2f} ms | {measured.server_p50_ms:.2f} ms |",
        f"| p90 | {measured.client_p90_ms:.2f} ms | — |",
        f"| p95 | {measured.client_p95_ms:.2f} ms | — |",
        f"| p99 | {measured.client_p99_ms:.2f} ms | {measured.server_p99_ms:.2f} ms |",
        f"| avg | {measured.client_avg_ms:.2f} ms | — |",
        f"| min / max | {measured.client_min_ms:.2f} / {measured.client_max_ms:.2f} ms | — |",
        "",
        f"## Load actually offered — `run_id: {rid}`",
        "",
        "| quantity | value |",
        "|---|---|",
        f"| requests | {measured.requests:,} |",
        f"| iterations started | {measured.iterations:,} |",
        f"| iterations dropped | {measured.dropped_iterations:,} |",
        f"| run duration | {measured.test_run_duration_s:.2f} s |",
        f"| achieved rate | {measured.achieved_tps:.2f} req/s |",
        "",
        f"## Responses — `run_id: {rid}`",
        "",
        "| class | count |",
        "|---|---|",
        f"| 2xx | {measured.http_2xx:,} |",
        f"| 4xx | {measured.http_4xx:,} |",
        f"| 429 | {measured.http_429:,} |",
        f"| 5xx | {measured.http_5xx:,} |",
        f"| degraded decisions | {measured.degraded_responses:,} |",
        "",
        f"## Decision mix — `run_id: {rid}`",
        "",
        "Reported because a run in which every transaction banded the same way exercised",
        "one branch of the rule engine at the offered rate, and its p99 would not describe",
        "production. The traffic is seeded and skewed (80% of it over 5% of the entities);",
        "the workload profile's k6 script says why.",
        "",
        "| band | count |",
        "|---|---|",
        f"| LOW | {measured.band_low:,} |",
        f"| MEDIUM | {measured.band_medium:,} |",
        f"| HIGH | {measured.band_high:,} |",
        f"| CRITICAL | {measured.band_critical:,} |",
        "",
        f"## Exit conditions — `run_id: {rid}`",
        "",
        "Asserted by `scripts/load_gateway.py`, which exits non-zero on any failure.",
        "Recorded as found, whichever way they went (CLAUDE.md §17).",
        "",
        "| check | kind | result | detail |",
        "|---|---|---|---|",
    ]
    lines += [
        f"| `{v.name}` | {v.kind} | {'PASS' if v.passed else 'FAIL'} | {v.detail} |"
        for v in verdicts
    ]
    lines += [
        "",
        "Displayed figures are rounded for reading; the unrounded values are in",
        f"`eval/manifest/{rid}.json`.",
        "",
    ]
    return "\n".join(lines)


# ------------------------------------------------------------------ main ----


def _print_verdicts(verdicts: list[Verdict]) -> None:
    print("\n  exit conditions")
    for verdict in verdicts:
        mark = "\033[32mPASS\033[0m" if verdict.passed else "\033[31mFAIL\033[0m"
        print(f"    [{mark}] {verdict.kind:<9} {verdict.name}: {verdict.detail}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url", default=os.environ.get("TRACE_GATEWAY_URL", "http://localhost:8010")
    )
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES),
        default=DEFAULT_PROFILE,
        help=(
            "which workload to run. `representative` is the acceptance gate; "
            "`triage-saturation` is the adversarial characterisation and is NOT the gate."
        ),
    )
    parser.add_argument("--target-tps", type=int, default=DEFAULT_TARGET_TPS)
    parser.add_argument("--duration-s", type=int, default=DEFAULT_DURATION_S)
    parser.add_argument("--seed", type=int, default=20260912)
    parser.add_argument("--pre-allocated-vus", type=int, default=120)
    parser.add_argument("--max-vus", type=int, default=800)
    parser.add_argument("--token", default=None, help="<token_id>.<secret>; prefer the environment")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="where k6 writes summary.json (default: a temporary directory OUTSIDE the "
        "repository, so the run does not dirty the worktree it is recording)",
    )
    args = parser.parse_args(argv)

    try:
        image = k6_image()
        token = resolve_token(args.token)
        print(f"  instrument: {image}")
        tool_version = k6_version(image)
        print(f"  k6 {tool_version}")

        print(f"  probing {args.base_url} for the live configuration ...")
        identity = probe_gateway(args.base_url, token)
        assert_configuration_agrees(identity, local_configuration())
        print(f"    {identity.service} {identity.service_version}")
        print(f"    rule pack   {identity.rule_pack_id} {identity.rule_pack_digest}")
        print(f"    thresholds  {identity.threshold_config_digest}")
        print(f"    feature set {identity.feature_set_version}")

        out_dir = args.out_dir or Path(tempfile.mkdtemp(prefix="trace-load-"))
        if args.out_dir is not None and ROOT in out_dir.resolve().parents:
            raise LoadHarnessError(
                f"--out-dir {out_dir} is inside the repository. k6's output would show up "
                f"as an untracked file, which makes `git status` dirty -- and a run "
                f"recorded with dirty_worktree: true is not publishable "
                f"(docs/EVALUATION.md §8 rule 4). Choose a path outside the tree."
            )

        nonce = uuid.uuid4().hex[:12]
        started = dt.datetime.now(dt.UTC)
        exit_code = run_k6(
            script=PROFILES[args.profile],
            image=image,
            base_url=args.base_url,
            token=token,
            target_tps=args.target_tps,
            duration_s=args.duration_s,
            seed=args.seed,
            nonce=nonce,
            pre_allocated_vus=args.pre_allocated_vus,
            max_vus=args.max_vus,
            out_dir=out_dir,
        )
        finished = dt.datetime.now(dt.UTC)

        measured = extract(load_summary(out_dir))
        verdicts = evaluate(measured, target_tps=args.target_tps)
        _print_verdicts(verdicts)

        integrity_failures = failures(verdicts, INTEGRITY)
        if integrity_failures:
            print(
                f"\n\033[31m  the run did not measure what it claims to "
                f"({len(integrity_failures)} integrity failure(s)).\033[0m"
            )
            print("  No record and no report are written: publishing these numbers under a")
            print("  latency heading would be a false claim, however honestly collected.")
            print(f"  Raw k6 summary kept at {out_dir / 'summary.json'}")
            return 1

        record = LoadTestRunRecord(
            workload_profile=args.profile,
            run_id=new_run_id(started),
            service=identity.service,
            service_version=identity.service_version,
            tool_version=tool_version,
            target_tps=args.target_tps,
            duration_s=args.duration_s,
            rule_pack_id=identity.rule_pack_id,
            rule_pack_digest=identity.rule_pack_digest,
            threshold_config_digest=identity.threshold_config_digest,
            feature_set_version=identity.feature_set_version,
            # Measured, not declared: the run is a degraded-mode run if the
            # gateway said any of its decisions were degraded.
            degraded_mode=measured.degraded_responses > 0,
            seed=args.seed,
            tool_image=image,
            k6_exit_code=exit_code,
            verdicts=[asdict(v) for v in verdicts],
            measured=asdict(measured),
            started_at=started.isoformat().replace("+00:00", "Z"),
            finished_at=finished.isoformat().replace("+00:00", "Z"),
        )
        path = record.write()
        print(f"\n  run record: {path.relative_to(ROOT)}")

        if not record.publishable:
            print(
                "\n\033[31m  WARNING: dirty worktree — this run is NOT publishable "
                "(docs/EVALUATION.md §8 rule 4).\033[0m"
            )
            print("  The record above is kept, because a measurement is evidence even when it")
            print("  cannot be cited. The report is NOT written: benchmarks/**/*.md is scanned")
            print("  by `make check-claims`, and a number from a tree that cannot be")
            print("  reconstructed has no business in a scanned document.")
            print("  Commit the worktree and re-run to publish.")
            return 1

        report_path = REPORT_FOR[args.profile]
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(render_report(record, measured, verdicts))
        print(f"  report:     {report_path.relative_to(ROOT)}")

        target_failures = failures(verdicts, TARGET)
        if target_failures:
            print(
                f"\n\033[31m  {len(target_failures)} ROADMAP Phase 2 target(s) not met. "
                f"Recorded and published as found (CLAUDE.md §17).\033[0m"
            )
            return 1
        print("\n\033[32m  every Phase 2 target met, and the numbers are attributable.\033[0m")
        return 0
    except LoadHarnessError as exc:
        print(f"\n\033[31mload-gateway: {exc}\033[0m", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
