#!/usr/bin/env python3
"""Preflight environment check for TRACE-X.

Fails LOUDLY and actionably rather than letting a downstream tool die with an
opaque error (the canonical example: Spark 4.0 on Java 25 produces an
UnsupportedClassVersionError that tells a developer nothing).

Exit codes: 0 = all required checks pass, 1 = at least one required check failed.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import socket
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# trace_core.stream.toolchain is stdlib-only by design, so it is importable here
# before `make setup` has installed anything -- one implementation of each check,
# shared with the Spark session factory, instead of a second copy that could drift.
sys.path.insert(0, str(ROOT / "packages"))
from trace_core.stream import toolchain  # noqa: E402

OK, WARN, FAIL = "PASS", "WARN", "FAIL"

# Resource floors. Disk is the binding constraint on the reference machine.
MIN_DISK_CORE_GB = 6.0  # core profile only
MIN_DISK_FULL_GB = 15.0  # streaming + graph + ml + obs
MIN_DOCKER_RAM_GB = 6.0

PORTS = {
    "POSTGRES_PORT": 5442,
    "REDIS_PORT": 6389,
    "TRACE_GATEWAY_PORT": 8010,
    "TRACE_API_PORT": 8011,
}


@dataclass
class Check:
    name: str
    status: str
    detail: str
    remedy: str = ""
    required: bool = True


@dataclass
class Report:
    checks: list[Check] = field(default_factory=list)

    def add(self, *a: object, **kw: object) -> None:
        self.checks.append(Check(*a, **kw))  # type: ignore[arg-type]

    @property
    def failed(self) -> list[Check]:
        return [c for c in self.checks if c.status == FAIL and c.required]


def pins() -> dict[str, str]:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return dict(data["tool"]["trace_x"]["pins"])


def tool_images() -> dict[str, str]:
    """Pinned container images for the non-Python tooling (ADR-0036)."""
    data = tomllib.loads((ROOT / "pyproject.toml").read_text())
    return dict(data["tool"]["trace_x"].get("tools", {}))


def _run(cmd: list[str]) -> tuple[int, str, str]:
    """Run a command, returning (returncode, stdout, stderr).

    The return code and the two streams are kept SEPARATE deliberately. An
    earlier version merged them and discarded the code, which made a failed
    command indistinguishable from a successful one that happened to print to
    stderr -- and that masked a down Docker daemon (see check_docker).
    """
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)  # noqa: S603
        return r.returncode, r.stdout.strip(), r.stderr.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        return 127, "", str(exc)


def _run_out(cmd: list[str]) -> str:
    """Convenience for probes where only best-effort text matters."""
    _, out, err = _run(cmd)
    return out or err


def check_python(rep: Report, want: str) -> None:
    got = f"{sys.version_info.major}.{sys.version_info.minor}"
    rep.add(
        "python",
        OK if got == want else FAIL,
        f"{got} (want {want})",
        f"Install Python {want}; Spark {pins()['spark']} supports 3.9+ "
        f"but the lockfile targets {want}.",
    )


STREAM_PHASE = 3
"""From this phase on, the JVM toolchain is required rather than advisory.

Before Phase 3 a wrong Java was a warning, deliberately: nothing ran Spark, and
blocking `make verify` on a JDK the phase did not use would have been noise. From
Phase 3 a warning is the wrong answer -- the ROADMAP entry condition is "make
doctor green on the full pin matrix", and a check that cannot fail cannot be
green in any meaningful sense. Phase 3 planning found exactly that: the Java
check warned, Hadoop and Scala were "declared" without being asserted, and a
non-interactive shell ran Java 25 while doctor reported success."""


def current_phase() -> int:
    """The phase number `tests/acceptance/status.json` records (4B counts as 4).

    Refuses to guess. A value it cannot read -- "Phase 3", an empty string -- would
    otherwise parse as phase 0 and silently turn every required toolchain check back
    into a warning, which is the failure this check exists to prevent.
    """
    data = json.loads((ROOT / "tests" / "acceptance" / "status.json").read_text())
    raw = str(data.get("current_phase", ""))
    match = re.fullmatch(r"(\d+)[A-Z]?", raw)
    if match is None:
        raise SystemExit(
            f"doctor: tests/acceptance/status.json current_phase {raw!r} is not a phase "
            f"number such as '3' or '4B'; refusing to guess which checks are required"
        )
    return int(match.group(1))


def _add_toolchain(rep: Report, name: str, finding: toolchain.Finding, phase: int) -> None:
    if finding.ok:
        rep.add(name, OK, finding.detail)
        return
    required = phase >= STREAM_PHASE
    rep.add(name, FAIL if required else WARN, finding.detail, finding.remedy, required=required)


def check_java(rep: Report, want: str, phase: int) -> None:
    """Spark 4.0 supports Java 17/21 ONLY. Java 25 is the system default here.

    Resolved exactly as Spark's launcher resolves it (`JAVA_HOME`, else `PATH`) and
    confirmed by running it. Under `make`, `JAVA_HOME` has already been pointed at
    the pinned JDK when one is installed (scripts/java_home.py); run directly from
    a shell that selects another Java, this check fails -- which is the point.
    """
    if want != toolchain.JAVA_MAJOR:
        rep.add(
            "java",
            FAIL,
            f"pyproject pins Java {want} but trace_core.stream.toolchain mirrors "
            f"{toolchain.JAVA_MAJOR}",
            "Change both together; tests/unit/test_stream_toolchain.py diffs them.",
        )
        return
    _add_toolchain(rep, "java", toolchain.check_java(), phase)


def check_stream_toolchain(rep: Report, pins: dict[str, str], phase: int) -> None:
    """pyspark, Delta, the Hadoop and Scala pyspark actually bundles, and the jars.

    Hadoop and Scala are read from the jar file names inside pyspark's own `jars/`
    directory -- a declared pin nobody compares against the installed artefact is
    a comment, not a check.
    """
    mirrored = {
        "spark": toolchain.SPARK_VERSION,
        "delta": toolchain.DELTA_VERSION,
        "hadoop": toolchain.HADOOP_LINE,
        "scala": toolchain.SCALA_LINE,
    }
    for name, value in mirrored.items():
        if pins[name] != value:
            rep.add(
                f"pin:{name}",
                FAIL,
                f"pyproject pins {pins[name]} but trace_core.stream.toolchain mirrors {value}",
                "Change both together; tests/unit/test_stream_toolchain.py diffs them.",
            )
            return
    jars = toolchain.pyspark_jars_dir()
    _add_toolchain(rep, "pin:spark", toolchain.check_distribution("pyspark", pins["spark"]), phase)
    _add_toolchain(
        rep, "pin:delta", toolchain.check_distribution("delta-spark", pins["delta"]), phase
    )
    _add_toolchain(rep, "pin:hadoop", toolchain.check_bundled("hadoop", jars), phase)
    _add_toolchain(rep, "pin:scala", toolchain.check_bundled("scala", jars), phase)
    findings = toolchain.check_locked_jars()
    bad = [f for f in findings if not f.ok]
    summary = toolchain.Finding(
        "spark_jars",
        not bad,
        f"{len(findings) - len(bad)}/{len(findings)} locked jars verified"
        + (f"; first problem: {bad[0].detail}" if bad else ""),
        bad[0].remedy if bad else "",
    )
    _add_toolchain(rep, "spark_jars", summary, phase)


def check_disk(rep: Report) -> None:
    free_gb = shutil.disk_usage(ROOT).free / 1024**3
    if free_gb >= MIN_DISK_FULL_GB:
        rep.add("disk", OK, f"{free_gb:.1f} GB free — full profile viable")
    elif free_gb >= MIN_DISK_CORE_GB:
        rep.add(
            "disk",
            WARN,
            f"{free_gb:.1f} GB free — core profile only",
            f"Phase 3+ needs >= {MIN_DISK_FULL_GB} GB (Kafka+Spark+Neo4j+MLflow images).",
            required=False,
        )
    else:
        rep.add(
            "disk",
            FAIL,
            f"{free_gb:.1f} GB free",
            f"Need >= {MIN_DISK_CORE_GB} GB for the core profile alone.",
        )


def check_docker(rep: Report) -> None:
    """Docker must be installed AND its daemon reachable.

    Subtlety worth keeping: with the daemon down, `docker info --format` still
    renders the template against a zero-valued struct and prints "0|0|" on
    stdout while the real error goes to stderr. Testing the output shape alone
    therefore reports a healthy daemon. The authoritative signals are the
    RETURN CODE and a non-empty ServerVersion.

    A down daemon is a FAIL, not a warning: `make up`, `make migrate` and the
    release-blocking isolation suite all depend on it, and the point of doctor
    is to surface that here with a fix rather than let a later command die
    confusingly.
    """
    if not shutil.which("docker"):
        rep.add(
            "docker",
            FAIL,
            "not installed",
            "Docker is required for `make up` and the integration suite.",
        )
        return

    code, out, err = _run(
        ["docker", "info", "--format", "{{.MemTotal}}|{{.NCPU}}|{{.ServerVersion}}"]
    )
    parts = out.split("|")
    version = parts[2].strip() if len(parts) >= 3 else ""

    if code != 0 or not version:
        detail = "daemon unreachable"
        if "permission denied" in err.lower():
            detail = "daemon unreachable (permission denied on the socket)"
        rep.add(
            "docker",
            FAIL,
            detail,
            "Start Docker Desktop (or `colima start`), then re-run `make doctor`.\n"
            "      `make up`, `make migrate` and `pytest -m integration` all require it.",
        )
        return

    try:
        mem_gb = int(parts[0]) / 1024**3
        cpus = parts[1]
    except (ValueError, IndexError):
        rep.add(
            "docker",
            FAIL,
            f"could not parse `docker info` output: {out[:60]!r}",
            "Check the Docker installation, then re-run `make doctor`.",
        )
        return

    status = OK if mem_gb >= MIN_DOCKER_RAM_GB else WARN
    rep.add(
        "docker",
        status,
        f"v{version}, {mem_gb:.1f} GB RAM, {cpus} CPU",
        f"Raise Docker RAM to >= {MIN_DOCKER_RAM_GB} GB for the full profile."
        if status == WARN
        else "",
        required=status != WARN,
    )


def check_ports(rep: Report) -> None:
    busy = []
    for name, default in PORTS.items():
        port = int(os.environ.get(name, default))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                busy.append(f"{name}={port}")
    rep.add(
        "ports",
        OK if not busy else WARN,
        "all free" if not busy else f"in use: {', '.join(busy)}",
        "Change the port in .env — another project is using it." if busy else "",
        required=False,
    )


def check_llm_tier(rep: Report) -> None:
    """A bare clone must be able to run the demo. SMOKE tier requires no key."""
    tier = os.environ.get("TRACE_LLM_TIER", "SMOKE").upper()
    detail, remedy, status = f"tier={tier}", "", OK
    if tier == "SMOKE":
        reachable = shutil.which("ollama") is not None
        detail += f", ollama {'found' if reachable else 'NOT found'}"
        if not reachable:
            status, remedy = WARN, "Needed from Phase 6: install Ollama, then `make pull-model`."
    elif tier == "DEV":
        if not os.environ.get("OPENAI_API_KEY"):
            status, remedy = WARN, "DEV tier needs OPENAI_API_KEY; falling back to SMOKE."
    elif tier == "EVAL":
        if not (os.environ.get("AWS_PROFILE") or os.environ.get("AWS_ACCESS_KEY_ID")):
            status, remedy = WARN, "EVAL tier (Bedrock) needs AWS credentials. Phase 12 only."
    rep.add("llm_tier", status, detail, remedy, required=False)
    if tier != "EVAL":
        rep.add(
            "llm_publication",
            OK,
            f"tier {tier} may NOT publish quality numbers (ADR-0016)",
            required=False,
        )


def check_tool_images(rep: Report) -> None:
    """The pinned non-Python tools (ADR-0036).

    Reported as WARN rather than FAIL: both are pulled on demand by the target
    that needs them, and a developer running `make test-fast` should not be told
    their environment is broken because they have not yet run a load test. What
    would be a real failure is an UNPINNED tool, and that cannot happen here --
    the pin lives in pyproject.toml and is asserted by a unit test.
    """
    images = tool_images()
    if not images:
        rep.add("tool_images", FAIL, "no pinned tool images declared in pyproject.toml")
        return
    if not shutil.which("docker"):
        rep.add(
            "tool_images",
            WARN,
            f"{len(images)} pinned image(s) declared; docker not found",
            "Needed for `make contracts-check` (Phase 2) and `make load-gateway`.",
            required=False,
        )
        return
    missing = []
    for name, ref in sorted(images.items()):
        rc, _out, _err = _run(["docker", "image", "inspect", ref])
        if rc != 0:
            missing.append(name)
    rep.add(
        "tool_images",
        OK if not missing else WARN,
        f"{len(images)} pinned: {', '.join(sorted(images))}"
        + (f" (not pulled: {', '.join(missing)})" if missing else ""),
        "They are pulled by the target that uses them; `docker pull` them ahead of time "
        "to avoid a first-run delay."
        if missing
        else "",
        required=False,
    )


def check_env_file(rep: Report) -> None:
    if (ROOT / ".env").exists():
        rep.add("env", OK, ".env present")
    else:
        rep.add(
            "env",
            WARN,
            ".env missing",
            "Run `make setup` or `cp .env.example .env`.",
            required=False,
        )


def main() -> int:
    p = pins()
    rep = Report()
    check_python(rep, p["python"])
    phase = current_phase()
    check_java(rep, p["java"], phase)
    check_stream_toolchain(rep, p, phase)
    check_disk(rep)
    check_docker(rep)
    check_ports(rep)
    check_tool_images(rep)
    check_env_file(rep)
    check_llm_tier(rep)

    if "--json" in sys.argv:
        print(json.dumps([c.__dict__ for c in rep.checks], indent=2))
        return 1 if rep.failed else 0

    colour = {OK: "\033[32m", WARN: "\033[33m", FAIL: "\033[31m"}
    print("\nTRACE-X doctor\n" + "-" * 62)
    for c in rep.checks:
        print(f"  {colour[c.status]}{c.status:<4}\033[0m {c.name:<18} {c.detail}")
        if c.remedy and c.status != OK:
            for line in c.remedy.splitlines():
                print(f"       \033[2m{line.strip()}\033[0m")
    print("-" * 62)
    if rep.failed:
        print(f"\033[31m{len(rep.failed)} required check(s) failed.\033[0m\n")
        return 1
    warns = sum(1 for c in rep.checks if c.status == WARN)
    print(
        "\033[32mAll required checks passed.\033[0m"
        + (f" \033[33m{warns} warning(s) — fine for the current phase.\033[0m" if warns else "")
        + "\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
