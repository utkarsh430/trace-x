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
import platform
import re
import shutil
import socket
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
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
    """Pinned container images for the non-Python tooling (ADR-0038)."""
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


def check_java(rep: Report, want: str) -> None:
    """Spark 4.0 supports Java 17/21 ONLY. Java 25 is the system default here."""
    java_home = os.environ.get("JAVA_HOME", "")
    exe = str(Path(java_home) / "bin" / "java") if java_home else (shutil.which("java") or "")
    if not exe:
        rep.add(
            "java",
            WARN,
            "java not found",
            f"Only needed from Phase 3. Install Temurin {want}.",
            required=False,
        )
        return
    out = _run_out([exe, "-version"])
    m = re.search(r'version "?(\d+)', out)
    major = m.group(1) if m else "?"
    if major == want:
        rep.add("java", OK, f"Java {major} via {'JAVA_HOME' if java_home else 'PATH'}")
        return
    hint = _run_out(["/usr/libexec/java_home", "-v", want]) if platform.system() == "Darwin" else ""
    remedy = (
        f"Spark {pins()['spark']} supports Java 17/21 only — Java {major} WILL FAIL.\n"
        f"      Fix: export JAVA_HOME={hint or f'<path to Temurin {want}>'}"
    )
    # Not required until Phase 3, but must be impossible to miss.
    rep.add(
        "java", WARN if major != want else OK, f"Java {major} (want {want})", remedy, required=False
    )


def check_pin(rep: Report, name: str, want: str, module: str | None = None) -> None:
    """Assert a declared pin matches what is installed, when it is installed."""
    if module is None:
        rep.add(f"pin:{name}", OK, f"declared {want}", required=False)
        return
    try:
        mod = __import__(module)
        got = getattr(mod, "__version__", "?")
    except ImportError:
        rep.add(
            f"pin:{name}",
            WARN,
            f"{module} not installed (declared {want})",
            f"Installed with its phase extra; pin is {want}.",
            required=False,
        )
        return
    ok = str(got).startswith(want)
    rep.add(
        f"pin:{name}",
        OK if ok else FAIL,
        f"{got} (want {want})",
        f"Version drift invalidates benchmark comparability (ADR-0017/0018). "
        f"Reinstall {module}=={want}.",
        required=ok is False,
    )


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
    """The pinned non-Python tools (ADR-0038).

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
    check_java(rep, p["java"])
    check_pin(rep, "spark", p["spark"], "pyspark")
    check_pin(rep, "delta", p["delta"], "delta")
    check_pin(rep, "hadoop", p["hadoop"])
    check_pin(rep, "scala", p["scala"])
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
