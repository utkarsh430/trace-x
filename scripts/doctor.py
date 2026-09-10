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


def _run(cmd: list[str]) -> str:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)  # noqa: S603
        return (r.stdout + r.stderr).strip()
    except (OSError, subprocess.SubprocessError):
        return ""


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
    out = _run([exe, "-version"])
    m = re.search(r'version "?(\d+)', out)
    major = m.group(1) if m else "?"
    if major == want:
        rep.add("java", OK, f"Java {major} via {'JAVA_HOME' if java_home else 'PATH'}")
        return
    hint = _run(["/usr/libexec/java_home", "-v", want]) if platform.system() == "Darwin" else ""
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
    if not shutil.which("docker"):
        rep.add("docker", FAIL, "not installed", "Docker is required for `make up`.")
        return
    out = _run(["docker", "info", "--format", "{{.MemTotal}}|{{.NCPU}}|{{.ServerVersion}}"])
    if "|" not in out:
        rep.add(
            "docker", FAIL, "daemon unreachable", "Start Docker Desktop, then re-run `make doctor`."
        )
        return
    mem_s, cpus, ver = out.split("|")[:3]
    mem_gb = int(mem_s) / 1024**3
    status = OK if mem_gb >= MIN_DOCKER_RAM_GB else WARN
    rep.add(
        "docker",
        status,
        f"v{ver}, {mem_gb:.1f} GB RAM, {cpus} CPU",
        f"Raise Docker RAM to >= {MIN_DOCKER_RAM_GB} GB for the full profile."
        if status == WARN
        else "",
        required=False,
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
