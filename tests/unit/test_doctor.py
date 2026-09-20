"""Preflight doctor checks.

These exist because of a real defect found during shutdown recovery: with the
Docker daemon down, `docker info --format` still renders the template against a
zero-valued struct and prints "0|0|" on stdout, sending the real error to
stderr. The original check tested only the output shape, so it reported a
healthy daemon and `make doctor` exited 0 -- while `make up` would then fail
confusingly. Preventing exactly that is the doctor's whole purpose.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def doctor():
    spec = importlib.util.spec_from_file_location("doctor", ROOT / "scripts" / "doctor.py")
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["doctor"] = mod
    spec.loader.exec_module(mod)
    return mod


def _fake_run(code: int, out: str, err: str = ""):
    def _run(cmd: list[str]) -> tuple[int, str, str]:
        return code, out, err

    return _run


# --------------------------------------------------------------- docker ----


def test_docker_daemon_down_is_a_failure_not_a_warning(doctor, monkeypatch) -> None:
    """The exact shutdown-recovery bug: daemon down must FAIL, and must be required."""
    monkeypatch.setattr(doctor.shutil, "which", lambda _: "/usr/local/bin/docker")
    monkeypatch.setattr(
        doctor,
        "_run",
        _fake_run(1, "0|0|", "Cannot connect to the Docker daemon at unix:///var/run/docker.sock."),
    )
    rep = doctor.Report()
    doctor.check_docker(rep)

    check = rep.checks[0]
    assert check.status == doctor.FAIL, (
        f"a down daemon must FAIL, got {check.status}. `docker info --format` "
        f"prints '0|0|' with the daemon down, so output shape alone is not a signal."
    )
    assert check.required is True, "a down daemon must fail `make doctor`, not merely warn"
    assert "unreachable" in check.detail
    assert "Docker Desktop" in check.remedy


def test_docker_zero_output_with_success_code_still_fails(doctor, monkeypatch) -> None:
    """Belt and braces: an empty ServerVersion is a failure even if the code is 0."""
    monkeypatch.setattr(doctor.shutil, "which", lambda _: "/usr/local/bin/docker")
    monkeypatch.setattr(doctor, "_run", _fake_run(0, "0|0|"))
    rep = doctor.Report()
    doctor.check_docker(rep)
    assert rep.checks[0].status == doctor.FAIL


def test_docker_permission_denied_is_reported_specifically(doctor, monkeypatch) -> None:
    monkeypatch.setattr(doctor.shutil, "which", lambda _: "/usr/local/bin/docker")
    monkeypatch.setattr(
        doctor, "_run", _fake_run(1, "", "Got permission denied while trying to connect")
    )
    rep = doctor.Report()
    doctor.check_docker(rep)
    assert "permission denied" in rep.checks[0].detail


def test_docker_not_installed_fails(doctor, monkeypatch) -> None:
    monkeypatch.setattr(doctor.shutil, "which", lambda _: None)
    rep = doctor.Report()
    doctor.check_docker(rep)
    assert rep.checks[0].status == doctor.FAIL
    assert rep.checks[0].required is True


def test_healthy_docker_passes(doctor, monkeypatch) -> None:
    monkeypatch.setattr(doctor.shutil, "which", lambda _: "/usr/local/bin/docker")
    monkeypatch.setattr(doctor, "_run", _fake_run(0, f"{8 * 1024**3}|10|29.2.1"))
    rep = doctor.Report()
    doctor.check_docker(rep)
    check = rep.checks[0]
    assert check.status == doctor.OK
    assert "29.2.1" in check.detail
    assert "8.0 GB RAM" in check.detail


def test_low_docker_ram_warns_but_does_not_block(doctor, monkeypatch) -> None:
    """Low RAM constrains the full profile; it must not block core development."""
    monkeypatch.setattr(doctor.shutil, "which", lambda _: "/usr/local/bin/docker")
    monkeypatch.setattr(doctor, "_run", _fake_run(0, f"{4 * 1024**3}|4|29.2.1"))
    rep = doctor.Report()
    doctor.check_docker(rep)
    assert rep.checks[0].status == doctor.WARN
    assert rep.checks[0].required is False


def test_unparseable_docker_output_fails(doctor, monkeypatch) -> None:
    monkeypatch.setattr(doctor.shutil, "which", lambda _: "/usr/local/bin/docker")
    monkeypatch.setattr(doctor, "_run", _fake_run(0, "garbage|nonsense|29.2.1"))
    rep = doctor.Report()
    doctor.check_docker(rep)
    assert rep.checks[0].status == doctor.FAIL


# ------------------------------------------------------------- _run shape --


def test_run_keeps_returncode_and_streams_separate(doctor) -> None:
    """Merging stderr into stdout and dropping the code is what masked the bug."""
    code, out, err = doctor._run([sys.executable, "-c", "import sys; print('o'); sys.exit(3)"])
    assert code == 3
    assert out == "o"
    assert err == ""


def test_run_reports_a_missing_executable_rather_than_raising(doctor) -> None:
    code, out, _ = doctor._run(["definitely-not-a-real-binary-xyz"])
    assert code != 0
    assert out == ""


# ----------------------------------------------------------------- disk ----


def test_disk_below_core_floor_is_a_required_failure(doctor, monkeypatch) -> None:
    monkeypatch.setattr(
        doctor.shutil, "disk_usage", lambda _: type("U", (), {"free": 2 * 1024**3})()
    )
    rep = doctor.Report()
    doctor.check_disk(rep)
    assert rep.checks[0].status == doctor.FAIL
    assert rep.checks[0].required is True


def test_disk_between_core_and_full_warns_only(doctor, monkeypatch) -> None:
    """Enough for `core`, not for Phase 3. Must not block ordinary development."""
    monkeypatch.setattr(
        doctor.shutil, "disk_usage", lambda _: type("U", (), {"free": 8 * 1024**3})()
    )
    rep = doctor.Report()
    doctor.check_disk(rep)
    assert rep.checks[0].status == doctor.WARN
    assert rep.checks[0].required is False


def test_ample_disk_passes(doctor, monkeypatch) -> None:
    monkeypatch.setattr(
        doctor.shutil, "disk_usage", lambda _: type("U", (), {"free": 40 * 1024**3})()
    )
    rep = doctor.Report()
    doctor.check_disk(rep)
    assert rep.checks[0].status == doctor.OK


# ----------------------------------------------------------------- pins ----


def _fake_jdk(root: Path, version: str) -> Path:
    home = root / "jdk"
    (home / "bin").mkdir(parents=True)
    java = home / "bin" / "java"
    java.write_text(f"#!/bin/sh\necho 'openjdk version \"{version}\" 2026-01-20 LTS' 1>&2\n")
    java.chmod(0o755)
    return home


def test_java_25_fails_doctor_from_phase_3(doctor, monkeypatch, tmp_path) -> None:
    """From Phase 3 a wrong Java is a required failure, not a warning.

    Phase 3 planning found the warning-level check reporting success while a
    non-interactive shell ran Java 25; a check that cannot fail cannot be green.
    """
    monkeypatch.setenv("JAVA_HOME", str(_fake_jdk(tmp_path, "25.0.2")))
    rep = doctor.Report()
    doctor.check_java(rep, "17", phase=3)
    check = rep.checks[0]
    assert check.status == doctor.FAIL
    assert check.required
    assert "Java 25" in check.detail
    assert "JAVA_HOME" in check.remedy


def test_java_25_only_warned_before_phase_3(doctor, monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("JAVA_HOME", str(_fake_jdk(tmp_path, "25.0.2")))
    rep = doctor.Report()
    doctor.check_java(rep, "17", phase=2)
    assert rep.checks[0].status == doctor.WARN
    assert not rep.checks[0].required


def test_the_pinned_java_passes(doctor, monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("JAVA_HOME", str(_fake_jdk(tmp_path, "17.0.18")))
    rep = doctor.Report()
    doctor.check_java(rep, "17", phase=3)
    assert rep.checks[0].status == doctor.OK


def test_missing_spark_jars_fail_doctor_from_phase_3(doctor, monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("TRACE_SPARK_JARS_DIR", str(tmp_path / "empty"))
    rep = doctor.Report()
    doctor.check_stream_toolchain(rep, doctor.pins(), phase=3)
    jars = next(c for c in rep.checks if c.name == "spark_jars")
    assert jars.status == doctor.FAIL
    assert jars.required
    assert "make stream-jars" in jars.remedy


def test_doctor_exits_non_zero_when_run_under_java_25(tmp_path) -> None:
    """End to end, the way a developer's shell would run it: no `make`, wrong JAVA_HOME."""
    import json
    import os
    import subprocess

    env = {**os.environ, "JAVA_HOME": str(_fake_jdk(tmp_path, "25.0.2"))}
    result = subprocess.run(  # noqa: S603 -- the running interpreter, a repository script
        [sys.executable, str(ROOT / "scripts" / "doctor.py"), "--json"],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
        check=False,
    )
    assert result.returncode == 1
    java = next(c for c in json.loads(result.stdout) if c["name"] == "java")
    assert java["status"] == "FAIL"
