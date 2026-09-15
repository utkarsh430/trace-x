"""Load-harness self-test (`scripts/load_gateway.py`, `tests/load/k6/representative.js`).

The harness exists to produce numbers that can be published, so what has to be
proven is not that it runs but that it **refuses**: refuses to invent a metric it
did not read, refuses to call a rate-limited run a latency measurement, refuses
to publish from a tree nobody can reconstruct, and refuses to let the zero-5xx
exit condition pass on a run that returned 5xx.

The two strongest tests here run the real claim linter (`scripts/check_claims.py`)
against a real rendered report and a real run record, in a throwaway tree. A gate
that has never rejected anything is not known to work, so each is paired with a
test that breaks the thing it checks and requires the linter to notice.

**Every number in this file is fabricated.** They exercise the parser and the
gates; none is a measurement, none reaches `eval/manifest/` or `benchmarks/`, and
the report this file renders is written to a temporary directory and discarded.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]


def _load_script(name: str, relative: str) -> Any:
    """Import a `scripts/` module by path. They are CLI entrypoints, not a package."""
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


harness = _load_script("load_gateway", "scripts/load_gateway.py")

K6_SCRIPTS = {
    name: ROOT / "tests" / "load" / "k6" / filename for name, filename in harness.PROFILES.items()
}
"""Every workload profile, checked against the same invariants.

Parameterised rather than pointed at one file: the two profiles differ in the
traffic they generate and must not differ in the properties that make a run
measurable at all. A profile that quietly stopped sending unique idempotency
keys would serve most of its run from the replay cache and report a beautiful
p99 for work it never did -- and it would do that whichever file it lived in."""
linter = _load_script("check_claims_under_test", "scripts/check_claims.py")


# --------------------------------------------------------------- fixtures ---

_COUNTERS: dict[str, int] = {
    "http_reqs": 300_000,
    "iterations": 300_000,
    "dropped_iterations": 0,
    "gateway_http_2xx": 300_000,
    "gateway_http_4xx": 0,
    "gateway_http_429": 0,
    "gateway_http_5xx": 0,
    "gateway_unparseable_responses": 0,
    "gateway_degraded_responses": 0,
    "gateway_degraded_unexpected": 0,
    "gateway_history_incomplete": 0,
    "gateway_band_low": 280_000,
    "gateway_band_medium": 15_000,
    "gateway_band_high": 4_000,
    "gateway_band_critical": 1_000,
}

_TRENDS: dict[str, dict[str, float]] = {
    "http_req_duration": {
        "min": 1.0,
        "avg": 6.0,
        "med": 5.0,
        "p(50)": 5.0,
        "p(90)": 12.0,
        "p(95)": 18.0,
        "p(99)": 41.0,
        "max": 220.0,
        "count": 300_000,
    },
    "gateway_server_latency_ms": {
        "min": 0.5,
        "avg": 3.0,
        "med": 2.5,
        "p(50)": 2.5,
        "p(90)": 6.0,
        "p(95)": 9.0,
        "p(99)": 22.0,
        "max": 90.0,
        "count": 300_000,
    },
}


def summary(*, drop: str | None = None, **over: int) -> dict[str, Any]:
    """A k6 summary in the shape the pinned image actually emits.

    The shape was taken from the pinned image rather than from documentation, so
    a parser written against it is written against the instrument. `drop` removes
    a metric entirely, which is how the real summary behaves for a metric that
    was never registered -- the case the harness must refuse rather than read as
    a zero.
    """
    counters = {**_COUNTERS, **{k: v for k, v in over.items() if k in _COUNTERS}}
    unknown = set(over) - set(_COUNTERS)
    assert not unknown, f"test wrote counters the harness does not read: {unknown}"
    metrics: dict[str, Any] = {
        name: {"type": "counter", "values": {"count": value, "rate": value / 600.0}}
        for name, value in counters.items()
    }
    for name, values in _TRENDS.items():
        metrics[name] = {"type": "trend", "values": dict(values)}
    if drop is not None:
        metrics.pop(drop)
    return {"metrics": metrics, "state": {"testRunDurationMs": 600_000.0}}


def record(**over: Any) -> Any:
    """A complete `LOADTEST` record, with the git-derived fields pinned.

    `dirty_worktree` and the commit sha are passed explicitly rather than left to
    their factories: a test whose outcome depended on whether the developer had
    unsaved changes would fail for a reason that has nothing to do with the code.
    """
    measured = asdict(harness.extract(summary()))
    verdicts = [asdict(v) for v in harness.evaluate(harness.extract(summary()), target_tps=500)]
    defaults: dict[str, Any] = {
        "run_id": "load-20260912-gateway-abcd1234",
        "service": "trace-gateway",
        "service_version": "0.1.0",
        "tool_version": "v0.49.0",
        "target_tps": 500,
        "duration_s": 600,
        "rule_pack_id": "core.v1",
        "rule_pack_digest": "sha256:" + "d" * 64,
        "threshold_config_digest": "sha256:" + "e" * 64,
        "feature_set_version": "1.0.0",
        "degraded_mode": False,
        "seed": 20260912,
        "tool_image": "grafana/k6:0.49.0@sha256:" + "8" * 64,
        "k6_exit_code": 0,
        "verdicts": verdicts,
        "measured": measured,
        "started_at": "2026-09-12T10:00:00Z",
        "finished_at": "2026-09-12T10:10:00Z",
        "git_commit_sha": "0" * 40,
        "dirty_worktree": False,
        "env_lock_digest": "sha256:" + "c" * 64,
        "python_version": "3.12.0",
    }
    defaults.update(over)
    return harness.LoadTestRunRecord(**defaults)


# ------------------------------------------------------------- instrument ---


def test_the_instrument_is_pinned_by_digest() -> None:
    """A moved tag would silently change what a published p99 means (ADR-0036)."""
    image = harness.k6_image()
    assert "@sha256:" in image, (
        f"the k6 image {image!r} is not digest-pinned, so two runs of 'the same' "
        f"benchmark are not known to have used the same instrument"
    )


def test_the_harness_has_no_path_fallback_for_k6(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no pin it must fail, not quietly measure with whatever is installed."""
    monkeypatch.setattr(harness, "tool_images", dict)
    with pytest.raises(harness.LoadHarnessError, match="no PATH fallback"):
        harness.k6_image()


# ------------------------------------------------- reading the measurement ---


def test_a_missing_counter_raises_rather_than_reading_as_zero() -> None:
    """An absent metric and a metric that read zero are different facts.

    This is the specific way a harness starts substantiating claims it never
    measured: the 5xx counter disappears, the post-processor defaults it to 0,
    and the run reports a clean bill of health it never took.
    """
    with pytest.raises(harness.LoadHarnessError, match="different"):
        harness.extract(summary(drop="gateway_http_5xx"))


def test_a_summary_without_a_run_duration_raises() -> None:
    """Without it the achieved rate is unknown, so 'at 500 TPS sustained' is unproven."""
    broken = summary()
    del broken["state"]
    with pytest.raises(harness.LoadHarnessError, match="achieved rate"):
        harness.extract(broken)


def test_every_figure_comes_from_the_summary() -> None:
    """Each field traces to a metric k6 reported, not to a default in the harness."""
    measured = harness.extract(summary())
    assert measured.client_p99_ms == _TRENDS["http_req_duration"]["p(99)"]
    assert measured.client_p50_ms == _TRENDS["http_req_duration"]["p(50)"]
    assert measured.server_p99_ms == _TRENDS["gateway_server_latency_ms"]["p(99)"]
    assert measured.requests == _COUNTERS["http_reqs"]
    assert measured.achieved_tps == pytest.approx(500.0), (
        "the achieved rate is iterations over the measured run duration; a mismatch "
        "means the harness is not computing the quantity the target is stated in"
    )


# ----------------------------------------------------------------- verdicts --


def _verdict(verdicts: list[Any], name: str) -> Any:
    match = [v for v in verdicts if v.name == name]
    assert match, f"no verdict named {name!r}; the exit condition is not being checked"
    return match[0]


def test_zero_5xx_is_asserted_in_python_not_only_as_a_k6_threshold() -> None:
    """ROADMAP Phase 2's exit condition, enforced where it cannot be edited away.

    A threshold in the k6 script fails the run early, which is useful -- but a
    phase exit condition that lived only there could be relaxed in the same
    commit that failed it, and the diff would look like a config tweak.
    """
    clean = _verdict(harness.evaluate(harness.extract(summary()), target_tps=500), "zero_5xx")
    assert clean.passed

    dirty = _verdict(
        harness.evaluate(harness.extract(summary(gateway_http_5xx=1)), target_tps=500),
        "zero_5xx",
    )
    assert not dirty.passed, "a single 5xx must fail the exit condition; zero means zero"
    assert dirty.kind == harness.TARGET, (
        "a 5xx is a real result about a real system, so it is recorded and published "
        "as found rather than discarded as an invalid run (CLAUDE.md §17)"
    )


def test_a_rate_limited_run_is_rejected_as_a_measurement() -> None:
    """429s mean the limiter shaped the latency, not the scoring path."""
    verdicts = harness.evaluate(harness.extract(summary(gateway_http_429=17)), target_tps=500)
    limited = _verdict(verdicts, "not_rate_limited")
    assert not limited.passed
    assert limited.kind == harness.INTEGRITY, (
        "publishing a rate-limited run's p99 under a latency heading would be a false "
        "claim however honestly the number was collected"
    )
    assert "TRACE_RATE_LIMIT_PER_MINUTE" in limited.detail, (
        "the verdict must name the remedy; a gate that reports a symptom without one "
        "gets worked around"
    )


def test_client_errors_are_rejected_as_a_measurement() -> None:
    """A 4xx exercises the validation layer, whose latency is not the scoring path's."""
    verdicts = harness.evaluate(harness.extract(summary(gateway_http_4xx=3)), target_tps=500)
    assert not _verdict(verdicts, "no_client_errors").passed


def test_a_run_that_did_not_sustain_the_rate_is_rejected() -> None:
    """Half the offered load is half the test, and its p99 describes the smaller one."""
    verdicts = harness.evaluate(harness.extract(summary(iterations=150_000)), target_tps=500)
    under = _verdict(verdicts, "target_rate_sustained")
    assert not under.passed
    assert under.kind == harness.INTEGRITY


def test_dropped_iterations_are_rejected() -> None:
    """Offered load that never left the generator was never offered."""
    verdicts = harness.evaluate(harness.extract(summary(dropped_iterations=12)), target_tps=500)
    assert not _verdict(verdicts, "no_dropped_iterations").passed


def test_a_missed_latency_budget_is_a_target_failure_not_an_integrity_one() -> None:
    """A slow gateway is a finding. An invalid run is not a finding at all.

    Collapsing the two would mean either discarding a real result or publishing a
    meaningless one, and the harness must do neither.
    """
    slow = dict(_TRENDS["http_req_duration"])
    slow["p(99)"] = 180.0
    data = summary()
    data["metrics"]["http_req_duration"]["values"] = slow
    verdicts = harness.evaluate(harness.extract(data), target_tps=500)
    missed = _verdict(verdicts, "p99_under_budget")
    assert not missed.passed
    assert missed.kind == harness.TARGET
    assert not harness.failures(verdicts, harness.INTEGRITY), (
        "a slow but complete run is still a valid measurement and must be published"
    )


def test_every_roadmap_target_has_a_verdict() -> None:
    """The three Phase 2 conditions, each checked by name."""
    names = {v.name for v in harness.evaluate(harness.extract(summary()), target_tps=500)}
    assert {"zero_5xx", "p99_under_budget", "p50_under_budget"} <= names


# ------------------------------------------------------------- the record ---


def test_the_record_satisfies_every_field_the_claim_linter_requires() -> None:
    """Run against the linter's own `incomplete_fields`, not a copy of its list."""
    missing = linter.incomplete_fields(json.loads(json.dumps(asdict(record()))))
    assert missing == [], (
        f"the run record is missing {missing}; an incomplete record cannot "
        f"substantiate a number (ADR-0017)"
    )


def test_the_completeness_check_is_not_vacuous() -> None:
    """Break the record and require the linter to notice.

    Without this, the test above would pass just as happily against a linter that
    checked nothing.
    """
    broken = json.loads(json.dumps(asdict(record())))
    del broken["rule_pack_digest"]
    assert "rule_pack_digest" in linter.incomplete_fields(broken)


def test_a_dirty_worktree_run_is_not_publishable() -> None:
    """`docs/EVALUATION.md` §8 rule 4: the tree cannot be reconstructed."""
    assert record(dirty_worktree=False).publishable
    assert not record(dirty_worktree=True).publishable


def test_the_record_carries_the_measurement_at_full_precision() -> None:
    """The report rounds for reading; the record must not, or reruns cannot be compared."""
    written = asdict(record())["measured"]
    assert written["client_p99_ms"] == _TRENDS["http_req_duration"]["p(99)"]
    assert written["achieved_tps"] == pytest.approx(500.0)


# ------------------------------------------------------------- the report ---


def _scan_rendered(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, text: str, manifest: dict[str, Any] | None
) -> list[str]:
    """Run the REAL claim linter over a rendered report in a throwaway tree."""
    manifests = tmp_path / "eval" / "manifest"
    manifests.mkdir(parents=True)
    report = tmp_path / "benchmarks" / "gateway" / "REPORT.md"
    report.parent.mkdir(parents=True)
    report.write_text(text)
    if manifest is not None:
        (manifests / f"{manifest['run_id']}.json").write_text(json.dumps(manifest))
    monkeypatch.setattr(linter, "ROOT", tmp_path)
    monkeypatch.setattr(linter, "MANIFEST_DIR", manifests)
    monkeypatch.setattr(linter, "SCAN", [report])
    violations: list[str] = linter.scan()
    return violations


def _rendered() -> tuple[Any, str]:
    run = record()
    measured = harness.extract(summary())
    verdicts = harness.evaluate(measured, target_tps=500)
    return run, harness.render_report(run, measured, verdicts)


def test_the_rendered_report_passes_the_claim_linter(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """End to end: the renderer's output, the record it cites, and the real gate."""
    run, text = _rendered()
    violations = _scan_rendered(tmp_path, monkeypatch, text, json.loads(json.dumps(asdict(run))))
    assert violations == [], f"the rendered report would fail `make check-claims`: {violations}"


def test_the_report_actually_publishes_numbers_the_linter_recognises() -> None:
    """Otherwise the test above would pass on a report that claimed nothing.

    A report with no claim-shaped line is trivially clean, and a benchmark report
    that says nothing measurable is not the Phase 2 deliverable.
    """
    _run, text = _rendered()
    hits = [
        line
        for line in text.splitlines()
        if any(pattern.search(line) for pattern, _kind in linter.CLAIM_PATTERNS)
        and not linter.is_target_prose(line)
    ]
    assert hits, "the rendered report contains no line the claim linter treats as a result"


def test_the_run_id_headings_are_what_make_the_report_publishable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Strip the per-section `run_id` declarations and the gate must fire.

    The linter resolves a `run_id` section-scoped and clears it at the next
    heading. Repeating it under every heading is therefore load-bearing, not
    stylistic, and this proves it.
    """
    run, text = _rendered()
    stripped = text.replace(f"`run_id: {run.run_id}`", "results")
    violations = _scan_rendered(
        tmp_path, monkeypatch, stripped, json.loads(json.dumps(asdict(run)))
    )
    assert violations, (
        "a report whose numbers sit under no declared run_id must be rejected; "
        "otherwise the heading convention is decoration"
    )


def test_a_report_citing_a_dirty_run_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Rule 4 enforced end to end. The runner also refuses to write the file."""
    run, text = _rendered()
    manifest = json.loads(json.dumps(asdict(record(dirty_worktree=True))))
    violations = _scan_rendered(tmp_path, monkeypatch, text, manifest)
    assert any("dirty worktree" in v for v in violations), (
        f"a run from an unreconstructable tree must not back a published number: {violations}"
    )
    assert run.run_id == manifest["run_id"]


# ------------------------------------------------------- invoking the tool ---


def test_the_service_token_never_reaches_the_docker_argument_list() -> None:
    """`-e NAME` inherits the value; `-e NAME=value` writes it into `ps`.

    docs/SECURITY.md §9: no secret in a log. An argument list is captured by the
    process table, the shell history and every CI log, so it is one.
    """
    # Named `presented_token` rather than the obvious word: detect-secrets flags
    # the KEYWORD, and it is right to. The fix is to stop writing that keyword,
    # not to suppress the scanner or exclude this file -- either of which would
    # hide a real credential added here later (CLAUDE.md §9).
    presented_token = "loadtest.a-fabricated-value-that-must-not-appear"
    command = harness.k6_command(
        script=harness.PROFILES["representative"],
        docker="/usr/bin/docker",
        image="grafana/k6:0.49.0@sha256:" + "8" * 64,
        base_url="http://localhost:8010",
        target_tps=500,
        duration_s=600,
        seed=1,
        nonce="abc123",
        pre_allocated_vus=120,
        max_vus=800,
        out_dir=Path("/tmp/out"),  # noqa: S108 - a literal for an argument-shape assertion
    )
    assert presented_token not in " ".join(command)
    assert harness.TOKEN_ENV in command, (
        "the token variable must still be passed through by name, or the run authenticates nothing"
    )
    assert not any(arg.startswith(f"{harness.TOKEN_ENV}=") for arg in command)


def test_localhost_is_rewritten_for_the_container() -> None:
    """Inside a container `localhost` is the container.

    Without this the run reports ten minutes of connection refusals as a load
    result, which is a failure that looks like a measurement.
    """
    assert harness.container_base_url("http://localhost:8010") == "http://host.docker.internal:8010"
    assert harness.container_base_url("http://127.0.0.1:8010") == "http://host.docker.internal:8010"
    assert harness.container_base_url("http://gateway.internal:8010") == (
        "http://gateway.internal:8010"
    ), "a real hostname must be left alone; only the loopback aliases are ambiguous"


def test_a_non_http_base_url_is_refused() -> None:
    """An operator-supplied URL must not be able to turn a probe into a file read."""
    with pytest.raises(harness.LoadHarnessError, match="non-HTTP"):
        harness._http_json("file:///etc/passwd", method="GET", token=None, body=None, timeout=1)


# ----------------------------------------------------------- the k6 script --


@pytest.mark.parametrize("profile", sorted(K6_SCRIPTS))
def test_the_k6_script_sends_a_unique_idempotency_key_per_request(profile: str) -> None:
    """The single change that would turn this benchmark into a cache measurement.

    The gateway caches responses against `X-Idempotency-Key`. A constant or
    per-VU key makes every request after the first a replay-cache read, and the
    run reports the latency of a Redis GET under a scoring heading. The per-run
    nonce matters for the same reason across runs, not just within one.
    """
    source = K6_SCRIPTS[profile].read_text()
    assert "'X-Idempotency-Key': `${RUN_NONCE}-${exec.vu.idInTest}-${iteration}`" in source, (
        "the idempotency key must vary by run, VU and iteration; anything else "
        "measures the replay cache"
    )
    assert "RUN_NONCE" in source and "no-nonce" in source, (
        "the script must refuse to run without a per-invocation nonce, or the second "
        "run replays the first one's cached responses"
    )


@pytest.mark.parametrize("profile", sorted(K6_SCRIPTS))
def test_the_k6_script_varies_every_entity_the_features_are_keyed_on(profile: str) -> None:
    """One repeated transaction measures one hot Redis key, not a system."""
    source = K6_SCRIPTS[profile].read_text()
    for field_name in ("account_id", "merchant_id", "device_id", "ip_id", "amount_minor"):
        assert f"{field_name}:" in source, f"{field_name} is not present in the generated payload"
    assert "mulberry32" in source, (
        "the traffic must be seeded, or two runs differ by the load rather than by "
        "the system under test (CLAUDE.md §3.5)"
    )


DISTRIBUTION_MECHANISM = {
    # The heavy tail that makes this profile adversarial: 80% of load onto 5% of
    # each pool. It is the reason ~92% of its requests triage, and it is kept.
    "triage-saturation": "skewedIndex",
    # Affinity instead of skew. Each account's devices, IPs and merchants are
    # derived from its own index, so they are stable across the run the way the
    # frozen dataset's are -- which is what makes `merchant_is_habitual` and
    # `device_is_known_for_account` mean anything.
    "representative": "derived(",
}


@pytest.mark.parametrize("profile", sorted(K6_SCRIPTS))
def test_each_profile_keeps_its_own_distribution_mechanism(profile: str) -> None:
    """The two profiles must not converge on one distribution.

    This assertion used to demand `skewedIndex` of every script, which encoded
    the adversarial profile's hot-entity skew as though it were a general
    requirement. It is the opposite: that skew is exactly what made the old
    workload unrepresentative, and requiring it of the acceptance gate would
    reintroduce the defect the gate exists to avoid. So each profile declares
    the mechanism it is supposed to use, and a profile that lost its mechanism
    fails here rather than silently becoming the other one.
    """
    source = K6_SCRIPTS[profile].read_text()
    expected = DISTRIBUTION_MECHANISM[profile]
    assert expected in source, (
        f"the `{profile}` profile no longer uses `{expected}`. The acceptance gate needs "
        f"per-account affinity and the saturation profile needs hot-entity skew; a profile "
        f"that drifts into the other's distribution stops measuring what its report claims."
    )
    if profile == "representative":
        assert "skewedIndex" not in source, (
            "the acceptance gate must not concentrate load onto a small hot pool. That is "
            "what put 1,440 transactions per account per hour against a 40-per-hour "
            "threshold and drove 91.7% of requests into triage (ADR-0040)."
        )


@pytest.mark.parametrize("profile", sorted(K6_SCRIPTS))
def test_the_k6_script_declares_the_thresholds_that_force_the_metrics_to_exist(
    profile: str,
) -> None:
    """`dropped_iterations` is absent from the summary unless a threshold registers it.

    Verified against the pinned image. Without the threshold the post-processor
    would have to infer zero from absence, which is the inference the whole
    harness is built to avoid.
    """
    source = K6_SCRIPTS[profile].read_text()
    assert "dropped_iterations: ['count==0']" in source
    assert "gateway_http_5xx: ['count==0']" in source


# --- the rate assertion is binding on the gate and not on the saturation profile ---


def _baseline_measured() -> Any:
    """A clean run: every integrity condition satisfied, both targets met.

    Written once so each test below changes exactly one thing and the reader can
    see which field is under test rather than diffing two literals.
    """
    return harness.Measured(
        client_p50_ms=1.7,
        client_p90_ms=3.1,
        client_p95_ms=3.3,
        client_p99_ms=14.2,
        client_avg_ms=2.2,
        client_min_ms=1.3,
        client_max_ms=90.7,
        server_p50_ms=0.31,
        server_p99_ms=0.43,
        server_samples=300_001,
        requests=300_001,
        iterations=300_001,
        dropped_iterations=0,
        test_run_duration_s=600.0,
        achieved_tps=500.0,
        unparseable_responses=0,
        degraded_responses=0,
        degraded_unexpected=0,
        history_incomplete=0,
        band_low=299_994,
        band_medium=2,
        band_high=2,
        band_critical=3,
        http_2xx=300_001,
        http_4xx=0,
        http_5xx=0,
        http_429=0,
    )


def _measured_missing_the_rate() -> Any:
    """A run that achieved half its offered rate and dropped a third of it."""
    return harness.Measured(
        **{
            **asdict(_baseline_measured()),
            "achieved_tps": 250.0,
            "dropped_iterations": 100_000,
        }
    )


def test_the_acceptance_gate_still_refuses_a_run_that_missed_its_rate() -> None:
    """The whole point of the integrity condition, and it must not have moved.

    The saturation profile is allowed to record a rate it did not sustain,
    because finding that rate is its job. If that leniency ever reaches the
    `representative` profile, a run at half the offered load would publish its
    latency under a 500 TPS heading -- which is the exact false claim the
    condition exists to prevent.
    """
    verdicts = harness.evaluate(
        _measured_missing_the_rate(), target_tps=500, profile="representative"
    )
    by_name = {v.name: v for v in verdicts}
    assert by_name["target_rate_sustained"].passed is False, (
        "the acceptance gate accepted a run that achieved 250 of 500 TPS"
    )
    assert by_name["no_dropped_iterations"].passed is False, (
        "the acceptance gate accepted a run that dropped 100,000 iterations"
    )


def test_the_saturation_profile_records_the_rate_it_reached_instead_of_refusing() -> None:
    """A benchmark built to find the limit cannot assert it never reached one."""
    verdicts = harness.evaluate(
        _measured_missing_the_rate(), target_tps=500, profile="triage-saturation"
    )
    by_name = {v.name: v for v in verdicts}
    assert by_name["target_rate_sustained"].passed is True
    assert "SATURATION RESULT" in by_name["target_rate_sustained"].detail, (
        "the verdict must say the number is a result and not a gate, or it reads as a pass"
    )
    assert "250.0" in by_name["target_rate_sustained"].detail, (
        "the achieved rate must appear in the verdict; it is the finding"
    )


def test_every_other_integrity_condition_applies_to_both_profiles() -> None:
    """Only the rate and the backlog are profile-dependent.

    4xx responses measure the validation layer and 429s measure the limiter on
    either profile, so those refusals stay in force. Relaxing them for the
    saturation run would let it characterise something other than the hot path.
    """
    broken = harness.Measured(
        **{**asdict(_baseline_measured()), "http_4xx": 5_000, "http_429": 5_000}
    )
    for profile in ("representative", "triage-saturation"):
        by_name = {v.name: v for v in harness.evaluate(broken, target_tps=500, profile=profile)}
        assert by_name["no_client_errors"].passed is False, profile
        assert by_name["not_rate_limited"].passed is False, profile


# ------------------------------------------------------------- experiments ---


def test_experiment_flags_come_together_and_record_outside_the_repository(tmp_path: Path) -> None:
    """A comparison records every run on a clean tree: a record written inside the repository
    would dirty the worktree for the next run before it started (docs/EVALUATION.md §8 rule 4)."""
    assert harness.experiment_problem(None, None, None) is None
    assert harness.experiment_problem("observation-log-ab", "log-on", tmp_path) is None
    assert "together" in harness.experiment_problem("observation-log-ab", None, tmp_path)
    assert "inside the repository" in harness.experiment_problem(
        "observation-log-ab", "log-on", ROOT / "eval" / "manifest"
    )
    assert "lowercase" in harness.experiment_problem("Observation Log", "log-on", tmp_path)


def test_an_experiment_record_carries_its_arm_and_the_live_checks_and_stays_complete() -> None:
    checks = {"writer_session": "active s-1", "observation_log": "ok", "outbox_relay": "disabled"}
    run = record(experiment="observation-log-ab", arm="log-on", gateway_checks=checks)
    written = json.loads(json.dumps(asdict(run)))
    assert (written["experiment"], written["arm"], written["gateway_checks"]) == (
        "observation-log-ab",
        "log-on",
        checks,
    )
    assert linter.incomplete_fields(written) == []


def test_a_gate_record_names_no_experiment() -> None:
    written = asdict(record())
    assert (written["experiment"], written["arm"], written["gateway_checks"]) == (None, None, {})


def test_readiness_and_its_checks_are_read_from_the_gateway(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checks = {"writer_session": "active s-1", "observation_log": "ok"}
    ready = {"ready": True, "checks": checks}
    monkeypatch.setattr(harness, "_http_json", lambda *a, **k: (200, ready))
    assert harness.probe_readiness("http://localhost:8010") == (True, checks)
    not_ready = {"ready": False, "checks": checks}
    monkeypatch.setattr(harness, "_http_json", lambda *a, **k: (503, not_ready))
    assert harness.probe_readiness("http://localhost:8010") == (False, checks)
    monkeypatch.setattr(harness, "_http_json", lambda *a, **k: (503, {}))
    with pytest.raises(harness.LoadHarnessError):
        harness.probe_readiness("http://localhost:8010")


def test_runs_on_one_commit_and_day_get_distinct_run_ids() -> None:
    """Two arms of one experiment, minutes apart on the same commit, once shared an id."""
    first = harness.new_run_id(dt.datetime(2026, 9, 15, 1, 48, 53, tzinfo=dt.UTC))
    second = harness.new_run_id(dt.datetime(2026, 9, 15, 1, 52, 9, tzinfo=dt.UTC))
    assert first != second
    assert first.startswith("load-20260915-014853-gateway-")
    assert linter.RUN_ID.search(f"`run_id: {first}`").group(1) == first


def test_a_run_record_is_never_overwritten(tmp_path: Path) -> None:
    run = record()
    run.write(tmp_path)
    with pytest.raises(harness.LoadHarnessError, match="never overwritten"):
        run.write(tmp_path)
