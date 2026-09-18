"""The compose file's shape is a control, not a convenience (ADR-0024).

`core` is promised to be a complete working product that fits on a laptop
(CLAUDE.md §12, docs/ARCHITECTURE.md §14). Three things can quietly break that
promise, and none of them shows up as a failing service:

* a service with no `profiles` key, which starts under every profile combination
  and puts the full stack's memory on a `make up`;
* a service with no memory limit, which is free to take the whole VM and
  OOM-kill its neighbours rather than itself;
* limits that individually look modest and collectively exceed the 2.7 GB the
  architecture budgets for the profile.

So the shape is asserted here rather than reviewed by eye. The gateway's own
wiring is asserted for the same reason: its health probe, its dependency
conditions and the ports it reaches its dependencies on are each a decision with
a documented failure behind it, and each is one edit away from being reversed.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path
from typing import Any

import pytest
import yaml
from services.gateway.app import POOL_READY_WAIT_S as POOL_OPEN_TIMEOUT_S

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "deploy" / "compose.yml"
GATEWAY_DOCKERFILE = ROOT / "deploy" / "gateway.Dockerfile"

CORE_BUDGET_MIB = 2.7 * 1024
"""docs/ARCHITECTURE.md §14 budgets the whole `core` profile at ~2.7 GB."""

CORE_SERVICES_STILL_TO_COME = ("api", "worker", "ui")
"""§14 puts these in `core` too; the 3 MCP servers are stdio and cost no
container. Whatever the containerised services claim, these three must still
fit in what is left."""

"""`services/gateway/app.py` opens the psycopg pool with `timeout=10` at
start-up and serves anyway when it expires."""

SENSITIVE_KEY = re.compile(r"TOKEN|PASSWORD|SECRET|KEY|AUTH", re.I)
INTERPOLATED = re.compile(r"\$\{[A-Z_][A-Z0-9_]*")


def _compose() -> dict[str, Any]:
    """The file as written, NOT `docker compose config`.

    Interpolation is exactly what these tests need to see: a committed
    credential and a reference to one render identically once compose has
    resolved them.
    """
    document: dict[str, Any] = yaml.safe_load(COMPOSE.read_text())
    return document


def _services() -> dict[str, Any]:
    services: dict[str, Any] = _compose()["services"]
    return services


def _profile(name: str) -> dict[str, Any]:
    return {key: value for key, value in _services().items() if name in value.get("profiles", [])}


def _memory_mib(service: dict[str, Any]) -> float | None:
    raw = service.get("deploy", {}).get("resources", {}).get("limits", {}).get("memory")
    if raw is None:
        return None
    match = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([KMG])", str(raw).strip())
    assert match, f"unparseable memory limit {raw!r}; expected a form like '768M'"
    return float(match.group(1)) * {"K": 1 / 1024, "M": 1.0, "G": 1024.0}[match.group(2)]


def _gateway() -> dict[str, Any]:
    return _services()["gateway"]


# ----------------------------------------------------------- profile shape --


def test_every_service_declares_a_profile() -> None:
    """A service with no profile starts under every profile combination.

    That is how `core` stops being the 2.7 GB product ADR-0024 promises without
    anyone editing ADR-0024.
    """
    unprofiled = sorted(name for name, svc in _services().items() if not svc.get("profiles"))
    assert not unprofiled, (
        f"services with no `profiles` key: {unprofiled}. Each would start under "
        f"every profile, including a bare `make up`."
    )


def test_the_gateway_is_in_the_core_profile() -> None:
    """§14 lists the gateway in `core`, and `core` must be a working product:
    a profile with a database and no way to score a transaction is not one."""
    assert "gateway" in _profile("core"), (
        "the gateway is missing from the `core` profile; `make up` would bring up "
        "storage with nothing serving the hot path (docs/ARCHITECTURE.md §14)"
    )


def test_the_gateway_is_in_core_only() -> None:
    """Listing it in a second profile would start two of it, both binding the
    same published port, and the second would fail in a way that reads as a
    port conflict rather than a duplicate declaration."""
    assert _gateway()["profiles"] == ["core"], (
        f"gateway declares profiles {_gateway()['profiles']}; `core` alone is correct "
        f"(CLAUDE.md §12: `core` runs with no cloud account and no API key)"
    )


def test_core_starts_without_any_paid_or_keyed_dependency() -> None:
    """CLAUDE.md §12: `make up` must work on a laptop with no API key.

    The `llm` profile is where a model lives; nothing in `core` may require one.
    """
    for name, service in _profile("core").items():
        rendered = yaml.safe_dump(service)
        for forbidden in ("OPENAI_API_KEY", "AWS_ACCESS_KEY", "ANTHROPIC_API_KEY", "KAGGLE_KEY"):
            assert forbidden not in rendered, (
                f"core service '{name}' references {forbidden}; `core` must start on a "
                f"bare clone with no account anywhere"
            )


# ---------------------------------------------------------------- budget ----


def test_every_core_service_declares_a_memory_limit() -> None:
    """An unbounded container does not fail first -- it takes the VM down with
    it, and the service that dies is whichever one asked for memory next."""
    missing = sorted(name for name, svc in _profile("core").items() if _memory_mib(svc) is None)
    assert not missing, f"core services with no memory limit: {missing}"


def test_core_memory_limits_fit_the_architecture_budget() -> None:
    """The limits are what the 2.7 GB in §14 actually means on this machine."""
    limits = {name: _memory_mib(svc) or 0.0 for name, svc in _profile("core").items()}
    claimed = sum(limits.values())
    remaining = CORE_BUDGET_MIB - claimed
    assert claimed <= CORE_BUDGET_MIB, (
        f"core profile claims {claimed:.0f} MiB against the {CORE_BUDGET_MIB:.0f} MiB "
        f"budget in docs/ARCHITECTURE.md §14: {limits}. Either the limits or §14 is "
        f"wrong, and a laptop finds out by OOM-killing a container."
    )
    assert remaining > 0, (
        f"nothing is left for {list(CORE_SERVICES_STILL_TO_COME)}, which §14 also puts in `core`"
    )


def test_the_gateway_limit_leaves_room_for_the_rest_of_core() -> None:
    """The gateway is the first of four service containers §14 puts in `core`.

    Sizing it by what one process happens to use today, with no regard for the
    three still to come, is how the budget is spent before they arrive.
    """
    gateway_mib = _memory_mib(_gateway())
    assert gateway_mib is not None
    claimed = sum(_memory_mib(svc) or 0.0 for svc in _profile("core").values())
    left = CORE_BUDGET_MIB - claimed
    assert left >= gateway_mib, (
        f"the gateway claims {gateway_mib:.0f} MiB and only {left:.0f} MiB remains for "
        f"{list(CORE_SERVICES_STILL_TO_COME)}; the worker runs the agent graph and is "
        f"not the cheapest of them"
    )


# --------------------------------------------------------------- gateway ----


def test_the_gateway_is_built_from_this_repository() -> None:
    """A pulled image could drift from the source tree beside it, and every
    latency number measured against it would be unattributable (ADR-0017)."""
    build = _gateway()["build"]
    assert build["context"] == "..", (
        f"build context is {build['context']!r}; it must be the repository root, which "
        f"is where pyproject.toml, requirements.lock, packages/ and services/ live"
    )
    dockerfile = ROOT / "deploy" / Path(build["dockerfile"]).name
    assert dockerfile.is_file(), f"{build['dockerfile']} does not exist"


def test_the_gateway_healthcheck_probes_liveness_not_readiness() -> None:
    """/healthz touches no dependency; /readyz connects to Postgres as trace_app.

    Two independent reasons this must be /healthz. `make up` runs `up --wait`
    and THEN `make migrate`, and trace_app is a role the migration creates -- a
    readiness probe here would never pass on a fresh clone, so `make up` would
    hang before running the migration that would have fixed it. And a probe that
    fails on a Postgres blip restarts a process that is degrading exactly as
    ARCHITECTURE §18 designed it to.
    """
    probe = " ".join(str(part) for part in _gateway()["healthcheck"]["test"])
    assert "/healthz" in probe, f"gateway healthcheck does not probe /healthz: {probe}"
    assert "/readyz" not in probe, (
        "the container healthcheck probes /readyz. It gates `up --wait`, which runs "
        "before `make migrate` creates the trace_app role, so a fresh clone would "
        "deadlock. /readyz is the load balancer's question (ADR-0035)."
    )


def test_the_gateway_start_period_outlasts_the_pool_open_timeout() -> None:
    """Start-up waits the pool's full timeout when Postgres is not ready yet.

    Measured at ~11 s on a first `make up`, because `open_pool`'s first checkout
    waits POOL_READY_WAIT_S and before the app logs postgres_pool_not_ready and serves anyway. A
    shorter grace period reports a gateway starting exactly as designed as a
    failed one.
    """
    raw = str(_gateway()["healthcheck"]["start_period"])
    seconds = float(raw.removesuffix("s"))
    assert seconds > POOL_OPEN_TIMEOUT_S, (
        f"start_period is {raw}, within the {POOL_OPEN_TIMEOUT_S}s the psycopg pool may "
        f"spend opening at start-up (services/gateway/app.py)"
    )


def test_the_gateway_waits_for_postgres_but_not_for_redis_health() -> None:
    """The asymmetry is ADR-0035's, and it is the whole design.

    Postgres loss means a decision cannot be durably recorded, so triage refuses.
    Redis loss is a visible degradation the hot path is built to survive, and
    gating start-up on its health would convert the dependency the gateway
    tolerates into the one that keeps it down.
    """
    depends: dict[str, Any] = _gateway()["depends_on"]
    assert depends["postgres"]["condition"] == "service_healthy", (
        "the gateway must wait for a healthy Postgres: triage has no fallback"
    )
    assert depends["redis"]["condition"] != "service_healthy", (
        "the gateway waits for Redis to be HEALTHY, which contradicts "
        "docs/ARCHITECTURE.md §18 -- a Redis-less gateway is a documented, working, "
        "degraded mode, not a start-up failure"
    )


def test_the_gateway_reaches_its_dependencies_on_in_network_ports() -> None:
    """.env's 5442/6389 are the ports published to the HOST (ADR-0024).

    Inside the compose network the containers listen on 5432 and 6379, and
    GatewaySettings builds both DSNs straight from these variables -- so
    inheriting the host values here is a connection refused on every request,
    from a value that looks right in .env.
    """
    environment = _gateway()["environment"]
    assert str(environment["POSTGRES_PORT"]) == "5432", (
        f"gateway POSTGRES_PORT is {environment['POSTGRES_PORT']!r}; inside the network "
        f"postgres listens on 5432, not on the port published to the host"
    )
    assert str(environment["REDIS_PORT"]) == "6379", (
        f"gateway REDIS_PORT is {environment['REDIS_PORT']!r}; inside the network redis "
        f"listens on 6379, not on the port published to the host"
    )
    assert environment["POSTGRES_HOST"] == "postgres"
    assert environment["REDIS_HOST"] == "redis"


def test_the_gateway_publishes_the_non_standard_port() -> None:
    """ADR-0024 keeps every published port off the common default, so the stack
    does not collide with whatever else the developer is running."""
    published = [str(p) for p in _gateway()["ports"]]
    assert any("TRACE_GATEWAY_PORT" in p and "8010" in p for p in published), (
        f"gateway ports are {published}; expected ${{TRACE_GATEWAY_PORT:-8010}} so the "
        f"default is the non-standard port and .env can still override it"
    )


def test_the_gateway_runs_on_the_app_role_not_the_superuser() -> None:
    """CLAUDE.md §11: trace_app has no grant on `groundtruth`, and that is the
    isolation control. Handing the gateway the superuser sitting next to it in
    .env would dissolve the control without touching a migration."""
    user = str(_gateway()["environment"]["TRACE_APP_DB_USER"])
    assert "trace_app" in user, f"gateway database user resolves from {user!r}"
    rendered = yaml.safe_dump(_gateway())
    for role in ("POSTGRES_SUPERUSER", "TRACE_EVAL_DB", "TRACE_GENERATOR_DB"):
        assert role not in rendered, (
            f"the gateway is given {role}; the hot path gets the application role only "
            f"(CLAUDE.md §11 -- trace_eval is the one role that may read ground truth)"
        )


def test_the_gateway_requires_a_service_token_loudly() -> None:
    """The gateway refuses to start without one (docs/SECURITY.md §3, Plane B).

    `${VAR:?message}` turns that into one readable line from `docker compose up`
    instead of a container crash-looping on a stack trace under
    `restart: unless-stopped`.
    """
    tokens = {
        key: str(value)
        for key, value in _gateway()["environment"].items()
        if key.startswith("TRACE_SERVICE_TOKEN_")
    }
    assert tokens, (
        "the gateway declares no TRACE_SERVICE_TOKEN_<ID>; it would fail at start-up "
        "with TokenConfigurationError on every `make up`"
    )
    for key, value in tokens.items():
        assert ":?" in value, (
            f"{key} has a default or no guard ({value!r}). A missing token must fail the "
            f"`up` with a message, not leave a container restarting forever."
        )


# ------------------------------------------------------------- no secrets --


@pytest.mark.parametrize("service_name", sorted(_services()))
def test_no_credential_is_written_literally_into_compose(service_name: str) -> None:
    """CLAUDE.md §9: no secret in code, image or log -- compose is committed."""
    environment: dict[str, Any] = _services()[service_name].get("environment", {}) or {}
    for key, value in environment.items():
        if not SENSITIVE_KEY.search(key):
            continue
        assert INTERPOLATED.search(str(value)), (
            f"{service_name}.{key} is set to a literal value. Credentials come from the "
            f"environment (.env is gitignored); a literal here is a committed secret."
        )


# ------------------------------------------------------------- dockerfile --


def _dockerfile() -> str:
    return GATEWAY_DOCKERFILE.read_text()


def test_the_gateway_image_uses_the_pinned_python() -> None:
    """ADR-0018 pins Python to 3.12, and requirements.lock was resolved for it.

    An image on another minor would install a different dependency set from the
    one every gate in this repository ran against.
    """
    pinned = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["trace_x"]["pins"]
    base = re.search(r"^ARG PYTHON_IMAGE=(\S+)", _dockerfile(), re.M)
    assert base, "the gateway Dockerfile declares no base image"
    assert f"python:{pinned['python']}-" in base.group(1), (
        f"base image {base.group(1)} is not the pinned Python {pinned['python']} "
        f"([tool.trace_x.pins], ADR-0018)"
    )
    assert "@sha256:" in base.group(1), (
        "the base image is pinned by tag alone. A tag can be moved and a digest cannot "
        "-- the same reasoning ADR-0036 applies to the oasdiff and k6 images, and "
        "image_digests is a field of every run manifest (ADR-0017)."
    )


def test_the_gateway_image_does_not_serve_as_root() -> None:
    """The gateway is the system's public surface (docs/SECURITY.md §3).

    A process that can rewrite its own source turns a code-execution bug into
    persistence, so the serving account owns none of the code it runs.
    """
    users = re.findall(r"^USER\s+(\S+)", _dockerfile(), re.M)
    assert users, "the gateway image never drops privileges: no USER instruction"
    assert users[-1] not in {"root", "0"}, f"the gateway image serves as {users[-1]!r}"
