"""Gateway configuration, resolved once at start-up.

Everything here is read from the environment and validated before the first
request, because the alternative is discovering a misconfiguration one request at
a time. Two settings in particular are fatal when absent — service tokens and the
rule pack — for the reason `docs/ARCHITECTURE.md` §18 gives about a corrupt model
artifact: a gateway serving traffic it cannot score correctly is worse than a
gateway that is down, because the first one looks like it is working.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Final

SERVICE_NAME: Final = "trace-gateway"
SERVICE_VERSION: Final = "0.1.0"

REDIS_TIMEOUT_S: Final = 0.02
"""20 ms, from `docs/ARCHITECTURE.md` §18's Redis row.

A hard ceiling on how much of the 100 ms budget a feature read may consume before
the hot path gives up and degrades to rules-only. Tight on purpose: a slow Redis
and an absent Redis must cost the same, or a degraded dependency becomes a
latency incident instead of a visible degradation.
"""

POSTGRES_TIMEOUT_S: Final = 2.0
"""Triage is a small write, so it can afford far more than a feature read -- but
not unbounded: a hung write would hold a hot-path request open until the client
gave up.

It is NOT a small fraction of traffic, which this comment used to claim. The
500 TPS load run opened **175,442 cases against ~183,000 requests** -- 96% --
because the load profile concentrates 80% of traffic on 5% of accounts and the
velocity rules fire on exactly that. Triage is the common path under load, and
the pool below is sized for that measurement rather than for the assumption."""

REDIS_MAX_CONNECTIONS: Final = 24
"""Ceiling on sockets the gateway will open to Redis.

Not a concurrency target. The routes run on the event loop, so the gateway
issues one Redis call at a time and a handful of connections is all it can use;
this is a BOUND, to stop a stalled Redis being answered by opening sockets until
the file-descriptor limit decides the outcome. redis-py's default is effectively
unbounded, which turns a slow dependency into a resource exhaustion.

It was briefly 64+8, sized for a worker threadpool that measurement then removed
(ADR-0039). The number is smaller now because the concurrency it was sized for
does not exist -- keeping the larger value would have left a bound that bounded
nothing and implied a threading model the code no longer has.
"""


@dataclass(frozen=True, slots=True)
class GatewaySettings:
    """Resolved gateway configuration."""

    redis_url: str
    postgres_dsn: str
    rate_limit: int
    rate_limit_window_s: int
    pool_min_size: int
    pool_max_size: int
    log_json: bool

    @classmethod
    def from_environment(cls, environ: dict[str, str] | None = None) -> GatewaySettings:
        env = environ if environ is not None else dict(os.environ)
        host = env.get("REDIS_HOST", "localhost")
        port = env.get("REDIS_PORT", "6389")
        db = env.get("REDIS_DB", "0")
        return cls(
            redis_url=f"redis://{host}:{port}/{db}",
            postgres_dsn=(
                "postgresql://{user}:{password}@{host}:{port}/{db}".format(
                    user=env.get("TRACE_APP_DB_USER", "trace_app"),
                    password=env.get("TRACE_APP_DB_PASSWORD", ""),
                    host=env.get("POSTGRES_HOST", "localhost"),
                    port=env.get("POSTGRES_PORT", "5442"),
                    db=env.get("POSTGRES_DB", "tracex"),
                )
            ),
            rate_limit=int(env.get("TRACE_RATE_LIMIT_PER_MINUTE", "1000")),
            rate_limit_window_s=int(env.get("TRACE_RATE_LIMIT_WINDOW_S", "60")),
            # Sized for the ROADMAP's 500 TPS target from the measured triage
            # rate (96%) and the measured transaction cost (1.434 ms mean,
            # 2.596 ms p99): 500 x 0.96 x 0.002596 = 1.25 connections busy at
            # p99. max_size is not that number -- it is the ceiling that keeps a
            # SLOW Postgres from parking all 64 request threads on the pool,
            # while staying far below the server's own max_connections. Beyond
            # it, `open_case` raises PoolTimeout, which surfaces as the 503
            # ADR-0035 specifies: refusing is correct when a case cannot be
            # durably recorded. min_size > 0 so the first triaged transaction
            # does not pay connection setup inside its own latency budget.
            pool_min_size=int(env.get("TRACE_PG_POOL_MIN", "4")),
            pool_max_size=int(env.get("TRACE_PG_POOL_MAX", "32")),
            log_json=env.get("TRACE_LOG_FORMAT", "json").lower() == "json",
        )

    @property
    def redacted_postgres_dsn(self) -> str:
        """The DSN with its password removed, for logs and health output."""
        if "@" not in self.postgres_dsn:
            return self.postgres_dsn
        scheme, _, rest = self.postgres_dsn.partition("://")
        credentials, _, location = rest.partition("@")
        user, _, _password = credentials.partition(":")
        return f"{scheme}://{user}:***@{location}"
