"""Service-token authentication for the gateway (docs/SECURITY.md §3, Plane B).

**This is Plane B, and only Plane B.** `docs/SECURITY.md` §3 describes three
independent authorization planes: human RBAC (Plane A), service identity (Plane
B) and agent capability tokens (Plane C). The gateway has no human users — its
callers are payment systems — so it authenticates service identity and nothing
else. Human RBAC belongs to `trace-api`, and capability tokens to the agent
runtime; implementing either here would put a check in a place nobody audits for
it.

**Comparison is constant-time.** A token check that returns early on the first
differing byte leaks the token one byte at a time to anyone who can measure
response latency, and a fraud gateway is exactly the kind of endpoint someone
will measure.

**The secret is never the identifier.** A token presents as `<token_id>.<secret>`
so logs, metrics and rate-limit keys can carry the id while the secret stays out
of every one of them. Keying a rate limiter by the whole token would write
credentials into Redis and into any dump of it.

**Secrets are configured, never defaulted.** There is no fallback token: a
gateway that authenticated a built-in credential when none was configured would
be open by default, and the failure would be silent. With no tokens configured
the gateway refuses to start.
"""

from __future__ import annotations

import hmac
import os
from dataclasses import dataclass
from typing import Final

from trace_core.domain.errors import TraceXError

ENV_PREFIX: Final = "TRACE_SERVICE_TOKEN_"
"""`TRACE_SERVICE_TOKEN_<ID>=<secret>` configures one caller.

Environment rather than a table: these are deployment credentials, and putting
them in the application database would make a database compromise an
authentication compromise too.
"""

MIN_SECRET_LENGTH: Final = 32
SEPARATOR: Final = "."


class TokenConfigurationError(TraceXError):
    """No usable service tokens are configured.

    Fatal at start-up rather than a per-request failure: a gateway that accepts
    connections and rejects every one of them looks like an outage in the caller,
    and the operator finds out last.
    """


@dataclass(frozen=True, slots=True)
class ServiceToken:
    """One authenticated caller."""

    token_id: str

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.token_id


class ServiceTokenVerifier:
    """Verifies `<token_id>.<secret>` against configured credentials."""

    def __init__(self, secrets: dict[str, str]) -> None:
        if not secrets:
            raise TokenConfigurationError(
                f"no service tokens configured. Set at least one "
                f"{ENV_PREFIX}<ID>=<secret> (>= {MIN_SECRET_LENGTH} characters). The "
                f"gateway refuses to start rather than authenticate a default "
                f"credential: open-by-default fails silently."
            )
        for token_id, secret in secrets.items():
            if len(secret) < MIN_SECRET_LENGTH:
                raise TokenConfigurationError(
                    f"service token {token_id!r} has a {len(secret)}-character secret; "
                    f"at least {MIN_SECRET_LENGTH} are required. A short secret is "
                    f"guessable, and a gateway is a public surface."
                )
        self._secrets = dict(secrets)

    @classmethod
    def from_environment(cls, environ: dict[str, str] | None = None) -> ServiceTokenVerifier:
        env = environ if environ is not None else dict(os.environ)
        secrets = {
            key[len(ENV_PREFIX) :].lower(): value
            for key, value in env.items()
            if key.startswith(ENV_PREFIX) and value
        }
        return cls(secrets)

    @property
    def token_ids(self) -> frozenset[str]:
        """The configured ids. Ids only -- the secrets never leave this object."""
        return frozenset(self._secrets)

    def verify(self, presented: str | None) -> ServiceToken | None:
        """The authenticated caller, or `None`.

        Returns `None` for every failure rather than distinguishing "no such
        token" from "wrong secret": telling a caller which one it got wrong turns
        the endpoint into an oracle for valid token ids.
        """
        if not presented:
            return None
        token_id, separator, secret = presented.partition(SEPARATOR)
        if not separator or not token_id or not secret:
            return None
        expected = self._secrets.get(token_id)
        if expected is None:
            # Compare anyway, against a value of the same length, so an unknown
            # id and a wrong secret take the same time. Skipping the comparison
            # here would make token-id enumeration a timing attack.
            hmac.compare_digest(secret, secret)
            return None
        if not hmac.compare_digest(secret, expected):
            return None
        return ServiceToken(token_id=token_id)
