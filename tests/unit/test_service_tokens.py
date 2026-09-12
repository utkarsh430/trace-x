"""Service-token authentication (docs/SECURITY.md §3, Plane B).

Most of these assert a refusal, because that is where an authenticator is worth
testing: one that accepts valid credentials and also accepts other things is
indistinguishable from a working one until it matters.
"""

from __future__ import annotations

import pytest

from trace_core.security.service_tokens import (
    ENV_PREFIX,
    MIN_SECRET_LENGTH,
    ServiceTokenVerifier,
    TokenConfigurationError,
)

pytestmark = [pytest.mark.unit, pytest.mark.security]

SECRET = "s" * MIN_SECRET_LENGTH
OTHER = "o" * MIN_SECRET_LENGTH


@pytest.fixture
def verifier() -> ServiceTokenVerifier:
    return ServiceTokenVerifier({"psp-one": SECRET, "psp-two": OTHER})


def test_a_valid_token_authenticates(verifier: ServiceTokenVerifier) -> None:
    token = verifier.verify(f"psp-one.{SECRET}")
    assert token is not None
    assert token.token_id == "psp-one"


@pytest.mark.parametrize(
    "presented",
    [
        None,
        "",
        "psp-one",
        f".{SECRET}",
        "psp-one.",
        f"psp-one{SECRET}",
        f"psp-unknown.{SECRET}",
        f"psp-one.{OTHER}",
        f"PSP-ONE.{SECRET}",
        f"psp-one.{SECRET} ",
        f" psp-one.{SECRET}",
    ],
)
def test_everything_else_is_refused(verifier: ServiceTokenVerifier, presented: str | None) -> None:
    assert verifier.verify(presented) is None


def test_an_unknown_id_and_a_wrong_secret_are_indistinguishable(
    verifier: ServiceTokenVerifier,
) -> None:
    """Telling a caller WHICH half it got wrong turns the endpoint into an oracle
    for valid token ids."""
    assert verifier.verify(f"psp-unknown.{SECRET}") is None
    assert verifier.verify(f"psp-one.{OTHER}") is None


def test_a_secret_that_is_a_prefix_of_the_real_one_is_refused(
    verifier: ServiceTokenVerifier,
) -> None:
    """Constant-time comparison must still be a full comparison."""
    assert verifier.verify(f"psp-one.{SECRET[:-1]}") is None
    assert verifier.verify(f"psp-one.{SECRET + 'x'}") is None


def test_the_verifier_exposes_ids_but_never_secrets(verifier: ServiceTokenVerifier) -> None:
    """Ids reach logs, metrics and rate-limit keys; secrets must reach none of
    them. Keying a rate limiter by the whole token would write credentials into
    Redis and into any dump of it."""
    assert verifier.token_ids == {"psp-one", "psp-two"}
    assert SECRET not in repr(verifier.token_ids)


# --- configuration ------------------------------------------------------------


def test_no_configured_tokens_is_fatal() -> None:
    """A gateway that authenticated a built-in credential when none was
    configured would be open by default, and the failure would be silent."""
    with pytest.raises(TokenConfigurationError, match="no service tokens configured"):
        ServiceTokenVerifier({})


def test_a_short_secret_is_refused_at_construction() -> None:
    """At start-up, not per request: a weak credential that works is worse than
    one that stops the process."""
    with pytest.raises(TokenConfigurationError, match="are required"):
        ServiceTokenVerifier({"psp-one": "short"})


def test_tokens_are_read_from_the_environment() -> None:
    """Environment, not the application database: putting deployment credentials
    there would make a database compromise an authentication compromise."""
    env = {
        f"{ENV_PREFIX}PSP_ONE": SECRET,
        f"{ENV_PREFIX}PSP_TWO": OTHER,
        "UNRELATED": "ignored",
        f"{ENV_PREFIX}EMPTY": "",
    }
    verifier = ServiceTokenVerifier.from_environment(env)
    assert verifier.token_ids == {"psp_one", "psp_two"}, "an empty value must not create a token"
    assert verifier.verify(f"psp_one.{SECRET}") is not None


def test_an_empty_environment_is_fatal() -> None:
    with pytest.raises(TokenConfigurationError):
        ServiceTokenVerifier.from_environment({})


def test_a_service_token_carries_identity_and_no_authority() -> None:
    """Plane B only (docs/SECURITY.md §3).

    A structural assertion rather than a grep over the source, which would fire
    on the docstring explaining the separation. The claim is about the TYPE: a
    service token says who is calling and nothing about what they may do. The
    moment it grows a role or a scope, the gateway has quietly acquired an
    authorization model that nothing audits -- human RBAC belongs to trace-api and
    capability tokens to the agent runtime.
    """
    import dataclasses

    from trace_core.security.service_tokens import ServiceToken

    fields = {f.name for f in dataclasses.fields(ServiceToken)}
    assert fields == {"token_id"}, (
        f"ServiceToken carries {sorted(fields - {'token_id'})}; identity and authority "
        f"belong in different planes"
    )
    public = {name for name in vars(ServiceToken) if not name.startswith("_")}
    for foreign in ("role", "roles", "scope", "scopes", "permissions", "allowed_tools"):
        assert foreign not in public
