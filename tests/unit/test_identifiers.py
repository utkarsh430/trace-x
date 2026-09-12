"""Entity identifiers and UUIDv7.

Two things matter beyond RFC conformance: UUIDv7 must be reproducible from a
seed, or the generated dataset digest moves on every run (ADR-0027); and minted
account ids must match the pattern the logging redactor knows, or they leak into
logs, which is a build failure by CLAUDE.md §9.
"""

from __future__ import annotations

import random
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st

from trace_core.domain.identifiers import (
    account_id,
    card_id,
    device_id,
    ip_id,
    merchant_id,
    uuid7,
    uuid7_millis,
)
from trace_core.observability.redaction import ACCOUNT, REDACTED, redact

pytestmark = pytest.mark.unit

MILLIS = st.integers(min_value=0, max_value=(1 << 48) - 1)


def test_uuid7_has_the_right_version_and_variant() -> None:
    value = uuid7(millis=1_757_683_200_000, rng=random.Random(0))
    assert value.version == 7
    assert (value.int >> 62) & 0b11 == 0b10, "RFC 9562 variant bits"


@given(millis=MILLIS, seed=st.integers(min_value=0, max_value=2**32 - 1))
@pytest.mark.property
def test_uuid7_embeds_its_timestamp(millis: int, seed: int) -> None:
    assert uuid7_millis(uuid7(millis=millis, rng=random.Random(seed))) == millis


@given(a=MILLIS, b=MILLIS)
@pytest.mark.property
def test_uuid7_sorts_by_time(a: int, b: int) -> None:
    """Time-ordering is why EVENT_CONTRACTS.md §2 chose v7 for event_id."""
    rng_a, rng_b = random.Random(1), random.Random(1)
    if a == b:
        return
    lo, hi = (a, b) if a < b else (b, a)
    assert uuid7(millis=lo, rng=rng_a) < uuid7(millis=hi, rng=rng_b)


@given(millis=MILLIS, seed=st.integers(min_value=0, max_value=2**32 - 1))
@pytest.mark.property
def test_uuid7_is_reproducible_from_a_seed(millis: int, seed: int) -> None:
    """Without this the dataset digest would move on every generation run."""
    assert uuid7(millis=millis, rng=random.Random(seed)) == uuid7(
        millis=millis, rng=random.Random(seed)
    )


def test_uuid7_differs_across_seeds() -> None:
    ms = 1_757_683_200_000
    assert uuid7(millis=ms, rng=random.Random(1)) != uuid7(millis=ms, rng=random.Random(2))


def test_uuid7_without_an_rng_is_not_reproducible() -> None:
    """The unseeded path must use real entropy, or ids would collide in production."""
    ms = 1_757_683_200_000
    assert len({uuid7(millis=ms) for _ in range(50)}) == 50


@pytest.mark.parametrize("bad", [-1, 1 << 48, 2**64])
def test_uuid7_rejects_out_of_range_timestamps(bad: int) -> None:
    with pytest.raises(ValueError, match="48 bits"):
        uuid7(millis=bad)


def test_uuid7_millis_rejects_a_non_v7_uuid() -> None:
    with pytest.raises(ValueError, match="not a UUIDv7"):
        uuid7_millis(UUID("00000000-0000-4000-8000-000000000000"))


# ------------------------------------------------ redaction coupling -------


@pytest.mark.parametrize("index", [0, 1, 999_999, 1_234_567])
def test_minted_account_ids_are_redactable(index: int) -> None:
    """The id format and the log redactor must agree, or identifiers leak.

    `redaction.ACCOUNT` matches `acct[_-]?\\d{6,}`. If ACCOUNT_FORMAT ever drops
    below six digits or changes prefix, this fails here rather than silently in
    production log output.
    """
    ident = account_id(index)
    assert ACCOUNT.search(ident), f"{ident} is not matched by the logging redactor"
    scrubbed = redact(f"scoring account {ident} now")
    assert ident not in scrubbed
    assert REDACTED in scrubbed


@pytest.mark.parametrize(
    ("factory", "prefix"),
    [
        (account_id, "acct_"),
        (card_id, "card_"),
        (device_id, "dev_"),
        (merchant_id, "mrch_"),
        (ip_id, "ip_"),
    ],
)
def test_identifier_formats_are_stable_and_distinct(factory: object, prefix: str) -> None:
    value = factory(7)  # type: ignore[operator]
    assert value.startswith(prefix)
    assert value[len(prefix) :].isdigit()
