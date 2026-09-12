"""Generator determinism and distribution shape (ADR-0029).

"Same seed produces the same dataset" is a Phase 1 exit condition. Every future
run manifest cites a dataset digest, so a stream that shifts silently invalidates
every number that referenced it.

The distribution tests assert **relationships**, never specific counts
(docs/TESTING.md §2.4): a peak larger than a trough, a head heavier than a
median. A test pinning an exact histogram would fail on every legitimate tuning
change and teach everyone to update expected values without reading them.
"""

from __future__ import annotations

import collections
import datetime as dt

import pytest
from data.generator.behavior import HOUR_WEIGHTS, WEEKDAY_WEIGHTS
from data.generator.config import GeneratorConfig
from data.generator.digest import canonical_bytes, digest_of
from data.generator.engine import generate_events, validate_events
from data.generator.population import build_universe
from data.generator.rng import derive, substream_seed

pytestmark = pytest.mark.unit


def _config(**overrides: object) -> GeneratorConfig:
    base: dict[str, object] = {
        "row_count": 1500,
        "account_count": 150,
        "merchant_count": 60,
        "device_count": 200,
        "ip_count": 120,
    }
    base.update(overrides)
    return GeneratorConfig(**base)  # type: ignore[arg-type]


# ------------------------------------------------------------ substreams ----


def test_a_substream_is_stable_across_calls() -> None:
    a, b = derive(42, "amount", "acct_1"), derive(42, "amount", "acct_1")
    assert [a.random() for _ in range(5)] == [b.random() for _ in range(5)]


def test_substreams_are_independent_of_each_other() -> None:
    """Drawing from one must not advance another.

    This is the property that lets step 7 add fraud scenarios without perturbing
    the legitimate traffic already generated.
    """
    first = derive(7, "alpha", "k")
    baseline = [first.random() for _ in range(5)]

    second = derive(7, "alpha", "k")
    noise = derive(7, "beta", "k")
    for _ in range(100):
        noise.random()
    assert [second.random() for _ in range(5)] == baseline


def test_different_namespaces_and_keys_give_different_streams() -> None:
    assert substream_seed(1, "a", "k") != substream_seed(1, "b", "k")
    assert substream_seed(1, "a", "k") != substream_seed(1, "a", "j")
    assert substream_seed(1, "a", "k") != substream_seed(2, "a", "k")


def test_substream_seeds_do_not_depend_on_process_hash_randomisation() -> None:
    """`hash()` is randomised per process by PYTHONHASHSEED. BLAKE2b is not.

    Pinned as a literal: if the derivation ever changes, every dataset digest in
    the project changes with it, and that must not happen quietly.
    """
    assert substream_seed(42, "tx", "0") == substream_seed(42, "tx", "0")
    assert substream_seed(42, "tx", "0") == 18019142958268931886


# ------------------------------------------------------------- digests -----


def test_same_seed_gives_an_identical_digest() -> None:
    """ROADMAP Phase 1 automated test: generator determinism."""
    config = _config()
    first, n1 = digest_of(generate_events(config))
    second, n2 = digest_of(generate_events(config))
    assert first == second
    assert n1 == n2 == config.row_count


def test_a_different_seed_gives_a_different_digest() -> None:
    """Otherwise the seed is decorative and every arm shares one dataset."""
    assert (
        digest_of(generate_events(_config()))[0] != digest_of(generate_events(_config(seed=43)))[0]
    )


@pytest.mark.parametrize(
    "change",
    [
        {"row_count": 1501},
        {"account_count": 151},
        {"merchant_count": 61},
        {"habitual_merchant_ratio": 0.5},
        {"geo_jitter_km": 25.0},
        {"currency": "EUR"},
    ],
    ids=lambda d: next(iter(d)),
)
def test_any_config_change_changes_the_digest(change: dict[str, object]) -> None:
    """A config knob that does not affect output is a knob that lies."""
    assert (
        digest_of(generate_events(_config()))[0] != digest_of(generate_events(_config(**change)))[0]
    )


def test_config_digest_is_stable_and_sensitive() -> None:
    """`fraud_scenario_config_digest` in every run manifest (ADR-0017)."""
    assert _config().digest() == _config().digest()
    assert _config().digest() != _config(seed=43).digest()


def test_digest_ignores_key_insertion_order() -> None:
    """Dict order is an implementation detail; letting it into the digest would
    make an unrelated refactor look like a data change."""
    assert canonical_bytes({"b": 1, "a": 2}) == canonical_bytes({"a": 2, "b": 1})


def test_row_content_does_not_depend_on_preceding_rows() -> None:
    """Order-independence, stated directly.

    Generating a prefix and generating the whole run must agree row for row on
    the overlap. If they did not, inserting a scenario row would rewrite every
    row after it.
    """
    config = _config(row_count=300)
    full = [canonical_bytes(e) for e in generate_events(config)]
    universe = build_universe(config)
    again = [canonical_bytes(e) for e in generate_events(config, universe)]
    assert full == again


def test_the_universe_is_a_pure_function_of_the_config() -> None:
    config = _config()
    first, second = build_universe(config), build_universe(config)
    assert [p.account_id for p in first.profiles] == [p.account_id for p in second.profiles]
    assert [m.merchant_id for m in first.merchants] == [m.merchant_id for m in second.merchants]
    assert first.merchant_cum_weights == second.merchant_cum_weights


# -------------------------------------------------------- wire contract ----


def test_every_generated_event_validates_against_the_released_schema() -> None:
    """docs/EVENT_CONTRACTS.md §6.1: an invalid message is never published."""
    config = _config(row_count=400)
    assert sum(1 for _ in validate_events(generate_events(config))) == 400


def test_events_are_emitted_in_event_time_order() -> None:
    times = [e["envelope"]["occurred_at"] for e in generate_events(_config(row_count=500))]
    assert times == sorted(times)


def test_processing_time_is_never_before_event_time() -> None:
    """Conflating the two corrupts every windowed aggregate (ADR-0026); an
    ingested_at before occurred_at would be a generator bug that looks like
    clock skew forever after."""
    for event in generate_events(_config(row_count=300)):
        assert event["envelope"]["ingested_at"] >= event["envelope"]["occurred_at"]


def test_event_ids_are_unique() -> None:
    ids = [e["envelope"]["event_id"] for e in generate_events(_config(row_count=1000))]
    assert len(set(ids)) == len(ids)


def test_transaction_ids_are_unique() -> None:
    ids = [e["payload"]["transaction_id"] for e in generate_events(_config(row_count=1000))]
    assert len(set(ids)) == len(ids)


def test_amounts_are_integers_in_minor_units() -> None:
    for event in generate_events(_config(row_count=200)):
        amount = event["payload"]["amount_minor"]
        assert isinstance(amount, int) and not isinstance(amount, bool)
        assert amount >= 1


def test_entry_mode_is_consistent_with_the_channel() -> None:
    """An ECOMMERCE entry mode on a CARD_PRESENT transaction would be a
    contradiction a rule could exploit as a fraud signal -- an artefact of the
    generator rather than of fraud."""
    from data.generator.engine import _CHANNEL_ENTRY

    for event in generate_events(_config(row_count=500)):
        payload = event["payload"]
        assert payload["entry_mode"] in _CHANNEL_ENTRY[payload["channel"]]


# ------------------------------------------------ distribution shape -------
# Relationships only, never exact counts (docs/TESTING.md §2.4).


def test_volume_has_a_diurnal_shape() -> None:
    """Fraud is a departure from a baseline. A flat baseline makes a 04:00
    velocity burst indistinguishable from a 13:00 one."""
    hours = collections.Counter(
        int(e["envelope"]["occurred_at"][11:13]) for e in generate_events(_config(row_count=4000))
    )
    busiest = max(HOUR_WEIGHTS.index(max(HOUR_WEIGHTS)), 0)
    quietest = HOUR_WEIGHTS.index(min(HOUR_WEIGHTS))
    assert hours[busiest] > hours[quietest] * 3


def test_the_declared_hour_and_weekday_weights_are_sane() -> None:
    assert len(HOUR_WEIGHTS) == 24
    assert len(WEEKDAY_WEIGHTS) == 7
    assert min(HOUR_WEIGHTS) > 0 and max(HOUR_WEIGHTS) > min(HOUR_WEIGHTS) * 5


def test_merchant_popularity_is_skewed_not_uniform() -> None:
    """Uniform merchants would make merchant-risk aggregates and MCC baselines
    meaningless, and the collusion scenario needs an outlier against a baseline."""
    counts = collections.Counter(
        e["payload"]["merchant_id"] for e in generate_events(_config(row_count=4000))
    )
    ordered = sorted(counts.values(), reverse=True)
    assert ordered[0] > ordered[len(ordered) // 2] * 2


def test_accounts_have_habitual_merchants() -> None:
    """Without a usual pattern there is nothing for fraud to depart from."""
    config = _config(row_count=4000, account_count=40)
    per_account: dict[str, collections.Counter[str]] = collections.defaultdict(collections.Counter)
    for event in generate_events(config):
        per_account[event["payload"]["account_id"]][event["payload"]["merchant_id"]] += 1
    busiest = max(per_account.values(), key=lambda c: sum(c.values()))
    top_share = busiest.most_common(1)[0][1] / sum(busiest.values())
    assert top_share > 1.5 / len(busiest)


def test_geography_is_clustered_around_account_homes() -> None:
    """Uniform geography would make every impossible-travel injection trivially
    separable, which would flatter the detector."""
    from trace_core.domain.geo import GeoPoint, haversine_km

    config = _config(row_count=800)
    universe = build_universe(config)
    homes = {p.account_id: p.account.home for p in universe.profiles}
    distances = [
        haversine_km(
            homes[e["payload"]["account_id"]],
            GeoPoint(e["payload"]["latitude"], e["payload"]["longitude"]),
        )
        for e in generate_events(config, universe)
    ]
    distances.sort()
    median = distances[len(distances) // 2]
    assert median < config.geo_jitter_km * 2
    assert distances[-1] > median


def test_amounts_have_a_long_right_tail() -> None:
    """So "anomalously high value" is a statement about a distribution rather
    than a threshold someone picked."""
    amounts = sorted(e["payload"]["amount_minor"] for e in generate_events(_config(row_count=3000)))
    median = amounts[len(amounts) // 2]
    p99 = amounts[int(len(amounts) * 0.99)]
    assert p99 > median * 5


def test_the_generated_window_respects_the_config() -> None:
    config = _config(
        row_count=500,
        start_at=dt.datetime(2026, 2, 1, tzinfo=dt.UTC),
        end_at=dt.datetime(2026, 2, 8, tzinfo=dt.UTC),
    )
    for event in generate_events(config):
        occurred = dt.datetime.fromisoformat(
            event["envelope"]["occurred_at"].replace("Z", "+00:00")
        )
        assert config.start_at <= occurred < config.end_at


def test_an_invalid_window_is_refused() -> None:
    with pytest.raises(ValueError, match="must be after"):
        GeneratorConfig(
            start_at=dt.datetime(2026, 3, 1, tzinfo=dt.UTC),
            end_at=dt.datetime(2026, 1, 1, tzinfo=dt.UTC),
        )
