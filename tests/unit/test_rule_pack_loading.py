"""Loading and hot-reloading rule packs (ADR-0033).

The failure that matters is not "the new pack is wrong" -- review catches most of
that -- but **"the new pack is invalid and the process now has no rules at
all"**. A gateway scoring every transaction zero is worse than one running
slightly stale rules, because it looks exactly like a quiet day.

So these tests are mostly about what happens when loading fails, and they assert
that the running pack survives, that the failure is counted, and that no partial
pack is ever served.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

import pytest
import yaml

from trace_core.domain.errors import ContractError
from trace_core.features.definitions import ONLINE_FEATURES
from trace_core.rules.loader import DEFAULT_PACK, RulePackLoader, parse_pack
from trace_core.rules.pack import RulePackModel, pack_digest

pytestmark = pytest.mark.unit

KNOWN: Final = frozenset(ONLINE_FEATURES.ids)

MINIMAL = """
pack_id: probe
version: 1.0.0
description: A minimal pack used to exercise the loader.
rules:
  - id: R001_probe
    version: 1.0.0
    description: Fires when the account transaction count in one minute is high.
    weight: 0.5
    when: { feature: account_tx_count_1m, op: ">=", value: 5 }
"""


def _write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


# --- the shipped pack --------------------------------------------------------


def test_the_shipped_pack_loads() -> None:
    """It is package data, not documentation: a wheel without it cannot boot."""
    assert DEFAULT_PACK.is_file()
    pack = parse_pack(DEFAULT_PACK.read_text(), known_features=KNOWN)
    assert len(pack) >= 15


def test_the_shipped_pack_only_references_registered_features() -> None:
    pack = parse_pack(DEFAULT_PACK.read_text(), known_features=KNOWN)
    assert pack.features_used <= KNOWN


# --- digests -----------------------------------------------------------------


def test_the_digest_is_stable_across_reloads() -> None:
    first = parse_pack(MINIMAL, known_features=KNOWN).digest
    second = parse_pack(MINIMAL, known_features=KNOWN).digest
    assert first == second
    assert first.startswith("sha256:")


def test_reformatting_the_yaml_does_not_move_the_digest() -> None:
    """A digest that changes for non-reasons trains everyone to ignore it."""
    reformatted = MINIMAL.replace("{ feature:", "\n      { feature:").replace(
        "description: A minimal", "# a comment\ndescription: A minimal"
    )
    assert parse_pack(reformatted, known_features=KNOWN).digest == (
        parse_pack(MINIMAL, known_features=KNOWN).digest
    )


@pytest.mark.parametrize(
    "change",
    [
        ("value: 5", "value: 6"),
        ("weight: 0.5", "weight: 0.6"),
        ('op: ">="', 'op: ">"'),
        ("version: 1.0.0\ndescription: A minimal", "version: 1.0.1\ndescription: A minimal"),
    ],
)
def test_changing_behaviour_moves_the_digest(change: tuple[str, str]) -> None:
    """Every recorded decision cites this digest; if it did not move, two
    different behaviours would be indistinguishable in the audit record."""
    altered = MINIMAL.replace(*change)
    assert altered != MINIMAL, "the test fixture did not actually change"
    assert parse_pack(altered, known_features=KNOWN).digest != (
        parse_pack(MINIMAL, known_features=KNOWN).digest
    )


def test_the_digest_covers_the_constant_sets() -> None:
    """ "Which MCCs are high risk" is behaviour, and must be pinned like a threshold."""
    with_constants = MINIMAL.replace(
        "rules:",
        'constants:\n  high_risk_mcc: ["7995"]\nrules:',
    )
    assert parse_pack(with_constants, known_features=KNOWN).digest != (
        parse_pack(MINIMAL, known_features=KNOWN).digest
    )


def test_the_digest_is_computed_from_the_parsed_model() -> None:
    model = RulePackModel.model_validate(yaml.safe_load(MINIMAL))
    assert pack_digest(model) == parse_pack(MINIMAL, known_features=KNOWN).digest


# --- rejection ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "match"),
    [
        ("pack_id: p\nversion: 1.0.0\n[", "not valid YAML"),
        ("- just\n- a\n- list\n", "mapping at the top level"),
        (
            "pack_id: p\nversion: 1.0.0\ndescription: long enough here\nrules: []\n",
            "failed validation",
        ),
    ],
)
def test_a_malformed_pack_is_refused(text: str, match: str) -> None:
    with pytest.raises(ContractError, match=match):
        parse_pack(text, known_features=KNOWN)


def test_a_rule_over_an_unknown_feature_is_refused_at_load_time() -> None:
    """Not at scoring time. A rule abstaining forever looks exactly like a rule
    that simply never matches, so the failure has to be loud and early."""
    broken = MINIMAL.replace("account_tx_count_1m", "no_such_feature")
    with pytest.raises(ContractError, match="unregistered feature"):
        parse_pack(broken, known_features=KNOWN)


def test_a_misspelled_field_is_refused_rather_than_ignored() -> None:
    """`extra="forbid"`. A silently-dropped `band_flor` would produce a rule that
    quietly never raises the band, and the only symptom would be fraud scoring
    lower than it should."""
    typo = MINIMAL.replace("weight: 0.5", "weight: 0.5\n    band_flor: HIGH")
    with pytest.raises(ContractError, match="failed validation"):
        parse_pack(typo, known_features=KNOWN)


def test_duplicate_rule_ids_are_refused() -> None:
    """A decision citing one of them would be ambiguous."""
    duplicated = MINIMAL + MINIMAL.split("rules:")[1]
    with pytest.raises(ContractError, match="failed validation"):
        parse_pack(duplicated, known_features=KNOWN)


def test_a_pack_with_no_enabled_rules_is_refused() -> None:
    """It would score every transaction zero, which is indistinguishable from a
    healthy day -- the single most dangerous way for this component to fail."""
    disabled = MINIMAL.replace("weight: 0.5", "weight: 0.5\n    enabled: false")
    with pytest.raises(ContractError, match="no ENABLED rules"):
        parse_pack(disabled, known_features=KNOWN)


def test_yaml_cannot_construct_arbitrary_python_objects() -> None:
    """`safe_load`, not `load`. The full loader instantiates Python objects named
    in the document, which would reintroduce the code-execution surface the
    closed grammar exists to remove."""
    payload = "!!python/object/apply:os.system ['echo pwned']\n"
    with pytest.raises(ContractError):
        parse_pack(payload, known_features=KNOWN)


# --- hot reload --------------------------------------------------------------


def test_reload_swaps_the_pack_when_the_candidate_is_valid(tmp_path: Path) -> None:
    path = _write(tmp_path / "pack.yaml", MINIMAL)
    loader = RulePackLoader(path, known_features=KNOWN)
    before = loader.load().digest

    _write(path, MINIMAL.replace("value: 5", "value: 9"))
    outcome = loader.reload()

    assert outcome.loaded and outcome.changed
    assert outcome.previous_digest == before
    assert loader.active().digest == outcome.digest != before


def test_a_failed_reload_keeps_the_running_pack(tmp_path: Path) -> None:
    """The property that makes hot reload safe to have at all."""
    path = _write(tmp_path / "pack.yaml", MINIMAL)
    loader = RulePackLoader(path, known_features=KNOWN)
    good = loader.load()

    _write(path, "pack_id: broken\nversion: nope\n")
    outcome = loader.reload()

    assert not outcome.loaded
    assert outcome.error is not None
    assert outcome.digest == good.digest
    assert loader.active() is good, "the running pack was replaced by a broken one"
    assert loader.failed_reloads == 1


def test_a_reload_failure_is_counted(tmp_path: Path) -> None:
    """Counted, not merely logged: a pack that keeps failing to reload is an
    operational condition someone has to be able to alert on."""
    path = _write(tmp_path / "pack.yaml", MINIMAL)
    loader = RulePackLoader(path, known_features=KNOWN)
    loader.load()
    _write(path, "not: a: pack")
    for expected in (1, 2, 3):
        loader.reload()
        assert loader.failed_reloads == expected


def test_a_pack_is_never_partially_applied(tmp_path: Path) -> None:
    """One bad rule must not load the good ones alongside it: the result would
    match neither what was reviewed nor what was intended."""
    path = _write(tmp_path / "pack.yaml", MINIMAL)
    loader = RulePackLoader(path, known_features=KNOWN)
    good = loader.load()

    two_rules_one_broken = (
        MINIMAL
        + """
  - id: R002_broken
    version: 1.0.0
    description: References a feature that does not exist anywhere in the registry.
    weight: 0.5
    when: { feature: no_such_feature, op: ">=", value: 1 }
"""
    )
    _write(path, two_rules_one_broken)
    outcome = loader.reload()

    assert not outcome.loaded
    assert loader.active() is good
    assert len(loader.active()) == 1


def test_the_first_load_failing_is_fatal(tmp_path: Path) -> None:
    """There is no previous pack to fall back to, and starting with no rules
    means serving traffic scored by nothing -- the same reasoning that makes a
    corrupt model artifact refuse to boot (docs/ARCHITECTURE.md §18)."""
    path = _write(tmp_path / "pack.yaml", "pack_id: broken\n")
    loader = RulePackLoader(path, known_features=KNOWN)
    with pytest.raises(ContractError):
        loader.load()


def test_a_missing_file_on_reload_keeps_the_running_pack(tmp_path: Path) -> None:
    path = _write(tmp_path / "pack.yaml", MINIMAL)
    loader = RulePackLoader(path, known_features=KNOWN)
    good = loader.load()
    path.unlink()

    outcome = loader.reload()
    assert not outcome.loaded
    assert loader.active() is good
