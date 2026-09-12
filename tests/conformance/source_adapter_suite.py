"""`SourceAdapterConformanceSuite` — one suite, every adapter (ADR-0022).

An abstraction with one implementation is not an abstraction (CLAUDE.md §3.4).
This suite is what makes the `SourceAdapter` port a real one: `GeneratorAdapter`
passes it in Phase 1 and `IeeeCisAdapter` must pass **this same file, unmodified**
in Phase 4B. If a check here has to be relaxed to admit the second adapter, the
port was wrong, and that is worth discovering in Phase 4B rather than papering
over.

The central check is `test_declared_coverage_matches_reality`. Declared coverage
is what every downstream `UNAVAILABLE` decision rests on, so an adapter that
claims a field it never populates causes a feature to look computable when its
inputs are fabricated -- the exact failure ADR-0022 exists to prevent.

Subclass it, provide `adapter` and `sample_rows`, and the whole suite runs:

    class TestMyAdapter(SourceAdapterConformanceSuite):
        @pytest.fixture
        def adapter(self): return MyAdapter()
        @pytest.fixture
        def sample_rows(self): return [...]
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from trace_core.contracts.canonical import (
    ALWAYS_REQUIRED,
    CanonicalField,
    CanonicalTransaction,
)
from trace_core.contracts.source_adapter import SourceAdapter, SourceProfile


class SourceAdapterConformanceSuite:
    """Every adapter must satisfy all of this."""

    # ------------------------------------------------------- fixtures ------

    @pytest.fixture
    def adapter(self) -> SourceAdapter:
        raise NotImplementedError("provide an adapter fixture")

    @pytest.fixture
    def sample_rows(self) -> list[Any]:
        raise NotImplementedError("provide a sample_rows fixture")

    @pytest.fixture
    def profile(self, adapter: SourceAdapter) -> SourceProfile:
        return adapter.describe()

    @pytest.fixture
    def canonical(
        self, adapter: SourceAdapter, sample_rows: list[Any]
    ) -> list[CanonicalTransaction]:
        return list(adapter.to_canonical(sample_rows))

    # ---------------------------------------------------------- port -------

    def test_satisfies_the_port(self, adapter: SourceAdapter) -> None:
        assert isinstance(adapter, SourceAdapter)

    def test_sample_is_not_empty(self, sample_rows: list[Any]) -> None:
        """A suite that runs over zero rows passes vacuously."""
        assert len(sample_rows) >= 3, "provide at least three representative rows"

    # ------------------------------------------------------- profile -------

    def test_profile_is_complete(self, profile: SourceProfile) -> None:
        assert profile.name and profile.version and profile.digest
        assert profile.field_coverage, "an adapter covering nothing cannot be used"

    def test_profile_covers_the_irreducible_minimum(self, profile: SourceProfile) -> None:
        """Without these a row cannot be scored or aggregated at all."""
        missing = ALWAYS_REQUIRED - profile.field_coverage
        assert not missing, f"{profile.name} omits {sorted(f.value for f in missing)}"

    def test_profile_is_stable_across_calls(self, adapter: SourceAdapter) -> None:
        """`describe()` feeds the run manifest; a profile that varies per call
        would make two runs of the same data disagree about their own inputs."""
        assert adapter.describe() == adapter.describe()

    def test_uncovered_is_the_exact_complement(self, profile: SourceProfile) -> None:
        assert profile.uncovered == frozenset(CanonicalField) - profile.field_coverage
        assert not profile.uncovered & profile.field_coverage

    # ------------------------------------------------------ mapping --------

    def test_every_row_maps(
        self, canonical: list[CanonicalTransaction], sample_rows: list[Any]
    ) -> None:
        """Silently dropping unmappable rows would bias every downstream metric."""
        assert len(canonical) == len(sample_rows)

    def test_rows_carry_the_declared_coverage(
        self, canonical: list[CanonicalTransaction], profile: SourceProfile
    ) -> None:
        for row in canonical:
            assert row.field_coverage == profile.field_coverage

    def test_declared_coverage_matches_reality(
        self, canonical: list[CanonicalTransaction], profile: SourceProfile
    ) -> None:
        """**The check this suite exists for.**

        A field declared covered must actually be populated by at least one
        sampled row. Claiming coverage an adapter does not deliver makes a
        downstream feature appear computable while its inputs are absent, and
        the resulting metric is fabricated rather than merely weak.

        Sampled rather than exhaustive, so the sample must be representative --
        which is why `test_sample_is_not_empty` demands at least three rows.
        """
        populated = {
            field
            for field in CanonicalField
            for row in canonical
            if row.value_of(field) is not None
        }
        overclaimed = profile.field_coverage - populated
        assert not overclaimed, (
            f"{profile.name} declares coverage of "
            f"{sorted(f.value for f in overclaimed)} but no sampled row populates them. "
            f"Every UNAVAILABLE decision downstream rests on this declaration (ADR-0022)."
        )

    def test_uncovered_fields_are_never_populated(
        self, canonical: list[CanonicalTransaction], profile: SourceProfile
    ) -> None:
        """The other direction: nothing appears that was not declared.

        Also enforced per row by `CanonicalTransaction`; asserted here too so the
        suite states the property rather than relying on a validator a future
        refactor might loosen.
        """
        for row in canonical:
            for field in profile.uncovered:
                assert row.value_of(field) is None, (
                    f"{profile.name} populates undeclared field {field.value}"
                )

    def test_no_uncovered_field_is_imputed_to_zero(
        self, canonical: list[CanonicalTransaction], profile: SourceProfile
    ) -> None:
        """ADR-0022's headline risk, stated as its own test.

        An uncovered numeric field imputed to 0 is not a weak signal; it is a
        fabricated one, and it looks identical to a real measurement in every
        report. `is None` is the only acceptable representation.
        """
        for row in canonical:
            for field in profile.uncovered:
                value = row.value_of(field)
                assert value is not 0 and value != 0.0 and value is None, (  # noqa: F632
                    f"{profile.name} imputed {field.value}={value!r} instead of leaving it absent"
                )

    # ----------------------------------------------------- provenance ------

    def test_every_row_is_traceable_to_its_source(
        self, canonical: list[CanonicalTransaction], profile: SourceProfile
    ) -> None:
        """`source_dataset` + `source_row_id` is the lineage pair
        (docs/DATA_ENGINEERING.md §7). Without it a surprising row cannot be
        traced back to the bytes it came from."""
        for row in canonical:
            assert row.source_dataset
            assert row.source_row_id

    def test_source_row_ids_are_unique(self, canonical: list[CanonicalTransaction]) -> None:
        ids = [row.source_row_id for row in canonical]
        assert len(set(ids)) == len(ids), "source_row_id must identify one source row"

    # ------------------------------------------------------- semantics -----

    def test_time_is_timezone_aware_and_event_time_is_present(
        self, canonical: list[CanonicalTransaction]
    ) -> None:
        """`occurred_at` drives every window; a naive value is ambiguous."""
        for row in canonical:
            assert row.occurred_at.tzinfo is not None
            assert row.ingested_at.tzinfo is not None

    def test_money_is_an_integer(self, canonical: list[CanonicalTransaction]) -> None:
        """CLAUDE.md §6: never a float. `bool` is an int subclass, so it is
        excluded explicitly rather than by an isinstance that would admit it."""
        for row in canonical:
            assert isinstance(row.amount_minor, int)
            assert not isinstance(row.amount_minor, bool)

    def test_currency_is_iso_4217(self, canonical: list[CanonicalTransaction]) -> None:
        for row in canonical:
            assert len(row.currency) == 3 and row.currency.isupper()

    def test_opaque_features_are_numeric(self, canonical: list[CanonicalTransaction]) -> None:
        """Opaque columns carry no canonical meaning, so they must at least be
        typed: a stringly-typed opaque map would be unusable by any model."""
        for row in canonical:
            for key, value in row.opaque_features.items():
                assert isinstance(key, str)
                assert isinstance(value, float | int) and not isinstance(value, bool)

    def test_rows_are_immutable(self, canonical: list[CanonicalTransaction]) -> None:
        """A canonical row records what arrived; mutating it after the fact would
        make lineage describe something other than the source."""
        with pytest.raises(ValidationError, match=r"frozen|Instance is frozen"):
            canonical[0].amount_minor = 1

    # --------------------------------------------------- ground truth ------

    def test_label_column_is_named_but_never_attached(
        self, adapter: SourceAdapter, canonical: list[CanonicalTransaction]
    ) -> None:
        """The adapter names the label column; it must never carry a label.

        Ground truth is unreachable by the application role (ADR-0004). An
        adapter attaching `is_fraud` to a canonical row would route it straight
        into the feature path and silently invalidate every metric.
        """
        name = adapter.label_column()
        assert isinstance(name, str) and name
        for row in canonical:
            assert not hasattr(row, name), f"adapter attached the label column {name!r} to a row"
            assert name not in row.opaque_features, (
                f"label column {name!r} smuggled into opaque_features"
            )

    def test_no_row_carries_an_obvious_ground_truth_field(
        self, canonical: list[CanonicalTransaction]
    ) -> None:
        """Belt and braces against a label arriving under a different name."""
        forbidden = {"is_fraud", "isfraud", "label", "fraud_pattern", "causal_evidence_keys"}
        for row in canonical:
            leaked = forbidden & set(row.opaque_features)
            assert not leaked, f"ground-truth-shaped key(s) in opaque_features: {sorted(leaked)}"
