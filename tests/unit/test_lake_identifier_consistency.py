"""Data-platform identifiers have one snake_case spelling everywhere they appear (U9, ADR-0048).

CLAUDE.md §6 keeps kebab-case for human-authored repository paths and makes persisted or externally
addressable data-platform identifiers snake_case, with no mapping layer between two spellings.

These tests pin the consequence. A canonical identifier is, character for character, the physical
lake directory, the last part of the Unity Catalog name, the checkpoint directory and the query
inside the Delta transaction app id. A different spelling -- kebab-case included -- is refused where
it enters, never translated. Databricks job and task identifiers, and metric and manifest
identifiers for the same logical name, join this file when the code that mints them lands.
"""

from __future__ import annotations

import uuid
from pathlib import Path

import pytest

from trace_core.domain.errors import LakeNameError
from trace_core.stream.checkpoints import query_directory
from trace_core.stream.lake import (
    APP_ID_PREFIX,
    CHECKPOINTS_DIRNAME,
    IDENTIFIER,
    UC_CATALOG,
    AppId,
    LakeConfig,
    Tier,
    require_identifier,
)
from trace_core.stream.tables import TableRef

pytestmark = pytest.mark.unit

TABLES = [(Tier.BRONZE, "tx_raw"), (Tier.SILVER, "late_events"), (Tier.GOLD, "tx_features")]
QUERIES = ["bronze_ingest", "silver_dedup", "gold_tx_features"]
OTHER_SPELLINGS = ["late-events", "Late_Events", "late events", "late.events"]


@pytest.mark.parametrize(("tier", "name"), TABLES)
def test_a_table_identifier_is_its_lake_directory_and_its_catalog_name(
    tmp_path: Path, tier: Tier, name: str
) -> None:
    lake = LakeConfig.at(tmp_path)
    table = TableRef(tier, name)
    path = table.local_path(lake)
    assert path == lake.root / tier.value / name
    assert table.uc_name() == f"{UC_CATALOG}.{tier.value}.{name}"
    assert table.uc_name().split(".") == [UC_CATALOG, path.parent.name, path.name]
    assert str(table) == f"{path.parent.name}.{path.name}"


@pytest.mark.parametrize("query", QUERIES)
def test_a_query_identifier_is_its_checkpoint_directory_and_its_app_id(
    tmp_path: Path, query: str
) -> None:
    lake = LakeConfig.at(tmp_path)
    directory = query_directory(lake, query)
    assert directory == lake.root / CHECKPOINTS_DIRNAME / query
    text = str(AppId.new(query, 3))
    assert text.startswith(f"{APP_ID_PREFIX}:{query}:v3:")
    parsed = AppId.parse(text)
    assert parsed is not None
    assert parsed.query == directory.name == query
    assert parsed.version == 3


def test_tiers_and_the_catalog_follow_the_identifier_rule() -> None:
    """A tier is both a lake directory and a Unity Catalog schema, so it obeys the same rule."""
    for tier in Tier:
        assert IDENTIFIER.fullmatch(tier.value), tier
    assert IDENTIFIER.fullmatch(UC_CATALOG)


@pytest.mark.parametrize("spelling", OTHER_SPELLINGS)
def test_another_spelling_is_refused_where_it_enters_rather_than_mapped(
    tmp_path: Path, spelling: str
) -> None:
    lake = LakeConfig.at(tmp_path)
    with pytest.raises(LakeNameError):
        require_identifier("table name", spelling)
    with pytest.raises(LakeNameError):
        TableRef(Tier.SILVER, spelling)
    with pytest.raises(LakeNameError):
        query_directory(lake, spelling)
    with pytest.raises(LakeNameError):
        AppId.new(spelling, 1)
    assert AppId.parse(f"{APP_ID_PREFIX}:{spelling}:v1:{uuid.uuid4().hex}") is None
