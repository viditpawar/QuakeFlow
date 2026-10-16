"""Keep the lakehouse table definitions in step with the Python event model."""

import re
from pathlib import Path

import pytest

from schemas.earthquake import EarthquakeEvent

SQL = (Path(__file__).resolve().parents[2] / "iceberg" / "sql" / "002_create_tables.sql").read_text()
STATEMENTS = re.findall(
    r"CREATE TABLE IF NOT EXISTS (local\.\w+\.\w+) \((.*?)\)\s*USING iceberg(.*?);", SQL, re.DOTALL
)


def columns(body: str) -> list[str]:
    # One column per line: "name TYPE [COMMENT '...']," (comments may contain commas).
    return [line.split()[0] for line in body.strip().splitlines() if line.strip()]


TABLES = {name: (columns(body), tail) for name, body, tail in STATEMENTS}


def test_all_expected_tables_are_defined():
    assert set(TABLES) == {
        "local.bronze.live_observations",
        "local.bronze.historical_observations",
        "local.silver.earthquake_versions",
        "local.silver.earthquakes_current",
        "local.silver.rejected_records",
        "local.silver.load_log",
    }


@pytest.mark.parametrize(
    "table", ["local.silver.earthquake_versions", "local.silver.earthquakes_current"]
)
def test_silver_tables_carry_every_event_field(table):
    missing = set(EarthquakeEvent.model_fields) - set(TABLES[table][0])
    assert not missing, f"{table} lacks event fields: {missing}"


def test_current_state_table_has_the_expected_columns_in_merge_order():
    # MERGE ... UPDATE SET * and INSERT * need the staging columns to match these exactly.
    assert TABLES["local.silver.earthquakes_current"][0] == [
        "event_id",
        "event_time",
        "source_updated_at",
        "magnitude",
        "latitude",
        "longitude",
        "depth_km",
        "place",
        "review_status",
        "event_type",
        "record_hash",
        "ingested_at",
        "curated_at",
    ]


@pytest.mark.parametrize("table", sorted(TABLES))
def test_every_table_uses_format_version_2(table):
    assert "'format-version' = '2'" in TABLES[table][1]


def test_columns_are_unique_within_each_table():
    for table, (cols, _) in TABLES.items():
        assert len(cols) == len(set(cols)), table
