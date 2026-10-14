"""Create the real tables in a throwaway namespace and check data survives a round trip.

Needs the lakehouse running: docker compose --profile batch up -d postgres rustfs
Run with: python -m pytest -m integration tests/integration/test_lakehouse_tables.py
"""

import json
import uuid
from datetime import UTC, datetime

import pytest

from processing.normalize import normalize_feature
from schemas.earthquake import utc_to_epoch_ms
from tests.flink_support import FIXTURE, ROOT
from tests.spark_support import lakehouse_is_up, spark_sql

pytestmark = pytest.mark.integration

CREATE_TABLES = (ROOT / "iceberg" / "sql" / "002_create_tables.sql").read_text()
TABLES = {
    "bronze": ["live_observations", "historical_observations"],
    "silver": ["earthquake_versions", "earthquakes_current", "rejected_records"],
}


def event(event_id: str, **property_overrides):
    feature = json.loads(FIXTURE.read_text())["features"][0]
    feature["id"] = event_id
    feature["properties"].update(property_overrides)
    return normalize_feature(feature, datetime(2026, 10, 13, 12, 0, tzinfo=UTC))


def ts(value: datetime) -> str:
    return f"TIMESTAMP '{value.astimezone(UTC):%Y-%m-%d %H:%M:%S.%f}'"


def sql_number(value) -> str:
    return "CAST(NULL AS DOUBLE)" if value is None else repr(value)


def test_tables_are_created_and_hold_events_faithfully():
    if not lakehouse_is_up():
        pytest.skip("the lakehouse (postgres, rustfs) is not running")
    suffix = uuid.uuid4().hex[:8]
    bronze, silver = f"local.zt{suffix}_bronze", f"local.zt{suffix}_silver"
    script = CREATE_TABLES.replace("local.bronze", bronze).replace("local.silver", silver)

    shallow = event("evt-shallow", mag=None)  # no magnitude yet
    shallow = shallow.model_copy(update={"depth_km": -1.5})  # above the reference surface
    normal = event("evt-normal", mag=4.6)

    def insert_current(e):
        return (
            f"INSERT INTO {silver}.earthquakes_current VALUES ('{e.event_id}', {ts(e.event_time)}, "
            f"{ts(e.source_updated_at)}, {sql_number(e.magnitude)}, {e.latitude}, {e.longitude}, "
            f"{sql_number(e.depth_km)}, '{e.place}', '{e.review_status}', '{e.event_type}', "
            f"'{e.record_hash}', {ts(e.ingested_at)}, {ts(e.ingested_at)});"
        )

    try:
        spark_sql(f"CREATE NAMESPACE {bronze}; CREATE NAMESPACE {silver};\n{script}")
        # Running the script again must change nothing and fail on nothing.
        spark_sql(script)
        rows = spark_sql(
            f"{insert_current(shallow)}\n{insert_current(normal)}\n"
            f"SELECT event_id, unix_millis(event_time), unix_millis(source_updated_at), "
            f"coalesce(cast(magnitude AS string), 'NULL'), depth_km, record_hash, "
            f"unix_millis(ingested_at) FROM {silver}.earthquakes_current ORDER BY event_id;"
        )
        by_id = {row[0]: row for row in rows}
        for e in (shallow, normal):
            row = by_id[e.event_id]
            assert int(row[1]) == utc_to_epoch_ms(e.event_time)
            assert int(row[2]) == utc_to_epoch_ms(e.source_updated_at)
            assert row[3] == ("NULL" if e.magnitude is None else str(e.magnitude))
            assert float(row[4]) == e.depth_km
            assert row[5] == e.record_hash
            assert int(row[6]) == utc_to_epoch_ms(e.ingested_at)
        assert float(by_id["evt-shallow"][4]) == -1.5

        partition_spec = spark_sql(f"DESCRIBE TABLE {silver}.earthquake_versions;")
        assert any("months(event_time)" in "".join(row) for row in partition_spec)
        created = {row[0] for row in spark_sql(f"SHOW TABLES IN {silver}; SHOW TABLES IN {bronze};")}
        assert created == {t for tables in TABLES.values() for t in tables}
    finally:
        drops = " ".join(
            f"DROP TABLE IF EXISTS {ns}.{t} PURGE;"
            for ns, names in ((bronze, TABLES["bronze"]), (silver, TABLES["silver"]))
            for t in names
        )
        spark_sql(f"{drops} DROP NAMESPACE IF EXISTS {bronze}; DROP NAMESPACE IF EXISTS {silver};")

