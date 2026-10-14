"""Backfill into the real lakehouse tables (throwaway namespaces) and check the safety rules.

Needs the lakehouse: docker compose --profile batch up -d postgres rustfs
Run with: python -m pytest -m integration tests/integration/test_ingest_history.py -s
"""

import shutil
import uuid
from datetime import date, timedelta

import pytest

from batch.backfill import run_backfill
from tests.catalog_support import BASE, FakeCatalog, feature, week_of_events
from tests.flink_support import ROOT
from tests.spark_support import lakehouse_is_up, run_ingest_history, spark_sql

pytestmark = pytest.mark.integration

CREATE_TABLES = (ROOT / "iceberg" / "sql" / "002_create_tables.sql").read_text()
TABLE_NAMES = {
    "bronze": ["live_observations", "historical_observations"],
    "silver": ["earthquake_versions", "earthquakes_current", "rejected_records"],
}
LIVE_HASH = "f" * 64


@pytest.fixture
def lake():
    if not lakehouse_is_up():
        pytest.skip("the lakehouse (postgres, rustfs) is not running")
    suffix = uuid.uuid4().hex[:8]
    bronze, silver = f"local.zt{suffix}_bronze", f"local.zt{suffix}_silver"
    history = ROOT / "data" / "history" / f"test_{suffix}"
    history.mkdir(parents=True)
    script = CREATE_TABLES.replace("local.bronze", bronze).replace("local.silver", silver)
    spark_sql(f"CREATE NAMESPACE {bronze}; CREATE NAMESPACE {silver};\n{script}")
    yield {"bronze": bronze, "silver": silver, "history": history, "container_dir": f"/data/history/test_{suffix}"}
    drops = " ".join(
        f"DROP TABLE IF EXISTS {ns}.{t} PURGE;"
        for ns, group in ((bronze, "bronze"), (silver, "silver"))
        for t in TABLE_NAMES[group]
    )
    spark_sql(f"{drops} DROP NAMESPACE IF EXISTS {bronze}; DROP NAMESPACE IF EXISTS {silver};")
    shutil.rmtree(history, ignore_errors=True)


def counts(lake) -> dict:
    b, s = lake["bronze"], lake["silver"]
    rows = spark_sql(
        f"SELECT 'bronze_historical', count(*) FROM {b}.historical_observations;"
        f"SELECT 'versions', count(*) FROM {s}.earthquake_versions;"
        f"SELECT 'current', count(*) FROM {s}.earthquakes_current;"
        f"SELECT 'rejected', count(*) FROM {s}.rejected_records;"
    )
    return {name: int(value) for name, value in rows}


def current(lake) -> dict:
    rows = spark_sql(
        f"SELECT event_id, magnitude, record_hash FROM {lake['silver']}.earthquakes_current ORDER BY event_id;"
    )
    return {event_id: (float(mag), record_hash) for event_id, mag, record_hash in rows}


def seed_newer_live_revision(lake, event):
    """What the live pipeline would already hold: a revision newer than the historical one."""
    props = event["properties"]
    newer = props["updated"] + 3_600_000
    ts = lambda ms: f"timestamp_millis({ms})"
    row = (
        f"'{event['id']}', {ts(props['time'])}, {ts(newer)}, 9.9, 36.5, -120.8, 12.4, "
        f"'live place', 'reviewed', 'earthquake'"
    )
    s = lake["silver"]
    spark_sql(
        f"INSERT INTO {s}.earthquake_versions VALUES ('{event['id']}', '{LIVE_HASH}', {ts(props['time'])}, "
        f"{ts(newer)}, 9.9, 36.5, -120.8, 12.4, 'live place', 'reviewed', 'earthquake', current_timestamp(), "
        f"'live', current_timestamp());"
        f"INSERT INTO {s}.earthquakes_current VALUES ({row}, '{LIVE_HASH}', current_timestamp(), current_timestamp());"
    )


def load(lake) -> None:
    run_ingest_history(lake["container_dir"], lake["bronze"], lake["silver"])


def test_backfill_never_overwrites_a_newer_live_revision_and_is_safe_to_repeat(lake):
    events = week_of_events(7)  # one event a day
    live_event = events[2]
    seed_newer_live_revision(lake, live_event)

    catalog = FakeCatalog(events)
    run_backfill(catalog, date(2025, 1, 1), date(2025, 1, 8), 2.5, lake["history"])
    load(lake)

    after_first = counts(lake)
    assert after_first == {"bronze_historical": 7, "versions": 8, "current": 7, "rejected": 0}
    rows = current(lake)
    assert rows[live_event["id"]] == (9.9, LIVE_HASH), "an old snapshot overwrote a newer live revision"
    assert all(mag == 3.0 for event_id, (mag, _) in rows.items() if event_id != live_event["id"])

    # The same backfill again: nothing is downloaded, and loading again changes nothing.
    fetches = len(catalog.fetch_calls)
    run_backfill(catalog, date(2025, 1, 1), date(2025, 1, 8), 2.5, lake["history"])
    assert len(catalog.fetch_calls) == fetches
    load(lake)
    assert counts(lake) == after_first
    assert current(lake) == rows

    # An overlapping backfill repeats features inside one load. They must not be inserted twice.
    run_backfill(catalog, date(2025, 1, 3), date(2025, 1, 10), 2.5, lake["history"])
    load(lake)
    assert counts(lake) == after_first
    assert current(lake) == rows

    # A genuinely newer revision from the catalog does replace the current row, and is kept as a version.
    revised = [dict(e, properties=dict(e["properties"])) for e in events]
    revised[4]["properties"].update(mag=5.5, updated=revised[4]["properties"]["updated"] + 600_000)
    run_backfill(FakeCatalog(revised), date(2025, 1, 1), date(2025, 1, 8), 2.5, lake["history"], force=True)
    load(lake)
    final = counts(lake)
    assert final["current"] == 7 and final["versions"] == 9 and final["bronze_historical"] == 8
    assert current(lake)[revised[4]["id"]][0] == 5.5
    assert current(lake)[live_event["id"]] == (9.9, LIVE_HASH)


def test_invalid_features_are_kept_as_rejected_records_exactly_once(lake):
    bad = feature(99, BASE + timedelta(days=1), 3.0)
    bad["geometry"]["coordinates"] = [500.0, 10.0, 1.0]
    run_backfill(FakeCatalog([*week_of_events(3), bad]), date(2025, 1, 1), date(2025, 1, 8), None, lake["history"])
    load(lake)
    load(lake)
    found = counts(lake)
    assert found == {"bronze_historical": 4, "versions": 3, "current": 3, "rejected": 1}
    reason = spark_sql(f"SELECT stage, source, reason FROM {lake['silver']}.rejected_records;")[0]
    assert reason[:2] == ["normalize", "historical"] and "longitude" in reason[2]
