"""Build Gold from known Silver data, then show the quality gate blocking bad data.

Needs the lakehouse: docker compose --profile batch up -d postgres rustfs
Run with: python -m pytest -m integration tests/integration/test_build_gold.py -s
"""

import subprocess
import uuid

import pytest

from tests.flink_support import ROOT
from tests.spark_support import lakehouse_is_up, spark_sql

pytestmark = pytest.mark.integration

SCRIPTS = "".join((ROOT / "iceberg" / "sql" / name).read_text() for name in ("002_create_tables.sql", "004_gold_models.sql"))
SILVER_TABLES = ["earthquake_versions", "earthquakes_current", "rejected_records"]
BRONZE_TABLES = ["live_observations", "historical_observations"]
GOLD_TABLES = [
    "daily_seismic_activity", "regional_activity", "magnitude_distribution",
    "revision_statistics", "ingestion_latency", "build_log",
]
HASH = "a" * 63


def sql_value(value) -> str:
    if value is None:
        return "NULL"
    return f"'{value}'" if isinstance(value, str) else repr(value)


def ts(text: str) -> str:
    return f"timestamp'{text}'"


def current_row(ns, event_id, when, updated, mag, place, event_type="earthquake", status="automatic",
                lat=10.0, lon=20.0, depth=5.0, record_hash=None, event_time=None):
    return (
        f"INSERT INTO {ns}.earthquakes_current VALUES ({sql_value(event_id)}, {event_time or ts(when)}, {ts(updated)}, "
        f"{sql_value(mag)}, {sql_value(lat)}, {lon}, {depth}, {sql_value(place)}, '{status}', '{event_type}', "
        f"'{record_hash or HASH + event_id[-1]}', {ts(updated)}, current_timestamp());"
    )


def version_row(ns, event_id, when, updated, mag, place, event_type="earthquake", status="automatic",
                lat=10.0, lon=20.0, depth=5.0, record_hash=None, source="live"):
    return (
        f"INSERT INTO {ns}.earthquake_versions VALUES ('{event_id}', '{record_hash or HASH + event_id[-1]}', {ts(when)}, "
        f"{ts(updated)}, {sql_value(mag)}, {lat}, {lon}, {depth}, {sql_value(place)}, '{status}', '{event_type}', "
        f"{ts(updated)}, '{source}', current_timestamp());"
    )


@pytest.fixture
def lake():
    if not lakehouse_is_up():
        pytest.skip("the lakehouse (postgres, rustfs) is not running")
    suffix = uuid.uuid4().hex[:8]
    ns = {k: f"local.zt{suffix}_{k}" for k in ("bronze", "silver", "gold")}
    script = SCRIPTS.replace("local.bronze", ns["bronze"]).replace("local.silver", ns["silver"]).replace("local.gold", ns["gold"])
    spark_sql(" ".join(f"CREATE NAMESPACE {v};" for v in ns.values()) + "\n" + script)
    yield ns
    drops = [f"DROP TABLE IF EXISTS {ns['bronze']}.{t} PURGE;" for t in BRONZE_TABLES]
    drops += [f"DROP TABLE IF EXISTS {ns['silver']}.{t} PURGE;" for t in SILVER_TABLES]
    drops += [f"DROP TABLE IF EXISTS {ns['gold']}.{t} PURGE;" for t in GOLD_TABLES]
    spark_sql(" ".join(drops) + " " + " ".join(f"DROP NAMESPACE IF EXISTS {v};" for v in ns.values()))


def seed_good_data(ns):
    s = ns["silver"]
    events = [
        # event_id, event_time, updated, magnitude, place, extra
        ("e1", "2025-01-01 10:00:00", "2025-01-01 10:05:00", 2.4, "10 km N of Town, California", {"status": "reviewed"}),
        ("e2", "2025-01-01 12:00:00", "2025-01-01 12:05:00", 5.2, "Fiji region", {}),
        ("e3", "2025-01-02 01:00:00", "2025-01-02 01:05:00", None, "5 km S of Place, California", {}),
        ("e4", "2025-01-02 02:00:00", "2025-01-02 02:05:00", 7.3, "Offshore, Japan", {}),
        ("e5", "2025-01-02 03:00:00", "2025-01-02 03:05:00", 1.0, "quarry site", {"event_type": "quarry blast"}),
        ("e6", "2025-02-01 00:00:00", "2025-02-01 00:05:00", -0.5, "1 km of Spot, California", {}),
    ]
    statements = []
    for event_id, when, updated, mag, place, extra in events:
        statements.append(current_row(s, event_id, when, updated, mag, place, **extra))
        statements.append(version_row(s, event_id, when, updated, mag, place, **extra))
    # e1 was revised 10 minutes later: magnitude 2.4 to 2.6, now current at 2.6 (reviewed).
    statements.append(version_row(s, "e1", "2025-01-01 10:00:00", "2025-01-01 10:15:00", 2.6,
                                  "10 km N of Town, California", status="reviewed", record_hash="b" * 64))
    # e2 was revised 30 minutes later: location, depth and status all changed.
    statements.append(version_row(s, "e2", "2025-01-01 12:00:00", "2025-01-01 12:35:00", 5.2, "Fiji region",
                                  status="reviewed", lat=11.0, depth=9.0, record_hash="c" * 64))
    spark_sql("".join(statements))
    spark_sql(
        f"UPDATE {s}.earthquakes_current SET magnitude = 2.6, source_updated_at = {ts('2025-01-01 10:15:00')}, "
        f"record_hash = '{'b' * 64}' WHERE event_id = 'e1';"
        f"UPDATE {s}.earthquakes_current SET latitude = 11.0, depth_km = 9.0, review_status = 'reviewed', "
        f"source_updated_at = {ts('2025-01-01 12:35:00')}, record_hash = '{'c' * 64}' WHERE event_id = 'e2';"
    )


def build_gold(ns) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", "--profile", "batch", "run", "--rm", "-T", "spark",
         "/opt/spark/bin/spark-submit", "/workspace/batch/build_gold.py",
         "--bronze-ns", ns["bronze"], "--silver-ns", ns["silver"], "--gold-ns", ns["gold"]],
        cwd=ROOT, capture_output=True, text=True, timeout=900, check=False,
    )


def table(ns, name, order_by) -> list[list[str]]:
    return spark_sql(f"SELECT * FROM {ns['gold']}.{name} ORDER BY {order_by};")


def gold_snapshot(ns) -> dict:
    return {
        "daily": table(ns, "daily_seismic_activity", "event_date"),
        "regional": table(ns, "regional_activity", "event_month, region"),
        "bands": table(ns, "magnitude_distribution", "band_min"),
        "revisions": table(ns, "revision_statistics", "event_date"),
    }


def last_log(ns) -> list[str]:
    return spark_sql(
        f"SELECT status, checks_failed, failed_checks FROM {ns['gold']}.build_log ORDER BY built_at DESC LIMIT 1;"
    )[0]


def test_gold_models_are_computed_correctly(lake):
    seed_good_data(lake)
    result = build_gold(lake)
    assert result.returncode == 0, (result.stdout + result.stderr)[-3000:]

    daily = {row[0]: row for row in table(lake, "daily_seismic_activity", "event_date")}
    assert set(daily) == {"2025-01-01", "2025-01-02", "2025-02-01"}
    # event_date, event_count, magnitude_count, avg, min, max, avg_depth, reviewed, non_earthquake
    assert daily["2025-01-01"][1:3] == ["2", "2"]
    assert float(daily["2025-01-01"][3]) == pytest.approx((2.6 + 5.2) / 2)
    assert (daily["2025-01-01"][7], daily["2025-01-01"][8]) == ("2", "0")  # both reviewed after revision
    assert daily["2025-01-02"][1:3] == ["2", "1"], "e3 has no magnitude and e5 is not an earthquake"
    assert float(daily["2025-01-02"][3]) == 7.3
    assert daily["2025-01-02"][8] == "1", "the quarry blast is reported separately, not counted"
    assert float(daily["2025-02-01"][3]) == -0.5

    bands = {row[0]: row for row in table(lake, "magnitude_distribution", "band_min")}
    assert {name: int(row[2]) for name, row in bands.items()} == {
        "<0": 1, "2-3": 1, "5-6": 1, "7+": 1, "unknown": 1,
    }, "the quarry blast must not appear in the earthquake bands"
    assert sum(float(row[3]) for row in bands.values()) == pytest.approx(1.0)

    regional = {(row[0], row[1]): row for row in table(lake, "regional_activity", "event_month, region")}
    assert set(regional) == {
        ("2025-01-01", "California"), ("2025-01-01", "Fiji region"), ("2025-01-01", "Japan"),
        ("2025-02-01", "California"),
    }
    assert regional[("2025-01-01", "California")][2] == "2"

    revisions = {row[0]: row for row in table(lake, "revision_statistics", "event_date")}
    jan1 = revisions["2025-01-01"]
    # event_date, events, revised, revisions, avg_minutes, mag_changed, depth_changed, loc_changed,
    # status_changed, max_abs_mag_change, conflicts
    assert jan1[1:4] == ["2", "2", "2"]
    assert float(jan1[4]) == pytest.approx(20.0)  # gaps of 10 and 30 minutes
    assert jan1[5:9] == ["1", "1", "1", "1"]
    assert float(jan1[9]) == pytest.approx(0.2)
    assert jan1[10] == "0"
    assert revisions["2025-01-02"][2:4] == ["0", "0"] and revisions["2025-01-02"][4] == "NULL"

    latency_total = sum(int(r[1]) for r in table(lake, "ingestion_latency", "load_date"))
    assert latency_total == 8, "six first versions plus two revisions, all live"

    status, failed, names = last_log(lake)
    assert (status, failed) == ("promoted", "0") and names == "NULL"


def test_the_quality_gate_blocks_bad_data_and_leaves_gold_untouched(lake):
    seed_good_data(lake)
    assert build_gold(lake).returncode == 0
    good = gold_snapshot(lake)
    s = lake["silver"]

    # Bad data one: a latitude that does not exist.
    spark_sql(current_row(s, "bad1", "2025-03-01 00:00:00", "2025-03-01 00:05:00", 4.0, "Nowhere, Atlantis", lat=95.0))
    blocked = build_gold(lake)
    assert blocked.returncode == 1
    output = blocked.stdout + blocked.stderr  # compose may put the container's stderr on stdout
    assert "quality gate FAILED" in output and "FAIL latitude_range" in output
    assert gold_snapshot(lake) == good, "a blocked build must not change any Gold table"
    status, failed, names = last_log(lake)
    assert (status, int(failed)) == ("blocked", 1) and names == "latitude_range"
    spark_sql(f"DELETE FROM {s}.earthquakes_current WHERE event_id = 'bad1';")

    # Bad data two: the same event twice, and a missing event time.
    spark_sql(current_row(s, "e1", "2025-01-01 10:00:00", "2025-01-01 10:20:00", 9.0, "Dup, California", record_hash="d" * 64))
    spark_sql(current_row(s, "bad2", None, "2025-03-01 00:05:00", 4.0, "Somewhere", event_time="CAST(NULL AS TIMESTAMP)"))
    blocked = build_gold(lake)
    assert blocked.returncode == 1
    status, failed, names = last_log(lake)
    assert status == "blocked"
    assert {"event_id_unique", "event_timestamp_required"} <= set(names.split("; "))
    assert gold_snapshot(lake) == good
    spark_sql(f"DELETE FROM {s}.earthquakes_current WHERE event_id = 'bad2' OR (event_id = 'e1' AND magnitude = 9.0);")

    # With the data fixed, and one genuinely new event, the build is promoted again.
    spark_sql(
        current_row(s, "e7", "2025-03-05 00:00:00", "2025-03-05 00:05:00", 3.3, "New, Texas")
        + version_row(s, "e7", "2025-03-05 00:00:00", "2025-03-05 00:05:00", 3.3, "New, Texas")
    )
    assert build_gold(lake).returncode == 0
    after = gold_snapshot(lake)
    assert after != good
    assert {row[0] for row in after["daily"]} == {"2025-01-01", "2025-01-02", "2025-02-01", "2025-03-05"}
    assert last_log(lake)[0] == "promoted"
    log = spark_sql(f"SELECT status FROM {lake['gold']}.build_log ORDER BY built_at;")
    assert [row[0] for row in log] == ["promoted", "blocked", "blocked", "promoted"]
