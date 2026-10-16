"""Record which Silver snapshots a load finished with, so that Gold can read exactly those.

Every ingestion job calls record_load as its last step. The row says: when this load (identified by the
orchestrator's run ID) finished, Silver's current-state table and its version table were at these Iceberg
snapshots. Gold then reads those snapshots, not whatever Silver holds when Gold happens to start, so a load
that begins later, or is still running, cannot change what a Gold build sees.

Runs inside the Spark image, whose Python is 3.10.
"""

import os
from datetime import datetime, timezone  # datetime.UTC needs Python 3.11, the image has 3.10
from pathlib import Path

from batch.sql_files import DEFAULT_SILVER, load_statements

SQL_DIR = Path(os.getenv("QUAKEFLOW_SQL_DIR", "/workspace/iceberg/sql"))
PINNED_TABLES = ("earthquakes_current", "earthquake_versions")


def latest_snapshot(spark, table: str) -> int | None:
    rows = spark.sql(f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1").collect()
    return int(rows[0][0]) if rows else None


def ensure_table(spark, silver: str = DEFAULT_SILVER) -> None:
    """Create the log table from its definition in 002_create_tables.sql, if it is not there yet."""
    for statement in load_statements(SQL_DIR / "002_create_tables.sql", silver=silver):
        if f"{silver}.load_log" in statement:
            spark.sql(statement)


def record_load(spark, silver: str, job: str, run_id: str) -> tuple[int | None, int | None]:
    """Append the current Silver snapshots under this run ID. Returns (current, versions) snapshot IDs."""
    ensure_table(spark, silver)
    current = latest_snapshot(spark, f"{silver}.earthquakes_current")
    versions = latest_snapshot(spark, f"{silver}.earthquake_versions")
    spark.createDataFrame(
        [(run_id, job, datetime.now(timezone.utc), current, versions)],
        "run_id string, job string, finished_at timestamp, "
        "current_snapshot_id long, versions_snapshot_id long",
    ).writeTo(f"{silver}.load_log").append()
    return current, versions


def find_load(spark, silver: str, run_id: str | None = None):
    """The newest load record, or the newest for this run ID. None if there is none.

    A task that was retried records more than once under the same ID, and the newest one is the one that
    describes the finished load."""
    ensure_table(spark, silver)
    table = spark.table(f"{silver}.load_log")
    if run_id is not None:
        table = table.where(table.run_id == run_id)
    rows = table.orderBy(table.finished_at.desc()).limit(1).collect()
    return rows[0] if rows else None


def pin_silver(sql: str, silver: str, current_id: int, versions_id: int) -> str:
    """Rewrite references to the two Silver tables so they read the given Iceberg snapshots."""
    import re

    ids = {"earthquakes_current": current_id, "earthquake_versions": versions_id}
    pattern = re.compile(rf"\b{re.escape(silver)}\.({'|'.join(PINNED_TABLES)})(?![\w.])")
    return pattern.sub(lambda m: f"{silver}.{m.group(1)}.snapshot_id_{ids[m.group(1)]}", sql)
