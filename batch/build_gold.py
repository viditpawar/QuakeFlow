"""Build the Gold tables from Silver, behind a data quality gate.

    docker compose --profile batch run --rm spark \
        /opt/spark/bin/spark-submit /workspace/batch/build_gold.py

Write, audit, publish:
  1. Stage the Gold results as temporary views. The Gold tables are not touched.
  2. Audit: run the checks in configs/quality.yaml on the Silver inputs, and reconcile each staged
     result against Silver.
  3. Publish: only if every check passed, replace each Gold table. If anything failed or could not
     run, nothing is published, the previous Gold tables stay as they were, and the process exits
     with status 1.
Every attempt is recorded in gold.build_log, with the Iceberg snapshots of Silver that were read.
"""

import argparse
import os
import re
import sys
import uuid
from datetime import datetime, timezone  # datetime.UTC needs Python 3.11, the image has 3.10
from pathlib import Path

from batch.quality import CheckResult, load_contract, run_checks
from batch.sql_files import (
    DEFAULT_BRONZE,
    DEFAULT_GOLD,
    DEFAULT_SILVER,
    load_statements,
    with_namespaces,
)

SQL_DIR = Path(os.getenv("QUAKEFLOW_SQL_DIR", "/workspace/iceberg/sql"))
DEFAULT_CONTRACT = os.getenv("QUAKEFLOW_QUALITY_CONTRACT", "/workspace/configs/quality.yaml")
STAGE_PREFIX = "CREATE OR REPLACE TEMP VIEW"
PUBLISH_PREFIX = "INSERT OVERWRITE"


def log(message: str) -> None:
    print(f"[build_gold] {message}", file=sys.stderr, flush=True)


def split_build_statements(statements: list[str]) -> tuple[list[str], list[str]]:
    """Separate the staging views from the publishing overwrites, refusing anything else."""
    stage = [s for s in statements if s.startswith(STAGE_PREFIX)]
    publish = [s for s in statements if s.startswith(PUBLISH_PREFIX)]
    if len(stage) + len(publish) != len(statements):
        raise ValueError("the build script may only contain staging views and INSERT OVERWRITE")
    return stage, publish


def publish_targets(publish: list[str]) -> list[tuple[str, str]]:
    """(Gold table, staged view) for each INSERT OVERWRITE ... SELECT * FROM <view> statement."""
    targets = []
    for statement in publish:
        match = re.fullmatch(
            r"INSERT OVERWRITE (\S+)\s+SELECT \* FROM (\S+)", " ".join(statement.split())
        )
        if not match:
            raise ValueError(f"not a plain overwrite from a staged view: {statement[:80]}")
        targets.append((match.group(1), match.group(2)))
    return targets


def column_checks(spark, targets: list[tuple[str, str]]) -> list[CheckResult]:
    """SELECT * fills columns by position, so a staged view must match its table exactly."""
    results = []
    for table, view in targets:
        staged, expected = spark.table(view).columns, spark.table(table).columns
        results.append(
            CheckResult(
                f"columns_match_{view}",
                staged == expected,
                "same columns in the same order" if staged == expected else f"{staged} versus {expected}",
            )
        )
    return results


def latest_snapshot(spark, table: str) -> int | None:
    rows = spark.sql(
        f"SELECT snapshot_id FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return int(rows[0][0]) if rows else None


def write_log(spark, gold: str, run_id, status, results: list[CheckResult], current_id, versions_id):
    failed = [r.name for r in results if not r.passed]
    spark.createDataFrame(
        [(run_id, datetime.now(timezone.utc), status, len(results) - len(failed), len(failed),
          "; ".join(failed) or None, current_id, versions_id)],
        "run_id string, built_at timestamp, status string, checks_passed int, checks_failed int, "
        "failed_checks string, source_current_snapshot_id long, source_versions_snapshot_id long",
    ).writeTo(f"{gold}.build_log").append()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bronze-ns", default=DEFAULT_BRONZE)
    parser.add_argument("--silver-ns", default=DEFAULT_SILVER)
    parser.add_argument("--gold-ns", default=DEFAULT_GOLD)
    parser.add_argument("--contract", default=DEFAULT_CONTRACT)
    args = parser.parse_args(argv)
    namespaces = (args.bronze_ns, args.silver_ns, args.gold_ns)

    contract = load_contract(args.contract)
    stage, publish = split_build_statements(
        load_statements(SQL_DIR / "005_build_gold.sql", *namespaces)
    )

    from pyspark.sql import SparkSession  # imported here so the helpers above work without Spark

    spark = SparkSession.builder.appName("quakeflow-build-gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    for statement in load_statements(SQL_DIR / "004_gold_models.sql", *namespaces):
        spark.sql(statement)

    run_id = str(uuid.uuid4())
    current_table = f"{args.silver_ns}.earthquakes_current"
    versions_table = f"{args.silver_ns}.earthquake_versions"
    current_id = latest_snapshot(spark, current_table)
    versions_id = latest_snapshot(spark, versions_table)

    try:
        for statement in stage:
            spark.sql(statement)
        results = column_checks(spark, publish_targets(publish)) + run_checks(
            lambda sql: float(spark.sql(sql).collect()[0][0]),
            contract,
            rewrite=lambda sql: with_namespaces(sql, *namespaces),
        )
    except Exception as exc:  # noqa: BLE001 - a build that cannot be audited must not publish
        log(f"could not stage and audit the Gold models: {exc}")
        results = [CheckResult("stage_and_audit", False, f"could not run: {str(exc)[:200]}")]

    for result in results:
        log(f"{'ok  ' if result.passed else 'FAIL'} {result.name}: {result.detail}")
    if not all(r.passed for r in results):
        write_log(spark, args.gold_ns, run_id, "blocked", results, current_id, versions_id)
        log("quality gate FAILED: nothing was published, Gold is unchanged")
        spark.stop()
        return 1

    try:
        for statement in publish:
            spark.sql(statement)
    except Exception as exc:  # noqa: BLE001
        log(f"publishing failed part way, run the build again to complete it: {exc}")
        write_log(spark, args.gold_ns, run_id, "publish_failed", results, current_id, versions_id)
        spark.stop()
        return 1
    write_log(spark, args.gold_ns, run_id, "promoted", results, current_id, versions_id)
    log(f"promoted: {len(results)} checks passed, {len(publish)} Gold tables replaced")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
