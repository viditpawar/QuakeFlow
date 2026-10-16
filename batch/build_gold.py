"""Build the Gold tables from Silver, behind a data quality gate, as versioned builds.

    docker compose --profile batch run --rm spark \
        /opt/spark/bin/spark-submit /workspace/batch/build_gold.py --load-run-id <run id>

Write, audit, publish:
  0. Choose the input. The Silver snapshots recorded by a finished load (silver.load_log) are read, not
     whatever Silver holds now. --load-run-id names the load, without it the newest one is used, and
     --pin-now records the current Silver state first, for a manual build.
  1. Stage the Gold results as temporary views. Nothing durable is touched.
  2. Audit: run the checks in configs/quality.yaml on the pinned Silver inputs, and reconcile each staged
     result against Silver.
  3. Publish: only if every check passed. Append the staged rows to each <name>_builds table under a new
     build_id (readers cannot see them yet), confirm the row counts, record the build in build_manifest,
     and then replace the one row of published_build in a single commit. The views called <name> show the
     published build, so readers see the previous build or the new one in full, never a mix. A failure
     before that commit leaves what readers see untouched.
If anything failed or could not run, nothing is published and the process exits with status 1.
Every attempt is recorded in gold.build_log, with the Iceberg snapshots of Silver that were read.

    --publish-existing <build_id>   roll back (or forward) to a build that is still in the manifest
"""

import argparse
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone  # datetime.UTC needs Python 3.11, the image has 3.10
from pathlib import Path

from batch.job_metrics import JobRun, job_run
from batch.load_log import find_load, pin_silver, record_load
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
BUILD_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
KEEP_BUILDS = 10


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


def builds_table(gold_table: str) -> str:
    """Where the rows of every build of a Gold table are kept. The Gold table's own name is a view."""
    return f"{gold_table}_builds"


def short_name(table: str) -> str:
    return table.rsplit(".", 1)[1]


def view_statement(table: str, columns: list[str], published: str) -> str:
    """The view that shows the published build's rows of one Gold table."""
    return (
        f"CREATE OR REPLACE VIEW {table} AS SELECT {', '.join(columns)} FROM {builds_table(table)} "
        f"WHERE build_id = (SELECT build_id FROM {published})"
    )


def keep_clause(build_ids: list[str]) -> str:
    """A SQL list of build IDs. They are UUIDs the build generated, and anything else is refused."""
    for build_id in build_ids:
        if not BUILD_ID.fullmatch(build_id):
            raise ValueError(f"not a build id: {build_id!r}")
    return ", ".join(f"'{b}'" for b in build_ids)


def column_checks(spark, targets: list[tuple[str, str]]) -> list[CheckResult]:
    """The staged view is inserted after the build_id column, so its columns must match the rest exactly."""
    results = []
    for table, view in targets:
        staged = spark.table(view).columns
        expected = [c for c in spark.table(builds_table(table)).columns if c != "build_id"]
        results.append(
            CheckResult(
                f"columns_match_{view}",
                staged == expected,
                "same columns in the same order" if staged == expected else f"{staged} versus {expected}",
            )
        )
    return results


def ensure_build_log_columns(spark, gold: str) -> None:
    """build_log predates load_run_id, so add the column to a log that was created without it."""
    if "load_run_id" not in spark.table(f"{gold}.build_log").columns:
        spark.sql(f"ALTER TABLE {gold}.build_log ADD COLUMN load_run_id string")


def write_log(spark, gold: str, run_id, status, results: list[CheckResult], current_id, versions_id, load_run_id):
    failed = [r.name for r in results if not r.passed]
    spark.createDataFrame(
        [(run_id, datetime.now(timezone.utc), status, len(results) - len(failed), len(failed),
          "; ".join(failed) or None, load_run_id, current_id, versions_id)],
        "run_id string, built_at timestamp, status string, checks_passed int, checks_failed int, "
        "failed_checks string, load_run_id string, source_current_snapshot_id long, "
        "source_versions_snapshot_id long",
    ).writeTo(f"{gold}.build_log").append()


def choose_load(spark, args):
    """The load record whose Silver snapshots this build reads, or None if there is none."""
    if args.pin_now:
        pinned_id = f"pin-{uuid.uuid4()}"
        record_load(spark, args.silver_ns, "build_gold", pinned_id)
        return find_load(spark, args.silver_ns, pinned_id)
    return find_load(spark, args.silver_ns, args.load_run_id)


def write_build(spark, build_id: str, targets: list[tuple[str, str]]) -> dict[str, int]:
    """Append the staged rows to each table's builds under this build_id and confirm the counts.

    Nothing here is visible to readers, because the views show only the published build."""
    counts = {}
    for table, view in targets:
        staged = spark.table(view).count()
        spark.sql(f"INSERT INTO {builds_table(table)} SELECT '{build_id}' AS build_id, * FROM {view}")
        written = spark.table(builds_table(table)).where(f"build_id = '{build_id}'").count()
        if written != staged:
            raise RuntimeError(f"{short_name(table)}: staged {staged} rows but {written} were written")
        counts[short_name(table)] = written
    return counts


def set_published(spark, gold: str, build_id: str) -> None:
    """Make a build the published one. One row, replaced in one Iceberg commit."""
    keep_clause([build_id])
    spark.sql(f"INSERT OVERWRITE {gold}.published_build SELECT '{build_id}', current_timestamp()")


def ensure_views(spark, gold: str, targets: list[tuple[str, str]]) -> None:
    """Create the views that readers use. The first time, they take over the names of the plain tables
    that Gold used before builds were versioned."""
    existing = {row[1] for row in spark.sql(f"SHOW VIEWS IN {gold}").collect()}
    for table, _ in targets:
        if short_name(table) not in existing:
            spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
        columns = [c for c in spark.table(builds_table(table)).columns if c != "build_id"]
        spark.sql(view_statement(table, columns, f"{gold}.published_build"))


def prune(spark, gold: str, targets: list[tuple[str, str]], keep: int) -> None:
    """Delete all but the newest `keep` builds and the published one, and any rows no manifest entry claims.

    A failure here is logged and ignored: old builds taking space is not a reason to fail a good build."""
    try:
        ids = [r[0] for r in spark.sql(
            f"SELECT build_id FROM {gold}.build_manifest ORDER BY completed_at DESC LIMIT {int(keep)}"
        ).collect()]
        ids += [r[0] for r in spark.table(f"{gold}.published_build").collect() if r[0] not in ids]
        clause = keep_clause(ids)
        for table in [builds_table(t) for t, _ in targets] + [f"{gold}.build_manifest"]:
            spark.sql(f"DELETE FROM {table} WHERE build_id NOT IN ({clause})")
    except Exception as exc:  # noqa: BLE001
        log(f"could not remove old builds, they will be removed by a later build: {exc}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bronze-ns", default=DEFAULT_BRONZE)
    parser.add_argument("--silver-ns", default=DEFAULT_SILVER)
    parser.add_argument("--gold-ns", default=DEFAULT_GOLD)
    parser.add_argument("--contract", default=DEFAULT_CONTRACT)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--load-run-id", help="build from the Silver snapshots this load recorded")
    source.add_argument("--pin-now", action="store_true", help="record the current Silver state and build from it")
    source.add_argument("--publish-existing", metavar="BUILD_ID", help="make an earlier build the published one")
    parser.add_argument("--keep-builds", type=int, default=KEEP_BUILDS, help="how many builds to keep")
    args = parser.parse_args(argv)
    with job_run("build_gold") as run:
        code = republish(args) if args.publish_existing else build(args, run)
        run.success = code == 0
    return code


def start_spark():
    from pyspark.sql import SparkSession  # imported here so the helpers above work without Spark

    spark = SparkSession.builder.appName("quakeflow-build-gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def setup_gold(spark, args, namespaces) -> None:
    for statement in load_statements(SQL_DIR / "004_gold_models.sql", *namespaces):
        spark.sql(statement)
    ensure_build_log_columns(spark, args.gold_ns)


def republish(args) -> int:
    """Make a build that is still in the manifest the published one again, without building anything."""
    namespaces = (args.bronze_ns, args.silver_ns, args.gold_ns)
    _, publish = split_build_statements(load_statements(SQL_DIR / "005_build_gold.sql", *namespaces))
    targets = publish_targets(publish)
    try:
        keep_clause([args.publish_existing])
    except ValueError as exc:
        log(str(exc))
        return 1
    spark = start_spark()
    setup_gold(spark, args, namespaces)
    manifest = spark.table(f"{args.gold_ns}.build_manifest").where(f"build_id = '{args.publish_existing}'").collect()
    if not manifest:
        log(f"build {args.publish_existing} is not in the manifest, it may have been removed")
        spark.stop()
        return 1
    entry = manifest[0]
    set_published(spark, args.gold_ns, args.publish_existing)
    ensure_views(spark, args.gold_ns, targets)
    write_log(spark, args.gold_ns, str(uuid.uuid4()), "republished", [], entry.source_current_snapshot_id,
              entry.source_versions_snapshot_id, entry.load_run_id)
    log(f"published the existing build {args.publish_existing}")
    spark.stop()
    return 0


def build(args, run: JobRun) -> int:
    namespaces = (args.bronze_ns, args.silver_ns, args.gold_ns)

    contract = load_contract(args.contract)
    stage, publish = split_build_statements(
        load_statements(SQL_DIR / "005_build_gold.sql", *namespaces)
    )
    targets = publish_targets(publish)

    spark = start_spark()
    setup_gold(spark, args, namespaces)

    build_id = str(uuid.uuid4())
    load = choose_load(spark, args)
    current_id = load.current_snapshot_id if load else None
    versions_id = load.versions_snapshot_id if load else None
    load_run_id = load.run_id if load else args.load_run_id

    if current_id is None or versions_id is None:
        what = f"load {args.load_run_id}" if args.load_run_id else "any load"
        results = [CheckResult(
            "silver_snapshots_recorded", False,
            f"no recorded Silver snapshots for {what}: run an ingestion first, or use --pin-now for a manual build",
        )]
    else:
        log(f"reading the Silver snapshots of load {load_run_id}: current {current_id}, versions {versions_id}")
        stage = [pin_silver(s, args.silver_ns, current_id, versions_id) for s in stage]
        try:
            for statement in stage:
                spark.sql(statement)
            results = column_checks(spark, targets) + run_checks(
                lambda sql: float(spark.sql(sql).collect()[0][0]),
                contract,
                rewrite=lambda sql: pin_silver(
                    with_namespaces(sql, *namespaces), args.silver_ns, current_id, versions_id
                ),
            )
        except Exception as exc:  # noqa: BLE001 - a build that cannot be audited must not publish
            log(f"could not stage and audit the Gold models: {exc}")
            results = [CheckResult("stage_and_audit", False, f"could not run: {str(exc)[:200]}")]

    for result in results:
        log(f"{'ok  ' if result.passed else 'FAIL'} {result.name}: {result.detail}")
    failed_checks = sum(1 for r in results if not r.passed)
    run.gauge("quakeflow_gold_checks_failed", "Quality checks that failed in the last build.", failed_checks)
    run.gauge("quakeflow_gold_build_blocked", "1 if the last build was blocked by the quality gate.", int(failed_checks > 0))
    if failed_checks:
        write_log(spark, args.gold_ns, build_id, "blocked", results, current_id, versions_id, load_run_id)
        log("quality gate FAILED: nothing was published, Gold is unchanged")
        spark.stop()
        return 1

    try:
        counts = write_build(spark, build_id, targets)
        spark.createDataFrame(
            [(build_id, datetime.now(timezone.utc), load_run_id, current_id, versions_id, len(results),
              json.dumps(counts, sort_keys=True))],
            "build_id string, completed_at timestamp, load_run_id string, source_current_snapshot_id long, "
            "source_versions_snapshot_id long, checks_passed int, table_row_counts string",
        ).writeTo(f"{args.gold_ns}.build_manifest").append()
        set_published(spark, args.gold_ns, build_id)
        ensure_views(spark, args.gold_ns, targets)
    except Exception as exc:  # noqa: BLE001
        log(f"publishing failed, readers still see the previous build, run the build again: {exc}")
        write_log(spark, args.gold_ns, build_id, "publish_failed", results, current_id, versions_id, load_run_id)
        spark.stop()
        return 1
    write_log(spark, args.gold_ns, build_id, "promoted", results, current_id, versions_id, load_run_id)
    prune(spark, args.gold_ns, targets, args.keep_builds)
    log(f"promoted build {build_id}: {len(results)} checks passed, {len(targets)} Gold tables, rows {counts}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
