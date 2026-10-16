"""Merge the windows written by batch/backfill.py into the lakehouse.

    docker compose --profile batch run --rm spark \
        /opt/spark/bin/spark-submit /workspace/batch/ingest_history.py

Only windows whose manifest says completed are read. Every write is a MERGE on the identity of
the target table (iceberg/sql/003_merge_history.sql), so loading the same windows again, or
overlapping windows, leaves the tables unchanged, and an older historical revision can never
replace a newer one that is already current.
"""

import argparse
import json
import os
import sys
import uuid
from pathlib import Path

from batch.job_metrics import job_run
from batch.load_log import record_load
from batch.sql_files import DEFAULT_BRONZE, DEFAULT_SILVER, load_statements

SQL_DIR = Path(os.getenv("QUAKEFLOW_SQL_DIR", "/workspace/iceberg/sql"))
DEFAULT_INPUT_DIR = "/data/history"
KINDS = {"bronze": ".bronze.jsonl", "silver": ".silver.jsonl", "rejected": ".rejected.jsonl"}


def log(message: str) -> None:
    print(f"[ingest_history] {message}", file=sys.stderr, flush=True)


def completed_windows(input_dir: Path) -> list[str]:
    """Window ids whose manifest says completed and whose three JSONL files all exist."""
    windows = []
    for manifest_path in sorted(input_dir.glob("*.manifest.json")):
        window_id = manifest_path.name.removesuffix(".manifest.json")
        try:
            status = json.loads(manifest_path.read_text(encoding="utf-8")).get("status")
        except (OSError, ValueError):
            status = None
        files_present = all((input_dir / f"{window_id}{s}").exists() for s in KINDS.values())
        if status == "completed" and files_present:
            windows.append(window_id)
        else:
            log(f"skipping {window_id}: not complete")
    return windows


def register_view(spark, name: str, paths: list[str]) -> None:
    spark.read.text(paths).withColumnRenamed("value", "line").createOrReplaceTempView(name)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--input-dir", type=Path, default=Path(DEFAULT_INPUT_DIR))
    parser.add_argument("--bronze-ns", default=DEFAULT_BRONZE)
    parser.add_argument("--silver-ns", default=DEFAULT_SILVER)
    parser.add_argument("--run-id", default=None, help="the orchestrator run, recorded with the Silver snapshots")
    args = parser.parse_args(argv)

    windows = completed_windows(args.input_dir)
    if not windows:
        log(f"no completed windows in {args.input_dir}, nothing to load")
        return 0
    statements = load_statements(
        SQL_DIR / "003_merge_history.sql", args.bronze_ns, args.silver_ns
    )

    from pyspark.sql import SparkSession  # imported here so the helpers above work without Spark

    spark = SparkSession.builder.appName("quakeflow-ingest-history").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    with job_run("ingest_history"):
        for view, kind in (
            ("raw_history_bronze", "bronze"),
            ("raw_history_silver", "silver"),
            ("raw_history_rejected", "rejected"),
        ):
            register_view(spark, view, [str(args.input_dir / f"{w}{KINDS[kind]}") for w in windows])
        for statement in statements:
            spark.sql(statement)
        log(
            f"loaded {len(windows)} windows: "
            f"{spark.table('incoming_hist_bronze').count()} features, "
            f"{spark.table('incoming_hist_versions').count()} distinct revisions, "
            f"{spark.table('incoming_hist_rejected').count()} rejected"
        )
        run_id = args.run_id or f"manual-{uuid.uuid4()}"
        current, versions = record_load(spark, args.silver_ns, "ingest_history", run_id)
        log(f"recorded load {run_id}: Silver snapshots {current} and {versions}")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
