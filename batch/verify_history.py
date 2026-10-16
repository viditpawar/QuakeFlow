"""Check that a backfilled range really is in Silver.

    docker compose --profile batch run --rm spark \
        /opt/spark/bin/spark-submit /workspace/batch/verify_history.py --start 2025-01-01 --end 2025-01-08

For every completed window of the range (and the same minimum magnitude) the backfill wrote, every revision it
normalized, identified by event id and record hash, must be in silver.earthquake_versions. It exits with 1 if the
range has no completed window, if the downloaded counts do not add up, or if any revision is missing.

Runs inside the Spark image, whose Python is 3.10.
"""

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

from batch.job_metrics import job_run
from batch.sql_files import DEFAULT_SILVER

DEFAULT_INPUT_DIR = os.getenv("QUAKEFLOW_HISTORY_DIR", "/data/history")


def log(message: str) -> None:
    print(f"[verify_history] {message}", file=sys.stderr, flush=True)


def parse_magnitude(text: str) -> float | None:
    return None if text.strip() == "" else float(text)


def select_windows(manifests: dict[str, dict], start: date, end: date, min_magnitude: float | None) -> list[str]:
    """Window ids whose manifest is completed, lies inside [start, end) and has this minimum magnitude."""
    chosen = []
    for window_id, manifest in sorted(manifests.items()):
        if manifest.get("status") != "completed" or manifest.get("min_magnitude") != min_magnitude:
            continue
        window_start = date.fromisoformat(str(manifest["start_date"])[:10])
        window_end = date.fromisoformat(str(manifest["end_date"])[:10])
        if window_start >= start and window_end <= end:
            chosen.append(window_id)
    return chosen


def count_problems(manifests: dict[str, dict], windows: list[str]) -> list[str]:
    """Manifests whose own numbers disagree: every downloaded feature is either valid or rejected."""
    return [
        f"{w}: {m['record_count']} downloaded but {m['valid_count']} valid + {m['rejected_count']} rejected"
        for w in windows
        for m in [manifests[w]]
        if m["record_count"] != m["valid_count"] + m["rejected_count"]
    ]


def read_manifests(input_dir: Path) -> dict[str, dict]:
    manifests = {}
    for path in sorted(input_dir.glob("*.manifest.json")):
        try:
            manifests[path.name.removesuffix(".manifest.json")] = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            log(f"ignoring unreadable manifest {path.name}")
    return manifests


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--min-magnitude", type=parse_magnitude, default=None)
    parser.add_argument("--input-dir", type=Path, default=Path(DEFAULT_INPUT_DIR))
    parser.add_argument("--silver-ns", default=DEFAULT_SILVER)
    args = parser.parse_args(argv)

    manifests = read_manifests(args.input_dir)
    windows = select_windows(manifests, args.start, args.end, args.min_magnitude)
    if not windows:
        log(f"no completed window of {args.start} to {args.end} with minimum magnitude {args.min_magnitude}")
        return 1
    problems = count_problems(manifests, windows)
    for problem in problems:
        log(f"FAIL {problem}")

    from pyspark.sql import SparkSession  # imported here so the helpers above work without Spark

    spark = SparkSession.builder.appName("quakeflow-verify-history").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    with job_run("verify_history") as run:
        paths = [str(args.input_dir / f"{w}.silver.jsonl") for w in windows]
        spark.read.text(paths).selectExpr(
            "from_json(value, 'event_id string, record_hash string') AS e"
        ).selectExpr("e.event_id AS event_id", "e.record_hash AS record_hash").distinct().createOrReplaceTempView(
            "downloaded_revisions"
        )
        expected = spark.table("downloaded_revisions").count()
        missing = spark.sql(
            f"SELECT d.event_id, d.record_hash FROM downloaded_revisions d LEFT ANTI JOIN "
            f"{args.silver_ns}.earthquake_versions v ON v.event_id = d.event_id AND v.record_hash = d.record_hash"
        )
        missing_count = missing.count()
        for row in missing.limit(10).collect():
            log(f"FAIL missing from Silver: {row.event_id} {row.record_hash[:12]}")
        log(f"{len(windows)} windows, {expected} distinct revisions downloaded, {missing_count} missing from Silver")
        run.gauge("quakeflow_history_missing_revisions", "Downloaded revisions missing from Silver.", missing_count)
        run.success = not problems and missing_count == 0
    spark.stop()
    return 0 if run.success else 1


if __name__ == "__main__":
    sys.exit(main())
