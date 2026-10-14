"""Kill the Spark load part way through, then retry: the tables must end up correct.

Each MERGE is its own Iceberg commit, so a kill can land between two of them and leave Bronze
loaded but Silver not yet. The job only advances its checkpoint after the whole batch succeeds,
so the retry reads the batch again, and the idempotent merges finish the work without
duplicating what already landed.

Needs Kafka and the lakehouse. Run with:
    python -m pytest -m integration tests/recovery/test_interrupted_batch.py -s
"""

import json
import subprocess
import time

import pytest

from tests.flink_support import ROOT, produce, valid_message
from tests.integration.test_ingest_live import ingest, lake, snapshot  # noqa: F401 - lake is a fixture
from tests.spark_support import spark_sql

pytestmark = pytest.mark.integration

KILL_AFTER_SECONDS = (10, 16, 22)  # spread across start-up, reading Kafka, and merging


def run_and_kill(lake, kill_after: float, name: str) -> float:  # noqa: F811 - a fixture value
    """Start the ingest job, kill its container after `kill_after` seconds. Returns seconds run."""
    n = lake["names"]
    command = [
        "docker", "compose", "--profile", "core", "--profile", "batch", "run", "--rm", "-T",
        "--name", name, "spark", "/opt/spark/bin/spark-submit", "/workspace/batch/ingest_live.py",
        "--topic", n["validated"], "--dlq-topic", n["dlq"], "--bronze-ns", lake["bronze"],
        "--silver-ns", lake["silver"], "--checkpoint-dir", lake["checkpoint"],
    ]
    process = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    started = time.time()
    while time.time() - started < kill_after:
        if process.poll() is not None:
            return time.time() - started  # finished before we could interrupt it
        time.sleep(0.5)
    subprocess.run(["docker", "kill", name], capture_output=True, check=False)
    process.wait(timeout=120)
    subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
    return time.time() - started


def test_a_load_killed_part_way_is_finished_correctly_by_the_retry(lake):  # noqa: F811
    # 300 distinct events, 100 of which also have a later revision: 400 versions in all.
    first = [valid_message(f"evt-{i:03d}", 3.0) for i in range(300)]
    revisions = [valid_message(f"evt-{i:03d}", 3.4, updated_offset=60_000) for i in range(100)]
    produce(lake["names"]["validated"], first + revisions)
    expected = {"bronze": 400, "versions": 400, "current": 300, "rejected_spark": 0}

    intermediate = []
    for attempt, delay in enumerate(KILL_AFTER_SECONDS):
        ran = run_and_kill(lake, delay, f"quakeflow-kill-test-{lake['names']['suffix']}-{attempt}")
        state = snapshot(lake["silver"], lake["bronze"])
        intermediate.append((round(ran), state))
        # Whatever the kill interrupted, no table may ever hold more than the data that exists.
        assert state.get("bronze", 0) <= 400 and state.get("versions", 0) <= 400
        assert state.get("current", 0) <= 300

    print("\nstate of the tables after each kill (seconds run, counts):")
    for ran, state in intermediate:
        print(f"  killed after {ran}s: {state}")

    ingest(lake)  # the retry runs to completion
    final = snapshot(lake["silver"], lake["bronze"])
    assert {k: final.get(k, 0) for k in expected} == expected, f"final state was {final}"

    # Nothing duplicated, and every event's current row is its newest revision.
    duplicates = spark_sql(
        f"SELECT count(*) FROM (SELECT event_id, record_hash FROM {lake['silver']}.earthquake_versions "
        f"GROUP BY event_id, record_hash HAVING count(*) > 1);"
        f"SELECT count(*) FROM (SELECT event_id FROM {lake['silver']}.earthquakes_current "
        f"GROUP BY event_id HAVING count(*) > 1);"
        f"SELECT count(*) FROM (SELECT kafka_partition, kafka_offset FROM {lake['bronze']}.live_observations "
        f"GROUP BY kafka_partition, kafka_offset HAVING count(*) > 1);"
        f"SELECT count(*) FROM {lake['silver']}.earthquakes_current c WHERE c.magnitude = 3.0 "
        f"AND CAST(substring(c.event_id, 5) AS INT) < 100;"
    )
    assert [int(row[0]) for row in duplicates] == [0, 0, 0, 0], (
        "duplicates in versions, current or bronze, or an old revision left as current"
    )
    json.dumps(intermediate)  # keep the record of intermediate states serialisable for the log
