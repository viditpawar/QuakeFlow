"""Failure-injection tests for the Flink job: kill things, then check the output through Kafka.

Needs the stack running: docker compose --profile core --profile streaming up -d
Run with: python -m pytest -m integration tests/recovery -s
Each test prints measured recovery times.
"""

import json
import time
from collections import Counter

import pytest

from tests.flink_support import (
    TASKMANAGER,
    cancel_job,
    docker,
    produce,
    read_committed,
    rest_get,
    revision_id,
    submit_job,
    valid_message,
    wait_for_job_state,
    wait_for_restore,
)

pytestmark = pytest.mark.integration


def distinct_revisions(messages: list[tuple[str, str]]) -> set[tuple[str, str]]:
    return {revision_id(m) for m in messages}


def validated_revisions(topic: str, expected: int, settle_seconds: int = 25) -> Counter:
    messages = read_committed(topic, expected=expected, settle_seconds=settle_seconds)
    return Counter((m.key().decode(), json.loads(m.value())["record_hash"]) for m in messages)


def test_taskmanager_killed_mid_stream_loses_and_duplicates_nothing(topics):
    job_id = submit_job(topics)

    first = [valid_message(f"evt-{i:03d}", 3.0) for i in range(200)]
    produce(topics["raw"], first + first[:50])  # 50 repeats on top of 200 distinct
    validated_revisions(topics["validated"], expected=200, settle_seconds=0)
    checkpoints = rest_get(f"/jobs/{job_id}/checkpoints")
    assert checkpoints["counts"]["completed"] >= 1

    # Kill the TaskManager hard, then keep sending while it is down: 100 revisions of
    # existing events, 100 new events, and repeats of earlier messages.
    killed_at = time.time()
    assert docker("kill", TASKMANAGER).returncode == 0
    second = (
        [valid_message(f"evt-{i:03d}", 3.5, updated_offset=60_000) for i in range(100)]
        + [valid_message(f"evt-{i:03d}", 3.0) for i in range(200, 300)]
    )
    produce(topics["raw"], second + first[100:150] + second[:20])
    time.sleep(8)  # long enough for the job to notice the loss
    assert docker("start", TASKMANAGER).returncode == 0

    restored = wait_for_restore(job_id)
    restored_at = time.time()
    wait_for_job_state(job_id, "RUNNING")
    expected = distinct_revisions(first + second)
    assert len(expected) == 400

    got = validated_revisions(topics["validated"], expected=len(expected))
    caught_up_at = time.time()
    print(
        f"\nTaskManager killed -> job restored from checkpoint {restored['id']} "
        f"{restored_at - killed_at:.0f}s later (includes the 8s deliberate outage and the restart); "
        f"all {len(expected)} revisions visible {caught_up_at - killed_at:.0f}s after the kill"
    )
    assert set(got) == expected, f"missing: {expected - set(got)}, unexpected: {set(got) - expected}"
    assert all(count == 1 for count in got.values()), {k: c for k, c in got.items() if c > 1}


def test_cancelled_job_restored_from_checkpoint_keeps_its_deduplication_state(topics):
    job_id = submit_job(topics)
    first = [valid_message(f"evt-{i:03d}", 3.0) for i in range(100)]
    produce(topics["raw"], first)
    validated_revisions(topics["validated"], expected=100, settle_seconds=0)

    latest = rest_get(f"/jobs/{job_id}/checkpoints")["latest"]["completed"]
    checkpoint = latest["external_path"]
    print(f"\nrestoring from {checkpoint}")

    cancel_job(job_id)
    wait_for_job_state(job_id, "CANCELED")

    # While no job is running: repeats of what was already processed, 50 revisions, 50 new events.
    second = (
        [valid_message(f"evt-{i:03d}", 3.8, updated_offset=60_000) for i in range(50)]
        + [valid_message(f"evt-{i:03d}", 3.0) for i in range(100, 150)]
    )
    produce(topics["raw"], first + second)

    started_at = time.time()
    new_job = submit_job(topics, recovery_path=checkpoint)
    expected = distinct_revisions(first + second)
    assert len(expected) == 200
    got = validated_revisions(topics["validated"], expected=len(expected))

    restored = rest_get(f"/jobs/{new_job}/checkpoints")["latest"].get("restored")
    print(f"restored job RUNNING and all {len(expected)} revisions visible after {time.time() - started_at:.0f}s")
    assert restored is not None
    assert set(got) == expected
    assert all(count == 1 for count in got.values()), (
        "repeats of already-processed revisions were emitted again, so state was not restored: "
        f"{sum(1 for c in got.values() if c > 1)} duplicated"
    )
