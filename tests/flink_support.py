"""Helpers shared by the integration and recovery tests that drive the real Docker stack."""

import json
import subprocess
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from processing.normalize import normalize_feature

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "flink" / "sql"
SQL_FILES = ("kafka_sources.sql", "validated_events.sql", "revision_tracking.sql")
JOBMANAGER = "quakeflow-flink-jobmanager"
TASKMANAGER = "quakeflow-flink-taskmanager"
BOOTSTRAP = "localhost:9092"
FIXTURE = ROOT / "tests" / "fixtures" / "usgs_all_hour_sample.geojson"
BASE_UPDATED = 1791878280000


def docker(*args: str, stdin: str | None = None) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", *args], input=stdin, capture_output=True, text=True, timeout=180, check=False
    )


def stack_is_up() -> bool:
    try:
        running = docker("inspect", "-f", "{{.State.Running}}", JOBMANAGER)
        if running.stdout.strip() != "true":
            return False
        AdminClient({"bootstrap.servers": BOOTSTRAP}).list_topics(timeout=3)
        return True
    except (KafkaException, OSError, subprocess.SubprocessError):
        return False


# --- messages -----------------------------------------------------------------------------


def valid_message(event_id: str, magnitude: float, updated_offset: int = 0) -> tuple[str, str]:
    feature = json.loads(FIXTURE.read_text())["features"][0]
    feature["id"] = event_id
    feature["properties"].update(mag=magnitude, updated=BASE_UPDATED + updated_offset)
    event = normalize_feature(feature, datetime(2026, 10, 13, 12, 0, tzinfo=UTC))
    return event_id, event.to_json()


def with_changes(message: tuple[str, str], **changes) -> tuple[str, str]:
    key, text = message
    data = json.loads(text)
    for name, value in changes.items():
        if value is None:
            data.pop(name, None)
        else:
            data[name] = value
    return key, json.dumps(data)


def revision_id(message: tuple[str, str]) -> tuple[str, str]:
    """(event_id, record_hash): what the pipeline treats as one distinct revision."""
    return message[0], json.loads(message[1])["record_hash"]


# --- topics -------------------------------------------------------------------------------


def create_topics() -> dict:
    suffix = uuid.uuid4().hex[:8]
    names = {
        "suffix": suffix,
        "raw": f"quakeflow.test.{suffix}.raw",
        "validated": f"quakeflow.test.{suffix}.validated",
        "dlq": f"quakeflow.test.{suffix}.dlq",
    }
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    new = [NewTopic(names[k], num_partitions=3, replication_factor=1) for k in ("raw", "validated")]
    new.append(NewTopic(names["dlq"], num_partitions=1, replication_factor=1))
    for future in admin.create_topics(new).values():
        future.result()
    return names


def delete_topics(names: dict) -> None:
    for job in running_jobs(f"-{names['suffix']}"):
        cancel_job(job)
    AdminClient({"bootstrap.servers": BOOTSTRAP}).delete_topics(
        [names["raw"], names["validated"], names["dlq"]]
    )


def produce(topic: str, messages: list[tuple[str, str]]) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for key, value in messages:
        producer.produce(topic, key=key, value=value)
    assert producer.flush(30) == 0


def read_committed(topic: str, expected: int, settle_seconds: int = 25, timeout: int = 150) -> list:
    """Messages visible to read_committed consumers.

    Returns as soon as `expected` have arrived and `settle_seconds` have passed with no need
    for more, so a count that is too high shows up as extra messages in the result.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": f"test-{uuid.uuid4()}",
            "isolation.level": "read_committed",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    consumer.assign([TopicPartition(topic, p, 0) for p in partitions])
    messages, deadline, settled_at = [], time.time() + timeout, None
    while time.time() < deadline:
        message = consumer.poll(1.0)
        if message is not None and not message.error():
            messages.append(message)
        if len(messages) >= expected and settled_at is None:
            settled_at = time.time() + settle_seconds
        if settled_at is not None and time.time() > settled_at:
            break
    consumer.close()
    return messages


# --- Flink --------------------------------------------------------------------------------


def rest_get(path: str) -> dict:
    result = docker("exec", JOBMANAGER, "curl", "-s", f"localhost:8081{path}")
    return json.loads(result.stdout or "{}")


def jobs(name_fragment: str) -> list[dict]:
    return [j for j in rest_get("/jobs/overview").get("jobs", []) if name_fragment in j["name"]]


def running_jobs(name_fragment: str) -> list[str]:
    return [j["jid"] for j in jobs(name_fragment) if j["state"] == "RUNNING"]


def cancel_job(job_id: str) -> None:
    docker("exec", JOBMANAGER, "curl", "-s", "-X", "PATCH", f"localhost:8081/jobs/{job_id}?mode=cancel")


def wait_for_job_state(job_id: str, wanted: str, timeout: int = 120) -> float:
    """Block until the job reaches `wanted`. Returns seconds waited."""
    start = time.time()
    while time.time() - start < timeout:
        if rest_get(f"/jobs/{job_id}").get("state") == wanted:
            return time.time() - start
        time.sleep(1)
    raise AssertionError(f"job {job_id} did not reach {wanted} within {timeout}s")


def job_sql(names: dict, recovery_path: str | None = None) -> str:
    sql = "\n".join((SQL_DIR / f).read_text() for f in SQL_FILES)
    suffix = names["suffix"]
    for original, replacement in {
        "quakeflow.events.raw": names["raw"],
        "quakeflow.events.validated": names["validated"],
        "quakeflow.events.dlq": names["dlq"],
        "quakeflow-flink": f"quakeflow-flink-{suffix}",
        "'quakeflow-validated'": f"'quakeflow-validated-{suffix}'",
        "'quakeflow-rejected'": f"'quakeflow-rejected-{suffix}'",
        "quakeflow-validate-and-dedupe": f"quakeflow-validate-and-dedupe-{suffix}",
    }.items():
        assert original in sql, original
        sql = sql.replace(original, replacement)
    if recovery_path:
        marker = "SET 'pipeline.name'"
        sql = sql.replace(marker, f"SET 'execution.state-recovery.path' = '{recovery_path}';\n{marker}")
    return sql


def submit_job(names: dict, recovery_path: str | None = None) -> str:
    """Submit the real job SQL against the test topics. Returns the Flink job id."""
    path = f"/tmp/job_{names['suffix']}_{int(time.time())}.sql"
    sql = job_sql(names, recovery_path)
    assert docker("exec", "-i", JOBMANAGER, "sh", "-c", f"cat > {path}", stdin=sql).returncode == 0
    result = docker("exec", JOBMANAGER, "./bin/sql-client.sh", "-f", path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Job ID: " in result.stdout, result.stdout
    job_id = result.stdout.split("Job ID: ")[1].split()[0].strip()
    wait_for_job_state(job_id, "RUNNING")
    return job_id


def wait_for_restore(job_id: str, timeout: int = 180) -> dict:
    """Block until the job reports it was restored from a checkpoint. Returns that checkpoint."""
    start = time.time()
    while time.time() - start < timeout:
        restored = rest_get(f"/jobs/{job_id}/checkpoints").get("latest", {}).get("restored")
        if restored:
            return restored
        time.sleep(1)
    raise AssertionError(f"job {job_id} was not restored from a checkpoint within {timeout}s")
