"""Run the real Flink validation job on throwaway topics.

Needs the stack running: docker compose --profile core --profile streaming up -d
Run with: python -m pytest -m integration tests/integration
Skipped automatically when Kafka or Flink is not reachable.
"""

import json
import subprocess
import time
import uuid
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pytest
from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from processing.normalize import normalize_feature

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = ROOT / "flink" / "sql"
SQL_FILES = ("kafka_sources.sql", "validated_events.sql", "revision_tracking.sql")
JOBMANAGER = "quakeflow-flink-jobmanager"
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


@pytest.fixture
def topics():
    if not stack_is_up():
        pytest.skip("Kafka and Flink are not running")
    suffix = uuid.uuid4().hex[:8]
    names = {
        "suffix": suffix,
        "raw": f"quakeflow.test.{suffix}.raw",
        "validated": f"quakeflow.test.{suffix}.validated",
        "dlq": f"quakeflow.test.{suffix}.dlq",
    }
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    created = [NewTopic(names[k], num_partitions=3, replication_factor=1) for k in ("raw", "validated")]
    created.append(NewTopic(names["dlq"], num_partitions=1, replication_factor=1))
    for future in admin.create_topics(created).values():
        future.result()
    yield names
    for job in running_jobs(f"-{suffix}"):
        docker("exec", JOBMANAGER, "curl", "-s", "-X", "PATCH", f"localhost:8081/jobs/{job}?mode=cancel")
    admin.delete_topics([names["raw"], names["validated"], names["dlq"]])


def running_jobs(name_fragment: str) -> list[str]:
    overview = docker("exec", JOBMANAGER, "curl", "-s", "localhost:8081/jobs/overview")
    jobs = json.loads(overview.stdout or "{}").get("jobs", [])
    return [j["jid"] for j in jobs if name_fragment in j["name"] and j["state"] == "RUNNING"]


def submit_job(names: dict) -> None:
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
    path = f"/tmp/job_{suffix}.sql"
    assert docker("exec", "-i", JOBMANAGER, "sh", "-c", f"cat > {path}", stdin=sql).returncode == 0
    result = docker("exec", JOBMANAGER, "./bin/sql-client.sh", "-f", path)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Job ID" in result.stdout, result.stdout


def produce(topic: str, messages: list[tuple[str, str]]) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for key, value in messages:
        producer.produce(topic, key=key, value=value)
    assert producer.flush(30) == 0


def read_committed(topic: str, expected: int, settle_seconds: int = 25, timeout: int = 150) -> list:
    """Messages visible to read_committed consumers: waits for `expected`, then checks for extras."""
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


def test_job_dedupes_keeps_every_revision_and_quarantines_bad_records(topics):
    a_v1 = valid_message("evt-a", 4.6)
    a_v2 = valid_message("evt-a", 4.8, updated_offset=60_000)
    a_late = valid_message("evt-a", 4.5, updated_offset=-60_000)  # older revision, arrives last
    b = valid_message("evt-b", 2.9)
    good = [a_v1, a_v1, b, a_v1, a_v2, a_v1, b, a_late]

    # (expected reason fragment, message)
    bad_payloads = [
        ("not a JSON object", ("bad-1", "this is not json")),
        ("not a JSON object", ("bad-2", "[1, 2, 3]")),
        ("event_id missing", with_changes(a_v1, id=None, event_id=None)),
        ("latitude missing or outside", with_changes(valid_message("evt-c", 3.0), latitude=95.0)),
        ("longitude missing or outside", with_changes(valid_message("evt-d", 3.0), longitude=200.0)),
        ("record_hash missing or not 64", with_changes(valid_message("evt-e", 3.0), record_hash="abc")),
        ("source_updated_at missing", with_changes(valid_message("evt-f", 3.0), source_updated_at=None)),
    ]

    submit_job(topics)
    produce(topics["raw"], good + [message for _, message in bad_payloads])

    validated = read_committed(topics["validated"], expected=4)
    revisions = Counter((m.key().decode(), json.loads(m.value())["magnitude"]) for m in validated)
    assert revisions == Counter(
        {("evt-a", 4.6): 1, ("evt-a", 4.8): 1, ("evt-a", 4.5): 1, ("evt-b", 2.9): 1}
    ), "duplicates must be dropped and every distinct revision (even a late one) kept"
    for message in validated:
        assert message.timestamp()[1] == json.loads(message.value())["source_updated_at"]

    rejected = read_committed(topics["dlq"], expected=len(bad_payloads), settle_seconds=5)
    records = [json.loads(m.value()) for m in rejected]
    assert len(records) == len(bad_payloads)
    reasons = Counter(r["reason"] for r in records)
    for expected_reason, count in Counter(reason for reason, _ in bad_payloads).items():
        matching = sum(n for reason, n in reasons.items() if expected_reason in reason)
        assert matching == count, (expected_reason, reasons)
    assert all(r["source_topic"] == topics["raw"] and "source_offset" in r for r in records)
    assert {r["payload"] for r in records} >= {"this is not json", "[1, 2, 3]"}
