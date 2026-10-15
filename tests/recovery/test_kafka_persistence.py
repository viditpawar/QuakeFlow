"""Kafka must keep its data when its container is removed, not only when it is restarted.

Background (D-024): the volume was mounted at a path the broker never wrote to, so the data lived
in the container itself. It survived `docker stop` and `docker start`, which is all the early
drills did, and was erased the first time the container was removed. These tests destroy the
container in the two ways that matter and compare the data before and after:

* `docker rm -f` removes the container immediately, like a crash that loses the container.
* `docker compose down kafka` is the documented way to take the service down. It removes the
  container and keeps the volumes.

Only Kafka is removed, and it is started again at the end whatever the outcome. Needs Kafka:
    python -m pytest -m integration tests/recovery/test_kafka_persistence.py -s
"""

import contextlib
import subprocess
import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from tests.flink_support import BOOTSTRAP, ROOT, stack_is_up_kafka

pytestmark = pytest.mark.integration

CONTAINER = "quakeflow-kafka"
VOLUME = "quakeflow_kafka-data"
LIFECYCLES = {
    "docker rm -f": ["docker", "rm", "-f", CONTAINER],
    "docker compose down kafka": ["docker", "compose", "--profile", "core", "down", "kafka"],
}


def compose_up_kafka() -> None:
    subprocess.run(
        ["docker", "compose", "--profile", "core", "up", "-d", "kafka"],
        cwd=ROOT, capture_output=True, text=True, timeout=180, check=False,
    )


def container_exists() -> bool:
    out = subprocess.run(
        ["docker", "ps", "-a", "--filter", f"name=^{CONTAINER}$", "-q"],
        capture_output=True, text=True, check=False,
    ).stdout
    return bool(out.strip())


def wait_until_healthy(timeout: int = 180) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Health.Status}}", CONTAINER],
            capture_output=True, text=True, check=False,
        ).stdout.strip()
        if status == "healthy":
            return
        time.sleep(3)
    raise AssertionError("Kafka did not become healthy again")


def snapshot(topic: str) -> tuple[dict[int, int], list[tuple[str, str]]]:
    """(end offset of each partition, every (key, value) in the topic)."""
    consumer = Consumer(
        {"bootstrap.servers": BOOTSTRAP, "group.id": f"p-{uuid.uuid4()}",
         "enable.auto.commit": False, "auto.offset.reset": "earliest"}
    )
    partitions = consumer.list_topics(topic, timeout=15).topics[topic].partitions
    ends = {p: consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=15)[1] for p in partitions}
    consumer.assign([TopicPartition(topic, p, 0) for p in partitions])
    found, idle = [], 0
    while len(found) < sum(ends.values()) and idle < 15:
        message = consumer.poll(1.0)
        if message is None:
            idle += 1
        elif not message.error():
            found.append((message.key().decode(), message.value().decode()))
    consumer.close()
    return ends, sorted(found)


@pytest.mark.parametrize("lifecycle", list(LIFECYCLES))
def test_topics_and_messages_survive_the_container_being_removed(lifecycle):
    if not stack_is_up_kafka():
        pytest.skip("Kafka is not running")
    topic = f"quakeflow.test.persist-{uuid.uuid4().hex[:6]}"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(timeout=60)
    try:
        producer = Producer({"bootstrap.servers": BOOTSTRAP})
        for i in range(12):
            producer.produce(topic, key=f"key-{i}", value=f"message-{i}")
        assert producer.flush(30) == 0
        before = snapshot(topic)
        assert sum(before[0].values()) == 12 and len(before[1]) == 12

        subprocess.run(LIFECYCLES[lifecycle], cwd=ROOT, capture_output=True, text=True, timeout=180, check=False)
        assert not container_exists(), f"{lifecycle} did not remove the container, so this test proved nothing"
        volumes = subprocess.run(["docker", "volume", "ls", "-q"], capture_output=True, text=True, check=False).stdout
        assert VOLUME in volumes.split(), "the data volume must survive the container being removed"

        compose_up_kafka()
        wait_until_healthy()
        after = snapshot(topic)
        print(f"\n{lifecycle}: end offsets {before[0]} -> {after[0]}; messages {len(before[1])} -> {len(after[1])}")
        assert after[0] == before[0], "partition end offsets changed"
        assert after[1] == before[1], "messages differ after the container was recreated"
    finally:
        compose_up_kafka()  # never leave Kafka removed, whatever happened above
        wait_until_healthy()
        admin = AdminClient({"bootstrap.servers": BOOTSTRAP})  # keep the client alive until done
        future = admin.delete_topics([topic], operation_timeout=30)[topic]
        with contextlib.suppress(Exception):  # cleanup must not hide the test's own result
            future.result(timeout=60)
