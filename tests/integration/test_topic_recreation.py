"""A recreated Kafka topic reuses old offsets. Bronze must still record the new messages.

Background (D-024): after Kafka lost its data the topics were recreated, and their new messages
carried the same topic, partition and offset as messages Bronze already held. Bronze identified a
message by those three values, decided the new ones were already loaded, and silently skipped
208 of them while Silver (identified by content) loaded them correctly.

Needs Kafka and the lakehouse. Run with:
    python -m pytest -m integration tests/integration/test_topic_recreation.py -s
"""

import contextlib

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from tests.flink_support import BOOTSTRAP, valid_message
from tests.integration.test_ingest_live import ingest, lake, snapshot  # noqa: F401 - lake is a fixture
from tests.spark_support import remove_checkpoint

pytestmark = pytest.mark.integration


def produce_to_partition_zero(topic: str, messages: list[tuple[str, str]]) -> None:
    """Everything on partition 0, so the offsets are certain to repeat after the topic is recreated."""
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    for key, value in messages:
        producer.produce(topic, key=key, value=value, partition=0)
    assert producer.flush(30) == 0


def recreate(topic: str) -> None:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})  # kept alive until the deletion is done
    with contextlib.suppress(Exception):
        admin.delete_topics([topic], operation_timeout=30)[topic].result(timeout=60)
    admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])[topic].result(timeout=60)


def test_messages_at_reused_offsets_of_a_recreated_topic_are_not_lost_from_bronze(lake):  # noqa: F811
    topic = lake["names"]["validated"]
    first_life = [valid_message(f"old-{i}", 3.0) for i in range(3)]
    produce_to_partition_zero(topic, first_life)
    ingest(lake)
    assert snapshot(lake["silver"], lake["bronze"])["bronze"] == 3

    # The topic is lost and recreated, as after the Kafka data loss. Its offsets start again at 0.
    recreate(topic)
    second_life = [valid_message(f"new-{i}", 4.0) for i in range(3)]  # same offsets, different messages
    produce_to_partition_zero(topic, second_life)
    remove_checkpoint(lake["checkpoint"])  # the documented step: old read positions must not be reused
    ingest(lake)

    counts = snapshot(lake["silver"], lake["bronze"])
    print(f"\nafter the topic was recreated: {counts}")
    assert counts["versions"] == 6, "Silver identifies revisions by content and must hold both lives"
    assert counts["current"] == 6
    assert counts["bronze"] == 6, (
        "Bronze skipped the new messages because their topic, partition and offset were already known"
    )
