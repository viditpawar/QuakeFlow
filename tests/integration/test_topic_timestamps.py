"""Kafka retention must count from when a record arrived, not from the USGS update time (D-032).

Needs Kafka: docker compose --profile core up -d kafka
Run with: python -m pytest -m integration tests/integration/test_topic_timestamps.py -s

The collector and the Flink job write the USGS update time as the record timestamp. Kafka deletes old segments
by record timestamp, so on a topic that keeps it, a revision that USGS updated more than the retention ago (a
resend, a replay) would be deleted within minutes of arriving, before the Spark load reads it. This showed up as
a silently incomplete load in the 200,000 revision benchmark. The test checks the setting that prevents it, and
shows the topic that does not have it behaving the other way.
"""

import time
import uuid

import pytest
from confluent_kafka import (
    TIMESTAMP_CREATE_TIME,
    TIMESTAMP_LOG_APPEND_TIME,
    Consumer,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic

from tests.flink_support import BOOTSTRAP, create_topics, delete_topics, stack_is_up_kafka

pytestmark = pytest.mark.integration

THIRTY_DAYS_MS = 30 * 24 * 3600 * 1000


def timestamp_of_only_message(topic: str) -> tuple[int, int]:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP, "group.id": f"ts-{uuid.uuid4()}", "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    consumer.assign([TopicPartition(topic, p, 0) for p in partitions])
    deadline = time.time() + 30
    try:
        while time.time() < deadline:
            message = consumer.poll(1.0)
            if message is not None and not message.error():
                kind, millis = message.timestamp()
                return kind, millis
    finally:
        consumer.close()
    raise AssertionError("no message arrived")


def send_with_old_timestamp(topic: str) -> int:
    old = int(time.time() * 1000) - THIRTY_DAYS_MS
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    producer.produce(topic, key="e", value="v", timestamp=old)
    assert producer.flush(30) == 0
    return old


@pytest.fixture
def kafka():
    if not stack_is_up_kafka():
        pytest.skip("Kafka is not running")


def test_a_topic_built_from_the_configuration_stamps_records_with_their_arrival_time(kafka):
    names = create_topics()  # the real settings from configs/kafka-topics.yaml
    try:
        send_with_old_timestamp(names["validated"])
        kind, millis = timestamp_of_only_message(names["validated"])
        assert kind == TIMESTAMP_LOG_APPEND_TIME
        assert abs(millis - time.time() * 1000) < 120_000, "the 30 day old update time must have been replaced"
    finally:
        delete_topics(names)


def test_negative_control_a_topic_without_the_setting_keeps_the_old_timestamp(kafka):
    name = f"quakeflow.test.{uuid.uuid4().hex[:8]}.control"
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    admin.create_topics([NewTopic(name, num_partitions=1, replication_factor=1)])[name].result()
    try:
        old = send_with_old_timestamp(name)
        kind, millis = timestamp_of_only_message(name)
        assert kind == TIMESTAMP_CREATE_TIME and millis == old, "this is what retention would have measured"
    finally:
        for future in admin.delete_topics([name], operation_timeout=30).values():
            future.result(timeout=60)
