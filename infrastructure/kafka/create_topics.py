"""Create the Kafka topics defined in configs/kafka-topics.yaml.

Safe to run repeatedly: topics that already exist are left unchanged.
"""

import argparse
import os
import sys
from pathlib import Path

import yaml
from confluent_kafka import KafkaException
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
)

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "kafka-topics.yaml"


def topic_config(topic: dict) -> dict[str, str]:
    """The per-topic settings a topic must have, as Kafka names them."""
    return {
        "retention.ms": str(topic["retention_ms"]),
        "message.timestamp.type": topic["message_timestamp_type"],
    }


def load_topics(path: Path) -> list[NewTopic]:
    with path.open() as f:
        spec = yaml.safe_load(f)
    return [
        NewTopic(
            t["name"],
            num_partitions=t["partitions"],
            replication_factor=t["replication_factor"],
            config=topic_config(t),
        )
        for t in spec["topics"]
    ]


def align_settings(admin: AdminClient, topic: NewTopic) -> bool:
    """Make an existing topic's settings match the configuration. Returns False if that failed.

    Settings apply to records written from now on, so changing one never rewrites existing data."""
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic.topic)
    current = admin.describe_configs([resource])[resource].result()
    changes = [
        ConfigEntry(name, value, incremental_operation=AlterConfigOpType.SET)
        for name, value in topic.config.items()
        if current[name].value != value
    ]
    if not changes:
        return True
    resource = ConfigResource(ConfigResource.Type.TOPIC, topic.topic, incremental_configs=changes)
    try:
        admin.incremental_alter_configs([resource])[resource].result()
    except KafkaException as exc:
        print(f"FAILED   {topic.topic}: could not set {[c.name for c in changes]}: {exc}", file=sys.stderr)
        return False
    print(f"updated  {topic.topic}: {', '.join(f'{c.name}={c.value}' for c in changes)}")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
    )
    args = parser.parse_args()

    admin = AdminClient({"bootstrap.servers": args.bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics)
    topics = load_topics(args.config)

    failed = False
    for topic in topics:
        if topic.topic in existing:
            print(f"exists   {topic.topic}")
            failed |= not align_settings(admin, topic)
    to_create = [t for t in topics if t.topic not in existing]
    if not to_create:
        return 1 if failed else 0

    for name, future in admin.create_topics(to_create).items():
        try:
            future.result()
            print(f"created  {name}")
        except KafkaException as exc:
            failed = True
            print(f"FAILED   {name}: {exc}", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
