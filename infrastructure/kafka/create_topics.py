"""Create the Kafka topics defined in configs/kafka-topics.yaml.

Safe to run repeatedly: topics that already exist are left unchanged.
"""

import argparse
import os
import sys
from pathlib import Path

import yaml
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs" / "kafka-topics.yaml"


def load_topics(path: Path) -> list[NewTopic]:
    with path.open() as f:
        spec = yaml.safe_load(f)
    return [
        NewTopic(
            t["name"],
            num_partitions=t["partitions"],
            replication_factor=t["replication_factor"],
            config={"retention.ms": str(t["retention_ms"])},
        )
        for t in spec["topics"]
    ]


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
    to_create = [t for t in topics if t.topic not in existing]
    if not to_create:
        return 0

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
