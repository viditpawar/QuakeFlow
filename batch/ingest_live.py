"""Load Kafka into the lakehouse: Bronze, Silver versions, Silver current state, rejected records.

Each run reads whatever is new in the topics since the last run (a Structured Streaming query
with the availableNow trigger) and then exits, so it can be scheduled or run by hand. Offsets
are tracked in a checkpoint, but correctness does not depend on it: every write is a MERGE on
the identity of the target table (see iceberg/sql/003_merge_current.sql), so reading a message
twice, or losing the checkpoint and starting over, leaves the tables unchanged.

    docker compose --profile core --profile batch run --rm spark \
        /opt/spark/bin/spark-submit /workspace/batch/ingest_live.py
"""

import argparse
import os
import sys
from collections.abc import Callable
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from batch.sql_files import DEFAULT_BRONZE, DEFAULT_SILVER, load_statements

SQL_DIR = Path(os.getenv("QUAKEFLOW_SQL_DIR", "/workspace/iceberg/sql"))
VALIDATED_TOPIC = "quakeflow.events.validated"
DLQ_TOPIC = "quakeflow.events.dlq"
MAX_OFFSETS_PER_BATCH = 50_000


def log(message: str) -> None:
    print(f"[ingest_live] {message}", file=sys.stderr, flush=True)


def kafka_stream(spark: SparkSession, bootstrap: str, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_BATCH)
        # Flink writes these topics in transactions, so skip uncommitted and aborted records.
        .option("kafka.isolation.level", "read_committed")
        # Transaction markers leave gaps in offsets. They are not lost data.
        .option("failOnDataLoss", "false")
        .load()
    )


def run_statements(spark: SparkSession, statements: list[str]) -> None:
    for statement in statements:
        spark.sql(statement)


def live_batch_handler(statements: list[str]) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        spark = batch.sparkSession
        messages = batch.selectExpr(
            "topic AS kafka_topic",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
            "timestamp AS kafka_timestamp",
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS payload",
        ).cache()
        try:
            count = messages.count()
            if count == 0:
                return
            messages.createOrReplaceTempView("raw_messages")
            run_statements(spark, statements)
            rejected = spark.table("incoming_rejected").count()
            log(f"batch {batch_id}: {count} messages, {rejected} rejected by Spark")
        finally:
            messages.unpersist()

    return handle


def dlq_batch_handler(statements: list[str]) -> Callable[[DataFrame, int], None]:
    def handle(batch: DataFrame, batch_id: int) -> None:
        spark = batch.sparkSession
        messages = batch.selectExpr(
            "timestamp AS kafka_timestamp", "CAST(value AS STRING) AS payload"
        ).cache()
        try:
            count = messages.count()
            if count == 0:
                return
            messages.createOrReplaceTempView("raw_dlq_messages")
            run_statements(spark, statements)
            log(f"batch {batch_id}: {count} Flink-rejected messages")
        finally:
            messages.unpersist()

    return handle


def consume(spark, bootstrap, topic, handler, checkpoint: str) -> None:
    query = (
        kafka_stream(spark, bootstrap, topic)
        .writeStream.foreachBatch(handler)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092"))
    parser.add_argument("--topic", default=VALIDATED_TOPIC)
    parser.add_argument("--dlq-topic", default=DLQ_TOPIC)
    parser.add_argument("--checkpoint-dir", default="/checkpoints/live")
    parser.add_argument("--bronze-ns", default=DEFAULT_BRONZE)
    parser.add_argument("--silver-ns", default=DEFAULT_SILVER)
    args = parser.parse_args(argv)

    live = load_statements(SQL_DIR / "003_merge_current.sql", args.bronze_ns, args.silver_ns)
    rejected = load_statements(SQL_DIR / "003_merge_flink_rejected.sql", args.bronze_ns, args.silver_ns)

    spark = SparkSession.builder.appName("quakeflow-ingest-live").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    consume(spark, args.bootstrap, args.topic, live_batch_handler(live), f"{args.checkpoint_dir}/validated")
    consume(spark, args.bootstrap, args.dlq_topic, dlq_batch_handler(rejected), f"{args.checkpoint_dir}/dlq")
    log("done")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
