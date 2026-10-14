"""Run the real Spark ingest job on throwaway topics and namespaces.

Needs Kafka and the lakehouse: docker compose --profile core --profile batch up -d kafka postgres rustfs
Run with: python -m pytest -m integration tests/integration/test_ingest_live.py -s
"""

import json
import uuid

import pytest

from tests.flink_support import (
    BOOTSTRAP,
    ROOT,
    create_topics,
    delete_topics,
    produce,
    revision_id,
    stack_is_up_kafka,
    valid_message,
    with_changes,
)
from tests.spark_support import lakehouse_is_up, remove_checkpoint, run_ingest, spark_sql

pytestmark = pytest.mark.integration

CREATE_TABLES = (ROOT / "iceberg" / "sql" / "002_create_tables.sql").read_text()
TABLE_NAMES = {
    "bronze": ["live_observations", "historical_observations"],
    "silver": ["earthquake_versions", "earthquakes_current", "rejected_records"],
}


def count(table: str) -> int:
    return int(spark_sql(f"SELECT count(*) FROM {table};")[0][0])


def snapshot(silver: str, bronze: str) -> dict:
    rows = spark_sql(
        f"SELECT 'bronze', count(*) FROM {bronze}.live_observations;"
        f"SELECT 'versions', count(*) FROM {silver}.earthquake_versions;"
        f"SELECT 'current', count(*) FROM {silver}.earthquakes_current;"
        f"SELECT 'rejected_' || stage, count(*) FROM {silver}.rejected_records GROUP BY stage;"
    )
    return {name: int(value) for name, value in rows}


def current_rows(silver: str) -> dict:
    rows = spark_sql(
        f"SELECT event_id, magnitude, record_hash FROM {silver}.earthquakes_current ORDER BY event_id;"
    )
    return {event_id: (float(mag), record_hash) for event_id, mag, record_hash in rows}


@pytest.fixture
def lake():
    if not (stack_is_up_kafka() and lakehouse_is_up()):
        pytest.skip("Kafka and the lakehouse are not running")
    names = create_topics()
    suffix = names["suffix"]
    bronze, silver = f"local.zt{suffix}_bronze", f"local.zt{suffix}_silver"
    checkpoint = f"/checkpoints/test_{suffix}"
    script = CREATE_TABLES.replace("local.bronze", bronze).replace("local.silver", silver)
    spark_sql(f"CREATE NAMESPACE {bronze}; CREATE NAMESPACE {silver};\n{script}")
    yield {"names": names, "bronze": bronze, "silver": silver, "checkpoint": checkpoint}
    drops = " ".join(
        f"DROP TABLE IF EXISTS {ns}.{t} PURGE;"
        for ns, group in ((bronze, "bronze"), (silver, "silver"))
        for t in TABLE_NAMES[group]
    )
    spark_sql(f"{drops} DROP NAMESPACE IF EXISTS {bronze}; DROP NAMESPACE IF EXISTS {silver};")
    remove_checkpoint(checkpoint)
    delete_topics(names)


def ingest(lake) -> str:
    n = lake["names"]
    return run_ingest(n["validated"], n["dlq"], lake["bronze"], lake["silver"], lake["checkpoint"])


def test_ingest_builds_versions_and_current_state_and_is_safe_to_repeat(lake):
    topic, dlq = lake["names"]["validated"], lake["names"]["dlq"]
    bronze, silver = lake["bronze"], lake["silver"]

    a_v1 = valid_message("evt-a", 4.6)
    a_v2 = valid_message("evt-a", 4.8, updated_offset=60_000)
    b = valid_message("evt-b", 2.9)
    c_one = valid_message("evt-c", 3.0, updated_offset=5_000)
    c_two = valid_message("evt-c", 3.1, updated_offset=5_000)  # same update time, different content
    bad_lat = with_changes(valid_message("evt-x", 3.0), latitude=95.0)
    first = [a_v1, a_v1, a_v2, a_v1, b, c_one, c_two, bad_lat, ("junk", "not json at all")]
    flink_reject = json.dumps(
        {"reason": "latitude missing or outside -90..90", "source_topic": "quakeflow.events.raw",
         "source_partition": 1, "source_offset": 42, "payload": a_v1[1]}
    )
    produce(topic, first)
    produce(dlq, [("k", flink_reject)])

    log = ingest(lake)
    print("\n" + "\n".join(line for line in log.splitlines() if "[ingest_live]" in line))

    assert snapshot(silver, bronze) == {
        "bronze": len(first),  # every message is kept, valid or not
        "versions": 5,  # a_v1, a_v2, b, c_one, c_two
        "current": 3,
        "rejected_spark": 2,
        "rejected_flink": 1,
    }
    current = current_rows(silver)
    assert current["evt-a"] == (4.8, revision_id(a_v2)[1]), "the newer revision must be current"
    assert current["evt-b"][0] == 2.9
    winner = max(revision_id(c_one)[1], revision_id(c_two)[1])
    assert current["evt-c"][1] == winner, "an exact tie must be won by the higher record_hash"

    # A later run with nothing new must not change anything.
    ingest(lake)
    assert snapshot(silver, bronze)["versions"] == 5

    # Late arrivals: an OLDER revision of evt-a, a replay of messages already loaded, a new event.
    a_old = valid_message("evt-a", 4.5, updated_offset=-60_000)
    d = valid_message("evt-d", 5.1)
    produce(topic, [a_old, a_v2, b, d])
    ingest(lake)
    after = snapshot(silver, bronze)
    assert after["versions"] == 7  # + a_old and d (the replays were already known)
    assert after["current"] == 4
    assert after["bronze"] == len(first) + 4  # Bronze records every message again, replays included
    assert current_rows(silver)["evt-a"] == (4.8, revision_id(a_v2)[1]), (
        "an older revision arriving later must not replace a newer one"
    )

    # Lose the checkpoint: everything is read again from the beginning of the topic.
    remove_checkpoint(lake["checkpoint"])
    ingest(lake)
    assert snapshot(silver, bronze) == after, "reprocessing the whole topic must change nothing"
    assert current_rows(silver)["evt-a"][0] == 4.8


def test_ingest_reads_only_committed_transactions_and_copes_with_offset_gaps(lake):
    """Flink writes its output in transactions, which leave gaps and can be aborted."""
    from confluent_kafka import Producer

    topic = lake["names"]["validated"]
    producer = Producer(
        {"bootstrap.servers": BOOTSTRAP, "transactional.id": f"test-{uuid.uuid4()}"}
    )
    producer.init_transactions()

    def transaction(messages, commit: bool) -> None:
        producer.begin_transaction()
        for key, value in messages:
            producer.produce(topic, key=key, value=value)
        # Without this, aborting purges the unsent messages and nothing aborted reaches the log.
        assert producer.flush(30) == 0
        producer.commit_transaction() if commit else producer.abort_transaction()

    committed_one = [valid_message("evt-a", 4.0), valid_message("evt-b", 3.0)]
    aborted = [valid_message("evt-aborted-1", 6.0), valid_message("evt-aborted-2", 6.5)]
    committed_two = [valid_message("evt-c", 2.0), valid_message("evt-a", 4.2, updated_offset=60_000)]
    transaction(committed_one, commit=True)
    transaction(aborted, commit=False)  # its records and the abort marker leave a gap
    transaction(committed_two, commit=True)

    ingest(lake)

    silver, bronze = lake["silver"], lake["bronze"]
    counts = snapshot(silver, bronze)
    assert counts["bronze"] == 4, "aborted records must not be read"
    assert counts["versions"] == 4
    ids = {row[0] for row in spark_sql(f"SELECT event_id FROM {silver}.earthquake_versions;")}
    assert ids == {"evt-a", "evt-b", "evt-c"}
    assert current_rows(silver)["evt-a"][0] == 4.2
