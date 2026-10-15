"""What happens when a revision is replayed after Flink has forgotten it?

Flink's repeat filter keeps its state for 7 days (D-014). This test shortens that to 20 seconds, so
that "after the state expired" can be tested in a minute, and replays an identical revision. It
documents which component keeps the data unique: Flink's filter has forgotten the revision and lets
the replay through, and the Iceberg merges still store it once.

Needs Kafka, Flink and the lakehouse, and the Spark job container. Run with:
    python -m pytest -m integration tests/integration/test_state_expiry.py -s
"""

import time

import pytest

from tests.flink_support import produce, read_committed, submit_job, valid_message
from tests.integration.test_ingest_live import ingest, lake, snapshot  # noqa: F401 - lake is a fixture

pytestmark = pytest.mark.integration

STATE_TTL = "20 s"


def test_a_revision_replayed_after_flinks_state_expired_is_still_stored_once(lake):  # noqa: F811
    names = lake["names"]
    revision = valid_message("evt-ttl", 4.6)
    submit_job(names, state_ttl=STATE_TTL)

    # Within the retention, a repeat is dropped. This is the normal behaviour.
    produce(names["raw"], [revision, revision])
    first = read_committed(names["validated"], expected=1, settle_seconds=15)
    assert len(first) == 1, "a repeat inside the retention must be dropped by Flink"

    # Let the state expire (twice the retention, plus a checkpoint), then replay the same revision.
    time.sleep(2 * 20 + 15)
    produce(names["raw"], [revision])
    after = read_committed(names["validated"], expected=2, settle_seconds=15)
    print(f"\nmessages in the validated topic: {len(first)} before the replay, {len(after)} after")
    assert len(after) == 2, (
        "scenario check: Flink should have forgotten the revision and let the replay through, "
        "otherwise this test proves nothing"
    )

    # The lakehouse does not care: it identifies a revision by (event_id, record_hash).
    ingest(lake)
    counts = snapshot(lake["silver"], lake["bronze"])
    print(f"lakehouse after loading both copies: {counts}")
    assert counts["bronze"] == 2, "Bronze keeps every message exactly as received"
    assert counts["versions"] == 1, "the revision must be stored once in Silver versions"
    assert counts["current"] == 1
