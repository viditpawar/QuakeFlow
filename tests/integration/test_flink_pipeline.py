"""Run the real Flink validation job on throwaway topics.

Needs the stack running: docker compose --profile core --profile streaming up -d
Run with: python -m pytest -m integration tests/integration
Skipped automatically when Kafka or Flink is not reachable.
"""

import json
from collections import Counter

import pytest

from tests.flink_support import (
    produce,
    read_committed,
    submit_job,
    valid_message,
    with_changes,
)

pytestmark = pytest.mark.integration


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
