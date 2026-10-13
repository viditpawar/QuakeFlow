import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from ingestion.change_detector import Change, ChangeDetector, ChangeKind
from processing.normalize import normalize_feature
from schemas.earthquake import EarthquakeEvent

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "usgs_all_hour_sample.geojson"
INGESTED = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)
BASE_UPDATED = 1791878280000  # fixture's example-quake-001 "updated"


def make_event(
    magnitude=4.6,
    updated_ms=BASE_UPDATED,
    event_id="example-quake-001",
    ingested_at=INGESTED,
    **properties,
) -> EarthquakeEvent:
    feature = json.loads(FIXTURE.read_text())["features"][0]
    feature["id"] = event_id
    feature["properties"].update(mag=magnitude, updated=updated_ms, **properties)
    return normalize_feature(feature, ingested_at)


@pytest.fixture
def detector():
    with ChangeDetector() as d:
        yield d


def kinds(changes: list[Change]) -> list[ChangeKind]:
    return [c.kind for c in changes]


def test_first_sighting_is_new_and_pending(detector):
    event = make_event()
    changes = detector.detect([event])
    assert changes == [Change(ChangeKind.NEW, event)]
    assert detector.pending() == changes
    assert detector.pending_count() == 1


def test_identical_payload_is_not_a_new_revision(detector):
    detector.detect([make_event()])
    assert detector.detect([make_event()]) == []
    assert detector.pending_count() == 1


def test_different_ingestion_time_is_still_unchanged(detector):
    detector.detect([make_event()])
    assert detector.detect([make_event(ingested_at=INGESTED + timedelta(hours=2))]) == []


def test_changed_payload_with_later_update_is_a_revision(detector):
    detector.detect([make_event(magnitude=4.6)])
    revised = make_event(magnitude=4.8, updated_ms=BASE_UPDATED + 60_000)
    assert kinds(detector.detect([revised])) == [ChangeKind.REVISED]
    assert [c.event.magnitude for c in detector.pending()] == [4.6, 4.8]


def test_older_revision_after_newer_is_out_of_order(detector):
    detector.detect([make_event(magnitude=4.8, updated_ms=BASE_UPDATED + 60_000)])
    older = make_event(magnitude=4.6, updated_ms=BASE_UPDATED)
    assert kinds(detector.detect([older])) == [ChangeKind.OUT_OF_ORDER]


def test_same_update_time_with_different_content_is_a_conflict(detector, caplog):
    detector.detect([make_event(magnitude=4.6)])
    with caplog.at_level(logging.WARNING, logger="ingestion.change_detector"):
        changes = detector.detect([make_event(magnitude=4.7)])
    assert kinds(changes) == [ChangeKind.CONFLICT]
    assert "conflict" in caplog.text and "example-quake-001" in caplog.text


def test_same_event_in_both_feeds_is_counted_once(detector):
    from_hour_feed = detector.detect([make_event()])
    from_day_feed = detector.detect([make_event(ingested_at=INGESTED + timedelta(minutes=9))])
    assert len(from_hour_feed) == 1
    assert from_day_feed == []
    assert detector.pending_count() == 1


def test_duplicates_inside_one_batch_are_collapsed(detector):
    changes = detector.detect([make_event(), make_event(), make_event()])
    assert kinds(changes) == [ChangeKind.NEW]


def test_different_events_are_independent(detector):
    changes = detector.detect([make_event(event_id="a"), make_event(event_id="b")])
    assert kinds(changes) == [ChangeKind.NEW, ChangeKind.NEW]


def test_event_without_hash_is_refused(detector):
    unhashed = make_event().model_copy(update={"record_hash": None})
    with pytest.raises(ValueError, match="record_hash"):
        detector.detect([unhashed])


def test_failed_batch_records_nothing(detector):
    unhashed = make_event(event_id="b").model_copy(update={"record_hash": None})
    with pytest.raises(ValueError):
        detector.detect([make_event(event_id="a"), unhashed])
    assert detector.pending_count() == 0
    assert kinds(detector.detect([make_event(event_id="a")])) == [ChangeKind.NEW]


def test_mark_published_clears_pending(detector):
    event = make_event()
    detector.detect([event])
    assert detector.mark_published(event.event_id, event.record_hash) is True
    assert detector.pending() == []
    assert detector.pending_count() == 0


def test_published_revision_is_not_detected_again(detector):
    event = make_event()
    detector.detect([event])
    detector.mark_published(event.event_id, event.record_hash)
    assert detector.detect([make_event()]) == []


def test_revision_after_publish_is_detected(detector):
    first = make_event(magnitude=4.6)
    detector.detect([first])
    detector.mark_published(first.event_id, first.record_hash)
    revised = make_event(magnitude=4.8, updated_ms=BASE_UPDATED + 1000)
    assert kinds(detector.detect([revised])) == [ChangeKind.REVISED]
    assert [c.event for c in detector.pending()] == [revised]


def test_mark_published_twice_or_for_unknown_revision_returns_false(detector):
    event = make_event()
    detector.detect([event])
    assert detector.mark_published(event.event_id, event.record_hash) is True
    assert detector.mark_published(event.event_id, event.record_hash) is False
    assert detector.mark_published("nope", "0" * 64) is False


def test_pending_keeps_detection_order_and_kinds(detector):
    a = make_event(event_id="a")
    b = make_event(event_id="b")
    a2 = make_event(event_id="a", magnitude=5.0, updated_ms=BASE_UPDATED + 5)
    detector.detect([a, b])
    detector.detect([a2])
    assert [(c.kind, c.event.event_id) for c in detector.pending()] == [
        (ChangeKind.NEW, "a"),
        (ChangeKind.NEW, "b"),
        (ChangeKind.REVISED, "a"),
    ]


def test_state_survives_restart(tmp_path):
    path = str(tmp_path / "state.db")
    event = make_event()
    with ChangeDetector(path) as first_run:
        first_run.detect([event])
        first_run.mark_published(event.event_id, event.record_hash)
    with ChangeDetector(path) as second_run:
        assert second_run.detect([make_event()]) == []
        assert second_run.pending() == []


def test_crash_before_kafka_ack_leaves_the_change_pending(tmp_path):
    path = str(tmp_path / "state.db")
    event = make_event()
    with ChangeDetector(path) as before_crash:
        before_crash.detect([event])
        # process dies here: Kafka never acknowledged the message
    with ChangeDetector(path) as after_restart:
        assert after_restart.pending() == [Change(ChangeKind.NEW, event)]
        # seeing it again does not queue a duplicate, but it is still owed to Kafka
        assert after_restart.detect([make_event()]) == []
        assert after_restart.pending_count() == 1


def test_pending_event_round_trips_exactly(tmp_path):
    path = str(tmp_path / "state.db")
    event = make_event(place="Año Nuevo, Mexico")
    with ChangeDetector(path) as d:
        d.detect([event])
    with ChangeDetector(path) as d:
        restored = d.pending()[0].event
    assert restored == event
    assert restored.ingested_at == event.ingested_at


def test_refuses_database_from_a_newer_schema(tmp_path):
    import sqlite3

    path = str(tmp_path / "state.db")
    connection = sqlite3.connect(path)
    connection.execute("PRAGMA user_version=99")
    connection.close()
    with pytest.raises(RuntimeError, match="schema version"):
        ChangeDetector(path)
