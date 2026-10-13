"""Scenarios over the whole pre-Kafka path: raw feature -> normalize -> hash -> detector."""

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingestion.change_detector import ChangeDetector, ChangeKind
from processing.normalize import normalize_features

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "usgs_all_hour_sample.geojson"
T0 = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)
BASE_UPDATED = 1791878280000


def snapshot(**per_event) -> list[dict]:
    """The fixture's two events, with per-event property overrides keyed by event id."""
    features = copy.deepcopy(json.loads(FIXTURE.read_text())["features"])
    for feature in features:
        feature["properties"].update(per_event.get(feature["id"], {}))
    return features


def ingest(detector: ChangeDetector, features: list[dict], fetched_at: datetime = T0):
    """What the poller will do each cycle; returns (changes, rejected)."""
    result = normalize_features(features, fetched_at)
    return detector.detect(result.valid), result.rejected


def summary(changes):
    return [(c.event.event_id, c.kind, c.event.magnitude) for c in changes]


def test_first_poll_then_identical_poll():
    with ChangeDetector() as detector:
        first, _ = ingest(detector, snapshot())
        second, _ = ingest(detector, snapshot(), T0 + timedelta(minutes=1))
    assert [c.kind for c in first] == [ChangeKind.NEW, ChangeKind.NEW]
    assert second == []


def test_magnitude_revision_is_one_event_with_two_versions():
    quake = "example-quake-001"
    with ChangeDetector() as detector:
        ingest(detector, snapshot())
        revised, _ = ingest(
            detector,
            snapshot(**{quake: {"mag": 4.8, "updated": BASE_UPDATED + 600_000}}),
            T0 + timedelta(minutes=10),
        )
        history = [c for c in detector.pending() if c.event.event_id == quake]
    assert summary(revised) == [(quake, ChangeKind.REVISED, 4.8)]
    assert [c.event.magnitude for c in history] == [4.6, 4.8]


def test_late_old_revision_does_not_look_like_the_newest():
    quake = "example-quake-001"
    revised = snapshot(**{quake: {"mag": 4.8, "updated": BASE_UPDATED + 600_000}})
    with ChangeDetector() as detector:
        ingest(detector, revised)
        late, _ = ingest(detector, snapshot(), T0 + timedelta(minutes=15))
    assert summary(late) == [(quake, ChangeKind.OUT_OF_ORDER, 4.6)]


def test_same_events_from_hour_and_day_feeds_are_not_double_counted():
    day_feed = snapshot()
    with ChangeDetector() as detector:
        hour, _ = ingest(detector, snapshot(), T0)
        day, _ = ingest(detector, day_feed, T0 + timedelta(minutes=15))
        assert detector.pending_count() == 2
    assert len(hour) == 2
    assert day == []


def test_day_feed_catches_a_revision_the_hour_feed_missed():
    quake = "example-quake-002"
    with ChangeDetector() as detector:
        ingest(detector, snapshot())
        revised_day_feed = snapshot(**{quake: {"mag": 3.1, "updated": BASE_UPDATED + 900_000}})
        changes, _ = ingest(detector, revised_day_feed, T0 + timedelta(minutes=15))
    assert summary(changes) == [(quake, ChangeKind.REVISED, 3.1)]


def test_malformed_record_is_rejected_and_never_reaches_the_detector():
    features = snapshot()
    broken = copy.deepcopy(features[0])
    broken["id"] = "broken-1"
    broken["geometry"]["coordinates"] = [500.0, 36.5, 1.0]
    with ChangeDetector() as detector:
        changes, rejected = ingest(detector, [*features, broken])
        assert detector.pending_count() == 2
    assert len(changes) == 2
    assert [r.event_id for r in rejected] == ["broken-1"]
    assert rejected[0].reasons


def test_empty_feed_is_a_valid_poll():
    with ChangeDetector() as detector:
        changes, rejected = ingest(detector, [])
    assert changes == [] and rejected == []


def test_restart_between_polls_keeps_state(tmp_path):
    path = str(tmp_path / "state.db")
    with ChangeDetector(path) as detector:
        ingest(detector, snapshot())
    with ChangeDetector(path) as detector:
        again, _ = ingest(detector, snapshot(), T0 + timedelta(minutes=1))
        pending = detector.pending()
    assert again == []
    assert len(pending) == 2  # still owed to Kafka: nothing was acknowledged


def test_acknowledged_changes_stay_quiet_after_restart(tmp_path):
    path = str(tmp_path / "state.db")
    with ChangeDetector(path) as detector:
        changes, _ = ingest(detector, snapshot())
        for change in changes:
            detector.mark_published(change.event.event_id, change.event.record_hash)
    with ChangeDetector(path) as detector:
        again, _ = ingest(detector, snapshot(), T0 + timedelta(minutes=1))
        assert detector.pending() == []
    assert again == []
