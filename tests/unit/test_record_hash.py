import json
import re
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from processing.normalize import normalize_feature
from processing.record_hash import (
    EXCLUDED_FIELDS,
    HASHED_FIELDS,
    canonical_json,
    compute_record_hash,
)
from schemas.earthquake import EarthquakeEvent

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "usgs_all_hour_sample.geojson"
INGESTED = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)


def base_event(**overrides) -> EarthquakeEvent:
    fields = {
        "event_id": "example-quake-001",
        "event_time": datetime(2026, 10, 11, 0, 55, tzinfo=UTC),
        "source_updated_at": datetime(2026, 10, 11, 0, 58, tzinfo=UTC),
        "magnitude": 4.6,
        "latitude": 36.5,
        "longitude": -120.8,
        "depth_km": 12.4,
        "place": "Example seismic region A",
        "review_status": "automatic",
        "event_type": "earthquake",
        "ingested_at": INGESTED,
    }
    fields.update(overrides)
    return EarthquakeEvent(**fields)


def test_every_model_field_is_either_hashed_or_explicitly_excluded():
    # Adding a field to EarthquakeEvent forces a decision about the fingerprint.
    model_fields = set(EarthquakeEvent.model_fields)
    assert set(HASHED_FIELDS) | set(EXCLUDED_FIELDS) == model_fields
    assert not set(HASHED_FIELDS) & set(EXCLUDED_FIELDS)


def test_hash_is_lowercase_sha256_hex():
    assert re.fullmatch(r"[0-9a-f]{64}", compute_record_hash(base_event()))


def test_same_content_gives_same_hash():
    assert compute_record_hash(base_event()) == compute_record_hash(base_event())


def test_known_answer_pins_the_canonical_encoding():
    event = base_event()
    assert canonical_json(event) == (
        '{"depth_km":12.4,"event_id":"example-quake-001","event_time":1791680100000,'
        '"event_type":"earthquake","hash_version":1,"latitude":36.5,"longitude":-120.8,'
        '"magnitude":4.6,"place":"Example seismic region A","review_status":"automatic",'
        '"source_updated_at":1791680280000}'
    )
    # Checked independently against a .NET SHA-256 of the string above.
    assert compute_record_hash(event) == (
        "1e0e0c5dd93fd1a7596434cebfab8085277e0d9df3d5d3bed8bb883ba5a17399"
    )


def test_ingested_at_does_not_affect_hash():
    later = base_event(ingested_at=INGESTED + timedelta(days=3))
    assert compute_record_hash(later) == compute_record_hash(base_event())


def test_record_hash_field_does_not_affect_hash():
    stamped = base_event(record_hash="a" * 64)
    assert compute_record_hash(stamped) == compute_record_hash(base_event())


@pytest.mark.parametrize(
    "change",
    [
        {"event_id": "example-quake-999"},
        {"event_time": datetime(2026, 10, 11, 0, 56, tzinfo=UTC)},
        {"source_updated_at": datetime(2026, 10, 11, 0, 59, tzinfo=UTC)},
        {"magnitude": 4.8},
        {"magnitude": None},
        {"latitude": 36.6},
        {"longitude": -120.9},
        {"depth_km": 12.5},
        {"depth_km": None},
        {"place": "Somewhere else"},
        {"place": None},
        {"review_status": "reviewed"},
        {"event_type": "quarry blast"},
    ],
)
def test_changing_any_source_field_changes_hash(change):
    assert compute_record_hash(base_event(**change)) != compute_record_hash(base_event())


def test_revision_with_new_magnitude_is_a_different_version():
    first = base_event(magnitude=4.6)
    revised = base_event(magnitude=4.8, source_updated_at=datetime(2026, 10, 11, 1, 30, tzinfo=UTC))
    assert compute_record_hash(first) != compute_record_hash(revised)


def test_same_instant_in_different_timezone_gives_same_hash():
    pacific = timezone(timedelta(hours=-7))
    shifted = base_event(event_time=datetime(2026, 10, 10, 17, 55, tzinfo=pacific))
    assert compute_record_hash(shifted) == compute_record_hash(base_event())


def test_negative_zero_equals_zero():
    assert compute_record_hash(base_event(depth_km=-0.0)) == compute_record_hash(
        base_event(depth_km=0.0)
    )


def test_integer_and_float_values_hash_the_same():
    assert compute_record_hash(base_event(depth_km=10)) == compute_record_hash(
        base_event(depth_km=10.0)
    )


def test_non_ascii_place_is_stable():
    event = base_event(place="Año Nuevo, Mexico")
    assert compute_record_hash(event) == compute_record_hash(base_event(place="Año Nuevo, Mexico"))
    assert "Año" in canonical_json(event)


def test_normalize_sets_the_hash_and_ignores_ingestion_time():
    raw = json.loads(FIXTURE.read_text())["features"][0]
    first = normalize_feature(raw, INGESTED)
    later = normalize_feature(raw, INGESTED + timedelta(hours=5))
    assert first.record_hash == later.record_hash == compute_record_hash(first)


def test_identical_feed_records_from_two_feeds_share_a_hash():
    # The same event seen in all_hour and all_day carries the same source content.
    raw = json.loads(FIXTURE.read_text())["features"][0]
    from_hour_feed = normalize_feature(raw, INGESTED)
    from_day_feed = normalize_feature(json.loads(json.dumps(raw)), INGESTED + timedelta(minutes=9))
    assert from_hour_feed.record_hash == from_day_feed.record_hash


def test_key_order_in_source_feature_does_not_matter():
    raw = json.loads(FIXTURE.read_text())["features"][0]
    reordered = {key: raw[key] for key in reversed(list(raw))}
    reordered["properties"] = {k: raw["properties"][k] for k in reversed(list(raw["properties"]))}
    assert (
        normalize_feature(reordered, INGESTED).record_hash
        == normalize_feature(raw, INGESTED).record_hash
    )


def test_invalid_hash_value_is_rejected_by_the_model():
    with pytest.raises(ValueError, match="record_hash"):
        base_event(record_hash="not-a-hash")
