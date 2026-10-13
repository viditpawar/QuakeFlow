import json
from datetime import UTC, datetime, timedelta, timezone

from schemas.earthquake import EarthquakeEvent, utc_to_epoch_ms

EVENT = EarthquakeEvent(
    event_id="example-quake-001",
    event_time=datetime(2026, 10, 11, 0, 55, tzinfo=UTC),
    source_updated_at=datetime(2026, 10, 11, 0, 58, tzinfo=UTC),
    magnitude=4.6,
    latitude=36.5,
    longitude=-120.8,
    depth_km=-1.5,
    place="Año Nuevo",
    review_status="automatic",
    event_type="earthquake",
    ingested_at=datetime(2026, 10, 13, 12, 0, 0, 123456, tzinfo=UTC),
    record_hash="a" * 64,
)


def test_json_round_trip_is_exact():
    assert EarthquakeEvent.from_json(EVENT.to_json()) == EVENT


def test_json_uses_epoch_milliseconds_for_source_times():
    data = json.loads(EVENT.to_json())
    assert data["event_time"] == 1791680100000
    assert data["source_updated_at"] == 1791680280000


def test_json_is_deterministic_and_keeps_non_ascii():
    assert EVENT.to_json() == EVENT.to_json()
    assert "Año Nuevo" in EVENT.to_json()


def test_round_trip_keeps_null_fields():
    sparse = EVENT.model_copy(update={"magnitude": None, "depth_km": None, "place": None})
    assert EarthquakeEvent.from_json(sparse.to_json()) == sparse


def test_epoch_ms_conversion_handles_other_timezones():
    local = datetime(2026, 10, 10, 17, 55, tzinfo=timezone(timedelta(hours=-7)))
    assert utc_to_epoch_ms(local) == 1791680100000
