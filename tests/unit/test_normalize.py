import copy
import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from processing.normalize import RecordRejected, normalize_feature, normalize_features

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "usgs_all_hour_sample.geojson"
INGESTED = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)


def fixture_features() -> list[dict]:
    return json.loads(FIXTURE.read_text())["features"]


def feature(**overrides) -> dict:
    """A valid feature; overrides use dotted keys, e.g. {'properties.mag': 3.1}."""
    base = copy.deepcopy(fixture_features()[0])
    for key, value in overrides.items():
        target = base
        *parents, leaf = key.split(".")
        for part in parents:
            target = target[part]
        target[leaf] = value
    return base


def test_normalizes_fixture_event():
    event = normalize_feature(fixture_features()[0], INGESTED)
    assert event.event_id == "example-quake-001"
    assert event.magnitude == 4.6
    assert event.longitude == -120.8
    assert event.latitude == 36.5
    assert event.depth_km == 12.4
    assert event.place == "Example seismic region A"
    assert event.review_status == "automatic"
    assert event.event_type == "earthquake"
    assert event.ingested_at == INGESTED


def test_epoch_milliseconds_become_utc():
    event = normalize_feature(feature(**{"properties.time": 0}), INGESTED)
    assert event.event_time == datetime(1970, 1, 1, tzinfo=UTC)
    assert event.event_time.utcoffset() == timedelta(0)
    assert event.source_updated_at.tzinfo is not None


def test_null_magnitude_is_allowed():
    event = normalize_feature(fixture_features()[1], INGESTED)
    assert event.magnitude is None


@pytest.mark.parametrize("depth", [-1.5, 0.0, 700.0])
def test_negative_and_zero_depths_are_valid(depth):
    event = normalize_feature(feature(**{"geometry.coordinates": [10.0, 20.0, depth]}), INGESTED)
    assert event.depth_km == depth


def test_missing_depth_is_allowed():
    event = normalize_feature(feature(**{"geometry.coordinates": [10.0, 20.0]}), INGESTED)
    assert event.depth_km is None


def test_integer_coordinates_are_accepted():
    event = normalize_feature(feature(**{"geometry.coordinates": [10, 20, 5]}), INGESTED)
    assert (event.longitude, event.latitude, event.depth_km) == (10.0, 20.0, 5.0)


def test_non_utc_ingested_at_is_converted_to_utc():
    local = datetime(2026, 10, 13, 5, 0, tzinfo=timezone(timedelta(hours=-7)))
    event = normalize_feature(fixture_features()[0], local)
    assert event.ingested_at == datetime(2026, 10, 13, 12, 0, tzinfo=UTC)
    assert event.ingested_at.utcoffset() == timedelta(0)


def test_naive_ingested_at_is_rejected():
    with pytest.raises(RecordRejected):
        normalize_feature(fixture_features()[0], datetime(2026, 10, 13, 12, 0))


@pytest.mark.parametrize(
    "coordinates",
    [[-180.0, 0.0, 1.0], [180.0, 0.0, 1.0], [0.0, -90.0, 1.0], [0.0, 90.0, 1.0]],
)
def test_boundary_coordinates_are_valid(coordinates):
    assert normalize_feature(feature(**{"geometry.coordinates": coordinates}), INGESTED)


@pytest.mark.parametrize(
    ("coordinates", "field"),
    [
        ([181.0, 0.0, 1.0], "longitude"),
        ([-180.1, 0.0, 1.0], "longitude"),
        ([0.0, 90.1, 1.0], "latitude"),
        ([0.0, -91.0, 1.0], "latitude"),
    ],
)
def test_out_of_range_coordinates_are_rejected(coordinates, field):
    with pytest.raises(RecordRejected) as info:
        normalize_feature(feature(**{"geometry.coordinates": coordinates}), INGESTED)
    assert any(reason.startswith(field) for reason in info.value.reasons)


@pytest.mark.parametrize(
    "overrides",
    [
        {"id": None},
        {"id": ""},
        {"id": 123},
        {"properties.time": None},
        {"properties.time": "yesterday"},
        {"properties.time": True},
        {"properties.updated": None},
        {"properties.updated": 1.5},
        {"properties.mag": "big"},
        {"properties.mag": float("nan")},
        {"geometry.coordinates": [None, 20.0, 1.0]},
        {"geometry.coordinates": ["10", 20.0, 1.0]},
        {"geometry.coordinates": [10.0]},
        {"geometry.coordinates": "nowhere"},
        {"geometry": None},
        {"properties": None},
    ],
)
def test_malformed_records_are_rejected(overrides):
    with pytest.raises(RecordRejected) as info:
        normalize_feature(feature(**overrides), INGESTED)
    assert info.value.reasons


@pytest.mark.parametrize("value", [None, "text", 42, []])
def test_non_object_feature_is_rejected(value):
    with pytest.raises(RecordRejected):
        normalize_feature(value, INGESTED)


def test_out_of_range_timestamp_is_rejected_not_raised_as_overflow():
    with pytest.raises(RecordRejected):
        normalize_feature(feature(**{"properties.time": 10**30}), INGESTED)


def test_all_problems_are_reported_together():
    bad = feature(**{"properties.time": None, "geometry.coordinates": [200.0, 100.0, 1.0]})
    with pytest.raises(RecordRejected) as info:
        normalize_feature(bad, INGESTED)
    assert len(info.value.reasons) == 3


def test_normalize_features_separates_valid_and_rejected():
    bad = feature(id="bad-1", **{"geometry.coordinates": [999.0, 0.0, 1.0]})
    result = normalize_features([*fixture_features(), bad, "junk"], INGESTED)
    assert [e.event_id for e in result.valid] == ["example-quake-001", "example-quake-002"]
    assert [r.event_id for r in result.rejected] == ["bad-1", None]
    assert result.rejected[0].raw == bad
    assert result.rejected[1].raw == "junk"


def test_normalize_features_accepts_empty_input():
    result = normalize_features([], INGESTED)
    assert result.valid == [] and result.rejected == []
