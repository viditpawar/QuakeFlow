"""Turn raw USGS GeoJSON features into EarthquakeEvent objects.

Invalid features are never dropped silently: they come back as RejectedRecord with the
reasons and the original feature, so they can be sent to the dead-letter topic.
"""

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime

from pydantic import ValidationError

from processing.record_hash import compute_record_hash
from schemas.earthquake import EarthquakeEvent


class RecordRejected(Exception):
    def __init__(self, reasons: list[str]):
        super().__init__("; ".join(reasons))
        self.reasons = tuple(reasons)


@dataclass(frozen=True)
class RejectedRecord:
    event_id: str | None
    reasons: tuple[str, ...]
    raw: object


@dataclass(frozen=True)
class NormalizationResult:
    valid: list[EarthquakeEvent]
    rejected: list[RejectedRecord]


def normalize_feature(feature: object, ingested_at: datetime) -> EarthquakeEvent:
    """Normalize one GeoJSON feature, or raise RecordRejected with every reason found."""
    if not isinstance(feature, dict):
        raise RecordRejected(["feature: not a JSON object"])
    properties = feature.get("properties")
    geometry = feature.get("geometry")
    if not isinstance(properties, dict):
        raise RecordRejected(["properties: missing or not an object"])
    coordinates = geometry.get("coordinates") if isinstance(geometry, dict) else None
    if not isinstance(coordinates, list) or len(coordinates) < 2:
        raise RecordRejected(["geometry.coordinates: need at least [longitude, latitude]"])

    candidate = {
        "event_id": feature.get("id"),
        "event_time": properties.get("time"),
        "source_updated_at": properties.get("updated"),
        "magnitude": properties.get("mag"),
        "longitude": coordinates[0],
        "latitude": coordinates[1],
        "depth_km": coordinates[2] if len(coordinates) > 2 else None,
        "place": properties.get("place"),
        "review_status": properties.get("status"),
        "event_type": properties.get("type"),
        "ingested_at": ingested_at,
    }
    try:
        event = EarthquakeEvent.model_validate(candidate)
    except ValidationError as exc:
        reasons = [f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors()]
        raise RecordRejected(reasons) from None
    return event.model_copy(update={"record_hash": compute_record_hash(event)})


def normalize_features(features: Iterable[object], ingested_at: datetime) -> NormalizationResult:
    valid: list[EarthquakeEvent] = []
    rejected: list[RejectedRecord] = []
    for feature in features:
        try:
            valid.append(normalize_feature(feature, ingested_at))
        except RecordRejected as exc:
            event_id = feature.get("id") if isinstance(feature, dict) else None
            rejected.append(
                RejectedRecord(
                    event_id=event_id if isinstance(event_id, str) else None,
                    reasons=exc.reasons,
                    raw=feature,
                )
            )
    return NormalizationResult(valid=valid, rejected=rejected)
