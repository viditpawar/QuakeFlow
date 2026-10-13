"""Deterministic content fingerprint for an earthquake observation.

The hash identifies *what the source said*, so it covers the source fields and excludes
anything QuakeFlow adds locally (ingested_at). Two observations with the same hash are
the same revision, however many times or from whichever feed we saw them. It is also
the deterministic tie-breaker when two revisions share a source_updated_at.

Changing HASHED_FIELDS or the canonical encoding changes every hash, so bump
HASH_VERSION if that is ever done on purpose.
"""

import hashlib
import json
from datetime import datetime, timedelta

from schemas.earthquake import EarthquakeEvent

HASH_VERSION = 1

HASHED_FIELDS = (
    "event_id",
    "event_time",
    "source_updated_at",
    "magnitude",
    "latitude",
    "longitude",
    "depth_km",
    "place",
    "review_status",
    "event_type",
)
# Local metadata, deliberately not part of the fingerprint. Every EarthquakeEvent field
# must be listed in exactly one of the two tuples (enforced by a test).
EXCLUDED_FIELDS = ("ingested_at", "record_hash")

_ONE_MS = timedelta(milliseconds=1)


def _canonical_value(value: object) -> object:
    if isinstance(value, datetime):
        # Integer epoch milliseconds: exact, and the same unit the source uses.
        return (value - datetime(1970, 1, 1, tzinfo=value.tzinfo)) // _ONE_MS
    if isinstance(value, float) and value == 0.0:
        return 0.0  # -0.0 and 0.0 are the same measurement
    return value


def canonical_json(event: EarthquakeEvent) -> str:
    payload = {name: _canonical_value(getattr(event, name)) for name in HASHED_FIELDS}
    payload["hash_version"] = HASH_VERSION
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def compute_record_hash(event: EarthquakeEvent) -> str:
    """SHA-256 hex digest of the canonical source content of the event."""
    return hashlib.sha256(canonical_json(event).encode("utf-8")).hexdigest()
