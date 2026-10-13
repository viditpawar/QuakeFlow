"""Normalized earthquake observation.

One EarthquakeEvent is one observation of one USGS event at one source revision.
All timestamps are timezone-aware UTC. Magnitude and depth may be missing, and depth
may be negative: USGS reports depth relative to the reference surface, so shallow or
above-reference events are legitimate.
"""

from datetime import UTC, datetime, timedelta

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def epoch_ms_to_utc(value: object) -> datetime:
    """Convert USGS epoch milliseconds to an aware UTC datetime."""
    if isinstance(value, bool) or not isinstance(value, int):
        # ValueError, not TypeError: pydantic only converts ValueError into a validation error.
        raise ValueError("must be an integer number of epoch milliseconds")  # noqa: TRY004
    try:
        return _EPOCH + timedelta(milliseconds=value)
    except OverflowError:
        raise ValueError("epoch milliseconds out of range") from None


class EarthquakeEvent(BaseModel):
    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    event_id: str = Field(min_length=1, strict=True)
    event_time: AwareDatetime
    source_updated_at: AwareDatetime
    magnitude: float | None = Field(default=None, strict=True, allow_inf_nan=False)
    latitude: float = Field(ge=-90, le=90, strict=True, allow_inf_nan=False)
    longitude: float = Field(ge=-180, le=180, strict=True, allow_inf_nan=False)
    depth_km: float | None = Field(default=None, strict=True, allow_inf_nan=False)
    place: str | None = Field(default=None, strict=True)
    review_status: str | None = Field(default=None, strict=True)
    event_type: str | None = Field(default=None, strict=True)
    ingested_at: AwareDatetime
    # SHA-256 of the source content, set by processing.normalize (see processing.record_hash).
    record_hash: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    @field_validator("event_time", "source_updated_at", mode="before")
    @classmethod
    def _parse_epoch_ms(cls, value: object) -> object:
        # Accept USGS epoch milliseconds, and pass datetimes through for pydantic to check.
        if isinstance(value, datetime):
            return value
        return epoch_ms_to_utc(value)

    @field_validator("event_time", "source_updated_at", "ingested_at")
    @classmethod
    def _to_utc(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)
