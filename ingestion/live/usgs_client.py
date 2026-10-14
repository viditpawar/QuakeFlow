"""Client for the live USGS earthquake GeoJSON feed.

The feed is a snapshot refreshed about once a minute (Cache-Control max-age=60),
not a message stream, so callers should poll no faster than that.
"""

import os
from dataclasses import dataclass
from datetime import UTC, datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_FEED_URL = (
    "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
)
USER_AGENT = "quakeflow-data-platform (learning project)"


class FeedError(Exception):
    """The feed could not be fetched or is not a valid GeoJSON FeatureCollection."""


@dataclass(frozen=True)
class FeedSnapshot:
    features: list[dict]
    generated_ms: int | None  # metadata.generated: when USGS built this snapshot
    fetched_at: datetime  # when we received it


def build_session(retries: int = 3, backoff_factor: float = 1.0) -> requests.Session:
    """Session that retries transient failures with exponential backoff."""
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = USER_AGENT
    return session


def parse_snapshot(document: object, fetched_at: datetime) -> FeedSnapshot:
    if not isinstance(document, dict) or document.get("type") != "FeatureCollection":
        raise FeedError("response is not a GeoJSON FeatureCollection")
    features = document.get("features")
    if not isinstance(features, list):
        raise FeedError("FeatureCollection has no 'features' list")
    metadata = document.get("metadata")
    generated = metadata.get("generated") if isinstance(metadata, dict) else None
    return FeedSnapshot(features=features, generated_ms=generated, fetched_at=fetched_at)


class UsgsLiveClient:
    def __init__(
        self,
        url: str | None = None,
        timeout: float = 15.0,
        session: requests.Session | None = None,
    ):
        self.url = url or os.getenv("USGS_LIVE_FEED_URL", DEFAULT_FEED_URL)
        self.timeout = timeout
        self.session = session or build_session()

    def fetch(self) -> FeedSnapshot:
        try:
            response = self.session.get(self.url, timeout=self.timeout)
            response.raise_for_status()
            document = response.json()
        except (requests.RequestException, ValueError) as exc:
            raise FeedError(f"could not fetch {self.url}: {exc}") from exc
        return parse_snapshot(document, datetime.now(UTC))


if __name__ == "__main__":
    import sys

    sys.stdout.reconfigure(encoding="utf-8")  # place names contain non-ASCII letters
    snapshot = UsgsLiveClient().fetch()
    print(f"{len(snapshot.features)} events in snapshot generated at {snapshot.generated_ms}")
    for feature in snapshot.features[:5]:
        props = feature.get("properties", {})
        print(feature.get("id"), props.get("mag"), props.get("place"))
