"""Client for the USGS FDSN event catalog API, used for historical backfills.

The service refuses any query that matches more than 20,000 events (HTTP 400), so callers ask
for the count first and split the time range when it is too large. Requests are paced, and
transient failures (timeouts, 429, 5xx) are retried with exponential backoff by the session.
"""

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

import requests

from ingestion.live.usgs_client import FeedError, build_session

BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1"
MAX_EVENTS_PER_QUERY = 20_000


@dataclass(frozen=True)
class CatalogWindow:
    """Raw GeoJSON text of one time window, exactly as the service returned it."""

    body: bytes
    count: int  # number of features, from the parsed document


def _timestamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%S")


class FdsnClient:
    def __init__(
        self,
        base_url: str = BASE_URL,
        session: requests.Session | None = None,
        pause_seconds: float = 1.0,
        timeout: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = session or build_session(retries=5, backoff_factor=2.0)
        self.pause_seconds = pause_seconds
        self.timeout = timeout
        self._clock = clock
        self._sleep = sleep
        self._last_request: float | None = None

    def _params(self, start: datetime, end: datetime, min_magnitude: float | None) -> dict:
        params = {"starttime": _timestamp(start), "endtime": _timestamp(end)}
        if min_magnitude is not None:
            params["minmagnitude"] = min_magnitude
        return params

    def _get(self, endpoint: str, params: dict) -> requests.Response:
        # Be polite: never send two requests closer together than the pause.
        if self._last_request is not None:
            wait = self.pause_seconds - (self._clock() - self._last_request)
            if wait > 0:
                self._sleep(wait)
        try:
            response = self.session.get(
                f"{self.base_url}/{endpoint}", params=params, timeout=self.timeout
            )
        except requests.RequestException as exc:
            raise FeedError(f"{endpoint} request failed: {exc}") from exc
        finally:
            self._last_request = self._clock()
        if response.status_code >= 400:
            raise FeedError(f"{endpoint} returned HTTP {response.status_code}: {response.text[:200]}")
        return response

    def count(self, start: datetime, end: datetime, min_magnitude: float | None = None) -> int:
        """How many events match. The window is start inclusive, end exclusive, in UTC."""
        text = self._get("count", self._params(start, end, min_magnitude)).text.strip()
        try:
            return int(text)
        except ValueError:
            raise FeedError(f"count returned something that is not a number: {text[:100]!r}") from None

    def fetch(
        self, start: datetime, end: datetime, min_magnitude: float | None = None
    ) -> CatalogWindow:
        """Download one window as GeoJSON. Call count() first and split if it is too large."""
        params = {
            **self._params(start, end, min_magnitude),
            "format": "geojson",
            "orderby": "time-asc",
        }
        body = self._get("query", params).content
        try:
            document = json.loads(body)
        except ValueError as exc:
            raise FeedError(f"query returned invalid JSON: {exc}") from exc
        features = document.get("features") if isinstance(document, dict) else None
        if not isinstance(features, list):
            raise FeedError("query response is not a GeoJSON FeatureCollection")
        return CatalogWindow(body=body, count=len(features))
