"""A fake USGS catalog shared by the backfill tests: same semantics as the FDSN service."""

import copy
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingestion.historical_client import CatalogWindow
from ingestion.live.usgs_client import FeedError

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "usgs_all_hour_sample.geojson"
BASE = datetime(2025, 1, 1, tzinfo=UTC)


def feature(index: int, when: datetime, magnitude: float | None = 3.0) -> dict:
    f = copy.deepcopy(json.loads(FIXTURE.read_text())["features"][0])
    f["id"] = f"hist-{index:04d}"
    f["properties"].update(
        mag=magnitude,
        time=int(when.timestamp() * 1000),
        updated=int(when.timestamp() * 1000) + 60_000,
    )
    return f


class FakeCatalog:
    """Behaves like the FDSN service: start inclusive, end exclusive, magnitude filter."""

    def __init__(self, features: list[dict]):
        self.features = features
        self.count_calls = 0
        self.fetch_calls: list[tuple[datetime, datetime]] = []
        self.fail_fetch_for: set[datetime] = set()

    def _matching(self, start, end, min_magnitude):
        lo, hi = int(start.timestamp() * 1000), int(end.timestamp() * 1000)
        return [
            f for f in self.features
            if lo <= f["properties"]["time"] < hi
            and (min_magnitude is None or (f["properties"]["mag"] or 0) >= min_magnitude)
        ]

    def count(self, start, end, min_magnitude=None):
        self.count_calls += 1
        return len(self._matching(start, end, min_magnitude))

    def fetch(self, start, end, min_magnitude=None):
        self.fetch_calls.append((start, end))
        if start in self.fail_fetch_for:
            raise FeedError("simulated outage")
        matching = self._matching(start, end, min_magnitude)
        document = {"type": "FeatureCollection", "metadata": {"count": len(matching)}, "features": matching}
        return CatalogWindow(body=json.dumps(document).encode(), count=len(matching))


def week_of_events(n: int = 7) -> list[dict]:
    return [feature(i, BASE + timedelta(days=i, hours=3)) for i in range(n)]
