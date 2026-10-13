import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
import requests

from ingestion.live.usgs_client import FeedError, UsgsLiveClient, parse_snapshot

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "usgs_all_hour_sample.geojson"
NOW = datetime(2026, 10, 13, tzinfo=UTC)


class FakeResponse:
    def __init__(self, payload=None, status=200):
        self._payload = payload
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f"{self.status} error")

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error

    def get(self, url, timeout):
        if self.error:
            raise self.error
        return self.response


def load_fixture() -> dict:
    return json.loads(FIXTURE.read_text())


def test_parse_snapshot_reads_features_and_generated_time():
    snapshot = parse_snapshot(load_fixture(), NOW)
    assert [f["id"] for f in snapshot.features] == ["example-quake-001", "example-quake-002"]
    assert snapshot.generated_ms == 1791878400000
    assert snapshot.fetched_at == NOW


def test_parse_snapshot_accepts_empty_feed():
    document = {"type": "FeatureCollection", "metadata": {}, "features": []}
    snapshot = parse_snapshot(document, NOW)
    assert snapshot.features == []
    assert snapshot.generated_ms is None


@pytest.mark.parametrize(
    "document",
    [None, [], {"type": "Feature"}, {"type": "FeatureCollection"}, {"type": "FeatureCollection", "features": {}}],
)
def test_parse_snapshot_rejects_invalid_documents(document):
    with pytest.raises(FeedError):
        parse_snapshot(document, NOW)


def test_fetch_returns_snapshot():
    client = UsgsLiveClient(url="https://example.test/feed", session=FakeSession(FakeResponse(load_fixture())))
    assert len(client.fetch().features) == 2


def test_fetch_wraps_http_errors():
    client = UsgsLiveClient(url="https://example.test/feed", session=FakeSession(FakeResponse(status=503)))
    with pytest.raises(FeedError):
        client.fetch()


def test_fetch_wraps_network_errors():
    client = UsgsLiveClient(url="https://example.test/feed", session=FakeSession(error=requests.ConnectionError("down")))
    with pytest.raises(FeedError):
        client.fetch()


def test_fetch_wraps_invalid_json():
    client = UsgsLiveClient(url="https://example.test/feed", session=FakeSession(FakeResponse(ValueError("bad json"))))
    with pytest.raises(FeedError):
        client.fetch()
