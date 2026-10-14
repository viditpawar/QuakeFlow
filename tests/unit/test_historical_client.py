import json
from datetime import UTC, datetime

import pytest
import requests

from ingestion.historical_client import FdsnClient
from ingestion.live.usgs_client import FeedError

START = datetime(2025, 1, 1, tzinfo=UTC)
END = datetime(2025, 1, 8, 6, 30, tzinfo=UTC)


class FakeResponse:
    def __init__(self, text="", status=200):
        self.text = text
        self.content = text.encode()
        self.status_code = status


class FakeSession:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append((url, params, timeout))
        response = self.responses[min(len(self.calls), len(self.responses)) - 1]
        if isinstance(response, Exception):
            raise response
        return response


class FakeTime:
    def __init__(self):
        self.now = 100.0
        self.slept: list[float] = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds


def client(session, pause=1.0, fake_time=None):
    fake_time = fake_time or FakeTime()
    return FdsnClient(
        base_url="https://example.test/fdsnws/event/1/",
        session=session,
        pause_seconds=pause,
        clock=fake_time.clock,
        sleep=fake_time.sleep,
    )


def test_count_sends_utc_times_and_the_magnitude_filter():
    session = FakeSession(FakeResponse("591\n"))
    assert client(session).count(START, END, 2.5) == 591
    url, params, _ = session.calls[0]
    assert url == "https://example.test/fdsnws/event/1/count"
    assert params == {
        "starttime": "2025-01-01T00:00:00",
        "endtime": "2025-01-08T06:30:00",
        "minmagnitude": 2.5,
    }


def test_magnitude_filter_is_left_out_when_not_given():
    session = FakeSession(FakeResponse("5"))
    client(session).count(START, END)
    assert "minmagnitude" not in session.calls[0][1]


@pytest.mark.parametrize("text", ["", "lots", "12.5"])
def test_a_count_that_is_not_an_integer_is_an_error(text):
    with pytest.raises(FeedError, match="not a number"):
        client(FakeSession(FakeResponse(text))).count(START, END)


def test_fetch_returns_the_exact_bytes_and_the_feature_count():
    document = {"type": "FeatureCollection", "features": [{"id": "a"}, {"id": "b"}]}
    session = FakeSession(FakeResponse(json.dumps(document)))
    window = client(session).fetch(START, END, 3.0)
    assert window.count == 2 and json.loads(window.body) == document
    params = session.calls[0][1]
    assert params["format"] == "geojson" and params["orderby"] == "time-asc"


@pytest.mark.parametrize("body", ["not json", "[]", '{"features": "nope"}', '{"type": "FeatureCollection"}'])
def test_a_response_that_is_not_a_feature_collection_is_an_error(body):
    with pytest.raises(FeedError):
        client(FakeSession(FakeResponse(body))).fetch(START, END)


def test_http_errors_become_feed_errors_with_the_service_message():
    over_limit = FakeResponse("155899 matching events exceeds search limit of 20000", status=400)
    with pytest.raises(FeedError, match="exceeds search limit"):
        client(FakeSession(over_limit)).fetch(START, END)


def test_network_errors_become_feed_errors():
    with pytest.raises(FeedError, match="down"):
        client(FakeSession(requests.ConnectionError("down"))).count(START, END)


def test_requests_are_paced():
    fake_time = FakeTime()
    c = client(FakeSession(FakeResponse("1")), pause=2.0, fake_time=fake_time)
    c.count(START, END)
    assert fake_time.slept == []  # nothing to wait for before the first request
    c.count(START, END)
    assert fake_time.slept == [2.0]
    fake_time.now += 5  # a long gap: no extra waiting needed
    c.count(START, END)
    assert fake_time.slept == [2.0]


def test_pacing_also_applies_after_a_failed_request():
    fake_time = FakeTime()
    c = client(FakeSession(requests.ConnectionError("down"), FakeResponse("1")), fake_time=fake_time)
    with pytest.raises(FeedError):
        c.count(START, END)
    c.count(START, END)
    assert fake_time.slept == [1.0]
