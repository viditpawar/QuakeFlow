import copy
import json
import threading
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.change_detector import ChangeDetector, ChangeKind
from ingestion.live.poller import (
    BACKOFF_CAP_SECONDS,
    ConfigError,
    FeedConfig,
    Poller,
    backoff_delay,
    load_feed_configs,
    main,
)
from ingestion.live.usgs_client import FeedError, FeedSnapshot

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "usgs_all_hour_sample.geojson"
FETCHED = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)

HOUR = FeedConfig("all_hour", "https://example.test/hour", 60)
DAY = FeedConfig("all_day", "https://example.test/day", 900)


def fixture_features() -> list[dict]:
    return copy.deepcopy(json.loads(FIXTURE.read_text())["features"])


class FakeClient:
    """Serves queued responses: a feature list, or an exception to raise."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = 0

    def fetch(self) -> FeedSnapshot:
        self.calls += 1
        response = self.responses[min(self.calls, len(self.responses)) - 1]
        if isinstance(response, Exception):
            raise response
        return FeedSnapshot(features=response, generated_ms=None, fetched_at=FETCHED)


class FakeTime:
    """A clock that only moves when the poller sleeps."""

    def __init__(self, stop: threading.Event | None = None, stop_after: float | None = None):
        self.now = 0.0
        self.stop = stop
        self.stop_after = stop_after

    def clock(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        if self.stop is not None and self.stop_after is not None and self.now > self.stop_after:
            self.stop.set()


def write_config(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "sources.yaml"
    path.write_text(text, encoding="utf-8")
    return path


# --- configuration -------------------------------------------------------------------


def test_shipped_config_defines_both_feeds():
    feeds = {feed.name: feed for feed in load_feed_configs(ROOT / "configs" / "sources.yaml")}
    assert feeds["all_hour"].interval_seconds == 60
    assert feeds["all_day"].interval_seconds == 900
    assert feeds["all_hour"].url.endswith("all_hour.geojson")
    assert feeds["all_day"].url.endswith("all_day.geojson")


def test_timeout_defaults_when_omitted(tmp_path):
    path = write_config(tmp_path, "feeds:\n  - {name: a, url: 'https://x', interval_seconds: 60}\n")
    assert load_feed_configs(path)[0].timeout_seconds == 15.0


@pytest.mark.parametrize(
    "text",
    [
        "",
        "feeds: []",
        "feeds: nope",
        "- just a list",
        "feeds:\n  - nope",
        "feeds:\n  - {url: 'https://x', interval_seconds: 60}",
        "feeds:\n  - {name: a, interval_seconds: 60}",
        "feeds:\n  - {name: a, url: 'https://x'}",
        "feeds:\n  - {name: a, url: 'https://x', interval_seconds: 59}",
        "feeds:\n  - {name: a, url: 'https://x', interval_seconds: fast}",
        "feeds:\n  - {name: a, url: 'https://x', interval_seconds: true}",
        "feeds:\n  - {name: a, url: 'https://x', interval_seconds: 60, timeout_seconds: 0}",
        (
            "feeds:\n  - {name: a, url: 'https://x', interval_seconds: 60}\n"
            "  - {name: a, url: 'https://y', interval_seconds: 60}"
        ),
    ],
)
def test_invalid_configs_are_refused(tmp_path, text):
    with pytest.raises(ConfigError):
        load_feed_configs(write_config(tmp_path, text))


# --- backoff -------------------------------------------------------------------------


def test_backoff_doubles_and_is_capped():
    assert backoff_delay(HOUR, 0) == 60
    assert [backoff_delay(HOUR, n) for n in (1, 2, 3)] == [120, 240, 480]
    assert backoff_delay(HOUR, 20) == BACKOFF_CAP_SECONDS


def test_backoff_never_shortens_a_long_interval():
    slow = FeedConfig("slow", "https://example.test", BACKOFF_CAP_SECONDS * 2)
    assert backoff_delay(slow, 3) == slow.interval_seconds


# --- one poll ------------------------------------------------------------------------


def test_poll_records_changes_as_pending_without_publishing():
    with ChangeDetector() as detector:
        poller = Poller([HOUR], detector, lambda feed: FakeClient(fixture_features()))
        result = poller.poll_feed(HOUR)
        assert result.ok and result.fetched == 2
        assert [c.kind for c in result.changes] == [ChangeKind.NEW, ChangeKind.NEW]
        assert detector.pending_count() == 2


def test_polling_the_same_snapshot_twice_reports_no_new_changes():
    with ChangeDetector() as detector:
        poller = Poller([HOUR], detector, lambda feed: FakeClient(fixture_features()))
        poller.poll_feed(HOUR)
        assert poller.poll_feed(HOUR).changes == []


def test_failed_fetch_is_reported_not_raised():
    with ChangeDetector() as detector:
        poller = Poller([HOUR], detector, lambda feed: FakeClient(FeedError("USGS is down")))
        result = poller.poll_feed(HOUR)
        assert not result.ok and "USGS is down" in result.error
        assert detector.pending_count() == 0


def test_malformed_records_are_returned_as_rejected_and_the_rest_still_processed():
    features = fixture_features()
    broken = copy.deepcopy(features[0])
    broken["id"] = "broken"
    broken["geometry"]["coordinates"] = [999.0, 0.0, 1.0]
    with ChangeDetector() as detector:
        poller = Poller([HOUR], detector, lambda feed: FakeClient([*features, broken]))
        result = poller.poll_feed(HOUR)
        assert len(result.changes) == 2
        assert [r.event_id for r in result.rejected] == ["broken"]


def test_empty_feed_is_a_successful_poll():
    with ChangeDetector() as detector:
        result = Poller([HOUR], detector, lambda feed: FakeClient([])).poll_feed(HOUR)
    assert result.ok and result.fetched == 0 and result.changes == []


def test_same_event_from_both_feeds_is_detected_once():
    with ChangeDetector() as detector:
        poller = Poller([HOUR, DAY], detector, lambda feed: FakeClient(fixture_features()))
        results = poller.poll_all_once()
        assert [len(r.changes) for r in results] == [2, 0]
        assert detector.pending_count() == 2


# --- scheduling ----------------------------------------------------------------------


def test_each_feed_runs_on_its_own_interval():
    stop = threading.Event()
    fake = FakeTime(stop, stop_after=1000)
    clients = {HOUR.name: FakeClient([]), DAY.name: FakeClient([])}
    with ChangeDetector() as detector:
        poller = Poller([HOUR, DAY], detector, lambda f: clients[f.name], fake.clock, fake.sleep)
        poller.run(stop)
    assert clients["all_hour"].calls == 17  # t = 0, 60, ... 960
    assert clients["all_day"].calls == 2  # t = 0, 900


def test_failing_feed_backs_off_without_blocking_the_other():
    stop = threading.Event()
    fake = FakeTime(stop, stop_after=1000)
    times: list[float] = []

    class Recording(FakeClient):
        def fetch(self):
            times.append(fake.now)
            return super().fetch()

    clients = {
        HOUR.name: Recording(FeedError("down")),
        DAY.name: FakeClient([]),
    }
    with ChangeDetector() as detector:
        poller = Poller([HOUR, DAY], detector, lambda f: clients[f.name], fake.clock, fake.sleep)
        poller.run(stop)
    assert times == [0, 120, 360, 840]  # delays 120, 240, 480
    assert clients["all_day"].calls == 2  # unaffected by the other feed failing


def test_feed_recovers_to_its_normal_interval_after_success():
    stop = threading.Event()
    fake = FakeTime(stop, stop_after=500)
    times: list[float] = []

    class Recording(FakeClient):
        def fetch(self):
            times.append(fake.now)
            return super().fetch()

    client = Recording(FeedError("down"), FeedError("down"), [])  # then succeeds forever
    with ChangeDetector() as detector:
        Poller([HOUR], detector, lambda f: client, fake.clock, fake.sleep).run(stop)
    # fail at 0 (+120), fail at 120 (+240), succeed at 360, then every 60s
    assert times[:5] == [0, 120, 360, 420, 480]


def test_run_stops_after_max_polls():
    with ChangeDetector() as detector:
        client = FakeClient([])
        fake = FakeTime()
        poller = Poller([HOUR], detector, lambda f: client, fake.clock, fake.sleep)
        poller.run(threading.Event(), max_polls=3)
    assert client.calls == 3


# --- command line --------------------------------------------------------------------


def test_main_once_polls_and_stores_state(tmp_path):
    db = tmp_path / "nested" / "state.db"
    code = main(
        ["--config", str(ROOT / "configs" / "sources.yaml"), "--db", str(db), "--once"],
        client_factory=lambda feed: FakeClient(fixture_features()),
    )
    assert code == 0
    with ChangeDetector(str(db)) as detector:
        assert detector.pending_count() == 2


def test_main_once_exits_nonzero_when_a_feed_fails(tmp_path):
    code = main(
        ["--config", str(ROOT / "configs" / "sources.yaml"), "--db", str(tmp_path / "s.db"), "--once"],
        client_factory=lambda feed: FakeClient(FeedError("down")),
    )
    assert code == 1


def test_main_reports_a_bad_config(tmp_path):
    bad = write_config(tmp_path, "feeds: []")
    assert main(["--config", str(bad), "--db", str(tmp_path / "s.db"), "--once"]) == 2
    assert main(["--config", str(tmp_path / "missing.yaml"), "--once"]) == 2
