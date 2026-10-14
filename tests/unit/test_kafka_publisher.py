import copy
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion.change_detector import ChangeDetector
from ingestion.kafka_publisher import RAW_TOPIC, KafkaPublisher
from ingestion.live.poller import FeedConfig, Poller, main
from ingestion.live.usgs_client import FeedSnapshot
from processing.normalize import normalize_features
from schemas.earthquake import EarthquakeEvent
from tests.kafka_support import FakeProducer

ROOT = Path(__file__).resolve().parents[2]
FIXTURE = ROOT / "tests" / "fixtures" / "usgs_all_hour_sample.geojson"
INGESTED = datetime(2026, 10, 13, 12, 0, tzinfo=UTC)


def features() -> list[dict]:
    return copy.deepcopy(json.loads(FIXTURE.read_text())["features"])


def detect(detector: ChangeDetector, feature_list=None) -> list[EarthquakeEvent]:
    result = normalize_features(feature_list or features(), INGESTED)
    detector.detect(result.valid)
    return result.valid


@pytest.fixture
def detector():
    with ChangeDetector() as d:
        yield d


def test_publishes_pending_revisions_with_key_value_headers_and_timestamp(detector):
    events = detect(detector)
    producer = FakeProducer()
    report = KafkaPublisher(detector, producer).publish_pending()

    assert (report.attempted, report.delivered, report.undelivered) == (2, 2, 0)
    first = producer.sent[0]
    assert first["topic"] == RAW_TOPIC
    assert first["key"] == "example-quake-001"
    assert EarthquakeEvent.from_json(first["value"]) == events[0]
    assert first["timestamp"] == 1791878280000  # the source update time
    assert first["headers"] == {
        "source": b"usgs",
        "schema_version": b"1",
        "change_kind": b"new",
        "record_hash": events[0].record_hash.encode(),
    }


def test_acknowledged_revisions_are_marked_published_and_not_resent(detector):
    detect(detector)
    producer = FakeProducer()
    publisher = KafkaPublisher(detector, producer)
    publisher.publish_pending()
    assert detector.pending_count() == 0

    detect(detector)  # same snapshot again
    assert publisher.publish_pending().attempted == 0
    assert len(producer.sent) == 2


def test_delivery_error_leaves_the_revision_pending_then_retries_it(detector):
    detect(detector)
    producer = FakeProducer(mode="fail")
    publisher = KafkaPublisher(detector, producer)

    report = publisher.publish_pending()
    assert report.delivered == 0 and report.undelivered == 2
    assert "broker transport failure" in report.errors[0]
    assert detector.pending_count() == 2  # nothing was wrongly marked published

    producer.mode = "ack"  # Kafka is back
    assert publisher.publish_pending().delivered == 2
    assert detector.pending_count() == 0


def test_no_acknowledgement_before_the_timeout_leaves_everything_pending(detector):
    detect(detector)
    report = KafkaPublisher(detector, FakeProducer(mode="silent")).publish_pending(0.1)
    assert report.delivered == 0
    assert detector.pending_count() == 2
    assert any("not acknowledged" in error for error in report.errors)


def test_partial_failure_only_marks_the_delivered_revision(detector):
    detect(detector)
    producer = FakeProducer()
    producer.fail_ids = {"example-quake-002"}
    report = KafkaPublisher(detector, producer).publish_pending()

    assert (report.delivered, report.undelivered) == (1, 1)
    assert [c.event.event_id for c in detector.pending()] == ["example-quake-002"]


def test_message_refused_by_produce_does_not_block_the_others(detector):
    detect(detector)
    producer = FakeProducer()
    producer.reject_ids = {"example-quake-001"}
    report = KafkaPublisher(detector, producer).publish_pending()

    assert report.delivered == 1
    assert [c.event.event_id for c in detector.pending()] == ["example-quake-001"]
    assert "message too large" in report.errors[0]


def test_full_local_queue_is_retried_once_after_draining(detector):
    detect(detector)
    producer = FakeProducer()
    producer.buffer_full_times = 1
    assert KafkaPublisher(detector, producer).publish_pending().delivered == 2


def test_persistently_full_queue_stops_and_leaves_the_rest_pending(detector):
    detect(detector)
    producer = FakeProducer()
    producer.always_buffer_full = True
    report = KafkaPublisher(detector, producer).publish_pending()

    assert report.delivered == 0
    assert detector.pending_count() == 2
    assert any("queue is full" in error for error in report.errors)


def test_revisions_of_one_event_are_sent_in_detection_order(detector):
    detect(detector)
    revised = features()
    revised[0]["properties"].update(mag=4.8, updated=1791878280000 + 600_000)
    detect(detector, revised)
    producer = FakeProducer()
    KafkaPublisher(detector, producer).publish_pending()

    sent = [(m["key"], EarthquakeEvent.from_json(m["value"]).magnitude) for m in producer.sent]
    assert sent == [("example-quake-001", 4.6), ("example-quake-002", None), ("example-quake-001", 4.8)]
    assert producer.sent[2]["headers"]["change_kind"] == b"revised"


def test_nothing_pending_sends_nothing(detector):
    producer = FakeProducer()
    report = KafkaPublisher(detector, producer).publish_pending()
    assert report.attempted == 0 and producer.sent == []


def test_failed_delivery_survives_a_restart_and_is_sent_afterwards(tmp_path):
    path = str(tmp_path / "state.db")
    with ChangeDetector(path) as before:
        detect(before)
        KafkaPublisher(before, FakeProducer(mode="fail")).publish_pending()
    with ChangeDetector(path) as after:
        producer = FakeProducer()
        assert KafkaPublisher(after, producer).publish_pending().delivered == 2


# --- wired into the poller --------------------------------------------------------------


class FakeClient:
    def __init__(self, feature_list):
        self.feature_list = feature_list

    def fetch(self):
        return FeedSnapshot(features=self.feature_list, generated_ms=None, fetched_at=INGESTED)


def test_poller_publishes_after_each_poll(detector):
    producer = FakeProducer()
    feed = FeedConfig("all_hour", "https://example.test", 60)
    poller = Poller(
        [feed], detector, lambda f: FakeClient(features()), publisher=KafkaPublisher(detector, producer)
    )
    result = poller.poll_feed(feed)
    assert result.publish.delivered == 2 and result.fully_delivered
    assert poller.poll_feed(feed).publish.attempted == 0  # nothing new, nothing owed


def test_poller_retries_owed_revisions_even_when_the_feed_fetch_fails(detector):
    producer = FakeProducer(mode="fail")
    feed = FeedConfig("all_hour", "https://example.test", 60)
    publisher = KafkaPublisher(detector, producer)
    good = Poller([feed], detector, lambda f: FakeClient(features()), publisher=publisher)
    assert not good.poll_feed(feed).fully_delivered

    class Down:
        def fetch(self):
            from ingestion.live.usgs_client import FeedError

            raise FeedError("USGS down")

    producer.mode = "ack"
    result = Poller([feed], detector, lambda f: Down(), publisher=publisher).poll_feed(feed)
    assert not result.ok  # the fetch failed...
    assert result.publish.delivered == 2  # ...but the owed revisions still went out


def test_main_publishes_and_reports_success(tmp_path):
    producer = FakeProducer()
    code = main(
        ["--config", str(ROOT / "configs" / "sources.yaml"), "--db", str(tmp_path / "s.db"), "--once"],
        client_factory=lambda feed: FakeClient(features()),
        producer_factory=lambda: producer,
    )
    assert code == 0
    assert len(producer.sent) == 2  # same two events from both feeds, sent once
    with ChangeDetector(str(tmp_path / "s.db")) as d:
        assert d.pending_count() == 0


def test_main_exits_nonzero_when_kafka_does_not_confirm(tmp_path):
    code = main(
        ["--config", str(ROOT / "configs" / "sources.yaml"), "--db", str(tmp_path / "s.db"), "--once"],
        client_factory=lambda feed: FakeClient(features()),
        producer_factory=lambda: FakeProducer(mode="fail"),
    )
    assert code == 1
    with ChangeDetector(str(tmp_path / "s.db")) as d:
        assert d.pending_count() == 2
