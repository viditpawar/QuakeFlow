import copy
import json
import math
import socket
from datetime import UTC, datetime, timedelta
from pathlib import Path

import requests

from ingestion.change_detector import Change, ChangeDetector, ChangeKind
from ingestion.kafka_publisher import KafkaPublisher
from ingestion.live.poller import FeedConfig, Poller, PollResult
from ingestion.live.usgs_client import FeedError, FeedSnapshot
from ingestion.metrics import LATENCY_BUCKETS, CollectorMetrics
from processing.normalize import normalize_feature, normalize_features
from tests.kafka_support import FakeProducer

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "usgs_all_hour_sample.geojson"
NOW = datetime(2026, 10, 14, 12, 0, tzinfo=UTC)
ALLOWED_LABELS = {"feed", "kind", "outcome", "le"}


class Clock:
    def __init__(self, now: datetime = NOW):
        self.seconds = now.timestamp()

    def __call__(self) -> float:
        return self.seconds


def make_metrics(clock=None):
    clock = clock or Clock()
    metrics = CollectorMetrics(clock=clock)
    return metrics, clock


def value(metrics, name, **labels):
    return metrics.registry.get_sample_value(name, labels)


def event(updated_offset=0, event_id="example-quake-001"):
    feature = json.loads(FIXTURE.read_text())["features"][0]
    feature["id"] = event_id
    feature["properties"]["updated"] += updated_offset
    return normalize_feature(feature, NOW)


def poll_result(*, fetched=2, changes=(), rejected=0, latest=None, ok=True):
    result = normalize_features(
        [{"broken": True}] * rejected, NOW
    )  # rejected records, built the same way the poller builds them
    return PollResult(
        "all_hour",
        ok=ok,
        fetched=fetched,
        changes=list(changes),
        rejected=result.rejected,
        latest_source_update=latest,
    )


# --- counters ------------------------------------------------------------------------------


def test_a_successful_poll_updates_the_counters_and_timestamp():
    metrics, clock = make_metrics()
    changes = [Change(ChangeKind.NEW, event()), Change(ChangeKind.REVISED, event(60_000))]
    metrics.record_poll("all_hour", poll_result(fetched=10, changes=changes, rejected=3))

    assert value(metrics, "quakeflow_events_ingested_total", feed="all_hour") == 7  # 10 - 3 invalid
    assert value(metrics, "quakeflow_invalid_records_total", feed="all_hour") == 3
    assert value(metrics, "quakeflow_revisions_detected_total", kind="new") == 1
    assert value(metrics, "quakeflow_revisions_detected_total", kind="revised") == 1
    assert value(metrics, "quakeflow_feed_polls_total", feed="all_hour", outcome="ok") == 1
    assert value(metrics, "quakeflow_last_successful_poll_timestamp_seconds", feed="all_hour") == clock.seconds


def test_a_failed_poll_only_counts_the_failure():
    metrics, _ = make_metrics()
    metrics.record_poll("all_hour", poll_result(ok=False, fetched=0))
    assert value(metrics, "quakeflow_feed_polls_total", feed="all_hour", outcome="error") == 1
    assert value(metrics, "quakeflow_events_ingested_total", feed="all_hour") is None
    assert value(metrics, "quakeflow_last_successful_poll_timestamp_seconds", feed="all_hour") is None


def test_counters_accumulate_across_polls_and_feeds():
    metrics, _ = make_metrics()
    metrics.record_poll("all_hour", poll_result(fetched=4))
    metrics.record_poll("all_hour", poll_result(fetched=6))
    metrics.record_poll("all_day", poll_result(fetched=100))
    assert value(metrics, "quakeflow_events_ingested_total", feed="all_hour") == 10
    assert value(metrics, "quakeflow_events_ingested_total", feed="all_day") == 100


def test_an_empty_feed_is_a_successful_poll_with_no_events():
    metrics, _ = make_metrics()
    metrics.record_poll("all_hour", poll_result(fetched=0))
    assert value(metrics, "quakeflow_feed_polls_total", feed="all_hour", outcome="ok") == 1
    assert value(metrics, "quakeflow_events_ingested_total", feed="all_hour") == 0


def test_pending_revisions_gauge():
    metrics, _ = make_metrics()
    metrics.set_pending(7)
    assert value(metrics, "quakeflow_pending_revisions") == 7
    metrics.set_pending(0)
    assert value(metrics, "quakeflow_pending_revisions") == 0


def test_initialise_creates_every_series_at_zero_so_dashboards_show_zero_not_no_data():
    metrics, _ = make_metrics()
    assert value(metrics, "quakeflow_invalid_records_total", feed="all_hour") is None
    metrics.initialise(["all_hour", "all_day"])
    for feed in ("all_hour", "all_day"):
        assert value(metrics, "quakeflow_invalid_records_total", feed=feed) == 0
        assert value(metrics, "quakeflow_events_ingested_total", feed=feed) == 0
        assert value(metrics, "quakeflow_feed_polls_total", feed=feed, outcome="ok") == 0
        assert value(metrics, "quakeflow_feed_polls_total", feed=feed, outcome="error") == 0
    for kind in ChangeKind:
        assert value(metrics, "quakeflow_revisions_detected_total", kind=kind.value) == 0


def test_initialise_does_not_disturb_counts_already_recorded():
    metrics, _ = make_metrics()
    metrics.record_poll("all_hour", poll_result(fetched=5))
    metrics.initialise(["all_hour"])
    assert value(metrics, "quakeflow_events_ingested_total", feed="all_hour") == 5


# --- freshness -----------------------------------------------------------------------------


def test_freshness_is_unknown_until_an_event_is_seen():
    metrics, _ = make_metrics()
    assert math.isnan(value(metrics, "quakeflow_source_freshness_seconds"))
    metrics.record_poll("all_hour", poll_result(latest=None))
    assert math.isnan(value(metrics, "quakeflow_source_freshness_seconds"))


def test_freshness_is_the_age_of_the_newest_update_and_keeps_growing_between_polls():
    metrics, clock = make_metrics()
    metrics.record_poll("all_hour", poll_result(latest=NOW - timedelta(seconds=90)))
    assert value(metrics, "quakeflow_source_freshness_seconds") == 90
    clock.seconds += 600  # ten minutes pass with no new poll
    assert value(metrics, "quakeflow_source_freshness_seconds") == 690


def test_freshness_follows_the_newest_update_and_ignores_older_ones():
    metrics, _ = make_metrics()
    metrics.record_poll("all_hour", poll_result(latest=NOW - timedelta(seconds=60)))
    metrics.record_poll("all_day", poll_result(latest=NOW - timedelta(hours=5)))  # older, ignored
    assert value(metrics, "quakeflow_source_freshness_seconds") == 60
    metrics.record_poll("all_hour", poll_result(latest=NOW - timedelta(seconds=10)))
    assert value(metrics, "quakeflow_source_freshness_seconds") == 10


def test_an_update_in_the_future_never_gives_negative_freshness():
    metrics, _ = make_metrics()
    metrics.record_poll("all_hour", poll_result(latest=NOW + timedelta(minutes=5)))
    assert value(metrics, "quakeflow_source_freshness_seconds") == 0


# --- latency histogram ---------------------------------------------------------------------


def test_delivery_records_the_latency_from_the_source_update():
    metrics, _ = make_metrics()
    metrics.record_delivery(NOW - timedelta(seconds=90))
    assert value(metrics, "quakeflow_revisions_published_total") == 1
    assert value(metrics, "quakeflow_ingestion_latency_seconds_count") == 1
    assert value(metrics, "quakeflow_ingestion_latency_seconds_sum") == 90
    assert value(metrics, "quakeflow_ingestion_latency_seconds_bucket", le="60.0") == 0
    assert value(metrics, "quakeflow_ingestion_latency_seconds_bucket", le="120.0") == 1
    assert value(metrics, "quakeflow_ingestion_latency_seconds_bucket", le="+Inf") == 1


def test_a_source_update_slightly_ahead_of_the_clock_counts_as_zero_latency():
    metrics, _ = make_metrics()
    metrics.record_delivery(NOW + timedelta(seconds=5))
    assert value(metrics, "quakeflow_ingestion_latency_seconds_sum") == 0


def test_latency_buckets_cover_seconds_to_days():
    assert LATENCY_BUCKETS[0] <= 60 and LATENCY_BUCKETS[-1] >= 86400
    assert list(LATENCY_BUCKETS) == sorted(LATENCY_BUCKETS)


# --- cardinality and naming ----------------------------------------------------------------


def test_only_small_fixed_label_sets_are_used():
    metrics, _ = make_metrics()
    changes = [Change(k, event(i, f"evt-{i}")) for i, k in enumerate(ChangeKind)]
    metrics.record_poll("all_hour", poll_result(fetched=9, changes=changes, rejected=2, latest=NOW))
    metrics.record_delivery(NOW - timedelta(seconds=30))
    labels = {name for family in metrics.registry.collect() for s in family.samples for name in s.labels}
    assert labels <= ALLOWED_LABELS, f"unexpected labels: {labels - ALLOWED_LABELS}"
    # No sample carries an event id, even though several different events were recorded.
    assert not any("evt-" in str(v) for f in metrics.registry.collect() for s in f.samples for v in s.labels.values())


def test_the_exposed_metric_names_match_the_documented_ones():
    metrics, _ = make_metrics()
    names = {family.name for family in metrics.registry.collect()}
    assert {
        "quakeflow_events_ingested",
        "quakeflow_revisions_detected",
        "quakeflow_invalid_records",
        "quakeflow_source_freshness_seconds",
        "quakeflow_ingestion_latency_seconds",
    } <= names


def test_metrics_are_served_over_http():
    metrics, _ = make_metrics()
    metrics.record_poll("all_hour", poll_result(fetched=3))
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    metrics.serve(port)
    body = requests.get(f"http://127.0.0.1:{port}/metrics", timeout=5).text
    assert 'quakeflow_events_ingested_total{feed="all_hour"} 3.0' in body
    assert "quakeflow_source_freshness_seconds" in body


# --- wired into the poller and publisher ----------------------------------------------------


def features() -> list[dict]:
    return copy.deepcopy(json.loads(FIXTURE.read_text())["features"])


class StubClient:
    def __init__(self, feature_list=None, error=None):
        self.feature_list, self.error = feature_list, error

    def fetch(self):
        if self.error:
            raise self.error
        return FeedSnapshot(features=self.feature_list, generated_ms=None, fetched_at=NOW)


def test_the_poller_records_a_poll_end_to_end():
    metrics, _ = make_metrics()
    feed = FeedConfig("all_hour", "https://example.test", 60)
    bad = {"id": "bad", "properties": {}, "geometry": None}
    with ChangeDetector() as detector:
        poller = Poller([feed], detector, lambda f: StubClient([*features(), bad]), metrics=metrics)
        result = poller.poll_feed(feed)

    assert result.latest_source_update == datetime.fromtimestamp(1791878300, UTC)
    assert value(metrics, "quakeflow_events_ingested_total", feed="all_hour") == 2
    assert value(metrics, "quakeflow_invalid_records_total", feed="all_hour") == 1
    assert value(metrics, "quakeflow_revisions_detected_total", kind="new") == 2
    assert value(metrics, "quakeflow_pending_revisions") == 2  # nothing publishes without a publisher


def test_a_fetch_failure_is_counted_as_an_error_poll():
    metrics, _ = make_metrics()
    feed = FeedConfig("all_hour", "https://example.test", 60)
    with ChangeDetector() as detector:
        poller = Poller([feed], detector, lambda f: StubClient(error=FeedError("down")), metrics=metrics)
        poller.poll_feed(feed)
    assert value(metrics, "quakeflow_feed_polls_total", feed="all_hour", outcome="error") == 1


def test_the_delivery_hook_fires_only_for_revisions_kafka_confirmed():
    metrics, clock = make_metrics()
    feed = FeedConfig("all_hour", "https://example.test", 60)
    producer = FakeProducer()
    producer.fail_ids = {"example-quake-002"}  # Kafka rejects this one
    with ChangeDetector() as detector:
        publisher = KafkaPublisher(
            detector, producer, on_delivered=lambda e: metrics.record_delivery(e.source_updated_at)
        )
        poller = Poller([feed], detector, lambda f: StubClient(features()), publisher=publisher, metrics=metrics)
        poller.poll_feed(feed)

    assert value(metrics, "quakeflow_revisions_published_total") == 1
    assert value(metrics, "quakeflow_ingestion_latency_seconds_count") == 1
    assert value(metrics, "quakeflow_pending_revisions") == 1, "the rejected revision is still owed"
    assert clock.seconds == NOW.timestamp()
