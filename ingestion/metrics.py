"""Prometheus metrics for the collector.

Every metric uses a small, fixed set of labels (feed name, kind of change, outcome). Event ids
and other per-record values never become labels, because each distinct value would create a new
time series.

The object owns its own registry, so tests can create as many as they like. Freshness is
computed when Prometheus scrapes, so it keeps growing between polls instead of freezing at the
value from the last poll.
"""

import math
import time
from collections.abc import Callable
from datetime import datetime

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

from ingestion.change_detector import ChangeKind

# Seconds from a USGS update to Kafka confirming the revision. Revisions found in the day feed or
# a backlog can be hours old, so the buckets run from half a minute to two days.
LATENCY_BUCKETS = (30, 60, 120, 300, 600, 1800, 3600, 7200, 21600, 43200, 86400, 172800)


class CollectorMetrics:
    def __init__(
        self,
        registry: CollectorRegistry | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self.registry = registry or CollectorRegistry()
        self._clock = clock
        self._latest_source_update: float | None = None

        self.events_ingested = Counter(
            "quakeflow_events_ingested",
            "Valid events read from a feed, before any deduplication.",
            ["feed"],
            registry=self.registry,
        )
        self.revisions_detected = Counter(
            "quakeflow_revisions_detected",
            "Distinct revisions seen for the first time, by kind of change.",
            ["kind"],
            registry=self.registry,
        )
        self.revisions_published = Counter(
            "quakeflow_revisions_published",
            "Revisions that Kafka has confirmed.",
            registry=self.registry,
        )
        self.invalid_records = Counter(
            "quakeflow_invalid_records",
            "Feed records that failed validation.",
            ["feed"],
            registry=self.registry,
        )
        self.polls = Counter(
            "quakeflow_feed_polls",
            "Polls of a feed, by outcome.",
            ["feed", "outcome"],
            registry=self.registry,
        )
        self.last_successful_poll = Gauge(
            "quakeflow_last_successful_poll_timestamp_seconds",
            "Unix time of the last poll of the feed that succeeded.",
            ["feed"],
            registry=self.registry,
        )
        self.pending_revisions = Gauge(
            "quakeflow_pending_revisions",
            "Revisions detected but not yet confirmed by Kafka.",
            registry=self.registry,
        )
        self.outbox_batches = Counter(
            "quakeflow_outbox_batches",
            "Reconciliation batches from Airflow, by outcome: accepted, published or rejected.",
            ["outcome"],
            registry=self.registry,
        )
        self.outbox_ready = Gauge(
            "quakeflow_outbox_ready_batches",
            "Reconciliation batches waiting to be accepted.",
            registry=self.registry,
        )
        self.outbox_errors = Counter(
            "quakeflow_outbox_errors",
            "Times the outbox intake failed with an unexpected error.",
            registry=self.registry,
        )
        self.source_freshness = Gauge(
            "quakeflow_source_freshness_seconds",
            "Age of the newest source update seen so far, measured at scrape time. NaN until one is seen.",
            registry=self.registry,
        )
        self.source_freshness.set_function(self._freshness)
        self.ingestion_latency = Histogram(
            "quakeflow_ingestion_latency_seconds",
            "Time from a revision's USGS update to Kafka confirming it.",
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )

    def initialise(self, feeds: list[str]) -> None:
        """Create every labelled series at zero, so a dashboard shows 0 before the first event
        instead of 'no data'. A labelled counter does not exist until it is first incremented."""
        for feed in feeds:
            self.events_ingested.labels(feed)
            self.invalid_records.labels(feed)
            self.polls.labels(feed, "ok")
            self.polls.labels(feed, "error")
        for kind in ChangeKind:
            self.revisions_detected.labels(kind.value)
        for outcome in ("accepted", "published", "rejected"):
            self.outbox_batches.labels(outcome)

    def _freshness(self) -> float:
        if self._latest_source_update is None:
            return math.nan
        return max(0.0, self._clock() - self._latest_source_update)

    def record_poll(self, feed: str, result) -> None:
        """Record one poll. `result` is an ingestion.live.poller.PollResult."""
        if not result.ok:
            self.polls.labels(feed, "error").inc()
            return
        self.polls.labels(feed, "ok").inc()
        self.last_successful_poll.labels(feed).set(self._clock())
        rejected = len(result.rejected)
        self.events_ingested.labels(feed).inc(max(0, result.fetched - rejected))
        if rejected:
            self.invalid_records.labels(feed).inc(rejected)
        for change in result.changes:
            self.revisions_detected.labels(change.kind.value).inc()
        latest = result.latest_source_update
        if latest is not None:
            seconds = latest.timestamp()
            if self._latest_source_update is None or seconds > self._latest_source_update:
                self._latest_source_update = seconds

    def record_delivery(self, source_updated_at: datetime) -> None:
        """Kafka confirmed a revision whose source update time was `source_updated_at`."""
        self.revisions_published.inc()
        self.ingestion_latency.observe(max(0.0, self._clock() - source_updated_at.timestamp()))

    def record_intake(self, report, published: int, ready: int) -> None:
        """Record what one pass over the outbox did. `report` is an ingestion.outbox.IntakeReport."""
        self.outbox_batches.labels("accepted").inc(len(report.accepted))
        self.outbox_batches.labels("rejected").inc(report.rejected)
        self.outbox_batches.labels("published").inc(published)
        if report.invalid_features:
            self.invalid_records.labels("reconciliation").inc(report.invalid_features)
        for change in report.changes:
            self.revisions_detected.labels(change.kind.value).inc()
        self.outbox_ready.set(ready)

    def set_pending(self, count: int) -> None:
        self.pending_revisions.set(count)

    def serve(self, port: int, address: str = "127.0.0.1") -> None:
        """Expose /metrics on a background thread."""
        start_http_server(port, addr=address, registry=self.registry)
