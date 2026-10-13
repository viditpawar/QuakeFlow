"""Publish pending earthquake revisions to Kafka and record only confirmed deliveries.

A revision is marked published in the change detector from Kafka's delivery callback,
and only when that callback reports success. If the broker is down, the message times
out, or the process dies first, the revision simply stays pending and is sent again on
the next cycle. Delivery is at-least-once, so a crash between Kafka's acknowledgement
and the SQLite write can send one revision twice; downstream deduplicates on record_hash.

Messages are keyed by event_id, so all revisions of one earthquake share a partition and
keep their order. A retry after a failure can still land behind a newer revision of the
same event, which is why consumers order by source_updated_at, not arrival.
"""

import logging
import os
from dataclasses import dataclass, field

from confluent_kafka import KafkaException, Producer

from ingestion.change_detector import Change, ChangeDetector
from schemas.earthquake import utc_to_epoch_ms

logger = logging.getLogger(__name__)

RAW_TOPIC = "quakeflow.events.raw"
SOURCE_NAME = "usgs"
SCHEMA_VERSION = "1"
DEFAULT_BOOTSTRAP = "localhost:9092"


def build_producer(bootstrap_servers: str | None = None) -> Producer:
    """Producer tuned for safe delivery; failures surface within about 30 seconds."""
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers
            or os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP),
            "client.id": "quakeflow-collector",
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 30_000,  # give up and report an error after this long
            "linger.ms": 20,
            "compression.type": "lz4",
        }
    )


@dataclass(frozen=True)
class PublishReport:
    attempted: int
    delivered: int
    errors: list[str] = field(default_factory=list)

    @property
    def undelivered(self) -> int:
        return self.attempted - self.delivered


class KafkaPublisher:
    def __init__(
        self,
        detector: ChangeDetector,
        producer: Producer | None = None,
        topic: str = RAW_TOPIC,
    ):
        self._detector = detector
        self._producer = producer or build_producer()
        self._topic = topic

    def publish_pending(self, flush_timeout: float = 30.0) -> PublishReport:
        """Send every unconfirmed revision, wait for acknowledgements, and report the outcome."""
        changes = self._detector.pending()
        delivered = 0
        errors: list[str] = []

        def on_delivery(error, message, *, event_id: str, record_hash: str) -> None:
            nonlocal delivered
            if error is not None:
                errors.append(f"{event_id}: {error}")
                return  # stays pending, retried next cycle
            if self._detector.mark_published(event_id, record_hash):
                delivered += 1

        for change in changes:
            try:
                self._produce(change, on_delivery)
            except BufferError:
                self._producer.poll(1.0)  # let the local queue drain once, then retry
                try:
                    self._produce(change, on_delivery)
                except BufferError:
                    errors.append("producer queue is full; the rest stay pending")
                    break
            except KafkaException as exc:
                errors.append(f"{change.event.event_id}: {exc}")
            self._producer.poll(0)  # serve delivery callbacks as we go

        still_queued = self._producer.flush(flush_timeout)
        if still_queued:
            errors.append(f"{still_queued} messages not acknowledged within {flush_timeout}s")

        report = PublishReport(attempted=len(changes), delivered=delivered, errors=errors)
        if report.undelivered:
            logger.warning(
                "kafka: %d of %d revisions not delivered, left pending (%s)",
                report.undelivered,
                report.attempted,
                "; ".join(errors[:3]) or "no error reported",
            )
        elif report.attempted:
            logger.info("kafka: delivered %d revisions to %s", delivered, self._topic)
        return report

    def _produce(self, change: Change, on_delivery) -> None:
        event = change.event
        record_hash = event.record_hash
        self._producer.produce(
            self._topic,
            key=event.event_id,
            value=event.to_json(),
            headers=[
                ("source", SOURCE_NAME.encode()),
                ("schema_version", SCHEMA_VERSION.encode()),
                ("change_kind", change.kind.value.encode()),
                ("record_hash", record_hash.encode()),
            ],
            timestamp=utc_to_epoch_ms(event.source_updated_at),
            on_delivery=lambda error, message: on_delivery(
                error, message, event_id=event.event_id, record_hash=record_hash
            ),
        )
