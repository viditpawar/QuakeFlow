"""Poll the configured USGS feeds, each on its own interval, and detect changes.

Each poll fetches a snapshot, normalizes it, and records unseen revisions as pending in
the change detector. Publishing to Kafka is a separate step: this module never marks
anything as published. A feed that fails is retried with exponential backoff and never
stops the other feeds or the loop.

    python -m ingestion.live.poller --once
"""

import argparse
import logging
import os
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from pathlib import Path

import yaml

from ingestion.change_detector import Change, ChangeDetector
from ingestion.kafka_publisher import KafkaPublisher, PublishReport, build_producer
from ingestion.live.usgs_client import FeedError, UsgsLiveClient
from processing.normalize import RejectedRecord, normalize_features

logger = logging.getLogger(__name__)

MIN_INTERVAL_SECONDS = 60  # USGS rebuilds the feeds about once a minute
BACKOFF_CAP_SECONDS = 1800
DEFAULT_CONFIG = "configs/sources.yaml"
DEFAULT_DB = "data/quakeflow_state.db"


class ConfigError(ValueError):
    """The feed configuration file is missing something or has an invalid value."""


@dataclass(frozen=True)
class FeedConfig:
    name: str
    url: str
    interval_seconds: float
    timeout_seconds: float = 15.0


@dataclass(frozen=True)
class PollResult:
    feed: str
    ok: bool
    fetched: int = 0
    changes: list[Change] = field(default_factory=list)
    rejected: list[RejectedRecord] = field(default_factory=list)
    error: str | None = None
    publish: PublishReport | None = None  # None when publishing is off

    @property
    def fully_delivered(self) -> bool:
        return self.ok and (self.publish is None or self.publish.undelivered == 0)


def load_feed_configs(path: str | Path) -> list[FeedConfig]:
    document = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    entries = document.get("feeds") if isinstance(document, dict) else None
    if not isinstance(entries, list) or not entries:
        raise ConfigError(f"{path}: expected a non-empty 'feeds' list")

    feeds: list[FeedConfig] = []
    for index, entry in enumerate(entries):
        where = f"{path}: feeds[{index}]"
        if not isinstance(entry, dict):
            raise ConfigError(f"{where}: expected a mapping")
        name, url = entry.get("name"), entry.get("url")
        if not isinstance(name, str) or not name:
            raise ConfigError(f"{where}: 'name' is required")
        if not isinstance(url, str) or not url:
            raise ConfigError(f"{where}: 'url' is required")
        interval = _number(entry.get("interval_seconds"), f"{where}: 'interval_seconds'")
        if interval < MIN_INTERVAL_SECONDS:
            raise ConfigError(
                f"{where}: interval_seconds must be at least {MIN_INTERVAL_SECONDS} "
                "(USGS refreshes feeds about once a minute)"
            )
        timeout = _number(entry.get("timeout_seconds", 15), f"{where}: 'timeout_seconds'")
        if timeout <= 0:
            raise ConfigError(f"{where}: timeout_seconds must be positive")
        feeds.append(FeedConfig(name, url, interval, timeout))

    names = [feed.name for feed in feeds]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ConfigError(f"{path}: duplicate feed names: {', '.join(duplicates)}")
    return feeds


def _number(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ConfigError(f"{label} must be a number")
    return float(value)


def _default_client(feed: FeedConfig) -> UsgsLiveClient:
    return UsgsLiveClient(url=feed.url, timeout=feed.timeout_seconds)


def backoff_delay(feed: FeedConfig, consecutive_failures: int) -> float:
    """Seconds until the next try: the interval, doubled per consecutive failure, capped."""
    if consecutive_failures <= 0:
        return feed.interval_seconds
    delay = feed.interval_seconds * 2**consecutive_failures
    return max(feed.interval_seconds, min(delay, BACKOFF_CAP_SECONDS))


class Poller:
    def __init__(
        self,
        feeds: list[FeedConfig],
        detector: ChangeDetector,
        client_factory: Callable[[FeedConfig], object] = _default_client,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], object] | None = None,
        publisher: KafkaPublisher | None = None,
    ):
        self.feeds = feeds
        self.detector = detector
        self.publisher = publisher
        self._client_factory = client_factory
        self._clock = clock
        self._sleep = sleep
        self._clients: dict[str, object] = {}

    def poll_feed(self, feed: FeedConfig) -> PollResult:
        """Poll one feed, then send anything owed to Kafka, including earlier leftovers."""
        result = self._detect(feed)
        if self.publisher is not None:
            result = replace(result, publish=self.publisher.publish_pending())
        return result

    def _detect(self, feed: FeedConfig) -> PollResult:
        """One fetch, normalize and detect cycle. Fetch failures are returned, not raised."""
        client = self._clients.setdefault(feed.name, self._client_factory(feed))
        try:
            snapshot = client.fetch()
        except FeedError as exc:
            logger.warning("feed %s: fetch failed: %s", feed.name, exc)
            return PollResult(feed.name, ok=False, error=str(exc))

        normalized = normalize_features(snapshot.features, snapshot.fetched_at)
        changes = self.detector.detect(normalized.valid)
        for rejected in normalized.rejected:
            logger.warning(
                "feed %s: rejected %s: %s",
                feed.name,
                rejected.event_id or "<no id>",
                "; ".join(rejected.reasons),
            )
        kinds = Counter(change.kind.value for change in changes)
        logger.info(
            "feed %s: %d fetched, %d changes %s, %d rejected, %d pending",
            feed.name,
            len(snapshot.features),
            len(changes),
            dict(kinds),
            len(normalized.rejected),
            self.detector.pending_count(),
        )
        return PollResult(
            feed.name,
            ok=True,
            fetched=len(snapshot.features),
            changes=changes,
            rejected=normalized.rejected,
        )

    def poll_all_once(self) -> list[PollResult]:
        return [self.poll_feed(feed) for feed in self.feeds]

    def run(self, stop: threading.Event, max_polls: int | None = None) -> None:
        """Poll until stop is set (or max_polls polls have happened), each feed on its schedule."""
        sleep = self._sleep or stop.wait
        next_due = {feed.name: self._clock() for feed in self.feeds}  # everything polls at once
        failures = {feed.name: 0 for feed in self.feeds}
        polls = 0
        while not stop.is_set():
            for feed in self.feeds:
                if next_due[feed.name] > self._clock():
                    continue
                result = self.poll_feed(feed)
                failures[feed.name] = 0 if result.ok else failures[feed.name] + 1
                next_due[feed.name] = self._clock() + backoff_delay(feed, failures[feed.name])
                polls += 1
                if max_polls is not None and polls >= max_polls:
                    return
            sleep(max(0.0, min(next_due.values()) - self._clock()))


def main(
    argv: list[str] | None = None,
    client_factory=_default_client,
    producer_factory=build_producer,
) -> int:
    parser = argparse.ArgumentParser(description="Poll the USGS earthquake feeds.")
    parser.add_argument("--config", default=os.getenv("QUAKEFLOW_SOURCES_CONFIG", DEFAULT_CONFIG))
    parser.add_argument("--db", default=os.getenv("QUAKEFLOW_STATE_DB", DEFAULT_DB))
    parser.add_argument("--once", action="store_true", help="poll every feed once and exit")
    parser.add_argument(
        "--no-publish",
        action="store_true",
        help="detect changes only; leave them pending and do not contact Kafka",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    try:
        feeds = load_feed_configs(args.config)
    except (ConfigError, OSError, yaml.YAMLError) as exc:
        logger.error("cannot load feed configuration: %s", exc)
        return 2

    Path(args.db).parent.mkdir(parents=True, exist_ok=True)
    with ChangeDetector(args.db) as detector:
        publisher = None if args.no_publish else KafkaPublisher(detector, producer_factory())
        poller = Poller(feeds, detector, client_factory, publisher=publisher)
        if args.once:
            results = poller.poll_all_once()
            return 0 if all(result.fully_delivered for result in results) else 1
        stop = threading.Event()
        try:
            poller.run(stop)
        except KeyboardInterrupt:
            logger.info("stopping")
    return 0


if __name__ == "__main__":
    sys.exit(main())
