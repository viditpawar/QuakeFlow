"""SQLite memory of which event revisions we have seen and which Kafka has confirmed.

Each distinct revision (event_id + record_hash) is stored once. Using it takes two steps
so a crash can never lose data:

    changes = detector.detect(events)        # recorded as *pending*
    ...publish each change to Kafka...
    detector.mark_published(event_id, hash)  # only after Kafka acknowledges it

Anything detected but never acknowledged stays pending and is returned by pending(), so
a restart simply retries it. Re-seeing a pending or published revision is UNCHANGED and is
not queued again. Delivery is therefore at-least-once, never at-most-once.

Not thread-safe: use one detector per thread.
"""

import logging
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Self

from schemas.earthquake import EarthquakeEvent, utc_to_epoch_ms

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS observations (
    event_id          TEXT    NOT NULL,
    record_hash       TEXT    NOT NULL,
    source_updated_ms INTEGER NOT NULL,
    kind              TEXT    NOT NULL,
    payload           TEXT    NOT NULL,
    detected_at       TEXT    NOT NULL,
    published_at      TEXT,
    PRIMARY KEY (event_id, record_hash)
);
CREATE INDEX IF NOT EXISTS observations_pending
    ON observations (published_at) WHERE published_at IS NULL;
"""


class ChangeKind(StrEnum):
    NEW = "new"  # first revision we have seen of this event
    REVISED = "revised"  # newer source_updated_at than anything seen before
    OUT_OF_ORDER = "out_of_order"  # older than a revision we already have
    CONFLICT = "conflict"  # same source_updated_at as a known revision, different content


@dataclass(frozen=True)
class Change:
    kind: ChangeKind
    event: EarthquakeEvent


class ChangeDetector:
    def __init__(self, path: str = ":memory:"):
        self._db = sqlite3.connect(path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")  # a confirmed publish must survive power loss
        version = self._db.execute("PRAGMA user_version").fetchone()[0]
        if version not in (0, SCHEMA_VERSION):
            raise RuntimeError(f"unsupported change-detector schema version {version}")
        with self._db:
            self._db.executescript(_SCHEMA)
            self._db.execute(f"PRAGMA user_version={SCHEMA_VERSION}")

    def close(self) -> None:
        self._db.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def detect(self, events: Iterable[EarthquakeEvent]) -> list[Change]:
        """Record revisions not seen before as pending and return them, in input order."""
        now = _now()
        changes: list[Change] = []
        with self._db:  # one transaction: all new revisions are recorded, or none
            for event in events:
                if event.record_hash is None:
                    raise ValueError(f"event {event.event_id} has no record_hash")
                change = self._classify(event)
                if change is None:
                    continue
                self._db.execute(
                    "INSERT INTO observations (event_id, record_hash, source_updated_ms, kind,"
                    " payload, detected_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        event.event_id,
                        event.record_hash,
                        utc_to_epoch_ms(event.source_updated_at),
                        change.kind.value,
                        event.to_json(),
                        now,
                    ),
                )
                changes.append(change)
        return changes

    def pending(self) -> list[Change]:
        """Every detected revision Kafka has not yet confirmed, oldest detection first."""
        rows = self._db.execute(
            "SELECT kind, payload FROM observations WHERE published_at IS NULL ORDER BY rowid"
        ).fetchall()
        return [
            Change(ChangeKind(kind), EarthquakeEvent.from_json(payload)) for kind, payload in rows
        ]

    def mark_published(self, event_id: str, record_hash: str) -> bool:
        """Record a Kafka acknowledgement. Returns False if there was nothing pending to mark."""
        with self._db:
            cursor = self._db.execute(
                "UPDATE observations SET published_at = ?"
                " WHERE event_id = ? AND record_hash = ? AND published_at IS NULL",
                (_now(), event_id, record_hash),
            )
        return cursor.rowcount == 1

    def pending_count(self) -> int:
        return self._db.execute(
            "SELECT COUNT(*) FROM observations WHERE published_at IS NULL"
        ).fetchone()[0]

    def _classify(self, event: EarthquakeEvent) -> Change | None:
        known = self._db.execute(
            "SELECT record_hash, source_updated_ms FROM observations WHERE event_id = ?",
            (event.event_id,),
        ).fetchall()
        if any(record_hash == event.record_hash for record_hash, _ in known):
            return None  # already seen: pending or published, either way not new
        if not known:
            return Change(ChangeKind.NEW, event)

        updated_ms = utc_to_epoch_ms(event.source_updated_at)
        known_updates = [ms for _, ms in known]
        if updated_ms in known_updates:
            logger.warning(
                "conflict: event %s has two different revisions with source_updated_at=%d",
                event.event_id,
                updated_ms,
            )
            return Change(ChangeKind.CONFLICT, event)
        if updated_ms > max(known_updates):
            return Change(ChangeKind.REVISED, event)
        return Change(ChangeKind.OUT_OF_ORDER, event)


def _now() -> str:
    return datetime.now(UTC).isoformat()
