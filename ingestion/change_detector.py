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

import json
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

-- Reconciliation batches handed over by Airflow (see ingestion/outbox.py). A batch is accepted when its row
-- and every new revision in it are committed together, and published when all of those revisions are.
CREATE TABLE IF NOT EXISTS outbox_batches (
    batch_id       TEXT    PRIMARY KEY,
    sha256         TEXT    NOT NULL,
    accepted_at    TEXT    NOT NULL,
    published_at   TEXT,
    feature_count  INTEGER NOT NULL,
    valid_count    INTEGER NOT NULL,
    rejected_count INTEGER NOT NULL,
    new_revisions  INTEGER NOT NULL,
    source         TEXT    NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox_batch_revisions (
    batch_id    TEXT NOT NULL,
    event_id    TEXT NOT NULL,
    record_hash TEXT NOT NULL,
    PRIMARY KEY (batch_id, event_id, record_hash)
);
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


class BatchConflict(Exception):
    """A batch ID that was accepted before arrived again with different content."""


@dataclass(frozen=True)
class BatchRecord:
    batch_id: str
    sha256: str
    accepted_at: str
    published_at: str | None
    feature_count: int
    valid_count: int
    rejected_count: int
    new_revisions: int
    source: dict


@dataclass(frozen=True)
class BatchAcceptance:
    record: BatchRecord
    changes: list[Change]  # revisions this call recorded, empty if the batch was accepted before
    already_accepted: bool


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
        with self._db:  # one transaction: all new revisions are recorded, or none
            return self._detect(events, _now())

    def _detect(self, events: Iterable[EarthquakeEvent], now: str) -> list[Change]:
        """The body of detect(), for callers that already hold the transaction."""
        changes: list[Change] = []
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

    def accept_batch(
        self,
        batch_id: str,
        sha256: str,
        events: list[EarthquakeEvent],
        feature_count: int,
        source: dict,
    ) -> BatchAcceptance:
        """Take over a reconciliation batch: its new revisions become pending, and the batch is recorded,
        in one transaction. A batch that was accepted before is not processed again.

        Returning means the batch is durable here. It does not mean Kafka has the revisions yet."""
        with self._db:
            existing = self.get_batch(batch_id)
            if existing is not None:
                if existing.sha256 != sha256:
                    raise BatchConflict(f"batch {batch_id} was accepted with different content")
                return BatchAcceptance(existing, [], already_accepted=True)
            now = _now()
            changes = self._detect(events, now)
            self._db.execute(
                "INSERT INTO outbox_batches (batch_id, sha256, accepted_at, feature_count, valid_count,"
                " rejected_count, new_revisions, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (batch_id, sha256, now, feature_count, len(events), feature_count - len(events),
                 len(changes), json.dumps(source, sort_keys=True)),
            )
            self._db.executemany(
                "INSERT INTO outbox_batch_revisions (batch_id, event_id, record_hash) VALUES (?, ?, ?)",
                [(batch_id, c.event.event_id, c.event.record_hash) for c in changes],
            )
        return BatchAcceptance(self.get_batch(batch_id), changes, already_accepted=False)

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        row = self._db.execute(
            "SELECT batch_id, sha256, accepted_at, published_at, feature_count, valid_count,"
            " rejected_count, new_revisions, source FROM outbox_batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        return None if row is None else BatchRecord(*row[:8], json.loads(row[8]))

    def settle_batches(self) -> list[BatchRecord]:
        """Mark accepted batches whose revisions Kafka has all confirmed as published, and return them."""
        ids = [
            row[0]
            for row in self._db.execute(
                "SELECT b.batch_id FROM outbox_batches b WHERE b.published_at IS NULL AND NOT EXISTS ("
                " SELECT 1 FROM outbox_batch_revisions r JOIN observations o"
                "  ON o.event_id = r.event_id AND o.record_hash = r.record_hash"
                " WHERE r.batch_id = b.batch_id AND o.published_at IS NULL)"
            ).fetchall()
        ]
        if ids:
            with self._db:
                self._db.executemany(
                    "UPDATE outbox_batches SET published_at = ? WHERE batch_id = ?", [(_now(), i) for i in ids]
                )
        return [self.get_batch(i) for i in ids]

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

    def mark_unsent_unless_known(
        self, known: set[tuple[str, str]], dry_run: bool = True
    ) -> list[tuple[str, str]]:
        """Make revisions that were sent, but are not in `known`, pending again, and return them.

        `known` is the set of (event_id, record_hash) pairs that downstream already holds, for example
        read from the lakehouse. This repairs the case where Kafka lost messages that were confirmed
        and so recorded as sent: the revisions are resent, in full, because the content of every
        revision is kept here. With dry_run (the default) nothing is changed.
        """
        rows = self._db.execute(
            "SELECT event_id, record_hash FROM observations WHERE published_at IS NOT NULL ORDER BY rowid"
        ).fetchall()
        missing = [(event_id, record_hash) for event_id, record_hash in rows if (event_id, record_hash) not in known]
        if missing and not dry_run:
            with self._db:
                self._db.executemany(
                    "UPDATE observations SET published_at = NULL WHERE event_id = ? AND record_hash = ?", missing
                )
        return missing

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
