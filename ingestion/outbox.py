"""The hand-over between Airflow and the collector for reconciliation batches.

Airflow finds revisions that the live feeds missed (events USGS updated after they left the one-day feed) and
hands them to the collector, which is the only writer of its SQLite state. They meet on a shared volume:

    tmp/      files being written. The collector never reads here.
    ready/    complete batches, moved in with one atomic rename from tmp/ (same file system).
    status/   the collector's answer for each batch: accepted, published or rejected.
    rejected/ batches that failed validation, kept for inspection.

A batch moves through states that are tracked separately, because each one means something different:

    downloaded   Airflow wrote the file.
    ready        the file is in ready/.
    accepted     the collector validated it and committed its new revisions to its own database, as pending.
                 Only now does the status file say so, and only then is the ready file removed.
    published    Kafka has confirmed every one of those revisions (the collector's existing delivery).

Airflow advances its watermark when a batch is accepted, never earlier, and accepted does not mean that
the data is in Kafka. If the collector stops between committing and answering, the next scan finds the file
again, recognises the batch ID and answers without recording anything twice.
"""

import contextlib
import hashlib
import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from ingestion.change_detector import BatchConflict, BatchRecord, Change, ChangeDetector
from processing.normalize import normalize_features

logger = logging.getLogger(__name__)

FORMAT = 1
SUBDIRECTORIES = ("tmp", "ready", "status", "rejected")
_UNSAFE = re.compile(r"[^A-Za-z0-9_.+-]")
_SAFE = re.compile(r"[A-Za-z0-9_+-][A-Za-z0-9_.+-]{0,127}")


class BatchInvalid(Exception):
    """A batch file that must not be accepted, with the reason."""


def safe_batch_id(raw: str) -> str:
    """Turn an Airflow run ID such as 'scheduled__2026-10-16T00:00:00+00:00' into a file-name-safe ID."""
    cleaned = _UNSAFE.sub("_", raw).lstrip(".")[:128]
    if not _SAFE.fullmatch(cleaned):
        raise ValueError(f"cannot make a batch id from {raw!r}")
    return cleaned


def features_sha256(features: list) -> str:
    """Checksum of the features in a form that does not depend on how the file was written."""
    canonical = json.dumps(features, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _fsync_directory(path: Path) -> None:
    with contextlib.suppress(OSError):  # not possible on every platform, and only narrows a crash window
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


class Outbox:
    def __init__(self, root: str | Path):
        self.root = Path(root)

    def ensure(self) -> None:
        for name in SUBDIRECTORIES:
            (self.root / name).mkdir(parents=True, exist_ok=True)

    def ready_path(self, batch_id: str) -> Path:
        return self.root / "ready" / f"{batch_id}.json"

    def status_path(self, batch_id: str) -> Path:
        return self.root / "status" / f"{batch_id}.json"

    def _write_atomically(self, destination: Path, document: dict) -> None:
        """Write to tmp/ and rename into place, so a reader sees all of the file or none of it."""
        temporary = self.root / "tmp" / f"{destination.parent.name}-{destination.name}.part"
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(document, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)

    # --- Airflow's side ------------------------------------------------------------------------

    def write_batch(self, batch_id: str, features: list, source: dict) -> dict:
        """Publish a batch to ready/. Returns the batch's header (everything but the features)."""
        self.ensure()
        header = {
            "format": FORMAT,
            "batch_id": batch_id,
            "created_at": _now(),
            "source": source,
            "count": len(features),
            "sha256": features_sha256(features),
        }
        self._write_atomically(self.ready_path(batch_id), {**header, "features": features})
        return header

    def read_header(self, batch_id: str) -> dict | None:
        """The header of a batch that is waiting in ready/, or None."""
        try:
            document = json.loads(self.ready_path(batch_id).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        return {k: v for k, v in document.items() if k != "features"}

    def read_status(self, batch_id: str) -> dict | None:
        try:
            return json.loads(self.status_path(batch_id).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None

    # --- the collector's side ------------------------------------------------------------------

    def write_status(self, batch_id: str, state: str, **fields: object) -> None:
        self._write_atomically(self.status_path(batch_id), {"batch_id": batch_id, "state": state, **fields})

    def read_batch(self, path: Path) -> dict:
        """Load and validate a ready file. Raises BatchInvalid with the reason."""
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            raise BatchInvalid(f"unreadable: {exc}") from exc
        if not isinstance(document, dict) or document.get("format") != FORMAT:
            raise BatchInvalid("not a batch file of a known format")
        if document.get("batch_id") != path.stem:
            raise BatchInvalid("the batch id inside the file does not match its name")
        features = document.get("features")
        if not isinstance(features, list):
            raise BatchInvalid("no features list")
        if document.get("count") != len(features):
            raise BatchInvalid(f"header says {document.get('count')} features, file has {len(features)}")
        if document.get("sha256") != features_sha256(features):
            raise BatchInvalid("checksum does not match the features")
        if not isinstance(document.get("source"), dict):
            raise BatchInvalid("no source description")
        return document

    def reject(self, path: Path, reason: str, source: dict | None = None) -> None:
        batch_id = path.stem
        os.replace(path, self.root / "rejected" / path.name)
        self.write_status(batch_id, "rejected", reason=reason, rejected_at=_now(), **_source_fields(source))
        logger.error("outbox batch %s rejected: %s", batch_id, reason)

    def prune(self, older_than_days: float, now: float | None = None) -> int:
        """Delete finished status files, rejected batches and stray temporary files older than this."""
        cutoff = (now if now is not None else time.time()) - older_than_days * 86400
        removed = 0
        for name in ("status", "rejected", "tmp"):
            for path in (self.root / name).glob("*"):
                if not path.is_file() or path.stat().st_mtime >= cutoff:
                    continue
                if name == "status" and (self.read_status(path.stem) or {}).get("state") == "accepted":
                    continue  # still waiting for Kafka, keep the answer Airflow may read
                path.unlink()
                removed += 1
        return removed


def _source_fields(source: dict | None) -> dict:
    source = source or {}
    return {"queried_at": source.get("queried_at"), "updated_after": source.get("updated_after")}


@dataclass
class IntakeReport:
    accepted: list[BatchRecord] = field(default_factory=list)
    changes: list[Change] = field(default_factory=list)
    rejected: int = 0
    invalid_features: int = 0


class Intake:
    """Runs inside the collector's loop, in the same thread as everything else that writes its database."""

    def __init__(self, outbox: Outbox, detector: ChangeDetector):
        self.outbox = outbox
        self.detector = detector

    def ready_count(self) -> int:
        return len(list((self.outbox.root / "ready").glob("*.json")))

    def accept_ready(self) -> IntakeReport:
        """Validate and commit every complete batch in ready/, oldest name first."""
        self.outbox.ensure()
        report = IntakeReport()
        for path in sorted((self.outbox.root / "ready").glob("*.json")):
            try:
                document = self.outbox.read_batch(path)
            except BatchInvalid as exc:
                self.outbox.reject(path, str(exc))
                report.rejected += 1
                continue
            source = document["source"]
            normalized = normalize_features(document["features"], datetime.now(UTC))
            try:
                acceptance = self.detector.accept_batch(
                    document["batch_id"], document["sha256"], normalized.valid, len(document["features"]), source
                )
            except BatchConflict as exc:
                self.outbox.reject(path, str(exc), source)
                report.rejected += 1
                continue
            record = acceptance.record
            # The batch is durable in the database. Only now answer Airflow, and only then drop the file.
            self.outbox.write_status(
                record.batch_id,
                "published" if record.published_at else "accepted",
                sha256=record.sha256,
                accepted_at=record.accepted_at,
                published_at=record.published_at,
                feature_count=record.feature_count,
                valid_count=record.valid_count,
                rejected_count=record.rejected_count,
                new_revisions=record.new_revisions,
                **_source_fields(source),
            )
            path.unlink()
            report.accepted.append(record)
            report.changes.extend(acceptance.changes)
            report.invalid_features += len(normalized.rejected)
            logger.info(
                "outbox batch %s accepted: %d features, %d new revisions, %d invalid",
                record.batch_id, record.feature_count, record.new_revisions, record.rejected_count,
            )
        return report

    def settle(self) -> list[BatchRecord]:
        """Tell Airflow which accepted batches Kafka has now confirmed completely."""
        settled = self.detector.settle_batches()
        for record in settled:
            self.outbox.write_status(
                record.batch_id, "published", sha256=record.sha256, accepted_at=record.accepted_at,
                published_at=record.published_at, feature_count=record.feature_count,
                valid_count=record.valid_count, rejected_count=record.rejected_count,
                new_revisions=record.new_revisions, **_source_fields(record.source),
            )
            logger.info("outbox batch %s published: Kafka confirmed %d revisions", record.batch_id, record.new_revisions)
        return settled
