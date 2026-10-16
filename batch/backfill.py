"""Download historical USGS events window by window and prepare them for the Spark load.

    python -m batch.backfill --start 2025-01-01 --end 2025-01-08 --min-magnitude 2.5

The range is start inclusive, end exclusive, in UTC. It is cut into windows (7 days by default),
and any window that would match more than the service's 20,000-event limit is split in half
until it fits. For every window this writes, into the output directory:

    <window>.geojson        the response exactly as downloaded
    <window>.bronze.jsonl   one line per feature as received, with its SHA-256
    <window>.silver.jsonl   the valid features normalized by the same code as the live path
    <window>.rejected.jsonl the features that failed validation, with the reasons
    <window>.manifest.json  written last, so a window is complete only when it exists

Re-running the same command skips windows whose manifest is complete and whose download still
matches its checksum. Load the files into Iceberg with batch/ingest_history.py.
"""

import argparse
import hashlib
import json
import logging
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from ingestion.historical_client import MAX_EVENTS_PER_QUERY, FdsnClient
from ingestion.live.usgs_client import FeedError
from processing.normalize import normalize_features

logger = logging.getLogger(__name__)

SOURCE = "usgs-fdsn"
DEFAULT_OUT_DIR = "data/history"
MIN_WINDOW = timedelta(hours=1)
FILE_SUFFIXES = (".geojson", ".bronze.jsonl", ".silver.jsonl", ".rejected.jsonl")


class BackfillError(Exception):
    """A window could not be completed."""


@dataclass(frozen=True)
class Window:
    start: datetime
    end: datetime

    def label(self) -> tuple[str, str]:
        """Whole days read as dates, anything finer as date and time."""
        whole_days = all(t.hour == t.minute == t.second == 0 for t in (self.start, self.end))
        fmt = "%Y-%m-%d" if whole_days else "%Y-%m-%dT%H:%M:%S"
        return self.start.strftime(fmt), self.end.strftime(fmt)

    def window_id(self, min_magnitude: float | None) -> str:
        start, end = (label.replace(":", "") for label in self.label())
        magnitude = "all" if min_magnitude is None else f"{min_magnitude:g}"
        return f"{start}_{end}_m{magnitude}"

    def halves(self) -> tuple["Window", "Window"]:
        if self.end - self.start <= MIN_WINDOW:
            raise BackfillError(
                f"window {self.label()} still exceeds {MAX_EVENTS_PER_QUERY} events at one hour"
            )
        middle = self.start + (self.end - self.start) / 2
        middle = middle.replace(minute=0, second=0, microsecond=0)
        if middle <= self.start:
            middle = self.start + MIN_WINDOW
        return Window(self.start, middle), Window(middle, self.end)


def initial_windows(start: date, end: date, days: int) -> list[Window]:
    if start >= end:
        raise ValueError("--start must be before --end")
    if days < 1:
        raise ValueError("--window-days must be at least 1")
    windows, cursor = [], datetime(start.year, start.month, start.day, tzinfo=UTC)
    last = datetime(end.year, end.month, end.day, tzinfo=UTC)
    while cursor < last:
        windows.append(Window(cursor, min(cursor + timedelta(days=days), last)))
        cursor += timedelta(days=days)
    return windows


# --- files ---------------------------------------------------------------------------------


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def write_atomic(path: Path, data: bytes) -> None:
    """Write via a temporary file so a reader never sees a half-written file."""
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_bytes(data)
    os.replace(temporary, path)


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def lines(records: list[str]) -> bytes:
    return "".join(f"{record}\n" for record in records).encode("utf-8")


def read_manifest(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def is_complete(out_dir: Path, window_id: str) -> bool:
    """True if the manifest says completed and every file is there and the download is intact."""
    manifest = read_manifest(out_dir / f"{window_id}.manifest.json")
    if not manifest or manifest.get("status") != "completed":
        return False
    files = [out_dir / f"{window_id}{suffix}" for suffix in FILE_SUFFIXES]
    if not all(f.exists() for f in files):
        return False
    return sha256_bytes(files[0].read_bytes()) == manifest.get("checksum")


def normalize_window(
    body: bytes, window: Window, min_magnitude: float | None, checksum: str, downloaded_at: datetime
) -> dict[str, bytes]:
    """Turn a downloaded window into the three JSONL files. Deterministic for the same input."""
    features = json.loads(body)["features"]
    result = normalize_features(features, downloaded_at)
    window_start, window_end = window.start.date().isoformat(), window.end.date().isoformat()

    bronze = []
    for feature in features:
        payload = canonical(feature)
        feature_id = feature.get("id") if isinstance(feature, dict) else None
        properties = feature.get("properties") if isinstance(feature, dict) else None
        updated = properties.get("updated") if isinstance(properties, dict) else None
        bronze.append(
            canonical(
                {
                    "event_id": feature_id if isinstance(feature_id, str) else None,
                    "payload_sha256": sha256_bytes(payload.encode("utf-8")),
                    "source_updated_at": updated if isinstance(updated, int) else None,
                    "payload": payload,
                    "window_start": window_start,
                    "window_end": window_end,
                    "min_magnitude": min_magnitude,
                    "source_checksum": checksum,
                }
            )
        )
    rejected = [
        canonical(
            {
                "event_id": record.event_id,
                "reason": "; ".join(record.reasons),
                "payload": canonical(record.raw),
            }
        )
        for record in result.rejected
    ]
    return {
        ".bronze.jsonl": lines(bronze),
        ".silver.jsonl": lines([event.to_json() for event in result.valid]),
        ".rejected.jsonl": lines(rejected),
    }


# --- the run -------------------------------------------------------------------------------


@dataclass
class Summary:
    downloaded: int = 0
    skipped: int = 0
    split: int = 0
    events: int = 0
    valid: int = 0
    rejected: int = 0
    failed: list[str] = field(default_factory=list)


def process_window(
    client: FdsnClient,
    window: Window,
    min_magnitude: float | None,
    out_dir: Path,
    summary: Summary,
    force: bool = False,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> None:
    window_id = window.window_id(min_magnitude)
    if not force and is_complete(out_dir, window_id):
        logger.info("skip %s: already complete", window_id)
        summary.skipped += 1
        return

    count = client.count(window.start, window.end, min_magnitude)
    if count > MAX_EVENTS_PER_QUERY:
        logger.info("split %s: %d events is over the limit", window_id, count)
        summary.split += 1
        for half in window.halves():
            process_window(client, half, min_magnitude, out_dir, summary, force, clock)
        return

    manifest_path = out_dir / f"{window_id}.manifest.json"
    start_label, end_label = window.label()
    manifest = {
        "source": SOURCE,
        "start_date": start_label,
        "end_date": end_label,
        "min_magnitude": min_magnitude,
    }
    try:
        downloaded = client.fetch(window.start, window.end, min_magnitude)
        checksum = sha256_bytes(downloaded.body)
        downloaded_at = clock()
        files = normalize_window(downloaded.body, window, min_magnitude, checksum, downloaded_at)
    except (FeedError, ValueError, KeyError) as exc:
        write_atomic(manifest_path, lines([canonical({**manifest, "status": "failed", "error": str(exc)})]))
        raise BackfillError(f"{window_id}: {exc}") from exc

    # Data files first, the manifest last: the manifest is what makes a window count as done.
    write_atomic(out_dir / f"{window_id}.geojson", downloaded.body)
    for suffix, content in files.items():
        write_atomic(out_dir / f"{window_id}{suffix}", content)
    valid = files[".silver.jsonl"].count(b"\n")
    rejected = files[".rejected.jsonl"].count(b"\n")
    manifest.update(
        status="completed",
        record_count=downloaded.count,
        checksum=checksum,
        downloaded_at=downloaded_at.isoformat(),
        valid_count=valid,
        rejected_count=rejected,
    )
    write_atomic(manifest_path, (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode())
    logger.info("done %s: %d events (%d valid, %d rejected)", window_id, downloaded.count, valid, rejected)
    summary.downloaded += 1
    summary.events += downloaded.count
    summary.valid += valid
    summary.rejected += rejected


def run_backfill(
    client: FdsnClient,
    start: date,
    end: date,
    min_magnitude: float | None,
    out_dir: Path,
    window_days: int = 7,
    force: bool = False,
) -> Summary:
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = Summary()
    for window in initial_windows(start, end, window_days):
        try:
            process_window(client, window, min_magnitude, out_dir, summary, force)
        except (BackfillError, FeedError) as exc:
            logger.error("window failed: %s", exc)
            summary.failed.append(str(exc))
    return summary


def parse_date(text: str) -> date:
    return date.fromisoformat(text)


def parse_magnitude(text: str) -> float | None:
    """A minimum magnitude, or None for all magnitudes. An empty text means none, so an orchestrator can pass
    the option with no value."""
    return None if text.strip() == "" else float(text)


def main(argv: list[str] | None = None, client: FdsnClient | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--start", type=parse_date, required=True, help="first day, YYYY-MM-DD")
    parser.add_argument("--end", type=parse_date, required=True, help="day after the last, YYYY-MM-DD")
    parser.add_argument("--min-magnitude", type=parse_magnitude, default=None)
    parser.add_argument("--window-days", type=int, default=7)
    parser.add_argument("--out-dir", type=Path, default=Path(DEFAULT_OUT_DIR))
    parser.add_argument("--pause", type=float, default=1.0, help="seconds between requests")
    parser.add_argument("--force", action="store_true", help="download again even if complete")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    client = client or FdsnClient(pause_seconds=args.pause)
    try:
        summary = run_backfill(
            client, args.start, args.end, args.min_magnitude, args.out_dir, args.window_days, args.force
        )
    except ValueError as exc:
        logger.error("%s", exc)
        return 2
    logger.info(
        "windows: %d downloaded, %d skipped, %d split | events: %d (%d valid, %d rejected) | failed: %d",
        summary.downloaded, summary.skipped, summary.split, summary.events,
        summary.valid, summary.rejected, len(summary.failed),
    )
    return 1 if summary.failed else 0


if __name__ == "__main__":
    sys.exit(main())
