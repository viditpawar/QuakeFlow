"""Commands for the reconciliation DAG. Airflow runs them in the collector image (see ingestion/outbox.py).

    python -m ingestion.reconcile fetch --batch-id ID --updated-after 2026-10-15T00:00:00+00:00
    python -m ingestion.reconcile await-ack --batch-id ID
    python -m ingestion.reconcile await-published --batch-id ID
    python -m ingestion.reconcile prune

fetch asks USGS for every event updated after the watermark minus an overlap, and writes the answer to the
outbox as one batch. The overlap makes sure that an event updated while the previous run was fetching is not
missed: the same event is simply seen twice, and the collector's change detector keeps one copy of each
revision. Without a watermark, the first run looks back --initial-lookback-hours.

Each command prints one line of JSON last, which Airflow reads. Everything else goes to stderr.
Exit codes: 0 ok, 3 too many events for one query, 4 batch rejected, 5 no answer in time, 99 not published in time.
"""

import argparse
import json
import logging
import os
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingestion.historical_client import MAX_EVENTS_PER_QUERY, FdsnClient
from ingestion.live.usgs_client import FeedError
from ingestion.outbox import Outbox, safe_batch_id

logger = logging.getLogger(__name__)

DEFAULT_OUTBOX = os.getenv("QUAKEFLOW_OUTBOX_DIR", "/outbox")
DEFAULT_OVERLAP_HOURS = 48.0
DEFAULT_INITIAL_LOOKBACK_HOURS = 48.0
POLL_SECONDS = 5.0
EXIT_TOO_MANY, EXIT_REJECTED, EXIT_NO_ANSWER, EXIT_NOT_PUBLISHED = 3, 4, 5, 99


def parse_time(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def query_start(
    watermark: str | None, queried_at: datetime, overlap_hours: float, initial_lookback_hours: float
) -> datetime:
    """Where to start asking: the watermark minus the overlap, or a fixed look-back on the first run."""
    if watermark:
        return parse_time(watermark) - timedelta(hours=overlap_hours)
    return queried_at - timedelta(hours=initial_lookback_hours)


def fetch(args, client: FdsnClient | None = None, now: Callable[[], datetime] = lambda: datetime.now(UTC)) -> int:
    outbox = Outbox(args.outbox)
    outbox.ensure()
    batch_id = safe_batch_id(args.batch_id)

    # A retried task must not fetch and write a second batch under the same ID.
    existing = outbox.read_status(batch_id)
    if existing is not None and existing.get("state") != "rejected":
        print(json.dumps({"batch_id": batch_id, "queried_at": existing["queried_at"], "reused": "status"}))
        return 0
    header = outbox.read_header(batch_id)
    if header is not None:
        print(json.dumps({"batch_id": batch_id, "queried_at": header["source"]["queried_at"], "reused": "ready"}))
        return 0

    # The time is taken before the request, so anything updated after this moment is found next time.
    queried_at = now()
    start = query_start(args.updated_after, queried_at, args.overlap_hours, args.initial_lookback_hours)
    client = client or FdsnClient()
    expected = client.count_updated(start, args.min_magnitude)
    if expected > MAX_EVENTS_PER_QUERY:
        logger.error("%d events were updated after %s, more than one query may return (%d)", expected, start, MAX_EVENTS_PER_QUERY)
        return EXIT_TOO_MANY
    window = client.fetch_updated(start, args.min_magnitude)
    features = json.loads(window.body)["features"]
    source = {
        "kind": "usgs-fdsn-updatedafter",
        "queried_at": queried_at.isoformat(),
        "updated_after": start.isoformat(),
        "watermark": args.updated_after or None,
        "overlap_hours": args.overlap_hours,
        "min_magnitude": args.min_magnitude,
    }
    written = outbox.write_batch(batch_id, features, source)
    logger.info("batch %s written: %d events updated after %s", batch_id, len(features), start.isoformat())
    print(json.dumps({"batch_id": batch_id, "queried_at": source["queried_at"], "updated_after": source["updated_after"],
                      "count": written["count"], "sha256": written["sha256"]}))
    return 0


def wait_for_state(
    outbox: Outbox, batch_id: str, wanted: set[str], timeout: float,
    sleep: Callable[[float], None] = time.sleep, clock: Callable[[], float] = time.monotonic,
) -> dict | None:
    """The batch's status once its state is one of `wanted` or 'rejected', or None if time runs out."""
    deadline = clock() + timeout
    while True:
        status = outbox.read_status(batch_id)
        if status is not None and status.get("state") in wanted | {"rejected"}:
            return status
        if clock() >= deadline:
            return None
        sleep(POLL_SECONDS)


def await_ack(args, **kwargs) -> int:
    """Exit 0 only once the collector has durably accepted the batch. Airflow moves its watermark on that."""
    status = wait_for_state(Outbox(args.outbox), safe_batch_id(args.batch_id), {"accepted", "published"}, args.timeout, **kwargs)
    if status is None:
        logger.error("the collector has not answered for batch %s, is it running?", args.batch_id)
        return EXIT_NO_ANSWER
    print(json.dumps(status))
    return EXIT_REJECTED if status["state"] == "rejected" else 0


def await_published(args, **kwargs) -> int:
    status = wait_for_state(Outbox(args.outbox), safe_batch_id(args.batch_id), {"published"}, args.timeout, **kwargs)
    if status is None or status["state"] != "published":
        return EXIT_REJECTED if status and status["state"] == "rejected" else EXIT_NOT_PUBLISHED
    print(json.dumps(status))
    return 0


def prune(args) -> int:
    print(json.dumps({"removed": Outbox(args.outbox).prune(args.older_than_days)}))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--outbox", type=Path, default=DEFAULT_OUTBOX)
    commands = parser.add_subparsers(dest="command", required=True)

    fetch_parser = commands.add_parser("fetch", help="query USGS and write a batch to the outbox")
    fetch_parser.add_argument("--batch-id", required=True)
    fetch_parser.add_argument("--updated-after", default="", help="the watermark, empty on the first run")
    fetch_parser.add_argument("--overlap-hours", type=float, default=DEFAULT_OVERLAP_HOURS)
    fetch_parser.add_argument("--initial-lookback-hours", type=float, default=DEFAULT_INITIAL_LOOKBACK_HOURS)
    fetch_parser.add_argument("--min-magnitude", type=float, default=None)
    fetch_parser.set_defaults(run=fetch)

    for name, run, timeout in (("await-ack", await_ack, 600.0), ("await-published", await_published, 900.0)):
        sub = commands.add_parser(name)
        sub.add_argument("--batch-id", required=True)
        sub.add_argument("--timeout", type=float, default=timeout)
        sub.set_defaults(run=run)

    prune_parser = commands.add_parser("prune", help="remove old status files and rejected batches")
    prune_parser.add_argument("--older-than-days", type=float, default=14.0)
    prune_parser.set_defaults(run=prune)
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = build_parser().parse_args(argv)
    try:
        return args.run(args)
    except (FeedError, ValueError) as exc:
        logger.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
