"""Resend revisions that the collector sent but that never reached the lakehouse.

When Kafka loses messages that it had confirmed, the collector still records them as sent and will
not send them again. This marks every sent revision that is missing from a list of what the lakehouse
holds as pending, so the collector sends it again, in full. Stop the collector first: its state file
has a single writer.

    python -m ingestion.resend --known known_revisions.tsv            # dry run: shows what would be resent
    python -m ingestion.resend --known known_revisions.tsv --apply

The known file has one revision per line, event id and record hash separated by a tab, for example the
output of: SELECT event_id, record_hash FROM local.silver.earthquake_versions
"""

import argparse
import os
import re
import sys
from pathlib import Path

from ingestion.change_detector import ChangeDetector

_HASH = re.compile(r"^[0-9a-f]{64}$")
DEFAULT_LIMIT = 1000


def parse_known(text: str) -> set[tuple[str, str]]:
    """Revisions listed in the text. Lines that are not 'event id <tab> 64-character hash', such as
    log output mixed into a query result, are ignored."""
    known = set()
    for line in text.splitlines():
        fields = line.rstrip("\r").split("\t")
        if len(fields) == 2 and fields[0] and _HASH.match(fields[1]):
            known.add((fields[0], fields[1]))
    return known


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", default=os.getenv("QUAKEFLOW_STATE_DB", "data/quakeflow_state.db"))
    parser.add_argument("--known", type=Path, required=True, help="revisions the lakehouse holds")
    parser.add_argument("--apply", action="store_true", help="make the change (the default is a dry run)")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="refuse to resend more than this")
    args = parser.parse_args(argv)

    known = parse_known(args.known.read_text(encoding="utf-8"))
    if not known:
        print("refusing: the known file lists no revisions, so every revision would be resent", file=sys.stderr)
        return 2
    with ChangeDetector(args.db) as detector:
        missing = detector.mark_unsent_unless_known(known, dry_run=True)
        print(f"{len(known)} revisions known downstream; {len(missing)} sent revisions are missing from them")
        for event_id, record_hash in missing[:20]:
            print(f"  {event_id}  {record_hash[:12]}")
        if len(missing) > args.limit:
            print(f"refusing: {len(missing)} is more than --limit {args.limit}. Check the known file.", file=sys.stderr)
            return 2
        if not args.apply:
            print("dry run: nothing changed. Add --apply to mark them unsent.")
            return 0
        detector.mark_unsent_unless_known(known, dry_run=False)
        print(f"marked {len(missing)} revisions unsent; the collector will send them when it starts")
    return 0


if __name__ == "__main__":
    sys.exit(main())
