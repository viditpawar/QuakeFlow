"""The reconciliation watermark: when the last batch that the collector durably accepted was queried.

Standard library only, because it runs inside the Airflow image.
"""

import json
from datetime import UTC, datetime

KEY = "quakeflow_reconcile_watermark"
ACCEPTED_STATES = {"accepted", "published"}


class NotAcknowledged(Exception):
    """The collector has not durably accepted this batch, so the watermark must stay where it is."""


def _parse(text: str) -> datetime:
    value = datetime.fromisoformat(text)
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def next_watermark(current: str | None, acknowledgement: str | dict | None) -> str:
    """The watermark after a batch. It moves to the time the batch was queried, and only if the collector's
    answer says it accepted the batch. It never moves backwards, so a late retry cannot undo a later run."""
    if isinstance(acknowledgement, str):
        try:
            acknowledgement = json.loads(acknowledgement.strip().splitlines()[-1])
        except (ValueError, IndexError):
            acknowledgement = None
    if not isinstance(acknowledgement, dict) or acknowledgement.get("state") not in ACCEPTED_STATES:
        raise NotAcknowledged(f"no acceptance from the collector: {acknowledgement!r}")
    queried_at = acknowledgement.get("queried_at")
    if not queried_at:
        raise NotAcknowledged("the acknowledgement does not say when the batch was queried")
    if current and _parse(current) >= _parse(queried_at):
        return current
    return _parse(queried_at).isoformat()
