"""Container health check for the collector.

Healthy means the metrics endpoint answers and the collector is actually polling: some feed has
polled successfully within the last 20 minutes, which is longer than the slowest feed's interval
plus a retry. A collector that has only just started has no successful poll yet and gets the same
20 minutes to make one. An endpoint that answers while the polling loop is stuck is not healthy.

    python -m ingestion.healthcheck        exits 0 if healthy, 1 if not
"""

import os
import re
import sys
import time
import urllib.request

METRICS_URL = "http://127.0.0.1:9108/metrics"
MAX_SILENCE_SECONDS = 1200
_LAST_POLL = re.compile(r"^quakeflow_last_successful_poll_timestamp_seconds\{[^}]*\} ([0-9.e+]+)$", re.MULTILINE)


def evaluate(metrics_text: str, now: float, started_at: float, max_silence: float = MAX_SILENCE_SECONDS) -> tuple[bool, str]:
    """(healthy, reason) from the text of the metrics endpoint."""
    polls = [float(value) for value in _LAST_POLL.findall(metrics_text)]
    if polls:
        age = now - max(polls)
        if age <= max_silence:
            return True, f"last successful poll {age:.0f}s ago"
        return False, f"no successful poll for {age:.0f}s"
    uptime = now - started_at
    if uptime <= max_silence:
        return True, f"no successful poll yet, started {uptime:.0f}s ago"
    return False, f"no successful poll since starting {uptime:.0f}s ago"


def main() -> int:
    try:
        with urllib.request.urlopen(METRICS_URL, timeout=3) as response:
            text = response.read().decode("utf-8")
    except OSError as exc:
        print(f"unhealthy: metrics endpoint not reachable: {exc}")
        return 1
    started = os.stat("/proc/1").st_ctime  # when the container's main process started
    healthy, reason = evaluate(text, time.time(), started)
    print(("healthy: " if healthy else "unhealthy: ") + reason)
    return 0 if healthy else 1


if __name__ == "__main__":
    sys.exit(main())
