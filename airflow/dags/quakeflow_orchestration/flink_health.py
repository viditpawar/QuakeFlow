"""Is the Flink job healthy, and if not, what should be done about it?

Standard library only, because it runs inside the Airflow image.

The job restarts itself after a failure (it has an unlimited exponential-delay restart strategy), so a job
that is RESTARTING is working on it and is left alone. What it cannot do for itself is come back after being
cancelled, after ending for good, or after the JobManager lost it. That is the one case where this module says
to resubmit, from the retained checkpoint of the job that Airflow last saw running.
"""

import json
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass

JOB_NAME = "quakeflow-validate-and-dedupe"
BASE_URL = "http://flink-jobmanager:8081"
VARIABLE = "quakeflow_flink_job_id"
TIMEOUT_SECONDS = 5
MAX_CHECKPOINT_AGE_SECONDS = 300  # checkpoints are taken every 10 seconds

HEALTHY, RECOVERING, RESUBMIT, STALLED, UNREACHABLE = "healthy", "recovering", "resubmit", "stalled", "unreachable"
LIVE_STATES = {"RUNNING"}
SELF_RECOVERING_STATES = {"CREATED", "INITIALIZING", "RESTARTING", "RECONCILING", "SUSPENDED_RECOVERY"}


@dataclass(frozen=True)
class Health:
    state: str  # one of the constants above
    detail: str
    job_id: str | None = None  # the job this is about, when there is one


def newest_job(overview: dict, name: str = JOB_NAME) -> dict | None:
    """The most recently started job with this name."""
    jobs = [job for job in overview.get("jobs", []) if job.get("name") == name]
    return max(jobs, key=lambda job: job.get("start-time", 0), default=None)


def checkpoint_age_seconds(checkpoints: dict, job: dict, now_ms: int) -> float | None:
    """Seconds since the last completed checkpoint. For a job with none yet, since it started."""
    completed = (checkpoints.get("latest") or {}).get("completed") or {}
    acknowledged = completed.get("latest_ack_timestamp")
    if acknowledged:
        return max(0.0, (now_ms - acknowledged) / 1000)
    started = job.get("start-time")
    return None if started is None else max(0.0, (now_ms - started) / 1000)


def classify(
    overview: dict, checkpoints: dict | None, now_ms: int, max_age_seconds: float = MAX_CHECKPOINT_AGE_SECONDS
) -> Health:
    job = newest_job(overview)
    if job is None:
        return Health(RESUBMIT, f"{JOB_NAME} is not known to the JobManager")
    state, job_id = job.get("state"), job.get("jid")
    if state in LIVE_STATES:
        age = checkpoint_age_seconds(checkpoints or {}, job, now_ms)
        if age is None or age > max_age_seconds:
            seen = "no checkpoint yet" if age is None else f"last checkpoint {age:.0f} s ago"
            return Health(STALLED, f"{JOB_NAME} is RUNNING but is not checkpointing: {seen}", job_id)
        return Health(HEALTHY, f"{JOB_NAME} is RUNNING, last checkpoint {age:.0f} s ago", job_id)
    if state in SELF_RECOVERING_STATES:
        return Health(RECOVERING, f"{JOB_NAME} is {state}, Flink is retrying it by itself", job_id)
    return Health(RESUBMIT, f"{JOB_NAME} is {state}", job_id)


def _get(url: str, opener: Callable) -> dict:
    with opener(url, timeout=TIMEOUT_SECONDS) as response:
        document = json.loads(response.read().decode("utf-8"))
    if not isinstance(document, dict):
        raise TypeError(f"{url} did not answer with a JSON object")
    return document


def fetch_health(
    now_ms: int, base_url: str = BASE_URL, opener: Callable = urllib.request.urlopen,
    max_age_seconds: float = MAX_CHECKPOINT_AGE_SECONDS,
) -> Health:
    """Ask the JobManager. If it cannot be reached, that is a finding too, not an exception."""
    try:
        overview = _get(f"{base_url}/jobs/overview", opener)
        job = newest_job(overview)
        checkpoints = None
        if job is not None and job.get("state") in LIVE_STATES:
            checkpoints = _get(f"{base_url}/jobs/{job['jid']}/checkpoints", opener)
    except (urllib.error.URLError, OSError, ValueError, TypeError, KeyError) as error:
        return Health(UNREACHABLE, f"cannot get the job status from {base_url} ({error})")
    return classify(overview, checkpoints, now_ms, max_age_seconds)


def recovery_source(health: Health, remembered_job_id: str | None) -> str | None:
    """Whose checkpoints to resume from: the job Airflow last saw healthy, else the job the JobManager
    still lists. The checkpoint volume also holds directories of other jobs (test runs), which must never be used."""
    return remembered_job_id or health.job_id
