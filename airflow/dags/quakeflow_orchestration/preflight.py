"""Checks that run before a pipeline run starts, so a run fails early and says what is down.

Standard library only: this runs inside the Airflow image, which has none of the platform's packages.
Every check returns a Result instead of raising, and run_preflight raises one error that lists everything
that failed, so one look at the task log shows all the problems rather than the first.
"""

import json
import socket
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass

TIMEOUT_SECONDS = 5
FLINK_JOB_NAME = "quakeflow-validate-and-dedupe"


@dataclass(frozen=True)
class Result:
    name: str
    ok: bool
    detail: str


class PreflightFailed(Exception):
    pass


def check_tcp(name: str, host: str, port: int, connect: Callable = socket.create_connection) -> Result:
    try:
        connect((host, port), timeout=TIMEOUT_SECONDS).close()
    except OSError as error:
        return Result(name, False, f"cannot connect to {host}:{port} ({error})")
    return Result(name, True, f"{host}:{port} accepts connections")


def check_http(name: str, url: str, opener: Callable = urllib.request.urlopen) -> Result:
    try:
        with opener(url, timeout=TIMEOUT_SECONDS) as response:
            status = response.status
    except urllib.error.HTTPError as error:
        # The server answered, so it is up. Object stores answer 403 or 400 to a request without credentials.
        return Result(name, error.code < 500, f"{url} answered {error.code}")
    except (urllib.error.URLError, OSError) as error:
        return Result(name, False, f"cannot reach {url} ({error})")
    return Result(name, status < 500, f"{url} answered {status}")


def parse_flink_jobs(body: str, job_name: str = FLINK_JOB_NAME) -> Result:
    """The streaming job must exist and be RUNNING. Anything else means events are no longer flowing."""
    name = "flink job"
    try:
        jobs = json.loads(body)["jobs"]
    except (ValueError, KeyError, TypeError):
        return Result(name, False, "Flink answered with something that is not a job list")
    states = [job.get("state") for job in jobs if job.get("name") == job_name]
    if "RUNNING" in states:
        return Result(name, True, f"{job_name} is RUNNING")
    return Result(name, False, f"{job_name} is {states[-1] if states else 'not submitted'}, not RUNNING")


def check_flink_job(url: str, opener: Callable = urllib.request.urlopen) -> Result:
    try:
        with opener(url, timeout=TIMEOUT_SECONDS) as response:
            return parse_flink_jobs(response.read().decode("utf-8"))
    except (urllib.error.URLError, OSError) as error:
        return Result("flink job", False, f"cannot reach {url} ({error})")


def collector_check() -> Result:
    """The collector's metrics endpoint answers, so something is there to accept reconciliation batches."""
    return check_http("collector", "http://collector:9108/metrics")


def run_preflight(
    checks: list[Callable[[], Result]] | None = None,
    require_flink: bool = True,
) -> list[Result]:
    """Run every check. Raise PreflightFailed naming each failure, otherwise return the results."""
    if checks is None:
        checks = [
            lambda: check_tcp("kafka", "kafka", 19092),
            lambda: check_tcp("catalog database", "postgres", 5432),
            lambda: check_http("object storage", "http://rustfs:9000/"),
        ]
        if require_flink:
            checks.append(lambda: check_flink_job("http://flink-jobmanager:8081/jobs/overview"))
    results = [check() for check in checks]
    for result in results:
        print(f"[preflight] {'ok  ' if result.ok else 'FAIL'} {result.name}: {result.detail}", flush=True)
    failed = [result for result in results if not result.ok]
    if failed:
        raise PreflightFailed("; ".join(f"{result.name}: {result.detail}" for result in failed))
    return results
