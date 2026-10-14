"""Report how a Spark job went to a Prometheus Pushgateway.

Spark jobs run as one-off containers, so Prometheus cannot scrape them. Each job pushes a few
numbers when it finishes instead. Pushing is switched off unless PUSHGATEWAY_URL is set, and a
failure to push is logged and never fails the job.

Metrics, all labelled with the job name through the grouping key:
    quakeflow_spark_job_duration_seconds            how long the last run took
    quakeflow_spark_job_success                     1 if the last run succeeded, else 0
    quakeflow_spark_job_last_run_timestamp_seconds  when the last run finished
    quakeflow_spark_job_last_success_timestamp_seconds  when the last successful run finished

The push adds to what the gateway already holds for the job, so a failed run does not erase the
time of the last success. Runs inside the Spark image, whose Python is 3.10.
"""

import os
import sys
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager

from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

PUSH_TIMEOUT_SECONDS = 5


class JobRun:
    """Handed to the job so it can add its own numbers and mark a non-exception failure."""

    def __init__(self, registry: CollectorRegistry):
        self._registry = registry
        self.success = True

    def gauge(self, name: str, description: str, value: float) -> None:
        Gauge(name, description, registry=self._registry).set(value)


@contextmanager
def job_run(
    job: str,
    gateway: str | None = None,
    push: Callable[..., None] = pushadd_to_gateway,
    clock: Callable[[], float] = time.monotonic,
    wall_clock: Callable[[], float] = time.time,
) -> Iterator[JobRun]:
    """Time a job and push the outcome. An exception counts as a failure and still propagates."""
    gateway = gateway if gateway is not None else os.getenv("PUSHGATEWAY_URL", "")
    registry = CollectorRegistry()
    run = JobRun(registry)
    started = clock()
    try:
        yield run
    except BaseException:
        run.success = False
        raise
    finally:
        finished = wall_clock()
        Gauge("quakeflow_spark_job_duration_seconds", "Duration of the last run.", registry=registry).set(
            clock() - started
        )
        Gauge("quakeflow_spark_job_success", "1 if the last run succeeded.", registry=registry).set(
            1 if run.success else 0
        )
        Gauge(
            "quakeflow_spark_job_last_run_timestamp_seconds", "When the last run finished.", registry=registry
        ).set(finished)
        if run.success:
            Gauge(
                "quakeflow_spark_job_last_success_timestamp_seconds",
                "When the last successful run finished.",
                registry=registry,
            ).set(finished)
        if gateway:
            try:
                push(gateway, job=job, registry=registry, timeout=PUSH_TIMEOUT_SECONDS)
            except Exception as exc:  # noqa: BLE001 - monitoring must never fail a data job
                print(f"[job_metrics] could not push metrics for {job}: {exc}", file=sys.stderr, flush=True)
