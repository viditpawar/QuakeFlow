"""Flink health: every 5 minutes, check the streaming job, and bring it back if it is gone.

    check ──healthy / recovering / stalled / unreachable──> end (a stalled or unreachable job fails the run)
          └─resubmit──> resubmit from the checkpoint ──> confirm the job is healthy again

Flink and the collector are long-running services, and this DAG never restarts them on a schedule. The job already
retries by itself after a failure, so a job that is RESTARTING is left alone. The one thing it cannot do is come
back after being cancelled, after ending for good, or after the JobManager forgot it. Then this DAG submits it
again from the retained checkpoint of the job it last saw healthy, in the Flink image, and checks that it works.

It does not guess: with no known earlier job, with no complete checkpoint, or with the JobManager unreachable, it
fails the run and leaves the decision to a person. Starting from the beginning of the topic would emit every
revision a second time (docs/recovery-runbook.md).
"""

import os
import time
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Variable, dag, task
from docker.types import Mount
from quakeflow_orchestration.flink_health import (
    HEALTHY,
    RESUBMIT,
    VARIABLE,
    fetch_health,
    recovery_source,
)

FLINK_IMAGE = os.getenv("FLINK_IMAGE", "quakeflow/flink:2.2.1")
NETWORK = os.getenv("QUAKEFLOW_NETWORK", "quakeflow_default")
CHECKPOINT_VOLUME = os.getenv("FLINK_CHECKPOINT_VOLUME", "quakeflow_flink-checkpoints")
CONFIRM_SECONDS = 240


def now_ms() -> int:
    return int(time.time() * 1000)


@dag(
    dag_id="flink_health",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # two runs could both decide to resubmit
    default_args={"retries": 0},
    dagrun_timeout=timedelta(minutes=15),
    tags=["quakeflow"],
)
def flink_health():
    @task.branch
    def check(ti=None) -> str:
        health = fetch_health(now_ms())
        print(f"[flink_health] {health.state}: {health.detail}")
        if health.state == HEALTHY:
            Variable.set(VARIABLE, health.job_id)  # remembered, because recovery needs this job's checkpoints
        if health.state == RESUBMIT:
            source = recovery_source(health, Variable.get(VARIABLE, default=None))
            if source is None:
                raise RuntimeError(f"{health.detail}, and no earlier job is known to recover from")
            ti.xcom_push(key="recover_from", value=source)
            return "resubmit"
        if health.state in ("stalled", "unreachable"):
            raise RuntimeError(health.detail)  # visible in Airflow, and not something a resubmit would fix
        return "all_clear"

    @task
    def all_clear() -> None:
        print("[flink_health] nothing to do")

    resubmit = DockerOperator(
        task_id="resubmit",
        image=FLINK_IMAGE,
        # Not ["/opt/flink/resubmit.sh", ...]: Airflow reads an argument that ends in .sh as a template file to load.
        command=["sh", "-c", 'exec /opt/flink/resubmit.sh "$0"', "{{ ti.xcom_pull(task_ids='check', key='recover_from') }}"],
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK,
        mounts=[Mount(source=CHECKPOINT_VOLUME, target="/opt/flink/checkpoints", type="volume", read_only=True)],
        mount_tmp_dir=False,
        auto_remove="success",
        mem_limit="768m",
        execution_timeout=timedelta(minutes=5),
    )

    @task(trigger_rule="none_failed_min_one_success", execution_timeout=timedelta(minutes=8))
    def confirm() -> str:
        """After a resubmit: the job must be RUNNING and checkpointing again, or this run fails."""
        deadline = time.monotonic() + CONFIRM_SECONDS
        while True:
            health = fetch_health(now_ms())
            if health.state == HEALTHY:
                Variable.set(VARIABLE, health.job_id)
                print(f"[flink_health] recovered: {health.detail}")
                return health.job_id
            if time.monotonic() >= deadline:
                raise RuntimeError(f"the job did not become healthy within {CONFIRM_SECONDS} s: {health.detail}")
            time.sleep(10)

    decision = check()
    decision >> all_clear()
    decision >> resubmit >> confirm()


flink_health()
