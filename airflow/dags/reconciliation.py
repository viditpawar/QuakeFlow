"""Reconciliation: find revisions that the live feeds missed, and hand them to the collector.

    preflight -> read watermark -> fetch -> await acceptance -> advance watermark -> (wait published, prune)

The live feeds only show the last day. USGS keeps revising events for days afterwards, so every 6 hours this
asks the catalog for everything updated since the watermark, minus an overlap, and writes it to the outbox
volume as one batch. The collector (the only writer of its database) validates the batch, records the new
revisions as pending and answers. Delivery to Kafka is then the collector's usual at-least-once path.

The contract, in the order things happen:
  * fetch writes the file to tmp/ and renames it into ready/, so the collector never sees half a file.
  * The watermark moves only after the collector's acceptance has been read back, never because a task
    finished. A failed or missing acceptance leaves it, and the next run asks for the same period again.
  * Accepted does not mean delivered. The last tasks watch for the batch to be published, and are skipped
    (not failed) if Kafka is slow, because the delivery is the collector's job and has its own alerts.
All three steps that touch the outbox run the collector image, so normalization and hashing are the live path's.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Variable, dag, task
from docker.types import Mount
from quakeflow_orchestration.preflight import check_tcp, collector_check, run_preflight
from quakeflow_orchestration.watermark import KEY, next_watermark

COLLECTOR_IMAGE = os.getenv("COLLECTOR_IMAGE", "quakeflow/collector:0.1.0")
NETWORK = os.getenv("QUAKEFLOW_NETWORK", "quakeflow_default")
OUTBOX_VOLUME = os.getenv("OUTBOX_VOLUME", "quakeflow_reconcile-outbox")
NOT_PUBLISHED_EXIT_CODE = 99

DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True}


def collector_task(task_id: str, *arguments: str, **kwargs) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=COLLECTOR_IMAGE,
        command=["python", "-m", "ingestion.reconcile", *arguments],
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK,
        mounts=[Mount(source=OUTBOX_VOLUME, target="/outbox", type="volume")],
        mount_tmp_dir=False,
        auto_remove="success",
        mem_limit="256m",
        do_xcom_push=True,  # the last line the command prints, which is one line of JSON
        **kwargs,
    )


@dag(
    dag_id="reconciliation",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    tags=["quakeflow"],
)
def reconciliation():
    @task
    def preflight() -> None:
        # Kafka is checked too: the batch is only useful if the collector can deliver what it accepts.
        run_preflight([lambda: check_tcp("kafka", "kafka", 19092), collector_check])

    @task
    def read_watermark() -> str:
        return Variable.get(KEY, default=None) or ""

    batch = "{{ run_id }}"
    fetch = collector_task(
        "fetch",
        "fetch",
        "--batch-id", batch,
        "--updated-after={{ ti.xcom_pull(task_ids='read_watermark') or '' }}",
        execution_timeout=timedelta(minutes=15),
    )
    accepted = collector_task("await_acceptance", "await-ack", "--batch-id", batch, "--timeout", "600",
                              execution_timeout=timedelta(minutes=12))

    @task
    def advance_watermark(acknowledgement: str) -> str:
        new = next_watermark(Variable.get(KEY, default=None), acknowledgement)
        Variable.set(KEY, new)
        return new

    published = collector_task(
        "wait_published", "await-published", "--batch-id", batch, "--timeout", "900",
        skip_on_exit_code=NOT_PUBLISHED_EXIT_CODE, retries=0, execution_timeout=timedelta(minutes=17),
    )
    prune = collector_task("prune", "prune", retries=0)

    watermark = read_watermark()
    preflight() >> watermark >> fetch >> accepted
    moved = advance_watermark(accepted.output)
    moved >> [published, prune]


reconciliation()
