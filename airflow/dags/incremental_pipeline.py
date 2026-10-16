"""Incremental pipeline: Kafka into the lakehouse, then the Gold tables, every 30 minutes.

    preflight -> ingest (Spark) -> quality gate and Gold (Spark)

The collector and the Flink job stay running as services. This DAG only checks that they are healthy, and
never restarts them. Gold is built only if ingestion succeeded (the default trigger rule), and the quality gate
inside build_gold.py still decides whether anything is published.

Spark runs as the platform's own image, started through the Docker socket. The containers join the compose
network and use the checkpoint volume, so they behave like "docker compose run spark". Credentials are passed
as private environment, which Airflow does not show in the UI or the rendered task.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag, task
from docker.types import Mount
from quakeflow_orchestration.preflight import run_preflight

SPARK_IMAGE = os.getenv("SPARK_IMAGE", "quakeflow/spark:4.0.4")
NETWORK = os.getenv("QUAKEFLOW_NETWORK", "quakeflow_default")
CHECKPOINT_VOLUME = os.getenv("CHECKPOINT_VOLUME", "quakeflow_spark-checkpoints")
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"

DEFAULT_ARGS = {"retries": 2, "retry_delay": timedelta(minutes=2), "retry_exponential_backoff": True}


def spark_task(task_id: str, script: str, *arguments: str, **kwargs) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=SPARK_IMAGE,
        command=[SPARK_SUBMIT, f"/workspace/batch/{script}", *arguments],
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK,
        mounts=[Mount(source=CHECKPOINT_VOLUME, target="/checkpoints", type="volume")],
        mount_tmp_dir=False,  # the default bind-mounts a temp folder from the Airflow container's file system
        pool="quakeflow_spark",  # one Spark container at a time, across all DAGs
        auto_remove="success",  # a failed container stays for "docker logs"
        mem_limit="3g",  # same as the spark service in docker-compose.yml (D-033)
        environment={
            "POSTGRES_DB": os.getenv("POSTGRES_DB", "iceberg_catalog"),
            "POSTGRES_USER": os.getenv("POSTGRES_USER", "quakeflow"),
            "S3_ACCESS_KEY": os.getenv("S3_ACCESS_KEY", "quakeflow"),
            "WAREHOUSE_BUCKET": os.getenv("WAREHOUSE_BUCKET", "warehouse"),
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:19092",
            "PUSHGATEWAY_URL": os.getenv("PUSHGATEWAY_URL", ""),
        },
        private_environment={
            "POSTGRES_PASSWORD": os.getenv("POSTGRES_PASSWORD", ""),
            "S3_SECRET_KEY": os.getenv("S3_SECRET_KEY", ""),
        },
        **kwargs,
    )


@dag(
    dag_id="incremental_pipeline",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # two runs at once would compete for the same checkpoint
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=25),
    tags=["quakeflow"],
)
def incremental_pipeline():
    @task
    def preflight() -> None:
        run_preflight()

    # Ingestion records the Silver snapshots it finished with under this run ID, and Gold reads those snapshots,
    # so the two stay tied together even if a load starts while Gold is building.
    ingest = spark_task(
        "ingest_live", "ingest_live.py", "--run-id", "{{ run_id }}", execution_timeout=timedelta(minutes=20)
    )
    gold = spark_task(
        "build_gold", "build_gold.py", "--load-run-id", "{{ run_id }}", execution_timeout=timedelta(minutes=20)
    )

    preflight() >> ingest >> gold


incremental_pipeline()
