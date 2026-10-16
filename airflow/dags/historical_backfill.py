"""Historical backfill, started by hand with a date range and an optional minimum magnitude.

    validate parameters -> preflight -> download -> ingest -> verify the range is in Silver -> quality gate and Gold

Trigger it from the UI or with:
    airflow dags trigger historical_backfill --conf '{"start": "2025-02-01", "end": "2025-02-08", "min_magnitude": 4.5}'
(end is the day after the last day wanted).

The download runs batch/backfill.py in the collector image and leaves one set of files per window on the
`quakeflow_history-files` volume, which Docker creates on first use. The load reads that volume in the Spark
image. A window that was already downloaded and is intact is skipped, and every write is a MERGE, so running the
same range twice changes nothing. The load takes every completed window on the volume, not only this range.
The verification then checks that every revision downloaded for this range is in Silver, and Gold is built from
the Silver snapshots this load recorded (D-028). Spark tasks share a pool of one slot with the other DAGs.
"""

import os
from datetime import datetime, timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import Param, dag, task
from docker.types import Mount
from quakeflow_orchestration.backfill_params import validate_backfill
from quakeflow_orchestration.preflight import check_http, check_tcp, run_preflight

COLLECTOR_IMAGE = os.getenv("COLLECTOR_IMAGE", "quakeflow/collector:0.1.0")
SPARK_IMAGE = os.getenv("SPARK_IMAGE", "quakeflow/spark:4.0.4")
NETWORK = os.getenv("QUAKEFLOW_NETWORK", "quakeflow_default")
HISTORY_VOLUME = os.getenv("HISTORY_VOLUME", "quakeflow_history-files")

DEFAULT_ARGS = {"retries": 1, "retry_delay": timedelta(minutes=2)}

# The same values in the three places they are used. An empty minimum magnitude means all magnitudes.
RANGE = [
    "--start={{ params.start }}",
    "--end={{ params.end }}",
    "--min-magnitude={{ params.min_magnitude if params.min_magnitude is not none else '' }}",
]


def docker_task(task_id: str, image: str, command: list[str], history_target: str, read_only: bool, **kwargs):
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        docker_url="unix://var/run/docker.sock",
        network_mode=NETWORK,
        mounts=[Mount(source=HISTORY_VOLUME, target=history_target, type="volume", read_only=read_only)],
        mount_tmp_dir=False,
        auto_remove="success",
        **kwargs,
    )


def spark_task(task_id: str, script: str, *arguments: str, history_read_only: bool = True, **kwargs):
    return docker_task(
        task_id,
        SPARK_IMAGE,
        ["/opt/spark/bin/spark-submit", f"/workspace/batch/{script}", *arguments],
        "/data/history",
        history_read_only,
        pool="quakeflow_spark",
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
    dag_id="historical_backfill",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params={
        "start": Param("2025-01-01", type="string", format="date", description="First day, UTC"),
        "end": Param("2025-01-08", type="string", format="date", description="The day after the last day wanted"),
        "min_magnitude": Param(None, type=["null", "number"], description="Leave empty for all magnitudes"),
    },
    tags=["quakeflow"],
)
def historical_backfill():
    @task
    def validate_parameters(params=None) -> dict:
        checked = validate_backfill(params["start"], params["end"], params["min_magnitude"])
        print(f"backfill of {checked}")
        return checked

    @task
    def preflight() -> None:
        # No Kafka and no Flink: a backfill goes from the catalog to the lakehouse directly.
        run_preflight([
            lambda: check_tcp("catalog database", "postgres", 5432),
            lambda: check_http("object storage", "http://rustfs:9000/"),
            lambda: check_http("USGS catalog", "https://earthquake.usgs.gov/fdsnws/event/1/version"),
        ])

    download = docker_task(
        "download", COLLECTOR_IMAGE,
        ["python", "-m", "batch.backfill", *RANGE, "--out-dir", "/history"],
        "/history", read_only=False, mem_limit="512m", execution_timeout=timedelta(hours=2),
    )
    ingest = spark_task("ingest_history", "ingest_history.py", "--run-id", "{{ run_id }}",
                        execution_timeout=timedelta(hours=1))
    verify = spark_task("verify_history", "verify_history.py", *RANGE, execution_timeout=timedelta(minutes=30))
    gold = spark_task("build_gold", "build_gold.py", "--load-run-id", "{{ run_id }}",
                      execution_timeout=timedelta(minutes=30))

    validate_parameters() >> preflight() >> download >> ingest >> verify >> gold


historical_backfill()
