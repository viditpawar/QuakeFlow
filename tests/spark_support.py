"""Run Spark SQL in the real Spark container and return result rows (for integration tests)."""

import subprocess

from tests.flink_support import ROOT


def spark_sql(sql: str, timeout: int = 600) -> list[list[str]]:
    """Run statements with spark-sql -e. Returns the tab-separated result rows of all queries."""
    result = subprocess.run(
        ["docker", "compose", "--profile", "batch", "run", "--rm", "-T", "spark",
         "/opt/spark/bin/spark-sql", "-e", sql],
        cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False,
    )
    if result.returncode != 0:
        raise AssertionError(f"spark-sql failed:\n{result.stdout}\n{result.stderr[-3000:]}")
    return [
        line.split("\t")
        for line in result.stdout.splitlines()
        if line.strip() and not line.startswith("Time taken")
    ]


def lakehouse_is_up() -> bool:
    names = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True, check=False
    ).stdout.split()
    return "quakeflow-postgres" in names and "quakeflow-rustfs" in names


def run_ingest(
    topic: str, dlq_topic: str, bronze: str, silver: str, checkpoint: str, timeout: int = 900
) -> str:
    """Run batch/ingest_live.py once in the Spark container. Returns its log output."""
    result = subprocess.run(
        ["docker", "compose", "--profile", "core", "--profile", "batch", "run", "--rm", "-T",
         "spark", "/opt/spark/bin/spark-submit", "/workspace/batch/ingest_live.py",
         "--topic", topic, "--dlq-topic", dlq_topic, "--bronze-ns", bronze,
         "--silver-ns", silver, "--checkpoint-dir", checkpoint],
        cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False,
    )
    if result.returncode != 0:
        raise AssertionError(f"ingest failed:\n{result.stdout[-1500:]}\n{result.stderr[-4000:]}")
    return result.stdout + result.stderr  # compose may put the container's stderr on stdout


def remove_checkpoint(path: str) -> None:
    subprocess.run(
        ["docker", "compose", "--profile", "batch", "run", "--rm", "-T", "--entrypoint", "rm",
         "spark", "-rf", path],
        cwd=ROOT, capture_output=True, text=True, timeout=300, check=False,
    )


def run_ingest_history(input_dir: str, bronze: str, silver: str, timeout: int = 900) -> str:
    """Run batch/ingest_history.py once in the Spark container. Returns its log output."""
    result = subprocess.run(
        ["docker", "compose", "--profile", "batch", "run", "--rm", "-T", "spark",
         "/opt/spark/bin/spark-submit", "/workspace/batch/ingest_history.py",
         "--input-dir", input_dir, "--bronze-ns", bronze, "--silver-ns", silver],
        cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False,
    )
    if result.returncode != 0:
        raise AssertionError(f"ingest_history failed:\n{result.stdout[-1500:]}\n{result.stderr[-4000:]}")
    return result.stdout + result.stderr  # compose may put the container's stderr on stdout
