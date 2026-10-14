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
