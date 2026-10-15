"""What the collector container needs: a health check that notices a stuck loop, and every package it imports."""

import ast
import re
import sys
from pathlib import Path

import pytest

from ingestion.healthcheck import MAX_SILENCE_SECONDS, evaluate

ROOT = Path(__file__).resolve().parents[2]
NOW = 1_800_000_000.0


def metrics(*ages: float) -> str:
    lines = ["# HELP quakeflow_last_successful_poll_timestamp_seconds x"]
    for i, age in enumerate(ages):
        lines.append(f'quakeflow_last_successful_poll_timestamp_seconds{{feed="feed{i}"}} {NOW - age}')
    lines.append("quakeflow_pending_revisions 0.0")
    return "\n".join(lines) + "\n"


# --- the health check ----------------------------------------------------------------------


def test_a_recent_successful_poll_is_healthy():
    healthy, reason = evaluate(metrics(30), NOW, started_at=NOW - 3600)
    assert healthy and "30s ago" in reason


def test_the_newest_feed_counts_so_a_slow_feed_does_not_make_it_unhealthy():
    # all_hour polled a minute ago, all_day ten minutes ago: healthy because something is polling.
    assert evaluate(metrics(60, 600), NOW, started_at=NOW - 3600)[0]


def test_a_collector_that_stopped_polling_is_unhealthy_even_though_the_endpoint_answers():
    healthy, reason = evaluate(metrics(MAX_SILENCE_SECONDS + 60), NOW, started_at=NOW - 7200)
    assert not healthy and "no successful poll" in reason


def test_a_new_collector_gets_time_to_make_its_first_poll():
    assert evaluate(metrics(), NOW, started_at=NOW - 60)[0]


def test_a_collector_that_never_managed_a_poll_becomes_unhealthy():
    healthy, reason = evaluate(metrics(), NOW, started_at=NOW - MAX_SILENCE_SECONDS - 60)
    assert not healthy and "since starting" in reason


@pytest.mark.parametrize("age", [0, MAX_SILENCE_SECONDS])
def test_the_boundary_is_inclusive(age):
    assert evaluate(metrics(age), NOW, started_at=NOW - 7200)[0]


# --- the image has everything the code imports -----------------------------------------------


def locked_names() -> set[str]:
    lock = (ROOT / "docker" / "collector-requirements.txt").read_text(encoding="utf-8")
    return {m.lower().replace("_", "-") for m in re.findall(r"^([A-Za-z0-9_.-]+)==", lock, re.MULTILINE)}


def direct_names() -> set[str]:
    source = (ROOT / "docker" / "collector-requirements.in").read_text(encoding="utf-8")
    return {re.split(r"[<>=!~\[ ]", line)[0].lower().replace("_", "-")
            for line in source.splitlines() if line.strip() and not line.startswith("#")}


def imported_by_the_collector() -> set[str]:
    names = set()
    for folder in ("ingestion", "schemas", "processing"):
        for path in (ROOT / folder).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if isinstance(node, ast.Import):
                    names |= {a.name.split(".")[0] for a in node.names}
                elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                    names.add(node.module.split(".")[0])
    return {n for n in names if n not in sys.stdlib_module_names and n not in {"ingestion", "schemas", "processing"}}


def test_every_package_the_collector_imports_is_installed_in_the_image():
    distribution = {"yaml": "pyyaml", "confluent_kafka": "confluent-kafka", "prometheus_client": "prometheus-client"}
    missing = {n for n in imported_by_the_collector()
               if distribution.get(n, n).lower().replace("_", "-") not in locked_names()}
    assert not missing, f"imported by the collector but not installed by docker/collector-requirements.txt: {missing}"


def test_the_lock_file_contains_every_direct_requirement_with_hashes():
    lock = (ROOT / "docker" / "collector-requirements.txt").read_text(encoding="utf-8")
    assert direct_names() <= locked_names(), "regenerate collector-requirements.txt from the .in file"
    packages = len(locked_names())
    assert lock.count("--hash=sha256:") >= packages, "every locked package needs at least one hash"


def test_the_dockerfile_copies_the_packages_the_collector_needs():
    dockerfile = (ROOT / "docker" / "collector.Dockerfile").read_text(encoding="utf-8")
    for needed in ("ingestion", "schemas", "processing", "configs/sources.yaml"):
        assert f"COPY {needed}" in dockerfile, f"the image does not copy {needed}"
    assert "--require-hashes" in dockerfile
    assert "USER 10001" in dockerfile, "the collector must not run as root"
    assert "127.0.0.1:9092" not in dockerfile and "localhost" not in dockerfile.replace("127.0.0.1:9108", "")
    assert "KAFKA_BOOTSTRAP_SERVERS=kafka:19092" in dockerfile.replace("\\\n", " ").replace("  ", " ")
