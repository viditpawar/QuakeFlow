"""Benchmark the whole pipeline on synthetic data, on this machine, and report what was measured.

    python -m benchmarks.run_benchmark --sizes 2000 10000 40000 --repeats 3

For each size N and each repeat, on throwaway topics and namespaces (the real job SQL, the real Spark jobs):

    produce      N distinct revisions plus 10 percent exact repeats, shuffled, into the raw topic
    flink        until every distinct revision is visible to a read_committed consumer of the validated topic
    ingest       batch/ingest_live.py, wall time of the container, and the same job on empty topics as a baseline
    gold         batch/build_gold.py, wall time of the container
    checks       the row counts in Bronze, Silver and Gold, so a broken pipeline is never timed

Needs the core, streaming and batch services running. Pause the Airflow DAGs first, so nothing else competes for
the machine. The synthetic data is a measurement tool and says nothing about USGS: the shape of real data
(few revisions, bursts after large earthquakes) is different. Numbers are for this machine only.
"""

import argparse
import contextlib
import ctypes
import json
import platform
import random
import re
import statistics
import subprocess
import sys
import threading
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition

from processing.normalize import normalize_feature
from tests.flink_support import (
    BOOTSTRAP,
    FIXTURE,
    ROOT,
    cancel_job,
    create_topics,
    delete_topics,
    stack_is_up,
    submit_job,
)
from tests.spark_support import lakehouse_is_up, remove_checkpoint, spark_sql

RESULTS_DIR = ROOT / "benchmarks" / "results"
CREATE_TABLES = (ROOT / "iceberg" / "sql" / "002_create_tables.sql").read_text()
BRONZE_TABLES = ["live_observations", "historical_observations"]
SILVER_TABLES = ["earthquake_versions", "earthquakes_current", "rejected_records", "load_log"]
GOLD_VIEWS = [
    "daily_seismic_activity", "regional_activity", "magnitude_distribution", "revision_statistics",
    "ingestion_latency",
]
GOLD_TABLES = [f"{v}_builds" for v in GOLD_VIEWS] + ["build_log", "build_manifest", "published_build"]
PLACES = ["10 km N of Town, California", "Fiji region", "5 km S of Place, Alaska", "Offshore, Japan",
          "3 km E of Spot, Nevada", "Kermadec Islands, New Zealand", "12 km W of Village, Oklahoma"]
BASE_TIME = datetime(2025, 3, 1, tzinfo=UTC)


def log(message: str) -> None:
    print(f"[benchmark] {message}", file=sys.stderr, flush=True)


# --- data -------------------------------------------------------------------------------------

def synthetic_messages(total: int, tag: str, seed: int = 7) -> tuple[list[tuple[str, str]], dict]:
    """`total` distinct revisions: 90 percent first versions and 10 percent second versions, plus exact repeats
    of 10 percent of them, in random order. Returns the messages and what the pipeline must end up with."""
    rng = random.Random(seed)
    template = json.loads(FIXTURE.read_text())["features"][0]
    revisions = total // 10
    events = total - revisions
    ingested = BASE_TIME
    messages: list[tuple[str, str]] = []

    def message(event_id: str, i: int, version: int) -> tuple[str, str]:
        feature = json.loads(json.dumps(template))
        happened = BASE_TIME + timedelta(seconds=97 * i)
        feature["id"] = event_id
        feature["properties"].update(
            mag=round(rng.uniform(-0.5, 7.5), 1) if version == 0 else round(rng.uniform(0.0, 7.5), 1),
            time=int(happened.timestamp() * 1000),
            updated=int((happened + timedelta(minutes=5 + 30 * version)).timestamp() * 1000),
            place=PLACES[i % len(PLACES)],
            type="earthquake",
        )
        feature["geometry"]["coordinates"] = [rng.uniform(-179, 179), rng.uniform(-80, 80), rng.uniform(0, 300)]
        return event_id, normalize_feature(feature, ingested).to_json()

    for i in range(events):
        messages.append(message(f"bench{tag}-{i:07d}", i, 0))
    for i in range(revisions):
        messages.append(message(f"bench{tag}-{i:07d}", i, 1))
    messages.extend(rng.sample(messages, total // 10))
    rng.shuffle(messages)
    distinct = {(k, json.loads(v)["record_hash"]) for k, v in messages}
    assert len(distinct) == total, (len(distinct), total)
    return messages, {"revisions": total, "events": events, "raw_messages": len(messages)}


# --- measurement ------------------------------------------------------------------------------

def free_memory_gb() -> float | None:
    if sys.platform != "win32":
        return None

    class Status(ctypes.Structure):
        _fields_ = [("length", ctypes.c_ulong), ("load", ctypes.c_ulong)] + [
            (name, ctypes.c_ulonglong)
            for name in ("total", "avail", "pagetotal", "pageavail", "virtualtotal", "virtualavail", "ext")
        ]

    status = Status()
    status.length = ctypes.sizeof(status)
    ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
    return round(status.avail / 2**30, 2)


def _mib(text: str) -> float:
    """'519.1MiB' or '1.2GiB' as mebibytes."""
    match = re.fullmatch(r"([\d.]+)\s*([KMG])i?B", text.strip())
    if not match:
        raise ValueError(f"not a memory size: {text!r}")
    return float(match.group(1)) * {"K": 1 / 1024, "M": 1.0, "G": 1024.0}[match.group(2)]


class ResourceSampler(threading.Thread):
    """Peak memory of each group of containers, of all of them together, and the lowest free host memory."""

    GROUPS = ("kafka", "flink-taskmanager", "flink-jobmanager", "rustfs", "postgres", "spark-run")

    def __init__(self) -> None:
        super().__init__(daemon=True)
        self.stop_flag = threading.Event()
        self.peak: dict[str, float] = {}
        self.peak_total = 0.0
        self.min_free_gb: float | None = None

    def run(self) -> None:
        while not self.stop_flag.is_set():
            result = subprocess.run(
                ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}"],
                capture_output=True, text=True, check=False,
            )
            total = 0.0
            for line in result.stdout.splitlines():
                with contextlib.suppress(ValueError, IndexError):
                    name, usage = line.split("\t")
                    used = _mib(usage.split("/")[0].strip())
                    total += used
                    for group in self.GROUPS:
                        if group in name:
                            self.peak[group] = max(self.peak.get(group, 0.0), used)
            self.peak_total = max(self.peak_total, total)
            free = free_memory_gb()
            if free is not None:
                self.min_free_gb = free if self.min_free_gb is None else min(self.min_free_gb, free)
            self.stop_flag.wait(2)

    def finish(self) -> dict:
        self.stop_flag.set()
        self.join(timeout=30)
        return {"peak_container_mib": {k: round(v) for k, v in sorted(self.peak.items())},
                "peak_all_containers_mib": round(self.peak_total), "min_free_host_gb": self.min_free_gb}


def machine() -> dict:
    info = {"os": platform.platform(), "python": platform.python_version(), "logical_cpus": None,
            "docker_cpus": None, "docker_memory_gb": None, "host_free_gb_at_start": free_memory_gb()}
    with contextlib.suppress(OSError, ValueError):
        cpus, memory = subprocess.run(
            ["docker", "info", "--format", "{{.NCPU}} {{.MemTotal}}"], capture_output=True, text=True, check=False
        ).stdout.split()
        info["docker_cpus"], info["docker_memory_gb"] = int(cpus), round(int(memory) / 2**30, 1)
    import os

    info["logical_cpus"] = os.cpu_count()
    if sys.platform == "win32":
        with contextlib.suppress(OSError):
            info["cpu"] = subprocess.run(
                ["powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name"],
                capture_output=True, text=True, check=False,
            ).stdout.strip()
    return info


class Watcher:
    def __init__(self, topic: str, expected: int, timeout: float = 900, settle: float = 10):
        self.consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP, "group.id": f"bench-{uuid.uuid4()}",
            "isolation.level": "read_committed", "enable.auto.commit": False, "auto.offset.reset": "earliest",
        })
        partitions = self.consumer.list_topics(topic, timeout=10).topics[topic].partitions
        self.consumer.assign([TopicPartition(topic, p, 0) for p in partitions])
        self.expected, self.timeout, self.settle = expected, timeout, settle

    def wait(self) -> tuple[float | None, int]:
        start = time.monotonic()
        seen, reached, until = 0, None, None
        while True:
            message = self.consumer.poll(0.2)
            if message is not None and not message.error():
                seen += 1
            now = time.monotonic()
            if seen >= self.expected and reached is None:
                reached, until = now - start, now + self.settle
            if until is not None and now > until:
                break
            if now - start > self.timeout:
                break
        self.consumer.close()
        return reached, seen


def produce_all(topic: str, messages: list[tuple[str, str]]) -> float:
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20, "queue.buffering.max.messages": 200000})
    start = time.monotonic()
    for key, value in messages:
        while True:
            try:
                producer.produce(topic, key=key, value=value)
                break
            except BufferError:
                producer.poll(0.1)
    assert producer.flush(120) == 0
    return time.monotonic() - start


def timed(function, *args, **kwargs) -> tuple[float, object]:
    start = time.monotonic()
    value = function(*args, **kwargs)
    return time.monotonic() - start, value


def run_logged(command: list[str], label: str, timeout: int = 1800) -> str:
    """Run a Spark container. On failure keep its whole output and exit code in a file, because the cause
    (for example 137, which means killed for memory) is usually not in the last few lines."""
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False)
    output = result.stdout + result.stderr
    if result.returncode != 0:
        folder = RESULTS_DIR / "failures"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{label}.log"
        path.write_text(f"exit code {result.returncode}\n\n{output}", encoding="utf-8")
        raise RuntimeError(f"{label} failed with exit code {result.returncode}, full output in {path.relative_to(ROOT)}")
    return output


def ingest_live(lake: "Lake") -> str:
    names = lake.names
    return run_logged(
        ["docker", "compose", "--profile", "core", "--profile", "batch", "run", "--rm", "-T", "spark",
         "/opt/spark/bin/spark-submit", "/workspace/batch/ingest_live.py", "--topic", names["validated"],
         "--dlq-topic", names["dlq"], "--bronze-ns", lake.bronze, "--silver-ns", lake.silver,
         "--checkpoint-dir", lake.checkpoint],
        "ingest",
    )


def build_gold(bronze: str, silver: str, gold: str) -> str:
    return run_logged(
        ["docker", "compose", "--profile", "batch", "run", "--rm", "-T", "spark", "/opt/spark/bin/spark-submit",
         "/workspace/batch/build_gold.py", "--bronze-ns", bronze, "--silver-ns", silver, "--gold-ns", gold,
         "--pin-now"],
        "gold",
    )


# --- one run ----------------------------------------------------------------------------------

class Lake:
    """Throwaway topics and namespaces, removed even when the run fails."""

    def __enter__(self):
        self.names = create_topics()
        suffix = self.names["suffix"]
        self.bronze, self.silver, self.gold = (f"local.zb{suffix}_{k}" for k in ("bronze", "silver", "gold"))
        self.checkpoint = f"/checkpoints/bench_{suffix}"
        self.job_id = None
        script = CREATE_TABLES.replace("local.bronze", self.bronze).replace("local.silver", self.silver)
        spark_sql(f"CREATE NAMESPACE {self.bronze}; CREATE NAMESPACE {self.silver}; CREATE NAMESPACE {self.gold};\n{script}")
        return self

    def __exit__(self, *exc_info) -> None:
        if self.job_id:
            cancel_job(self.job_id)
        drops = [f"DROP TABLE IF EXISTS {self.bronze}.{t} PURGE;" for t in BRONZE_TABLES]
        drops += [f"DROP TABLE IF EXISTS {self.silver}.{t} PURGE;" for t in SILVER_TABLES]
        drops += [f"DROP VIEW IF EXISTS {self.gold}.{v};" for v in GOLD_VIEWS]
        drops += [f"DROP TABLE IF EXISTS {self.gold}.{t} PURGE;" for t in GOLD_TABLES]
        drops += [f"DROP NAMESPACE IF EXISTS {ns};" for ns in (self.bronze, self.silver, self.gold)]
        with contextlib.suppress(AssertionError):
            spark_sql(" ".join(drops))
        with contextlib.suppress(Exception):
            remove_checkpoint(self.checkpoint)
        delete_topics(self.names)


def check_counts(lake: Lake, expected: dict) -> dict:
    rows = spark_sql(
        f"SELECT 'bronze', count(*) FROM {lake.bronze}.live_observations;"
        f"SELECT 'versions', count(*) FROM {lake.silver}.earthquake_versions;"
        f"SELECT 'current', count(*) FROM {lake.silver}.earthquakes_current;"
        f"SELECT 'rejected', count(*) FROM {lake.silver}.rejected_records;"
        f"SELECT 'gold_events', coalesce(sum(event_count), 0) FROM {lake.gold}.daily_seismic_activity;"
        f"SELECT 'gold_status', status FROM {lake.gold}.build_log ORDER BY built_at DESC LIMIT 1;"
    )
    found = dict(rows)
    wanted = {"bronze": expected["revisions"], "versions": expected["revisions"], "current": expected["events"],
              "rejected": 0, "gold_events": expected["events"]}
    wrong = {k: (found.get(k), v) for k, v in wanted.items() if str(found.get(k)) != str(v)}
    if found.get("gold_status") != "promoted":
        wrong["gold_status"] = (found.get("gold_status"), "promoted")
    return {"counts": found, "correct": not wrong, "wrong": wrong}


def one_run(size: int, repeat: int, startup: float) -> dict:
    messages, expected = synthetic_messages(size, f"{size}r{repeat}", seed=1000 * repeat + 7)
    run: dict = {"size": size, "repeat": repeat, "expected": expected}
    sampler = ResourceSampler()
    with Lake() as lake:
        sampler.start()
        try:
            names = lake.names
            seconds, lake.job_id = timed(submit_job, names)
            run["flink_submit_s"] = round(seconds, 1)
            watcher = Watcher(names["validated"], expected["revisions"])
            run["produce_s"] = round(produce_all(names["raw"], messages), 2)
            reached, seen = watcher.wait()
            run["flink_visible_s"] = None if reached is None else round(reached, 2)
            run["validated_seen"] = seen
            if reached is None or seen != expected["revisions"]:
                raise RuntimeError(f"Flink output: {seen} visible, {expected['revisions']} expected")
            seconds, _ = timed(ingest_live, lake)
            run["ingest_wall_s"] = round(seconds, 1)
            run["ingest_net_s"] = round(max(0.0, seconds - startup), 1)
            seconds, _ = timed(build_gold, lake.bronze, lake.silver, lake.gold)
            run["gold_wall_s"] = round(seconds, 1)
            run.update(check_counts(lake, expected))
        except Exception as exc:  # noqa: BLE001 - a failed run is a result, not something to hide
            text = f"{type(exc).__name__}: {exc}"
            run["error"] = text if len(text) <= 3500 else text[:500] + " [...] " + text[-3000:]  # the cause is at the end
            run["correct"] = False
        finally:
            run["resources"] = sampler.finish()
    return run


def baseline_startup() -> float:
    """What a Spark ingest costs when there is nothing to read: container, JVM, Spark and catalog start-up."""
    with Lake() as lake:
        seconds, _ = timed(ingest_live, lake)
    return round(seconds, 1)


# --- summary ----------------------------------------------------------------------------------

def spread(values: list[float]) -> str:
    values = [v for v in values if v is not None]
    if not values:
        return "n/a"
    return f"{statistics.median(values):.1f} ({min(values):.1f} to {max(values):.1f})"


def end_to_end_s(run: dict) -> float | None:
    """From the first message produced until every distinct revision is visible in the validated topic."""
    if run.get("produce_s") is None or run.get("flink_visible_s") is None:
        return None
    return run["produce_s"] + run["flink_visible_s"]


def rate(run: dict, seconds: float | None) -> float | None:
    return None if not seconds else run["size"] / seconds


def summary_table(runs: list[dict], compact: bool = False) -> str:
    """Median with the range in brackets, over the runs whose counts were all correct."""
    if compact:
        header = ["Revisions", "Through Kafka and Flink (per s)", "Spark ingest (per s)", "Gold build (s)", "Peak memory, all containers (MiB)"]
    else:
        header = ["Revisions", "Runs correct", "Produce (s)", "Produce start to all validated (s)",
                  "Flink (per s)", "Spark ingest, net (s)", "Spark ingest (per s)", "Gold build (s)",
                  "Peak memory, all containers (MiB)"]
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for size in sorted({r["size"] for r in runs}):
        group = [r for r in runs if r["size"] == size]
        ok = [r for r in group if r.get("correct")]
        flink = spread([rate(r, end_to_end_s(r)) for r in ok])
        ingest_rate = spread([rate(r, r.get("ingest_net_s")) for r in ok])
        gold = spread([r.get("gold_wall_s") for r in ok])
        memory = spread([r["resources"]["peak_all_containers_mib"] for r in ok])
        if compact:
            cells = [f"{size:,}", flink, ingest_rate, gold, memory]
        else:
            cells = [f"{size:,}", f"{len(ok)} of {len(group)}", spread([r.get("produce_s") for r in ok]),
                     spread([end_to_end_s(r) for r in ok]), flink, spread([r.get("ingest_net_s") for r in ok]),
                     ingest_rate, gold, memory]
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sizes", type=int, nargs="+", default=[2000, 10000, 40000])
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--summarize", type=Path, help="print the table for a results file and exit")
    args = parser.parse_args(argv)
    if args.summarize:
        runs = json.loads(args.summarize.read_text())["runs"]
        print(summary_table(runs))
        print()
        print(summary_table(runs, compact=True))
        return 0
    if not (stack_is_up() and lakehouse_is_up()):
        log("Kafka, Flink and the lakehouse must be running")
        return 2

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    started = datetime.now(UTC)
    out = RESULTS_DIR / f"{started:%Y%m%dT%H%M%SZ}.json"
    document = {"started": started.isoformat(), "machine": machine(), "sizes": args.sizes, "repeats": args.repeats, "runs": []}
    log("measuring Spark start-up on empty topics")
    document["spark_startup_s"] = baseline_startup()
    log(f"start-up {document['spark_startup_s']} s")
    for repeat in range(args.repeats):
        for size in args.sizes:
            log(f"size {size}, repeat {repeat + 1} of {args.repeats}")
            run = one_run(size, repeat, document["spark_startup_s"])
            document["runs"].append(run)
            out.write_text(json.dumps(document, indent=2), encoding="utf-8")
            log(json.dumps({k: run.get(k) for k in ("size", "correct", "flink_visible_s", "ingest_net_s", "gold_wall_s", "error")}))
    document["finished"] = datetime.now(UTC).isoformat()
    out.write_text(json.dumps(document, indent=2), encoding="utf-8")
    print(summary_table(document["runs"]))
    log(f"results in {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
