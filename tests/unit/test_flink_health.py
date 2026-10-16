"""Deciding whether the Flink job is healthy, and the guards of the script that submits it again."""

import json
import shutil
import subprocess
import sys
import urllib.error
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "airflow" / "dags"))

from quakeflow_orchestration import flink_health as fh

NOW = 1_800_000_000_000
JOB = "a" * 32


def overview(*jobs):
    return {"jobs": [{"jid": j, "name": fh.JOB_NAME, "start-time": s, "state": st} for j, s, st in jobs]}


def checkpoints(seconds_ago):
    return {"latest": {"completed": {"latest_ack_timestamp": NOW - int(seconds_ago * 1000)}}}


def test_a_running_job_that_checkpoints_is_healthy():
    health = fh.classify(overview((JOB, NOW - 600_000, "RUNNING")), checkpoints(12), NOW)
    assert health.state == fh.HEALTHY and health.job_id == JOB and "12 s ago" in health.detail


def test_a_running_job_that_stopped_checkpointing_is_stalled_not_resubmitted():
    health = fh.classify(overview((JOB, NOW - 3_600_000, "RUNNING")), checkpoints(900), NOW)
    assert health.state == fh.STALLED


def test_a_job_that_just_started_has_time_to_take_its_first_checkpoint():
    young = overview((JOB, NOW - 30_000, "RUNNING"))
    assert fh.classify(young, {"latest": {"completed": None}}, NOW).state == fh.HEALTHY
    old = overview((JOB, NOW - 3_600_000, "RUNNING"))
    assert fh.classify(old, {"latest": {"completed": None}}, NOW).state == fh.STALLED


@pytest.mark.parametrize("state", ["RESTARTING", "INITIALIZING", "CREATED", "RECONCILING"])
def test_a_job_that_is_restarting_by_itself_is_left_alone(state):
    assert fh.classify(overview((JOB, NOW, state)), None, NOW).state == fh.RECOVERING


@pytest.mark.parametrize("state", ["FAILED", "CANCELED", "FINISHED", "SUSPENDED", "CANCELLING"])
def test_a_job_that_ended_must_be_resubmitted(state):
    health = fh.classify(overview((JOB, NOW, state)), None, NOW)
    assert health.state == fh.RESUBMIT and health.job_id == JOB


def test_a_job_the_jobmanager_does_not_know_must_be_resubmitted():
    assert fh.classify({"jobs": []}, None, NOW).state == fh.RESUBMIT
    other = {"jobs": [{"jid": JOB, "name": "someone else's job", "start-time": NOW, "state": "RUNNING"}]}
    assert fh.classify(other, None, NOW).state == fh.RESUBMIT


def test_the_newest_job_of_the_name_decides():
    jobs = overview((JOB, NOW - 5000, "FAILED"), ("b" * 32, NOW - 1000, "RUNNING"))
    assert fh.classify(jobs, checkpoints(5), NOW).job_id == "b" * 32
    jobs = overview(("c" * 32, NOW - 5000, "RUNNING"), (JOB, NOW - 1000, "FAILED"))
    assert fh.classify(jobs, None, NOW).state == fh.RESUBMIT


class Reply:
    def __init__(self, body):
        self.body = body

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return json.dumps(self.body).encode()


def test_fetch_health_reads_the_overview_then_the_checkpoints_of_a_running_job():
    asked = []

    def opener(url, timeout):
        asked.append(url)
        return Reply(overview((JOB, NOW - 600_000, "RUNNING")) if url.endswith("overview") else checkpoints(8))

    assert fh.fetch_health(NOW, opener=opener).state == fh.HEALTHY
    assert asked[1].endswith(f"/jobs/{JOB}/checkpoints")


def test_an_unreachable_jobmanager_is_reported_not_raised():
    def down(url, timeout):
        raise urllib.error.URLError("refused")

    assert fh.fetch_health(NOW, opener=down).state == fh.UNREACHABLE
    assert fh.fetch_health(NOW, opener=lambda url, timeout: Reply("not an overview")).state == fh.UNREACHABLE


def test_recovery_uses_the_job_airflow_saw_healthy_before_whatever_the_jobmanager_lists():
    health = fh.Health(fh.RESUBMIT, "x", job_id="b" * 32)
    assert fh.recovery_source(health, JOB) == JOB
    assert fh.recovery_source(health, None) == "b" * 32
    assert fh.recovery_source(fh.Health(fh.RESUBMIT, "x"), None) is None


# --- the DAG and the script ---------------------------------------------------------------------

def test_the_flink_health_dag_has_the_agreed_shape():
    source = (ROOT / "airflow" / "dags" / "flink_health.py").read_text(encoding="utf-8")
    assert 'schedule="*/5 * * * *"' in source and "max_active_runs=1" in source
    assert "decision >> resubmit >> confirm()" in source and "decision >> all_clear()" in source
    assert "read_only=True" in source, "recovery only reads the checkpoint volume"
    assert "restart_container" not in source and "docker.restart" not in source, "Flink is never restarted by schedule"


@pytest.fixture
def script_env(tmp_path):
    """resubmit.sh with a fake curl and sql-client and a fake file system, run in a real POSIX shell."""
    shell = shutil.which("sh")
    if shell is None or subprocess.run([shell, "-c", "exit 0"], check=False).returncode != 0:
        pytest.skip("no POSIX shell")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "curl").write_text(f"#!/bin/sh\ncat {tmp_path.as_posix()}/overview.json\n", newline="\n")
    (bin_dir / "sql-client.sh").write_text(
        f"#!/bin/sh\nfor last; do :; done\ncp \"$last\" {tmp_path.as_posix()}/submitted.sql\n", newline="\n"
    )
    (tmp_path / "overview.json").write_text(json.dumps({"jobs": []}))
    root = tmp_path.as_posix()
    script = (
        (ROOT / "flink" / "resubmit.sh").read_text(encoding="utf-8")
        .replace("curl -sf", f"{bin_dir.as_posix()}/curl -sf")
        .replace("/opt/flink/checkpoints", f"{root}/checkpoints")
        .replace("/opt/flink/sql", f"{root}/sql")
        .replace("/tmp/job.sql", f"{root}/job.sql")
        .replace("exec /opt/flink/bin/sql-client.sh", f"exec {bin_dir.as_posix()}/sql-client.sh")
    )
    (tmp_path / "resubmit.sh").write_text(script, newline="\n")
    sql = tmp_path / "sql"
    sql.mkdir()
    for name in ("kafka_sources", "validated_events", "revision_tracking"):
        (sql / f"{name}.sql").write_text(f"-- {name}\n")
    return tmp_path


def run_script(env, job_id):
    return subprocess.run(
        [shutil.which("sh"), (env / "resubmit.sh").as_posix(), job_id], capture_output=True, text=True, check=False
    )


def make_checkpoint(env, job_id, number, complete=True):
    directory = env / "checkpoints" / job_id / f"chk-{number}"
    directory.mkdir(parents=True)
    if complete:
        (directory / "_metadata").write_text("x")


def test_the_script_resumes_from_the_newest_complete_checkpoint_of_the_named_job(script_env):
    make_checkpoint(script_env, JOB, 9)
    make_checkpoint(script_env, JOB, 100)
    make_checkpoint(script_env, JOB, 101, complete=False)  # still being written
    make_checkpoint(script_env, "b" * 32, 5000)  # another job's, newer and bigger: must never be used
    result = run_script(script_env, JOB)
    assert result.returncode == 0, result.stderr
    submitted = (script_env / "job.sql").read_text()
    assert f"{JOB}/chk-100'" in submitted and "chk-101" not in submitted and "b" * 32 not in submitted
    marks = ("-- kafka_sources", "-- validated_events", "state-recovery.path", "-- revision_tracking")
    order = [submitted.index(m) for m in marks]
    assert order == sorted(order), "the recovery path must be set before the job statements"


def test_the_script_refuses_to_start_from_the_beginning_without_a_checkpoint(script_env):
    make_checkpoint(script_env, JOB, 3, complete=False)
    result = run_script(script_env, JOB)
    assert result.returncode == 3 and "no complete checkpoint" in result.stderr
    assert not (script_env / "job.sql").exists()


def test_the_script_does_not_start_a_second_copy(script_env):
    make_checkpoint(script_env, JOB, 3)
    running = (
        '{"jobs":[{"jid":"x","name":"quakeflow-validate-and-dedupe","start-time":1,"end-time":-1,'
        '"duration":5,"state":"RUNNING","last-modification":2}]}'
    )
    (script_env / "overview.json").write_text(running)
    result = run_script(script_env, JOB)
    assert result.returncode == 0 and "already running" in result.stderr
    assert not (script_env / "job.sql").exists()


@pytest.mark.parametrize("bad", ["", "xyz", "A" * 32, "a" * 31, "../" + "a" * 29, "a" * 32 + ";rm"])
def test_the_script_accepts_only_a_flink_job_id(script_env, bad):
    assert run_script(script_env, bad).returncode == 4
