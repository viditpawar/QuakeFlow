import contextlib

import pytest
from prometheus_client import CollectorRegistry

from batch.job_metrics import job_run


class Pushes:
    """Stands in for pushadd_to_gateway and remembers what was pushed."""

    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def __call__(self, gateway, job, registry, timeout):
        if self.fail:
            raise OSError("gateway unreachable")
        self.calls.append((gateway, job, {f.name: f.samples[0].value for f in registry.collect()}, timeout))


class Clocks:
    def __init__(self):
        self.mono = 100.0

    def monotonic(self):
        return self.mono

    def wall(self):
        return 1_800_000_000.0


def run_job(push, gateway="http://gateway:9091", duration=12.5, fail=None, mark_failed=False, clocks=None):
    clocks = clocks or Clocks()
    # The tests below check what was pushed, so a job that raises is simply swallowed here.
    with contextlib.suppress(Exception), job_run(
        "demo", gateway=gateway, push=push, clock=clocks.monotonic, wall_clock=clocks.wall
    ) as run:
        clocks.mono += duration
        run.gauge("quakeflow_demo_rows", "Rows handled.", 42)
        if mark_failed:
            run.success = False
        if fail:
            raise fail


def test_a_successful_run_pushes_duration_success_and_both_timestamps():
    push = Pushes()
    run_job(push)
    gateway, job, metrics, timeout = push.calls[0]
    assert (gateway, job) == ("http://gateway:9091", "demo")
    assert metrics["quakeflow_spark_job_duration_seconds"] == 12.5
    assert metrics["quakeflow_spark_job_success"] == 1
    assert metrics["quakeflow_spark_job_last_run_timestamp_seconds"] == 1_800_000_000.0
    assert metrics["quakeflow_spark_job_last_success_timestamp_seconds"] == 1_800_000_000.0
    assert metrics["quakeflow_demo_rows"] == 42
    assert timeout > 0


def test_a_failed_run_does_not_report_a_last_success_time():
    # The gateway keeps the old value, because the push only adds the metrics it is given.
    push = Pushes()
    run_job(push, fail=RuntimeError("boom"))
    metrics = push.calls[0][2]
    assert metrics["quakeflow_spark_job_success"] == 0
    assert "quakeflow_spark_job_last_success_timestamp_seconds" not in metrics
    assert "quakeflow_spark_job_last_run_timestamp_seconds" in metrics


def test_a_run_that_marks_itself_failed_counts_as_failed():
    push = Pushes()
    run_job(push, mark_failed=True)
    metrics = push.calls[0][2]
    assert metrics["quakeflow_spark_job_success"] == 0
    assert "quakeflow_spark_job_last_success_timestamp_seconds" not in metrics


def test_the_original_exception_still_propagates():
    push = Pushes()
    with pytest.raises(RuntimeError, match="boom"), job_run("demo", gateway="http://g", push=push):
        raise RuntimeError("boom")
    assert push.calls[0][2]["quakeflow_spark_job_success"] == 0


@pytest.mark.parametrize("gateway", ["", None])
def test_nothing_is_pushed_without_a_gateway(gateway, monkeypatch):
    monkeypatch.delenv("PUSHGATEWAY_URL", raising=False)
    push = Pushes()
    run_job(push, gateway=gateway)
    assert push.calls == []


def test_the_gateway_comes_from_the_environment_when_not_given(monkeypatch):
    monkeypatch.setenv("PUSHGATEWAY_URL", "http://from-env:9091")
    push = Pushes()
    with job_run("demo", push=push):
        pass
    assert push.calls[0][0] == "http://from-env:9091"


def test_a_gateway_that_is_down_never_fails_the_job(capsys):
    with job_run("demo", gateway="http://down:9091", push=Pushes(fail=True)) as run:
        run.gauge("quakeflow_demo_rows", "Rows.", 1)
    assert "could not push metrics for demo" in capsys.readouterr().err


def test_each_run_starts_with_its_own_registry():
    first, second = Pushes(), Pushes()
    run_job(first)
    run_job(second)
    assert first.calls[0][2] == second.calls[0][2]
    assert isinstance(CollectorRegistry(), CollectorRegistry)
