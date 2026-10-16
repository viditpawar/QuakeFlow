"""The pre-flight checks, the Airflow setup script, and the compose rules that keep Airflow contained."""

import importlib.util
import json
import re
import sys
import urllib.error
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "airflow" / "dags"))

from quakeflow_orchestration import preflight
from quakeflow_orchestration.preflight import PreflightFailed, Result

spec = importlib.util.spec_from_file_location("airflow_setup", ROOT / "scripts" / "airflow_setup.py")
setup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(setup)

COMPOSE = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))


# --- pre-flight ----------------------------------------------------------------------------

def jobs(*states, name=preflight.FLINK_JOB_NAME):
    return json.dumps({"jobs": [{"name": name, "state": s} for s in states]})


@pytest.mark.parametrize(
    ("body", "ok", "fragment"),
    [
        (jobs("RUNNING"), True, "RUNNING"),
        (jobs("FAILED", "RUNNING"), True, "RUNNING"),
        (jobs("FAILED"), False, "is FAILED"),
        (jobs("RUNNING", name="another job"), False, "not submitted"),
        (json.dumps({"jobs": []}), False, "not submitted"),
        ("<html>", False, "not a job list"),
        (json.dumps({"other": 1}), False, "not a job list"),
    ],
)
def test_the_flink_job_must_be_running(body, ok, fragment):
    result = preflight.parse_flink_jobs(body)
    assert result.ok is ok and fragment in result.detail


def test_an_unreachable_port_fails_and_a_reachable_one_passes():
    class Closed:
        def close(self):
            pass

    def refuses(*_args, **_kwargs):
        raise ConnectionRefusedError("refused")

    assert preflight.check_tcp("kafka", "kafka", 1, connect=lambda *a, **k: Closed()).ok
    failed = preflight.check_tcp("kafka", "kafka", 1, connect=refuses)
    assert not failed.ok and "cannot connect to kafka:1" in failed.detail


def test_http_server_errors_fail_but_authentication_errors_mean_the_server_is_up():
    def answering(code):
        def opener(url, timeout):
            raise urllib.error.HTTPError(url, code, "x", {}, None)
        return opener

    assert preflight.check_http("storage", "http://s", opener=answering(403)).ok
    assert not preflight.check_http("storage", "http://s", opener=answering(503)).ok

    def down(url, timeout):
        raise urllib.error.URLError("no route")

    assert not preflight.check_http("storage", "http://s", opener=down).ok


def test_preflight_reports_every_failure_not_only_the_first(capsys):
    checks = [
        lambda: Result("kafka", False, "down"),
        lambda: Result("catalog", True, "fine"),
        lambda: Result("flink job", False, "FAILED"),
    ]
    with pytest.raises(PreflightFailed) as raised:
        preflight.run_preflight(checks)
    assert "kafka: down" in str(raised.value) and "flink job: FAILED" in str(raised.value)
    assert "catalog" not in str(raised.value)
    assert "ok   catalog" in capsys.readouterr().out


def test_preflight_passes_when_everything_is_up():
    results = preflight.run_preflight([lambda: Result("a", True, "x")])
    assert [r.name for r in results] == ["a"]


# --- Airflow setup -------------------------------------------------------------------------

def test_new_secrets_are_distinct_and_the_fernet_key_is_valid():
    import base64

    values = setup.new_secrets()
    assert len(set(values.values())) == len(values)
    assert len(base64.urlsafe_b64decode(values["fernet_key"])) == 32
    assert setup.new_secrets() != values


def test_the_env_file_carries_every_secret_airflow_needs():
    values = setup.new_secrets()
    text = setup.render_env(values)
    for key in ("SQL_ALCHEMY_CONN", "FERNET_KEY", "JWT_SECRET", "API__SECRET_KEY"):
        assert key in text
    assert f":{values['db_password']}@postgres:5432/airflow" in text


def test_existing_secrets_are_read_back_and_incomplete_ones_are_not(tmp_path, monkeypatch):
    env, passwords = tmp_path / "airflow.env", tmp_path / "pw.json"
    monkeypatch.setattr(setup, "ENV_FILE", env)
    monkeypatch.setattr(setup, "PASSWORDS_FILE", passwords)
    assert setup.read_existing() is None
    values = setup.new_secrets()
    env.write_text(setup.render_env(values), encoding="utf-8")
    passwords.write_text(json.dumps({setup.UI_USER: values["ui_password"]}), encoding="utf-8")
    assert setup.read_existing() == values
    env.write_text("AIRFLOW_DB_PASSWORD=x\n", encoding="utf-8")
    assert setup.read_existing() is None


def test_a_password_that_could_break_out_of_the_sql_is_refused():
    assert "PASSWORD 'abc-_123'" in setup.database_sql("abc-_123")
    for bad in ("a'b", "a;b", "a b", 'a"b', "a$b"):
        with pytest.raises(ValueError):
            setup.database_sql(bad)


# --- compose rules -------------------------------------------------------------------------

def mounts(service):
    return [str(v) for v in COMPOSE["services"][service].get("volumes", [])]


def test_only_the_scheduler_has_the_docker_socket():
    holders = [name for name in COMPOSE["services"] if any("docker.sock" in m for m in mounts(name))]
    assert holders == ["airflow-scheduler"]


def test_the_airflow_ui_is_published_on_this_machine_only():
    assert COMPOSE["services"]["airflow-apiserver"]["ports"] == ["127.0.0.1:8080:8080"]
    for name in ("airflow-scheduler", "airflow-dag-processor", "airflow-migrate"):
        assert "ports" not in COMPOSE["services"][name]


def test_only_the_api_server_mounts_the_login_file_and_not_read_only():
    holders = {n: m for n in COMPOSE["services"] for m in mounts(n) if "airflow_passwords" in m}
    assert list(holders) == ["airflow-apiserver"]
    assert not holders["airflow-apiserver"].endswith(":ro")


def test_every_airflow_service_is_in_the_orchestration_profile():
    names = [n for n in COMPOSE["services"] if n.startswith("airflow-")]
    assert len(names) == 4
    assert all(COMPOSE["services"][n]["profiles"] == ["orchestration"] for n in names)


# --- the Spark image and the DAG -----------------------------------------------------------

def test_the_spark_image_contains_the_jobs_so_airflow_needs_no_host_paths():
    text = (ROOT / "docker" / "spark.Dockerfile").read_text(encoding="utf-8")
    for source in ("batch /workspace/batch", "iceberg/sql /workspace/iceberg/sql", "configs/quality.yaml"):
        assert f"COPY {source}" in text or f"COPY --chmod=644 {source}" in text or source in text
    assert "chmod 755 /opt/quakeflow /opt/quakeflow/conf" in text  # COPY --chmod would hide the folder


def test_the_image_workflow_rebuilds_when_the_baked_in_code_changes():
    text = (ROOT / ".github" / "workflows" / "docker-build.yml").read_text(encoding="utf-8")
    for path in ("batch/**", "iceberg/sql/**", "configs/quality.yaml"):
        assert text.count(f'"{path}"') == 2  # push and pull_request


def test_the_incremental_dag_has_the_agreed_shape():
    source = (ROOT / "airflow" / "dags" / "incremental_pipeline.py").read_text(encoding="utf-8")
    assert 'schedule="*/30 * * * *"' in source
    assert "max_active_runs=1" in source
    assert "catchup=False" in source
    assert "preflight() >> ingest >> gold" in source  # gold has the default rule: only after ingest succeeded
    assert "trigger_rule" not in source
    assert "mount_tmp_dir=False" in source
    assert "private_environment" in source and "POSTGRES_PASSWORD" in source
    assert not re.search(r"environment=\{[^}]*PASSWORD", source.split("private_environment")[0].split("environment=")[-1])


# --- reconciliation ---------------------------------------------------------------------------

from quakeflow_orchestration.watermark import NotAcknowledged, next_watermark


def ack(state="accepted", queried_at="2026-10-16T01:00:00+00:00"):
    return json.dumps({"batch_id": "b", "state": state, "queried_at": queried_at})


def test_the_watermark_moves_to_the_query_time_only_after_acceptance():
    assert next_watermark(None, ack()) == "2026-10-16T01:00:00+00:00"
    assert next_watermark("2026-10-15T00:00:00+00:00", ack("published")) == "2026-10-16T01:00:00+00:00"


@pytest.mark.parametrize(
    "answer",
    [ack("rejected"), ack("ready"), None, "", "not json", json.dumps({"state": "accepted"}), json.dumps(["accepted"])],
)
def test_the_watermark_stays_put_without_a_real_acceptance(answer):
    with pytest.raises(NotAcknowledged):
        next_watermark("2026-10-15T00:00:00+00:00", answer)


def test_the_watermark_never_moves_backwards():
    current = "2026-10-17T00:00:00+00:00"
    assert next_watermark(current, ack(queried_at="2026-10-16T01:00:00+00:00")) == current


def test_the_acknowledgement_is_the_last_line_of_the_task_output():
    assert next_watermark(None, "some log line\n" + ack()) == "2026-10-16T01:00:00+00:00"


def test_the_reconciliation_dag_has_the_agreed_shape():
    source = (ROOT / "airflow" / "dags" / "reconciliation.py").read_text(encoding="utf-8")
    assert 'schedule="0 */6 * * *"' in source and "max_active_runs=1" in source and "catchup=False" in source
    assert "preflight() >> watermark >> fetch >> accepted" in source
    assert "advance_watermark(accepted.output)" in source, "the watermark must depend on the acceptance"
    assert "skip_on_exit_code=NOT_PUBLISHED_EXIT_CODE" in source, "slow delivery must not fail the run"
    assert "mount_tmp_dir=False" in source and "OUTBOX_VOLUME" in source


def test_only_the_collector_service_uses_the_outbox_volume_in_compose():
    users = [n for n in COMPOSE["services"] if any("reconcile-outbox" in m for m in mounts(n))]
    assert users == ["collector"]
    assert "reconcile-outbox" in COMPOSE["volumes"]
    assert "QUAKEFLOW_OUTBOX_DIR=/outbox" in (ROOT / "docker" / "collector.Dockerfile").read_text(encoding="utf-8").replace(" \\n    ", "\n").replace(" ", "")
