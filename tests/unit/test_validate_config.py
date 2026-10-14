"""The configuration validator must pass on the repository and fail on broken files."""

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("validate_config", ROOT / "scripts" / "validate_config.py")
validate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validate)


def test_the_repository_passes_every_check():
    problems = [problem for check in validate.CHECKS for problem in check()]
    assert problems == []
    assert validate.main() == 0


@pytest.fixture
def fake_root(tmp_path, monkeypatch):
    """A throwaway repository root that the checks read from instead of the real one."""
    for folder in ("configs", "monitoring/dashboards", ".github/workflows"):
        (tmp_path / folder).mkdir(parents=True)
    monkeypatch.setattr(validate, "ROOT", tmp_path)
    return tmp_path


def test_invalid_yaml_is_reported_with_its_path(fake_root):
    (fake_root / "configs" / "broken.yaml").write_text("key: [unclosed\n")
    (fake_root / "monitoring" / "fine.yml").write_text("a: 1\n")
    problems = validate.check_yaml_syntax()
    assert len(problems) == 1 and problems[0].startswith("configs/broken.yaml: not valid YAML")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("topics: []", "non-empty"),
        ("topics:\n  - {partitions: 1, replication_factor: 1, retention_ms: 1}", "no name"),
        ("topics:\n  - {name: a, partitions: 0, replication_factor: 1, retention_ms: 1}", "a.partitions"),
        ("topics:\n  - {name: a, partitions: 1, replication_factor: true, retention_ms: 1}", "a.replication_factor"),
        (
            (
                "topics:\n  - {name: a, partitions: 1, replication_factor: 1, retention_ms: 1}\n"
                "  - {name: a, partitions: 1, replication_factor: 1, retention_ms: 1}"
            ),
            "duplicate topic a",
        ),
    ],
)
def test_bad_kafka_topic_definitions_are_reported(fake_root, text, expected):
    (fake_root / "configs" / "kafka-topics.yaml").write_text(text)
    assert any(expected in problem for problem in validate.check_kafka_topics())


def test_a_good_kafka_topic_file_has_no_problems(fake_root):
    (fake_root / "configs" / "kafka-topics.yaml").write_text(
        "topics:\n  - {name: a, partitions: 3, replication_factor: 1, retention_ms: 1000}\n"
    )
    assert validate.check_kafka_topics() == []


def test_dashboard_problems_are_reported(fake_root):
    folder = fake_root / "monitoring" / "dashboards"
    (folder / "notjson.json").write_text("{nope")
    (folder / "partial.json").write_text(json.dumps({"title": "x", "panels": []}))
    (folder / "dup.json").write_text(
        json.dumps({"uid": "u", "title": "t", "panels": [
            {"id": 1, "type": "stat", "title": "A", "targets": [{"expr": "up"}]},
            {"id": 1, "type": "stat", "title": "B", "targets": []},
        ]})
    )
    text = "\n".join(validate.check_dashboards())
    assert "notjson.json: not valid JSON" in text
    assert "partial.json: missing 'uid'" in text
    assert "dup.json: duplicate panel ids" in text
    assert "panel 'B' has no query" in text


def test_a_compose_variable_missing_from_env_example_is_reported(fake_root):
    (fake_root / "docker-compose.yml").write_text("x: ${KNOWN:-1} ${FORGOTTEN:-2}\n")
    (fake_root / ".env.example").write_text("# comment\nKNOWN=1\n")
    problems = validate.check_env_example_covers_compose()
    assert problems == [".env.example does not mention FORGOTTEN, which docker-compose.yml reads"]


def test_an_env_file_that_is_not_ignored_is_reported(fake_root):
    (fake_root / ".env").write_text("SECRET=1\n")
    (fake_root / ".gitignore").write_text("data/\n")
    assert any(".env exists" in p for p in validate.check_for_committed_secrets())
    (fake_root / ".gitignore").write_text("data/\n.env\n")
    assert validate.check_for_committed_secrets() == []


def test_a_private_key_in_a_config_file_is_reported(fake_root):
    (fake_root / ".gitignore").write_text(".env\n")
    (fake_root / "configs" / "oops.txt").write_text("-----BEGIN " + "PRIVATE KEY-----\nabc\n")
    assert any("contains a private key" in p for p in validate.check_for_committed_secrets())
