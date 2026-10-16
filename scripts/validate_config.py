"""Check that every configuration file in the repository is well formed.

    python scripts/validate_config.py

Used by CI, and safe to run locally. It reports every problem it finds and exits 1 if there are any.
Files that the application loads (the feed list, the quality contract) are loaded with the
application's own code, so a file that passes here will also load at run time.
"""

import json
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from batch.quality import ContractError, load_contract
from ingestion.live.poller import ConfigError, load_feed_configs

YAML_GLOBS = ("configs/*.yaml", "monitoring/*.yml", "monitoring/grafana/provisioning/**/*.yml", ".github/**/*.yml")
IGNORED = ("spark-defaults.conf.template",)


def check_yaml_syntax() -> list[str]:
    problems = []
    for pattern in YAML_GLOBS:
        for path in sorted(ROOT.glob(pattern)):
            try:
                yaml.safe_load(path.read_text(encoding="utf-8"))
            except (OSError, yaml.YAMLError) as exc:
                problems.append(f"{path.relative_to(ROOT).as_posix()}: not valid YAML: {str(exc).splitlines()[0]}")
    return problems


def check_kafka_topics() -> list[str]:
    path = ROOT / "configs" / "kafka-topics.yaml"
    topics = yaml.safe_load(path.read_text(encoding="utf-8")).get("topics")
    if not isinstance(topics, list) or not topics:
        return [f"{path.name}: expected a non-empty 'topics' list"]
    problems, seen = [], set()
    for topic in topics:
        name = topic.get("name") if isinstance(topic, dict) else None
        if not isinstance(name, str) or not name:
            problems.append(f"{path.name}: a topic has no name")
            continue
        if name in seen:
            problems.append(f"{path.name}: duplicate topic {name}")
        seen.add(name)
        for key in ("partitions", "replication_factor", "retention_ms"):
            value = topic.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                problems.append(f"{path.name}: {name}.{key} must be a positive integer")
    return problems


def check_dashboards() -> list[str]:
    problems = []
    for path in sorted((ROOT / "monitoring" / "dashboards").glob("*.json")):
        name = path.name
        try:
            dashboard = json.loads(path.read_text(encoding="utf-8"))
        except ValueError as exc:
            problems.append(f"{name}: not valid JSON: {exc}")
            continue
        for key in ("uid", "title", "panels"):
            if key not in dashboard:
                problems.append(f"{name}: missing '{key}'")
        ids = [p.get("id") for p in dashboard.get("panels", [])]
        if len(ids) != len(set(ids)):
            problems.append(f"{name}: duplicate panel ids")
        for panel in dashboard.get("panels", []):
            if panel.get("type") != "row" and not panel.get("targets"):
                problems.append(f"{name}: panel {panel.get('title')!r} has no query")
    return problems


def check_application_configs() -> list[str]:
    problems = []
    try:
        load_feed_configs(ROOT / "configs" / "sources.yaml")
    except (ConfigError, OSError, yaml.YAMLError) as exc:
        problems.append(f"configs/sources.yaml: {exc}")
    try:
        load_contract(ROOT / "configs" / "quality.yaml")
    except (ContractError, OSError, yaml.YAMLError) as exc:
        problems.append(f"configs/quality.yaml: {exc}")
    return problems


def check_env_example_covers_compose() -> list[str]:
    """Every ${VARIABLE} that docker-compose.yml reads should be documented in .env.example."""
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    # A doubled dollar sign, $${NAME}, is Compose's escape for a variable the shell inside the container expands,
    # so it is not something Compose reads from .env.
    used = set(re.findall(r"(?<!\$)\$\{([A-Z][A-Z0-9_]*)", compose))
    documented = {
        line.split("=", 1)[0].strip()
        for line in (ROOT / ".env.example").read_text(encoding="utf-8").splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    }
    return [f".env.example does not mention {name}, which docker-compose.yml reads" for name in sorted(used - documented)]


def check_ports_bind_to_localhost() -> list[str]:
    """Every published port must bind to 127.0.0.1. Kafka and the other services here have no
    authentication, so a port bound to all interfaces would be open to the whole network."""
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    problems = []
    for name, service in (compose.get("services") or {}).items():
        for port in service.get("ports") or []:
            text = str(port)
            if not text.startswith("127.0.0.1:"):
                problems.append(f"docker-compose.yml: {name} publishes port {text} on all interfaces")
    return problems


def check_kafka_data_is_on_a_volume() -> list[str]:
    """Kafka's data directory must be a mounted volume. The image's default is /tmp inside the
    container, so a volume that is mounted but not used leaves every topic to be erased whenever the
    container is removed (docker compose down), which is what happened once."""
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    kafka = (compose.get("services") or {}).get("kafka")
    if kafka is None:
        return []
    environment = kafka.get("environment") or {}
    log_dirs = environment.get("KAFKA_LOG_DIRS")
    volume_paths = [
        str(entry).split(":")[1]
        for entry in kafka.get("volumes") or []
        if not str(entry).startswith((".", "/", "~")) and ":" in str(entry)
    ]
    if log_dirs is None:
        return ["docker-compose.yml: kafka does not set KAFKA_LOG_DIRS, so it writes inside the container"]
    if log_dirs not in volume_paths:
        return [f"docker-compose.yml: kafka writes to {log_dirs}, which is not a mounted volume ({volume_paths})"]
    return []


def check_for_committed_secrets() -> list[str]:
    """Catch the obvious mistakes: a real .env, or a private key, in the working tree."""
    problems = []
    if (ROOT / ".env").exists():
        gitignore = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        if ".env" not in (line.strip() for line in gitignore):
            problems.append(".env exists but is not listed in .gitignore")
    private_key = "-----BEGIN " + "PRIVATE KEY-----"
    for pattern in ("configs/**/*", "monitoring/**/*", "docker/**/*", ".github/**/*", "iceberg/**/*"):
        for path in ROOT.glob(pattern):
            if path.is_file() and private_key in path.read_text(encoding="utf-8", errors="ignore"):
                problems.append(f"{path.relative_to(ROOT).as_posix()} contains a private key")
    return problems


CHECKS = (
    check_yaml_syntax,
    check_kafka_topics,
    check_dashboards,
    check_application_configs,
    check_env_example_covers_compose,
    check_ports_bind_to_localhost,
    check_kafka_data_is_on_a_volume,
    check_for_committed_secrets,
)


def main() -> int:
    problems = [problem for check in CHECKS for problem in check()]
    for problem in problems:
        print(f"PROBLEM  {problem}")
    print(f"{len(CHECKS)} checks run, {len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
