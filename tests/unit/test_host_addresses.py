"""Programs on the host must reach the Docker services by 127.0.0.1, not by the name localhost.

On Windows with Docker Desktop, localhost resolves to both 127.0.0.1 and ::1. A connection to the
IPv6 address is refused only after 2 seconds, so a client that tries it first stalls on every new
connection. For Kafka it is worse: the broker advertises an address to clients, and an advertised
localhost sent them back to the slow path, which made a test's availability check fail at random.
"""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FOLDERS = ("ingestion", "infrastructure", "batch", "processing", "schemas", "tests", "scripts")
SLOW = re.compile(r"localhost:(9092|9000|5432)\b")


def test_no_host_side_default_uses_localhost_for_a_docker_service():
    offenders = []
    for folder in FOLDERS:
        for path in (ROOT / folder).rglob("*.py"):
            if path.name == Path(__file__).name:
                continue  # this file explains the rule and quotes the bad pattern
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if SLOW.search(line):
                    offenders.append(f"{path.relative_to(ROOT).as_posix()}:{number}: {line.strip()}")
    assert not offenders, "use 127.0.0.1 instead of localhost:\n" + "\n".join(offenders)


def test_the_broker_advertises_the_same_address_the_clients_use():
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    assert "EXTERNAL://127.0.0.1:9092" in compose
    assert "EXTERNAL://localhost:9092" not in compose
    example = (ROOT / ".env.example").read_text(encoding="utf-8")
    assert "KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9092" in example
