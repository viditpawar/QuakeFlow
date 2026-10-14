"""Every third-party package the code imports must be declared in pyproject.toml.

A package that happens to be installed in a developer's environment hides a missing declaration,
and a clean install, such as CI, then fails on import. This reads the imports from the code.
"""

import ast
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE_FOLDERS = ("ingestion", "schemas", "processing", "batch", "scripts", "infrastructure")
# Imported name -> distribution that provides it, where the two differ.
DISTRIBUTION = {
    "yaml": "pyyaml",
    "confluent_kafka": "confluent-kafka",
    "prometheus_client": "prometheus-client",
}
# Packages that are not on the host. Spark code runs in the image, which installs its own set.
CONTAINER_ONLY = {"pyspark"}
# Provided by a declared package, so listing them again would be redundant.
PROVIDED_BY_DECLARED = {"botocore": "boto3", "urllib3": "requests"}
PROJECT_PACKAGES = {"ingestion", "schemas", "processing", "batch", "scripts", "tests"}


def declared() -> set[str]:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    names = set()
    for requirement in project["dependencies"]:
        name = requirement
        for separator in (">", "<", "=", "~", "!", " ", "["):
            name = name.split(separator)[0]
        names.add(name.lower().replace("_", "-"))
    return names


def imported_third_party() -> dict[str, set[str]]:
    """Top-level package name -> files that import it."""
    found: dict[str, set[str]] = {}
    for folder in SOURCE_FOLDERS:
        for path in sorted((ROOT / folder).rglob("*.py")):
            for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
                if isinstance(node, ast.Import):
                    names = [alias.name.split(".")[0] for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                    names = [node.module.split(".")[0]]
                else:
                    continue
                for name in names:
                    if name not in sys.stdlib_module_names and name not in PROJECT_PACKAGES:
                        found.setdefault(name, set()).add(path.relative_to(ROOT).as_posix())
    return found


def test_every_third_party_import_is_declared_as_a_dependency():
    declared_names = declared()
    missing = {}
    for name, files in imported_third_party().items():
        if name in CONTAINER_ONLY:
            continue
        distribution = DISTRIBUTION.get(name, name).lower().replace("_", "-")
        provider = PROVIDED_BY_DECLARED.get(name)
        if distribution not in declared_names and (provider is None or provider not in declared_names):
            missing[name] = sorted(files)
    assert not missing, f"imported but not declared in pyproject.toml: {missing}"


def test_the_scan_sees_the_packages_the_code_really_uses():
    found = imported_third_party()
    assert {"yaml", "pydantic", "requests", "confluent_kafka", "prometheus_client", "boto3"} <= set(found)
    assert "prometheus_client" in found and "ingestion/metrics.py" in found["prometheus_client"]


def test_prometheus_client_is_declared():
    assert "prometheus-client" in declared()
