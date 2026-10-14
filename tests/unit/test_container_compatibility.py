"""Code that runs in the Spark image must work on its Python 3.10, not just the host's 3.11.

The unit tests run on the host, so a 3.11-only feature would pass here and then fail inside the
container (this happened once with datetime.UTC). These checks catch the common cases early.
"""

import ast
from pathlib import Path

import pytest

BATCH = Path(__file__).resolve().parents[2] / "batch"
# Everything except backfill.py, which runs on the host.
CONTAINER_MODULES = sorted(p for p in BATCH.glob("*.py") if p.name != "backfill.py")

# (module, name) pairs that exist only from Python 3.11.
PYTHON_311_ONLY = {
    ("datetime", "UTC"),
    ("enum", "StrEnum"),
    ("typing", "Self"),
    ("typing", "LiteralString"),
    ("typing", "Never"),
    ("typing", "assert_never"),
}


def test_there_are_container_modules_to_check():
    names = {p.name for p in CONTAINER_MODULES}
    assert {"ingest_live.py", "ingest_history.py", "build_gold.py", "quality.py"} <= names


@pytest.mark.parametrize("path", CONTAINER_MODULES, ids=lambda p: p.name)
def test_module_parses_as_python_310(path):
    ast.parse(path.read_text(encoding="utf-8"), feature_version=(3, 10))


@pytest.mark.parametrize("path", CONTAINER_MODULES, ids=lambda p: p.name)
def test_module_imports_nothing_that_needs_python_311(path):
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = [
        f"{node.module}.{alias.name}"
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
        if (node.module, alias.name) in PYTHON_311_ONLY
    ]
    assert not found, f"{path.name} imports Python 3.11-only names: {found}"
    imported_modules = {
        alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names
    }
    assert "tomllib" not in imported_modules
    uses_datetime_utc = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and node.attr == "UTC"
        and isinstance(node.value, ast.Name)
        and node.value.id == "datetime"
    ]
    assert not uses_datetime_utc, "datetime.UTC needs Python 3.11"
    assert not any(isinstance(n, ast.TryStar) for n in ast.walk(tree)), "except* needs 3.11"


def test_the_image_installs_every_third_party_package_the_container_modules_import():
    imported = set()
    for path in CONTAINER_MODULES:
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            if isinstance(node, ast.Import):
                imported |= {a.name.split(".")[0] for a in node.names}
            elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
                imported.add(node.module.split(".")[0])
    # Standard library and what the Spark image already provides, plus our own packages.
    provided = {"pyspark", "batch", "yaml", "prometheus_client"}
    standard = {
        "argparse", "ast", "collections", "dataclasses", "datetime", "json", "os", "pathlib",
        "re", "sys", "uuid", "typing", "hashlib", "logging", "time", "contextlib",
    }
    unknown = imported - provided - standard
    assert not unknown, f"container modules import packages the image may not have: {unknown}"
    requirements = (BATCH.parent / "docker" / "spark-requirements.txt").read_text().lower()
    assert "pyyaml" in requirements
    assert "prometheus-client" in requirements
