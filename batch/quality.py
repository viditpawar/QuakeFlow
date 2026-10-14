"""Data quality contract: load the checks from YAML, turn them into SQL, and run them.

Every check becomes a query that returns the number of violations, so a check passes when the
count is zero. The runner takes a function that runs one query and returns a number, which keeps
this module free of Spark and lets the SQL be tested against an ordinary SQL engine.
A check that cannot be evaluated counts as failed: the gate fails closed.
"""

import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import yaml

RULES = ("not_null", "unique", "between", "min_rows")
_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")


class ContractError(ValueError):
    """The quality contract file is invalid."""


@dataclass(frozen=True)
class Check:
    name: str
    dataset: str
    table: str
    rule: str
    columns: tuple[str, ...] = ()
    minimum: float | None = None
    maximum: float | None = None


@dataclass(frozen=True)
class Reconciliation:
    name: str
    left: str
    right: str


@dataclass(frozen=True)
class Contract:
    checks: list[Check]
    reconciliations: list[Reconciliation]


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str  # the violation count, or why the check could not run


def _identifier(value: object, what: str) -> str:
    if not isinstance(value, str) or not _NAME.match(value):
        raise ContractError(f"{what} must be a plain identifier, got {value!r}")
    return value


def _number(value: object, what: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ContractError(f"{what} must be a number, got {value!r}")
    return float(value)


def parse_contract(document: object) -> Contract:
    if not isinstance(document, dict):
        raise ContractError("the contract must be a mapping")
    checks: list[Check] = []
    for dataset in document.get("datasets") or []:
        if not isinstance(dataset, dict):
            raise ContractError("each dataset must be a mapping")
        name = _identifier(dataset.get("dataset"), "dataset")
        table = _identifier(dataset.get("table"), f"{name}: table")
        for entry in dataset.get("checks") or []:
            checks.append(_parse_check(name, table, entry))
    if not checks:
        raise ContractError("the contract defines no checks")

    reconciliations = []
    for entry in document.get("reconciliations") or []:
        if not isinstance(entry, dict):
            raise ContractError("each reconciliation must be a mapping")
        rec = Reconciliation(
            _identifier(entry.get("name"), "reconciliation name"),
            str(entry.get("left", "")).strip(),
            str(entry.get("right", "")).strip(),
        )
        if not rec.left.lower().startswith("select") or not rec.right.lower().startswith("select"):
            raise ContractError(f"{rec.name}: left and right must be SELECT queries")
        reconciliations.append(rec)

    names = [c.name for c in checks] + [r.name for r in reconciliations]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ContractError(f"duplicate check names: {', '.join(duplicates)}")
    return Contract(checks, reconciliations)


def _parse_check(dataset: str, table: str, entry: object) -> Check:
    if not isinstance(entry, dict):
        raise ContractError(f"{dataset}: each check must be a mapping")
    name = _identifier(entry.get("name"), f"{dataset}: check name")
    rule = entry.get("rule")
    if rule not in RULES:
        raise ContractError(f"{name}: rule must be one of {', '.join(RULES)}, got {rule!r}")

    if "columns" in entry and "column" in entry:
        raise ContractError(f"{name}: give column or columns, not both")
    raw = entry.get("columns", [entry["column"]] if "column" in entry else [])
    if not isinstance(raw, list):
        raise ContractError(f"{name}: columns must be a list")
    columns = tuple(_identifier(c, f"{name}: column") for c in raw)

    minimum = _number(entry.get("min"), f"{name}: min")
    maximum = _number(entry.get("max"), f"{name}: max")
    if rule in ("not_null", "unique", "between") and not columns:
        raise ContractError(f"{name}: rule {rule} needs a column")
    if rule == "not_null" and len(columns) != 1:
        raise ContractError(f"{name}: not_null checks exactly one column")
    if rule == "between":
        if len(columns) != 1:
            raise ContractError(f"{name}: between checks exactly one column")
        if minimum is None and maximum is None:
            raise ContractError(f"{name}: between needs min, max, or both")
        if minimum is not None and maximum is not None and minimum > maximum:
            raise ContractError(f"{name}: min is greater than max")
    if rule == "min_rows" and (minimum is None or minimum < 0):
        raise ContractError(f"{name}: min_rows needs a non-negative min")
    return Check(name, dataset, table, rule, columns, minimum, maximum)


def load_contract(path: str | Path) -> Contract:
    return parse_contract(yaml.safe_load(Path(path).read_text(encoding="utf-8")))


def violation_sql(check: Check, table: str | None = None) -> str:
    """A query returning how many violations the check finds (zero means it passes)."""
    table = table or check.table
    if check.rule == "not_null":
        return f"SELECT count(*) FROM {table} WHERE {check.columns[0]} IS NULL"
    if check.rule == "unique":
        columns = ", ".join(check.columns)
        return f"SELECT count(*) FROM (SELECT 1 FROM {table} GROUP BY {columns} HAVING count(*) > 1) duplicates"
    if check.rule == "between":
        column = check.columns[0]
        bounds = []
        if check.minimum is not None:
            bounds.append(f"{column} < {check.minimum!r}")
        if check.maximum is not None:
            bounds.append(f"{column} > {check.maximum!r}")
        return f"SELECT count(*) FROM {table} WHERE {column} IS NOT NULL AND ({' OR '.join(bounds)})"
    minimum = int(check.minimum or 0)
    return f"SELECT CASE WHEN count(*) >= {minimum} THEN 0 ELSE 1 END FROM {table}"


def run_checks(
    run_scalar: Callable[[str], float], contract: Contract, rewrite: Callable[[str], str] = str
) -> list[CheckResult]:
    """Run every check and reconciliation. `rewrite` adjusts a query before it is run, for
    example to point it at other namespaces. Anything that raises counts as a failure."""
    results: list[CheckResult] = []
    for check in contract.checks:
        try:
            violations = run_scalar(rewrite(violation_sql(check)))
            results.append(CheckResult(check.name, violations == 0, f"{violations:g} violations"))
        except Exception as exc:  # noqa: BLE001 - any failure to evaluate must block promotion
            results.append(CheckResult(check.name, False, f"could not run: {str(exc)[:200]}"))
    for rec in contract.reconciliations:
        try:
            left, right = run_scalar(rewrite(rec.left)), run_scalar(rewrite(rec.right))
            results.append(CheckResult(rec.name, left == right, f"{left:g} versus {right:g}"))
        except Exception as exc:  # noqa: BLE001
            results.append(CheckResult(rec.name, False, f"could not run: {str(exc)[:200]}"))
    return results
