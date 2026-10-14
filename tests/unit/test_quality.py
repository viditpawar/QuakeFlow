import sqlite3
from pathlib import Path

import pytest

from batch.quality import (
    Check,
    ContractError,
    Reconciliation,
    load_contract,
    parse_contract,
    run_checks,
    violation_sql,
)

CONTRACT = Path(__file__).resolve().parents[2] / "configs" / "quality.yaml"


def contract(*checks, reconciliations=()):
    return parse_contract(
        {
            "datasets": [{"dataset": "quakes", "table": "quakes", "checks": list(checks)}],
            "reconciliations": list(reconciliations),
        }
    )


@pytest.fixture
def db():
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE quakes (event_id TEXT, record_hash TEXT, latitude REAL, longitude REAL)"
    )
    yield connection
    connection.close()


def scalar(db):
    return lambda sql: db.execute(sql).fetchone()[0]


def violations(db, **check):
    parsed = contract({"name": "c", **check}).checks[0]
    return db.execute(violation_sql(parsed)).fetchone()[0]


# --- the generated SQL, run on a real SQL engine -------------------------------------------


def test_not_null_counts_null_rows(db):
    db.executemany("INSERT INTO quakes (event_id) VALUES (?)", [("a",), (None,), (None,)])
    assert violations(db, column="event_id", rule="not_null") == 2


def test_unique_counts_keys_that_repeat(db):
    rows = [("a",), ("a",), ("a",), ("b",), ("c",), ("c",)]
    db.executemany("INSERT INTO quakes (event_id) VALUES (?)", rows)
    assert violations(db, column="event_id", rule="unique") == 2  # a and c


def test_unique_over_several_columns_uses_the_combination(db):
    db.executemany(
        "INSERT INTO quakes (event_id, record_hash) VALUES (?, ?)",
        [("a", "h1"), ("a", "h2"), ("b", "h1"), ("b", "h1")],
    )
    assert violations(db, columns=["event_id", "record_hash"], rule="unique") == 1
    assert violations(db, column="event_id", rule="unique") == 2


@pytest.mark.parametrize(
    ("latitude", "expected"),
    [(0.0, 0), (-90.0, 0), (90.0, 0), (90.0001, 1), (-91.0, 1), (None, 0)],
)
def test_between_flags_values_outside_the_range_and_ignores_nulls(db, latitude, expected):
    db.execute("INSERT INTO quakes (latitude) VALUES (?)", (latitude,))
    assert violations(db, column="latitude", rule="between", min=-90, max=90) == expected


def test_between_accepts_a_single_bound(db):
    db.executemany("INSERT INTO quakes (latitude) VALUES (?)", [(1.0,), (5.0,), (-3.0,)])
    assert violations(db, column="latitude", rule="between", min=0) == 1
    assert violations(db, column="latitude", rule="between", max=2) == 1


def test_min_rows(db):
    assert violations(db, rule="min_rows", min=1) == 1  # the table is empty
    db.execute("INSERT INTO quakes (event_id) VALUES ('a')")
    assert violations(db, rule="min_rows", min=1) == 0
    assert violations(db, rule="min_rows", min=2) == 1


# --- running a contract --------------------------------------------------------------------


def test_clean_data_passes_every_check(db):
    db.execute("INSERT INTO quakes VALUES ('a', 'h', 10.0, 20.0)")
    results = run_checks(
        scalar(db),
        contract(
            {"name": "id", "column": "event_id", "rule": "not_null"},
            {"name": "lat", "column": "latitude", "rule": "between", "min": -90, "max": 90},
        ),
    )
    assert all(r.passed for r in results)
    assert [r.name for r in results] == ["id", "lat"]


def test_bad_data_fails_only_the_checks_it_breaks(db):
    db.execute("INSERT INTO quakes VALUES (NULL, 'h', 95.0, 20.0)")
    results = {
        r.name: r
        for r in run_checks(
            scalar(db),
            contract(
                {"name": "id", "column": "event_id", "rule": "not_null"},
                {"name": "lat", "column": "latitude", "rule": "between", "min": -90, "max": 90},
                {"name": "lon", "column": "longitude", "rule": "between", "min": -180, "max": 180},
            ),
        )
    }
    assert (results["id"].passed, results["lat"].passed, results["lon"].passed) == (
        False,
        False,
        True,
    )
    assert results["lat"].detail == "1 violations"


def test_a_check_that_cannot_run_fails_closed(db):
    bad = contract({"name": "ghost", "column": "no_such_column", "rule": "not_null"})
    results = run_checks(scalar(db), bad)
    assert not results[0].passed
    assert "could not run" in results[0].detail


def test_reconciliations_pass_when_the_numbers_match_and_fail_when_they_do_not(db):
    db.executemany("INSERT INTO quakes (event_id) VALUES (?)", [("a",), ("b",)])
    same = {"name": "same", "left": "SELECT count(*) FROM quakes", "right": "SELECT 2"}
    lost = {"name": "lost_one", "left": "SELECT 1", "right": "SELECT count(*) FROM quakes"}
    results = {
        r.name: r
        for r in run_checks(
            scalar(db),
            contract(
                {"name": "n", "column": "event_id", "rule": "not_null"},
                reconciliations=[same, lost],
            ),
        )
    }
    assert results["same"].passed
    assert not results["lost_one"].passed
    assert results["lost_one"].detail == "1 versus 2"


def test_rewrite_is_applied_to_every_query():
    seen = []
    run_checks(
        lambda sql: seen.append(sql) or 0,
        contract({"name": "n", "column": "event_id", "rule": "not_null"}),
        rewrite=lambda sql: sql.replace("quakes", "other"),
    )
    assert seen
    assert all("other" in sql and "quakes" not in sql for sql in seen)


# --- the contract file ---------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_check",
    [
        {"name": "x", "rule": "nonsense", "column": "a"},
        {"name": "x", "rule": "not_null"},
        {"name": "x", "rule": "not_null", "columns": ["a", "b"]},
        {"name": "x", "rule": "between", "column": "a"},
        {"name": "x", "rule": "between", "column": "a", "min": 5, "max": 1},
        {"name": "x", "rule": "between", "column": "a", "min": "low"},
        {"name": "x", "rule": "min_rows"},
        {"name": "x", "rule": "unique", "column": "a; DROP TABLE t"},
        {"name": "x", "rule": "not_null", "column": "a", "columns": ["a"]},
        {"name": "bad name", "rule": "not_null", "column": "a"},
    ],
)
def test_invalid_checks_are_refused(bad_check):
    with pytest.raises(ContractError):
        contract(bad_check)


def test_duplicate_names_and_empty_contracts_are_refused():
    ok = {"name": "x", "rule": "not_null", "column": "a"}
    with pytest.raises(ContractError, match="duplicate"):
        contract(ok, ok)
    with pytest.raises(ContractError):
        parse_contract({"datasets": []})
    with pytest.raises(ContractError):
        parse_contract("nope")


def test_reconciliation_queries_must_be_selects():
    bad = [{"name": "r", "left": "DROP TABLE t", "right": "SELECT 1"}]
    with pytest.raises(ContractError, match="SELECT"):
        contract({"name": "n", "column": "a", "rule": "not_null"}, reconciliations=bad)


def test_the_shipped_contract_loads_and_covers_the_required_rules():
    loaded = load_contract(CONTRACT)
    names = {c.name for c in loaded.checks}
    required = {
        "event_id_required",
        "event_id_unique",
        "latitude_range",
        "longitude_range",
        "event_timestamp_required",
    }
    assert required <= names
    assert "daily_counts_match_current" in {r.name for r in loaded.reconciliations}
    lat = next(c for c in loaded.checks if c.name == "latitude_range")
    assert (lat.minimum, lat.maximum) == (-90.0, 90.0)
    assert isinstance(lat, Check)
    assert isinstance(loaded.reconciliations[0], Reconciliation)
