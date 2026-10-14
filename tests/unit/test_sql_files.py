from pathlib import Path

from batch.sql_files import load_statements, split_statements, with_namespaces

SQL_DIR = Path(__file__).resolve().parents[2] / "iceberg" / "sql"


def test_split_drops_comment_lines_and_blank_statements():
    sql = "-- header; with a semicolon\nSELECT 1;\n\n  -- note\nSELECT 2 ;;\n"
    assert split_statements(sql) == ["SELECT 1", "SELECT 2"]


def test_split_keeps_a_final_statement_without_semicolon():
    assert split_statements("SELECT 1;\nSELECT 2") == ["SELECT 1", "SELECT 2"]


def test_split_keeps_multiline_statements_together():
    assert split_statements("SELECT a,\n       b\nFROM t;") == ["SELECT a,\n       b\nFROM t"]


def test_with_namespaces_rewrites_every_table_reference():
    sql = "MERGE INTO local.silver.x USING local.bronze.y; SELECT * FROM local.silver.z"
    assert with_namespaces(sql, "local.b1", "local.s1") == (
        "MERGE INTO local.s1.x USING local.b1.y; SELECT * FROM local.s1.z"
    )


def test_with_namespaces_defaults_change_nothing():
    sql = "SELECT * FROM local.silver.earthquakes_current"
    assert with_namespaces(sql) == sql


def test_live_merge_file_splits_into_views_then_merges():
    statements = load_statements(SQL_DIR / "003_merge_current.sql")
    kinds = [s.split("(")[0].split()[0:2] for s in statements]
    assert all(s.startswith(("CREATE OR REPLACE TEMP VIEW", "MERGE INTO")) for s in statements)
    # Views are defined before any MERGE uses them.
    first_merge = next(i for i, s in enumerate(statements) if s.startswith("MERGE"))
    assert all(s.startswith("CREATE") for s in statements[:first_merge])
    assert sum(s.startswith("MERGE") for s in statements) == 4, kinds
    assert "raw_dlq_messages" not in "\n".join(statements)


def test_flink_rejected_file_is_self_contained():
    statements = load_statements(SQL_DIR / "003_merge_flink_rejected.sql")
    assert [s.split()[0] for s in statements] == ["CREATE", "MERGE"]
    assert "raw_messages" not in "\n".join(statements)


def test_statements_have_balanced_quotes_so_the_simple_split_is_safe():
    for name in ("003_merge_current.sql", "003_merge_flink_rejected.sql"):
        for statement in load_statements(SQL_DIR / name):
            assert statement.count("'") % 2 == 0, statement[:80]


def test_current_state_merge_encodes_the_ordering_rule():
    statements = load_statements(SQL_DIR / "003_merge_current.sql")
    merge = next(s for s in statements if "local.silver.earthquakes_current" in s and s.startswith("MERGE"))
    assert "s.source_updated_at > t.source_updated_at" in merge
    assert "s.source_updated_at = t.source_updated_at AND s.record_hash > t.record_hash" in merge
    assert "WHEN NOT MATCHED THEN INSERT *" in merge


def test_namespaces_apply_to_the_real_merge_file():
    statements = load_statements(SQL_DIR / "003_merge_current.sql", "local.tb", "local.ts")
    text = "\n".join(statements)
    assert "local.bronze" not in text and "local.silver" not in text
    assert "local.tb.live_observations" in text and "local.ts.earthquakes_current" in text
