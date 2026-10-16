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


def _merge_condition(file_name: str) -> str:
    text = (SQL_DIR / file_name).read_text()
    start = text.index("WHEN MATCHED AND (")
    return " ".join(text[start : text.index("THEN UPDATE SET *", start)].split())


def test_live_and_historical_current_state_merges_use_the_identical_ordering_rule():
    # The two files repeat the rule, so a change to one must be made to the other.
    assert _merge_condition("003_merge_current.sql") == _merge_condition("003_merge_history.sql")


def test_historical_merge_file_shape():
    statements = load_statements(SQL_DIR / "003_merge_history.sql")
    views = [s for s in statements if s.startswith("CREATE OR REPLACE TEMP VIEW")]
    merges = [s for s in statements if s.startswith("MERGE")]
    assert len(views) == 4 and len(merges) == 4
    assert all(s.count("'") % 2 == 0 for s in statements)
    assert statements.index(merges[0]) > statements.index(views[-1])


def test_every_incoming_historical_view_is_deduplicated_before_it_is_merged():
    # An unconditional insert of unmatched rows would insert a duplicate twice.
    statements = load_statements(SQL_DIR / "003_merge_history.sql")
    for view in (s for s in statements if s.startswith("CREATE OR REPLACE TEMP VIEW incoming_hist_")):
        if "incoming_hist_latest" in view.split("AS")[0]:
            assert "row_number()" in view
            continue
        assert "row_number()" in view and "copy_number = 1" in view, view[:60]


def test_gold_build_script_stages_before_it_publishes():
    from batch.build_gold import publish_targets, split_build_statements

    stage, publish = split_build_statements(load_statements(SQL_DIR / "005_build_gold.sql"))
    assert len(stage) == 5 and len(publish) == 5
    assert all(s.count("'") % 2 == 0 for s in stage + publish)
    targets = publish_targets(publish)
    assert {t for t, _ in targets} == {
        "local.gold.daily_seismic_activity",
        "local.gold.regional_activity",
        "local.gold.magnitude_distribution",
        "local.gold.revision_statistics",
        "local.gold.ingestion_latency",
    }
    staged_names = {s.split()[5] for s in stage}  # CREATE OR REPLACE TEMP VIEW <name> AS
    assert {v for _, v in targets} == staged_names


def test_gold_build_refuses_anything_but_views_and_overwrites():
    import pytest

    from batch.build_gold import publish_targets, split_build_statements

    with pytest.raises(ValueError):
        split_build_statements(["DROP TABLE local.gold.x"])
    with pytest.raises(ValueError):
        publish_targets(["INSERT OVERWRITE local.gold.x SELECT a FROM staged_x"])


def test_every_gold_table_is_created_before_the_build_writes_to_it():
    from batch.build_gold import builds_table, publish_targets, split_build_statements

    created = {
        s.split()[5]
        for s in load_statements(SQL_DIR / "004_gold_models.sql")
        if s.startswith("CREATE TABLE")
    }
    _, publish = split_build_statements(load_statements(SQL_DIR / "005_build_gold.sql"))
    # The public names are views, so the rows live in the <name>_builds tables.
    assert {builds_table(t) for t, _ in publish_targets(publish)} <= created
    assert not {t for t, _ in publish_targets(publish)} & created
    assert {"local.gold.build_log", "local.gold.build_manifest", "local.gold.published_build"} <= created


def test_every_builds_table_starts_with_the_build_id_column():
    import re

    text = (SQL_DIR / "004_gold_models.sql").read_text(encoding="utf-8")
    for match in re.finditer(r"CREATE TABLE IF NOT EXISTS (\S+_builds) \(\s*(\w+)", text):
        assert match.group(2) == "build_id", match.group(1)
    assert text.count("_builds (") == 5


def test_the_load_log_is_defined_with_the_silver_tables():
    statements = load_statements(SQL_DIR / "002_create_tables.sql")
    log = [s for s in statements if "local.silver.load_log" in s]
    assert len(log) == 1 and "current_snapshot_id" in log[0] and "versions_snapshot_id" in log[0]


def test_historical_bronze_and_rejected_merges_are_null_safe_on_event_id():
    text = "\n".join(load_statements(SQL_DIR / "003_merge_history.sql"))
    assert text.count("t.event_id <=> s.event_id") == 2


def _flink_settings() -> dict[str, str]:
    import re

    text = (SQL_DIR.parents[1] / "flink" / "sql" / "revision_tracking.sql").read_text(encoding="utf-8")
    return dict(re.findall(r"SET '([^']+)' = '([^']*)'", text))


def test_the_flink_job_retries_without_a_limit_so_it_survives_a_long_outage():
    # A fixed limit of 10 attempts once left the job failed for 50 minutes after a Kafka outage (D-023).
    settings = _flink_settings()
    assert settings["restart-strategy.type"] == "exponential-delay"
    assert not any(key.startswith("restart-strategy.fixed-delay") for key in settings)
    assert not any("attempts" in key for key in settings if key.startswith("restart-strategy."))


def test_the_flink_retry_delay_is_capped_so_recovery_after_an_outage_is_quick():
    settings = _flink_settings()
    assert settings["restart-strategy.exponential-delay.max-backoff"] in {"30 s", "1 min", "60 s"}
    assert settings["restart-strategy.exponential-delay.initial-backoff"] == "10 s"


def test_bronze_and_rejected_merges_include_the_message_text_in_their_identity():
    # Kafka offsets are reused when a topic is recreated, so coordinates alone skip new messages (D-024).
    live = "\n".join(load_statements(SQL_DIR / "003_merge_current.sql"))
    flink = "\n".join(load_statements(SQL_DIR / "003_merge_flink_rejected.sql"))
    for name, text, target in (
        ("live bronze", live, "local.bronze.live_observations"),
        ("spark rejected", live, "local.silver.rejected_records"),
        ("flink rejected", flink, "local.silver.rejected_records"),
    ):
        merges = [s for s in text.split("MERGE INTO")[1:] if s.strip().startswith(target)]
        assert merges, name
        assert all("t.payload <=> s.payload" in m for m in merges), f"{name} merge ignores the message text"
