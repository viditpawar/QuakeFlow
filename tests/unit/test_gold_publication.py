"""The parts of the versioned Gold build that need no Spark: pinning, view text, and the safety checks."""

import pytest

from batch.build_gold import builds_table, keep_clause, short_name, view_statement
from batch.load_log import pin_silver

UUID = "12345678-1234-1234-1234-123456789abc"


def test_both_silver_tables_are_pinned_to_their_own_snapshot():
    sql = (
        "SELECT * FROM local.silver.earthquakes_current c JOIN local.silver.earthquake_versions v "
        "ON c.event_id = v.event_id"
    )
    pinned = pin_silver(sql, "local.silver", 111, 222)
    assert "local.silver.earthquakes_current.snapshot_id_111 c" in pinned
    assert "local.silver.earthquake_versions.snapshot_id_222 v" in pinned


def test_pinning_follows_the_namespace_and_leaves_other_tables_alone():
    sql = "SELECT 1 FROM zt1_silver.earthquakes_current, local.silver.rejected_records, local.silver.earthquakes_current"
    pinned = pin_silver(sql, "zt1_silver", 5, 6)
    assert "zt1_silver.earthquakes_current.snapshot_id_5" in pinned
    assert "local.silver.rejected_records" in pinned and "local.silver.earthquakes_current" in pinned.split(",")[-1]


def test_pinning_twice_does_not_stack_selectors():
    once = pin_silver("FROM local.silver.earthquakes_current", "local.silver", 1, 2)
    assert pin_silver(once, "local.silver", 1, 2) == once


def test_the_published_view_filters_by_the_published_build():
    sql = view_statement("local.gold.regional_activity", ["event_month", "region"], "local.gold.published_build")
    assert sql == (
        "CREATE OR REPLACE VIEW local.gold.regional_activity AS SELECT event_month, region "
        "FROM local.gold.regional_activity_builds "
        "WHERE build_id = (SELECT build_id FROM local.gold.published_build)"
    )
    assert "build_id," not in sql.split("SELECT", 1)[1].split("FROM")[0], "readers must not see build_id"


def test_names():
    assert builds_table("local.gold.x") == "local.gold.x_builds"
    assert short_name("local.gold.x") == "x"


def test_only_uuids_can_reach_the_sql():
    assert keep_clause([UUID]) == f"'{UUID}'"
    for bad in ("x'; DROP TABLE t; --", UUID.upper(), UUID + "0", "", "pin-" + UUID):
        with pytest.raises(ValueError):
            keep_clause([bad])


def test_a_build_needs_exactly_one_input_choice():
    from batch.build_gold import main

    with pytest.raises(SystemExit):
        main(["--pin-now", "--load-run-id", "x"])
    with pytest.raises(SystemExit):
        main(["--load-run-id", "x", "--publish-existing", UUID])
