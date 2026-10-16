"""The parts of the backfill verification that need no Spark."""

from datetime import date

from batch.backfill import parse_magnitude
from batch.verify_history import count_problems, select_windows


def manifest(start, end, magnitude=2.5, status="completed", records=10, valid=9, rejected=1):
    return {"status": status, "start_date": start, "end_date": end, "min_magnitude": magnitude,
            "record_count": records, "valid_count": valid, "rejected_count": rejected}


MANIFESTS = {
    "a": manifest("2025-01-01", "2025-01-08"),
    "b": manifest("2025-01-08", "2025-01-15"),
    "c": manifest("2025-01-01", "2025-01-08", magnitude=None),
    "d": manifest("2025-01-01", "2025-01-08", status="failed"),
    "e": manifest("2025-01-01T00:00:00", "2025-01-01T12:00:00"),
}


def test_windows_inside_the_range_with_the_same_magnitude_are_selected():
    assert select_windows(MANIFESTS, date(2025, 1, 1), date(2025, 1, 8), 2.5) == ["a", "e"]
    assert select_windows(MANIFESTS, date(2025, 1, 1), date(2025, 1, 15), 2.5) == ["a", "b", "e"]


def test_other_magnitudes_and_unfinished_windows_are_never_selected():
    assert select_windows(MANIFESTS, date(2025, 1, 1), date(2025, 1, 8), None) == ["c"]
    assert "d" not in select_windows(MANIFESTS, date(2024, 1, 1), date(2026, 1, 1), 2.5)


def test_a_range_with_no_windows_selects_nothing():
    assert select_windows(MANIFESTS, date(2020, 1, 1), date(2020, 2, 1), 2.5) == []


def test_a_window_that_only_partly_overlaps_is_not_counted_as_covering_the_range():
    assert select_windows(MANIFESTS, date(2025, 1, 3), date(2025, 1, 8), 2.5) == []


def test_counts_that_do_not_add_up_are_reported():
    assert count_problems(MANIFESTS, ["a"]) == []
    broken = {"x": manifest("2025-01-01", "2025-01-08", records=10, valid=7, rejected=1)}
    assert "10 downloaded but 7 valid + 1 rejected" in count_problems(broken, ["x"])[0]


def test_an_empty_minimum_magnitude_means_all_magnitudes():
    assert parse_magnitude("") is None and parse_magnitude(" ") is None
    assert parse_magnitude("4.5") == 4.5
