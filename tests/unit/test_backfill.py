import hashlib
import itertools
import json
from datetime import UTC, date, datetime, timedelta

import pytest

from batch import backfill
from batch.backfill import (
    BackfillError,
    Summary,
    Window,
    initial_windows,
    is_complete,
    main,
    normalize_window,
    process_window,
    run_backfill,
)
from schemas.earthquake import EarthquakeEvent
from tests.catalog_support import BASE, FakeCatalog, feature, week_of_events

# --- planning ------------------------------------------------------------------------------


def test_initial_windows_cover_the_range_without_gaps_or_overlap():
    windows = initial_windows(date(2025, 1, 1), date(2025, 1, 18), 7)
    assert [(w.start.day, w.end.day) for w in windows] == [(1, 8), (8, 15), (15, 18)]
    assert all(a.end == b.start for a, b in itertools.pairwise(windows))
    assert windows[0].start == BASE and windows[-1].end == datetime(2025, 1, 18, tzinfo=UTC)


@pytest.mark.parametrize(
    ("start", "end", "days"),
    [(date(2025, 1, 5), date(2025, 1, 5), 7), (date(2025, 1, 6), date(2025, 1, 5), 7),
     (date(2025, 1, 1), date(2025, 1, 5), 0)],
)
def test_invalid_ranges_are_refused(start, end, days):
    with pytest.raises(ValueError):
        initial_windows(start, end, days)


def test_window_ids_are_readable_and_filename_safe():
    whole = Window(BASE, BASE + timedelta(days=7))
    assert whole.window_id(2.5) == "2025-01-01_2025-01-08_m2.5"
    assert whole.window_id(None) == "2025-01-01_2025-01-08_mall"
    part = Window(BASE, BASE + timedelta(hours=12))
    assert part.window_id(3) == "2025-01-01T000000_2025-01-01T120000_m3"
    assert ":" not in part.window_id(3)


def test_halves_are_contiguous_and_snap_to_the_hour():
    window = Window(BASE, BASE + timedelta(days=1, minutes=40))
    first, second = window.halves()
    assert first.start == window.start and second.end == window.end and first.end == second.start
    assert first.end.minute == 0 and first.end.second == 0


def test_a_window_cannot_be_split_below_one_hour():
    with pytest.raises(BackfillError):
        Window(BASE, BASE + timedelta(hours=1)).halves()


# --- one window ----------------------------------------------------------------------------


def run_one(tmp_path, catalog, min_magnitude=None, window=None, **kwargs):
    summary = Summary()
    window = window or Window(BASE, BASE + timedelta(days=7))
    process_window(catalog, window, min_magnitude, tmp_path, summary, **kwargs)
    return summary


def test_a_window_writes_its_files_and_a_complete_manifest(tmp_path):
    catalog = FakeCatalog(week_of_events())
    summary = run_one(tmp_path, catalog, min_magnitude=2.5)
    wid = "2025-01-01_2025-01-08_m2.5"
    manifest = json.loads((tmp_path / f"{wid}.manifest.json").read_text())

    assert manifest["source"] == "usgs-fdsn"
    assert (manifest["start_date"], manifest["end_date"], manifest["min_magnitude"]) == (
        "2025-01-01", "2025-01-08", 2.5
    )
    assert manifest["status"] == "completed"
    assert manifest["record_count"] == 7
    assert manifest["checksum"] == hashlib.sha256((tmp_path / f"{wid}.geojson").read_bytes()).hexdigest()
    assert (manifest["valid_count"], manifest["rejected_count"]) == (7, 0)
    assert (summary.downloaded, summary.events, summary.valid) == (1, 7, 7)
    for suffix in (".geojson", ".bronze.jsonl", ".silver.jsonl", ".rejected.jsonl"):
        assert (tmp_path / f"{wid}{suffix}").exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_an_empty_window_is_still_recorded_as_complete(tmp_path):
    run_one(tmp_path, FakeCatalog([]))
    manifest = json.loads((tmp_path / "2025-01-01_2025-01-08_mall.manifest.json").read_text())
    assert manifest["status"] == "completed" and manifest["record_count"] == 0


def test_magnitude_filter_is_applied(tmp_path):
    events = [feature(1, BASE + timedelta(hours=1), 1.0), feature(2, BASE + timedelta(hours=2), 4.0)]
    run_one(tmp_path, FakeCatalog(events), min_magnitude=2.5)
    manifest = json.loads(next(tmp_path.glob("*.manifest.json")).read_text())
    assert manifest["record_count"] == 1


def test_a_completed_window_is_skipped_on_the_next_run(tmp_path):
    catalog = FakeCatalog(week_of_events())
    run_one(tmp_path, catalog)
    second = run_one(tmp_path, catalog)
    assert (second.skipped, second.downloaded) == (1, 0)
    assert len(catalog.fetch_calls) == 1 and catalog.count_calls == 1


def test_force_downloads_again(tmp_path):
    catalog = FakeCatalog(week_of_events())
    run_one(tmp_path, catalog)
    run_one(tmp_path, catalog, force=True)
    assert len(catalog.fetch_calls) == 2


def test_a_corrupted_download_is_detected_and_fetched_again(tmp_path):
    catalog = FakeCatalog(week_of_events())
    run_one(tmp_path, catalog)
    wid = "2025-01-01_2025-01-08_mall"
    (tmp_path / f"{wid}.geojson").write_bytes(b"tampered")
    assert not is_complete(tmp_path, wid)
    run_one(tmp_path, catalog)
    assert len(catalog.fetch_calls) == 2 and is_complete(tmp_path, wid)


def test_a_missing_output_file_means_the_window_is_not_complete(tmp_path):
    run_one(tmp_path, FakeCatalog(week_of_events()))
    wid = "2025-01-01_2025-01-08_mall"
    (tmp_path / f"{wid}.silver.jsonl").unlink()
    assert not is_complete(tmp_path, wid)


def test_a_failed_fetch_leaves_a_failed_manifest_and_no_data_files(tmp_path):
    catalog = FakeCatalog(week_of_events())
    catalog.fail_fetch_for = {BASE}
    with pytest.raises(BackfillError, match="simulated outage"):
        run_one(tmp_path, catalog)
    manifest = json.loads(next(tmp_path.glob("*.manifest.json")).read_text())
    assert manifest["status"] == "failed" and "simulated outage" in manifest["error"]
    assert not list(tmp_path.glob("*.geojson")) and not list(tmp_path.glob("*.jsonl"))
    assert not is_complete(tmp_path, "2025-01-01_2025-01-08_mall")


# --- splitting -----------------------------------------------------------------------------


def test_an_oversized_window_is_split_until_every_part_fits(tmp_path, monkeypatch):
    monkeypatch.setattr(backfill, "MAX_EVENTS_PER_QUERY", 3)
    events = [feature(i, BASE + timedelta(hours=7 * i)) for i in range(20)]  # 20 events in 140 hours
    catalog = FakeCatalog(events)
    summary = run_one(tmp_path, catalog, window=Window(BASE, BASE + timedelta(days=7)))

    assert summary.split >= 1
    manifests = [json.loads(p.read_text()) for p in tmp_path.glob("*.manifest.json")]
    assert all(m["status"] == "completed" and m["record_count"] <= 3 for m in manifests)
    # Every event is in exactly one window: no gaps and no double counting.
    seen = [
        json.loads(line)["event_id"]
        for p in sorted(tmp_path.glob("*.silver.jsonl"))
        for line in p.read_text().splitlines()
    ]
    assert sorted(seen) == sorted(f["id"] for f in events)
    assert summary.events == 20


def test_split_windows_meet_exactly_at_their_boundaries(tmp_path, monkeypatch):
    monkeypatch.setattr(backfill, "MAX_EVENTS_PER_QUERY", 1)
    boundary = BASE + timedelta(days=3, hours=12)  # an event exactly on a likely split point
    events = [feature(0, BASE + timedelta(hours=1)), feature(1, boundary), feature(2, BASE + timedelta(days=6))]
    run_one(tmp_path, FakeCatalog(events), window=Window(BASE, BASE + timedelta(days=7)))
    seen = [
        json.loads(line)["event_id"]
        for p in tmp_path.glob("*.silver.jsonl")
        for line in p.read_text().splitlines()
    ]
    assert sorted(seen) == ["hist-0000", "hist-0001", "hist-0002"]


def test_a_window_that_cannot_be_split_further_fails_clearly(tmp_path, monkeypatch):
    monkeypatch.setattr(backfill, "MAX_EVENTS_PER_QUERY", 1)
    when = BASE + timedelta(minutes=5)
    catalog = FakeCatalog([feature(1, when), feature(2, when + timedelta(minutes=1))])
    with pytest.raises(BackfillError, match="one hour"):
        run_one(tmp_path, catalog, window=Window(BASE, BASE + timedelta(hours=2)))


# --- a whole run ---------------------------------------------------------------------------


def test_run_backfill_continues_after_a_failed_window_and_a_rerun_finishes_it(tmp_path):
    events = [feature(i, BASE + timedelta(days=i * 3)) for i in range(4)]  # across three windows
    catalog = FakeCatalog(events)
    catalog.fail_fetch_for = {BASE + timedelta(days=7)}  # the second window
    first = run_backfill(catalog, date(2025, 1, 1), date(2025, 1, 22), None, tmp_path)
    assert (first.downloaded, len(first.failed)) == (2, 1)

    catalog.fail_fetch_for = set()
    fetches_before = len(catalog.fetch_calls)
    second = run_backfill(catalog, date(2025, 1, 1), date(2025, 1, 22), None, tmp_path)
    assert (second.downloaded, second.skipped, second.failed) == (1, 2, [])
    assert len(catalog.fetch_calls) == fetches_before + 1, "only the failed window is fetched again"


def test_running_the_same_backfill_twice_produces_identical_files(tmp_path):
    catalog = FakeCatalog(week_of_events())
    run_backfill(catalog, date(2025, 1, 1), date(2025, 1, 8), 2.0, tmp_path)
    before = {p.name: p.read_bytes() for p in tmp_path.iterdir()}
    run_backfill(catalog, date(2025, 1, 1), date(2025, 1, 8), 2.0, tmp_path)
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


def test_main_runs_end_to_end_and_reports_the_exit_code(tmp_path):
    catalog = FakeCatalog(week_of_events())
    args = ["--start", "2025-01-01", "--end", "2025-01-08", "--min-magnitude", "2.5",
            "--out-dir", str(tmp_path)]
    assert main(args, client=catalog) == 0
    catalog.fail_fetch_for = {BASE}
    assert main([*args, "--force"], client=catalog) == 1
    assert main(["--start", "2025-01-08", "--end", "2025-01-01", "--out-dir", str(tmp_path)], client=catalog) == 2


# --- normalization -------------------------------------------------------------------------


def test_normalization_separates_valid_rejected_and_keeps_everything_in_bronze():
    good = feature(1, BASE + timedelta(hours=1))
    bad = feature(2, BASE + timedelta(hours=2))
    bad["geometry"]["coordinates"] = [500.0, 10.0, 1.0]
    body = json.dumps({"type": "FeatureCollection", "features": [good, bad]}).encode()
    window = Window(BASE, BASE + timedelta(days=7))
    files = normalize_window(body, window, 2.5, "checksum-abc", datetime(2026, 1, 1, tzinfo=UTC))

    bronze = [json.loads(line) for line in files[".bronze.jsonl"].decode().splitlines()]
    silver = [EarthquakeEvent.from_json(line) for line in files[".silver.jsonl"].decode().splitlines()]
    rejected = [json.loads(line) for line in files[".rejected.jsonl"].decode().splitlines()]

    assert [b["event_id"] for b in bronze] == ["hist-0001", "hist-0002"]
    assert all(b["source_checksum"] == "checksum-abc" and b["min_magnitude"] == 2.5 for b in bronze)
    assert (bronze[0]["window_start"], bronze[0]["window_end"]) == ("2025-01-01", "2025-01-08")
    assert bronze[0]["payload_sha256"] == hashlib.sha256(bronze[0]["payload"].encode()).hexdigest()
    assert json.loads(bronze[0]["payload"]) == good
    assert [e.event_id for e in silver] == ["hist-0001"]
    assert silver[0].record_hash and silver[0].ingested_at == datetime(2026, 1, 1, tzinfo=UTC)
    assert [r["event_id"] for r in rejected] == ["hist-0002"]
    assert "longitude" in rejected[0]["reason"]


def test_normalization_matches_the_live_path_hash_for_the_same_event():
    """A historical copy of an event and its live copy must share a record_hash."""
    from processing.normalize import normalize_feature

    f = feature(1, BASE + timedelta(hours=1))
    body = json.dumps({"type": "FeatureCollection", "features": [f]}).encode()
    files = normalize_window(body, Window(BASE, BASE + timedelta(days=1)), None, "c", datetime(2026, 1, 1, tzinfo=UTC))
    historical = EarthquakeEvent.from_json(files[".silver.jsonl"].decode().strip())
    live = normalize_feature(f, datetime(2026, 5, 5, tzinfo=UTC))
    assert historical.record_hash == live.record_hash
