import json

from batch.ingest_history import KINDS, completed_windows


def make_window(directory, window_id, status="completed", with_files=True):
    (directory / f"{window_id}.manifest.json").write_text(json.dumps({"status": status}))
    if with_files:
        for suffix in KINDS.values():
            (directory / f"{window_id}{suffix}").write_text("")


def test_only_completed_windows_with_all_files_are_loaded(tmp_path):
    make_window(tmp_path, "a")
    make_window(tmp_path, "b", status="failed")
    make_window(tmp_path, "c", with_files=False)
    (tmp_path / "d.manifest.json").write_text("not json")
    make_window(tmp_path, "e")
    assert completed_windows(tmp_path) == ["a", "e"]


def test_an_empty_or_missing_directory_has_nothing_to_load(tmp_path):
    assert completed_windows(tmp_path) == []
    assert completed_windows(tmp_path / "missing") == []
