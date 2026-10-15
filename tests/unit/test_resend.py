import sqlite3

import pytest

from ingestion.change_detector import ChangeDetector
from ingestion.resend import main, parse_known
from tests.unit.test_change_detector import make_event

HASH_A, HASH_B = "a" * 64, "b" * 64


def publish(detector, *events):
    detector.detect(list(events))
    for event in events:
        detector.mark_published(event.event_id, event.record_hash)


# --- parsing the list of what the lakehouse holds ---------------------------------------------


def test_parse_known_reads_tab_separated_pairs():
    text = f"evt-1\t{HASH_A}\nevt-2\t{HASH_B}\n"
    assert parse_known(text) == {("evt-1", HASH_A), ("evt-2", HASH_B)}


def test_parse_known_ignores_log_noise_and_malformed_lines():
    text = (
        "WARN NativeCodeLoader: Unable to load native-hadoop library\n"
        f"evt-1\t{HASH_A}\r\n"
        "Time taken: 1.2 seconds\n"
        "evt-2\tnot-a-hash\n"
        f"\t{HASH_B}\n"
        f"evt-3\t{HASH_B}\textra\n"
        "Fetched 3 row(s)\n"
    )
    assert parse_known(text) == {("evt-1", HASH_A)}


# --- marking revisions unsent ----------------------------------------------------------------


def test_only_sent_revisions_missing_from_the_known_set_become_pending():
    with ChangeDetector() as detector:
        kept, lost = make_event(event_id="kept"), make_event(event_id="lost")
        publish(detector, kept, lost)
        assert detector.pending_count() == 0

        missing = detector.mark_unsent_unless_known({("kept", kept.record_hash)}, dry_run=False)

        assert missing == [("lost", lost.record_hash)]
        assert [c.event.event_id for c in detector.pending()] == ["lost"]


def test_the_resent_revision_is_the_full_original_content():
    with ChangeDetector() as detector:
        original = make_event(event_id="lost", magnitude=4.6)
        publish(detector, original)
        detector.mark_unsent_unless_known({("other", HASH_A)}, dry_run=False)
        assert detector.pending()[0].event == original


def test_a_dry_run_changes_nothing():
    with ChangeDetector() as detector:
        publish(detector, make_event(event_id="lost"))
        assert len(detector.mark_unsent_unless_known({("x", HASH_A)})) == 1
        assert detector.pending_count() == 0


def test_revisions_still_waiting_to_be_sent_are_left_alone():
    with ChangeDetector() as detector:
        detector.detect([make_event(event_id="waiting")])  # pending, never sent
        assert detector.mark_unsent_unless_known({("x", HASH_A)}, dry_run=False) == []
        assert detector.pending_count() == 1


# --- the command ------------------------------------------------------------------------------


@pytest.fixture
def state(tmp_path):
    db = tmp_path / "state.db"
    with ChangeDetector(str(db)) as detector:
        publish(detector, make_event(event_id="kept"), make_event(event_id="lost"))
    known_file = tmp_path / "known.tsv"
    known_file.write_text(f"kept\t{make_event(event_id='kept').record_hash}\n", encoding="utf-8")
    return db, known_file


def pending_in(db):
    connection = sqlite3.connect(db)
    try:
        return connection.execute("SELECT count(*) FROM observations WHERE published_at IS NULL").fetchone()[0]
    finally:
        connection.close()


def test_the_command_is_a_dry_run_unless_asked_to_apply(state, capsys):
    db, known = state
    assert main(["--db", str(db), "--known", str(known)]) == 0
    assert "dry run" in capsys.readouterr().out
    assert pending_in(db) == 0
    assert main(["--db", str(db), "--known", str(known), "--apply"]) == 0
    assert pending_in(db) == 1


def test_the_command_refuses_an_empty_known_list(state, tmp_path):
    db, _ = state
    empty = tmp_path / "empty.tsv"
    empty.write_text("Time taken: 1 seconds\n", encoding="utf-8")
    assert main(["--db", str(db), "--known", str(empty), "--apply"]) == 2
    assert pending_in(db) == 0


def test_the_command_refuses_to_resend_more_than_the_limit(state):
    db, known = state
    assert main(["--db", str(db), "--known", str(known), "--apply", "--limit", "0"]) == 2
    assert pending_in(db) == 0
