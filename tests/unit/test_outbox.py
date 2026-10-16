"""The reconciliation hand-over: atomic files, durable acceptance, crash windows, and the delivery contract."""

import copy
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from ingestion import reconcile
from ingestion.change_detector import BatchConflict, ChangeDetector
from ingestion.live.poller import Poller
from ingestion.outbox import BatchInvalid, Intake, Outbox, features_sha256, safe_batch_id

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "usgs_all_hour_sample.geojson"
SOURCE = {"kind": "usgs-fdsn-updatedafter", "queried_at": "2026-10-16T01:00:00+00:00",
          "updated_after": "2026-10-14T01:00:00+00:00"}


def features(**overrides) -> list[dict]:
    items = copy.deepcopy(json.loads(FIXTURE.read_text())["features"])
    for item in items:
        item["properties"].update(overrides.get(item["id"], {}))
    return items


@pytest.fixture
def box(tmp_path):
    outbox = Outbox(tmp_path / "outbox")
    outbox.ensure()
    return outbox


@pytest.fixture
def detector(tmp_path):
    with ChangeDetector(str(tmp_path / "state.db")) as d:
        yield d


# --- the files -------------------------------------------------------------------------------

def test_a_written_batch_appears_in_ready_whole_and_leaves_nothing_in_tmp(box):
    header = box.write_batch("b1", features(), SOURCE)
    assert box.ready_path("b1").exists() and list((box.root / "tmp").iterdir()) == []
    assert header["count"] == 2 and header["sha256"] == features_sha256(features())
    document = box.read_batch(box.ready_path("b1"))
    assert document["source"] == SOURCE and len(document["features"]) == 2


def test_the_collector_ignores_files_that_are_still_being_written(box, detector):
    (box.root / "tmp" / "b1.json").write_text('{"half": ')
    assert Intake(box, detector).accept_ready().accepted == []


@pytest.mark.parametrize("damage", ["checksum", "count", "name", "format", "garbage"])
def test_a_damaged_batch_is_rejected_with_a_reason_and_records_nothing(box, detector, damage):
    box.write_batch("b1", features(), SOURCE)
    path = box.ready_path("b1")
    document = json.loads(path.read_text())
    if damage == "checksum":
        document["features"][0]["properties"]["mag"] = 9.9
    elif damage == "count":
        document["count"] = 5
    elif damage == "name":
        document["batch_id"] = "other"
    elif damage == "format":
        document["format"] = 99
    path.write_text("not json" if damage == "garbage" else json.dumps(document))

    report = Intake(box, detector).accept_ready()
    assert report.rejected == 1 and report.accepted == []
    assert box.read_status("b1")["state"] == "rejected" and box.read_status("b1")["reason"]
    assert (box.root / "rejected" / "b1.json").exists() and not path.exists()
    assert detector.pending_count() == 0 and detector.get_batch("b1") is None


def test_read_batch_names_what_is_wrong(box):
    box.write_batch("b1", features(), SOURCE)
    path = box.ready_path("b1")
    document = json.loads(path.read_text())
    document["sha256"] = "0" * 64
    path.write_text(json.dumps(document))
    with pytest.raises(BatchInvalid, match="checksum"):
        box.read_batch(path)


def test_batch_ids_from_airflow_run_ids_are_file_name_safe():
    assert safe_batch_id("scheduled__2026-10-16T00:00:00+00:00") == "scheduled__2026-10-16T00_00_00+00_00"
    assert safe_batch_id("../../etc/passwd") == "_.._etc_passwd"
    assert safe_batch_id(".hidden") == "hidden"
    with pytest.raises(ValueError):
        safe_batch_id("...")


# --- acceptance ------------------------------------------------------------------------------

def test_acceptance_commits_the_revisions_as_pending_then_answers_then_removes_the_file(box, detector):
    box.write_batch("b1", features(), SOURCE)
    report = Intake(box, detector).accept_ready()
    assert [r.batch_id for r in report.accepted] == ["b1"] and len(report.changes) == 2
    assert detector.pending_count() == 2, "committed as pending, not yet sent to Kafka"
    status = box.read_status("b1")
    assert status["state"] == "accepted" and status["new_revisions"] == 2
    assert status["queried_at"] == SOURCE["queried_at"]
    assert not box.ready_path("b1").exists()


def test_a_batch_where_everything_is_already_known_is_accepted_with_no_new_revisions(box, detector):
    for i in (1, 2):  # the same events twice: the overlap window
        box.write_batch(f"b{i}", features(), SOURCE)
        Intake(box, detector).accept_ready()
    assert box.read_status("b1")["new_revisions"] == 2 and box.read_status("b2")["new_revisions"] == 0
    assert detector.pending_count() == 2


def test_a_revision_in_a_later_batch_is_recorded_as_a_change(box, detector):
    box.write_batch("b1", features(), SOURCE)
    Intake(box, detector).accept_ready()
    box.write_batch("b2", features(**{"example-quake-001": {"mag": 5.1, "updated": 1791878280000 + 600000}}), SOURCE)
    report = Intake(box, detector).accept_ready()
    assert [c.kind.value for c in report.changes] == ["revised"] and report.changes[0].event.magnitude == 5.1


def test_invalid_features_are_counted_but_do_not_reject_the_batch(box, detector):
    items = features() + [{"id": "broken", "properties": {}, "geometry": None}]
    box.write_batch("b1", items, SOURCE)
    report = Intake(box, detector).accept_ready()
    assert report.invalid_features == 1 and report.rejected == 0
    status = box.read_status("b1")
    assert (status["feature_count"], status["valid_count"], status["rejected_count"]) == (3, 2, 1)


def test_a_crash_after_the_commit_and_before_the_answer_is_repaired_without_duplicates(box, detector, monkeypatch):
    box.write_batch("b1", features(), SOURCE)

    def crash(*_args, **_kwargs):
        raise RuntimeError("power cut")

    intake = Intake(box, detector)
    monkeypatch.setattr(box, "write_status", crash)
    with pytest.raises(RuntimeError):
        intake.accept_ready()
    # The database has the batch, but Airflow was never told, and the file is still there.
    assert detector.get_batch("b1") is not None and detector.pending_count() == 2
    assert box.read_status("b1") is None and box.ready_path("b1").exists()

    monkeypatch.undo()
    report = intake.accept_ready()
    assert box.read_status("b1")["state"] == "accepted" and not box.ready_path("b1").exists()
    assert report.changes == [] and detector.pending_count() == 2, "nothing was recorded a second time"


def test_the_same_batch_id_with_different_content_is_rejected(box, detector):
    box.write_batch("b1", features(), SOURCE)
    Intake(box, detector).accept_ready()
    box.write_batch("b1", features(**{"example-quake-001": {"mag": 1.0}}), SOURCE)
    report = Intake(box, detector).accept_ready()
    assert report.rejected == 1
    with pytest.raises(BatchConflict):
        detector.accept_batch("b1", "different", [], 0, {})


# --- published is a separate state -----------------------------------------------------------

def test_a_batch_is_published_only_when_kafka_has_confirmed_every_revision(box, detector):
    box.write_batch("b1", features(), SOURCE)
    intake = Intake(box, detector)
    intake.accept_ready()
    assert intake.settle() == [] and box.read_status("b1")["state"] == "accepted"

    first, second = detector.pending()
    detector.mark_published(first.event.event_id, first.event.record_hash)
    assert intake.settle() == [], "one of two confirmed is not enough"
    detector.mark_published(second.event.event_id, second.event.record_hash)
    settled = intake.settle()
    assert [r.batch_id for r in settled] == ["b1"]
    status = box.read_status("b1")
    assert status["state"] == "published" and status["published_at"]
    assert intake.settle() == [], "settled once"


def test_a_batch_with_no_new_revisions_is_published_at_once(box, detector):
    box.write_batch("b1", features(), SOURCE)
    Intake(box, detector).accept_ready()
    box.write_batch("b2", features(), SOURCE)
    intake = Intake(box, detector)
    intake.accept_ready()
    assert [r.batch_id for r in intake.settle()] == ["b2"]


def test_accepted_is_not_published_in_the_status_file(box, detector):
    box.write_batch("b1", features(), SOURCE)
    Intake(box, detector).accept_ready()
    assert box.read_status("b1")["state"] != "published" and box.read_status("b1")["published_at"] is None


# --- the collector's loop --------------------------------------------------------------------

def test_the_poller_keeps_running_when_the_outbox_fails(box, detector):
    class Broken(Intake):
        def accept_ready(self):
            raise OSError("disk full")

    poller = Poller([], detector, intake=Broken(box, detector))
    poller.run_intake()  # must not raise


def test_the_poller_does_nothing_without_an_outbox(detector):
    Poller([], detector).run_intake()


# --- the commands ----------------------------------------------------------------------------

class FakeClient:
    def __init__(self, items, count=None):
        self.items, self.count, self.asked = items, count, []

    def count_updated(self, after, min_magnitude=None):
        self.asked.append(after)
        return len(self.items) if self.count is None else self.count

    def fetch_updated(self, after, min_magnitude=None):
        from ingestion.historical_client import CatalogWindow

        body = json.dumps({"type": "FeatureCollection", "features": self.items}).encode()
        return CatalogWindow(body=body, count=len(self.items))


def fetch_args(outbox, **extra):
    base = ["--outbox", str(outbox), "fetch", "--batch-id", "scheduled__2026-10-16T01:00:00+00:00"]
    return reconcile.build_parser().parse_args(base + [x for k, v in extra.items() for x in (f"--{k.replace('_', '-')}", str(v))])


NOW = datetime(2026, 10, 16, 1, 0, tzinfo=UTC)


def test_fetch_asks_from_the_watermark_minus_the_overlap_and_records_the_query_boundaries(box, capsys):
    client = FakeClient(features())
    args = fetch_args(box.root, updated_after="2026-10-15T00:00:00+00:00", overlap_hours=24)
    assert reconcile.fetch(args, client=client, now=lambda: NOW) == 0
    assert client.asked == [datetime(2026, 10, 14, 0, 0, tzinfo=UTC)]
    header = box.read_header("scheduled__2026-10-16T01_00_00+00_00")
    assert header["source"]["queried_at"] == NOW.isoformat()
    assert header["source"]["updated_after"] == "2026-10-14T00:00:00+00:00"
    assert header["source"]["watermark"] == "2026-10-15T00:00:00+00:00" and header["count"] == 2
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["count"] == 2


def test_the_first_run_without_a_watermark_looks_back_a_fixed_time(box):
    client = FakeClient([])
    reconcile.fetch(fetch_args(box.root, initial_lookback_hours=10), client=client, now=lambda: NOW)
    assert client.asked == [datetime(2026, 10, 15, 15, 0, tzinfo=UTC)]


def test_too_many_events_for_one_query_fails_without_writing_a_batch(box):
    code = reconcile.fetch(fetch_args(box.root), client=FakeClient([], count=20_001), now=lambda: NOW)
    assert code == reconcile.EXIT_TOO_MANY and list((box.root / "ready").iterdir()) == []


def test_a_retried_fetch_does_not_write_a_second_batch(box, capsys):
    client = FakeClient(features())
    reconcile.fetch(fetch_args(box.root), client=client, now=lambda: NOW)
    capsys.readouterr()
    again = FakeClient(features(**{"example-quake-001": {"mag": 1.0}}))
    assert reconcile.fetch(fetch_args(box.root), client=again, now=lambda: NOW.replace(hour=5)) == 0
    assert again.asked == [], "no query was made"
    assert json.loads(capsys.readouterr().out)["queried_at"] == NOW.isoformat(), "the original query time is kept"


def test_await_ack_succeeds_only_when_the_collector_has_accepted(box, detector, capsys):
    args = reconcile.build_parser().parse_args(["--outbox", str(box.root), "await-ack", "--batch-id", "b1", "--timeout", "0"])
    assert reconcile.await_ack(args) == reconcile.EXIT_NO_ANSWER, "a file in ready/ is not an acknowledgement"
    box.write_batch("b1", features(), SOURCE)
    assert reconcile.await_ack(args) == reconcile.EXIT_NO_ANSWER
    Intake(box, detector).accept_ready()
    assert reconcile.await_ack(args) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["state"] == "accepted"


def test_await_ack_fails_for_a_rejected_batch(box, detector):
    box.write_batch("b1", features(), SOURCE)
    box.ready_path("b1").write_text("garbage")
    Intake(box, detector).accept_ready()
    args = reconcile.build_parser().parse_args(["--outbox", str(box.root), "await-ack", "--batch-id", "b1", "--timeout", "0"])
    assert reconcile.await_ack(args) == reconcile.EXIT_REJECTED


def test_await_published_waits_for_kafka_not_just_the_collector(box, detector):
    box.write_batch("b1", features(), SOURCE)
    intake = Intake(box, detector)
    intake.accept_ready()
    args = reconcile.build_parser().parse_args(["--outbox", str(box.root), "await-published", "--batch-id", "b1", "--timeout", "0"])
    assert reconcile.await_published(args) == reconcile.EXIT_NOT_PUBLISHED
    for change in detector.pending():
        detector.mark_published(change.event.event_id, change.event.record_hash)
    intake.settle()
    assert reconcile.await_published(args) == 0


def test_prune_removes_old_finished_files_but_keeps_batches_still_waiting_for_kafka(box, detector):
    box.write_batch("b1", features(), SOURCE)
    box.write_batch("b2", features(**{"example-quake-001": {"mag": 2.0}}), SOURCE)
    intake = Intake(box, detector)
    intake.accept_ready()
    for change in detector.pending():
        if change.event.magnitude != 2.0:
            detector.mark_published(change.event.event_id, change.event.record_hash)
    intake.settle()
    states = {b: box.read_status(b)["state"] for b in ("b1", "b2")}
    assert states == {"b1": "published", "b2": "accepted"}
    long_ago = 1_000_000_000
    for name in ("b1", "b2"):
        os.utime(box.status_path(name), (long_ago, long_ago))
    assert box.prune(older_than_days=14) == 1
    assert not box.status_path("b1").exists() and box.status_path("b2").exists()
