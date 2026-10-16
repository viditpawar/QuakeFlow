"""The parts of the benchmark that need no running services: the synthetic data and the arithmetic."""

import json

import pytest

from benchmarks.run_benchmark import _mib, spread, summary_table, synthetic_messages


def test_synthetic_data_has_exactly_the_distinct_revisions_and_events_it_promises():
    messages, expected = synthetic_messages(500, "t")
    assert expected == {"revisions": 500, "events": 450, "raw_messages": 550}
    assert len(messages) == 550
    revisions = {(key, json.loads(text)["record_hash"]) for key, text in messages}
    assert len(revisions) == 500 and len({key for key, _ in revisions}) == 450


def test_synthetic_data_is_repeatable_and_a_different_seed_gives_different_data():
    assert synthetic_messages(100, "t", seed=3)[0] == synthetic_messages(100, "t", seed=3)[0]
    assert synthetic_messages(100, "t", seed=3)[0] != synthetic_messages(100, "t", seed=4)[0]


def test_synthetic_events_are_valid_earthquakes_the_pipeline_will_keep():
    messages, _ = synthetic_messages(50, "t")
    for _, text in messages:
        event = json.loads(text)
        assert event["event_type"] == "earthquake" and -90 <= event["latitude"] <= 90
        assert -180 <= event["longitude"] <= 180 and event["record_hash"]


@pytest.mark.parametrize(("text", "mib"), [("519.1MiB", 519.1), ("1.5GiB", 1536.0), ("2048KiB", 2.0), ("0B", None)])
def test_docker_memory_sizes_are_parsed(text, mib):
    if mib is None:
        with pytest.raises(ValueError):
            _mib(text)
    else:
        assert _mib(text) == pytest.approx(mib)


def test_the_spread_is_the_median_and_the_range_and_ignores_missing_values():
    assert spread([3.0, 1.0, 2.0]) == "2.0 (1.0 to 3.0)"
    assert spread([None, 4.0]) == "4.0 (4.0 to 4.0)"
    assert spread([]) == "n/a"


def test_the_table_counts_only_correct_runs_and_says_how_many_failed():
    good = {"size": 100, "correct": True, "produce_s": 1.0, "flink_visible_s": 9.0, "ingest_net_s": 20.0,
            "gold_wall_s": 60.0, "resources": {"peak_all_containers_mib": 5000}}
    bad = {"size": 100, "correct": False, "error": "x", "resources": {"peak_all_containers_mib": 1}}
    table = summary_table([good, {**good, "flink_visible_s": 19.0}, bad])
    assert "| 100 | 2 of 3 |" in table
    assert "15.0 (10.0 to 20.0)" in table, "end to end is produce plus visible: 10 s and 20 s"
    assert "7.5 (5.0 to 10.0)" in table, "revisions per second: 100 / 10 s and 100 / 20 s"
    compact = summary_table([good], compact=True)
    assert compact.count("|") < table.count("|") and "5.0 (5.0 to 5.0)" in compact


def test_a_run_that_never_finished_has_no_rate():
    from benchmarks.run_benchmark import end_to_end_s, rate

    run = {"size": 100, "produce_s": 1.0, "flink_visible_s": None}
    assert end_to_end_s(run) is None and rate(run, None) is None and rate(run, 0) is None
