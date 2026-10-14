# Verification log

Results of running the whole pipeline on real USGS data. Each entry says what was run, what was measured, and what the result does not show.
Numbers come from one machine (16 GB Windows laptop, Docker Desktop), so they are examples, not benchmarks.

## 2026-10-14: full chain, USGS to the current-state table

**What was run.** Collector (`python -m ingestion.live.poller --once`), then the Flink validation job (restored from a retained checkpoint), then the Spark load (`batch/ingest_live.py`). One day earlier, the same chain had loaded the first 259 events, so this run saw a full day of USGS changes.

**What came through.**

| Stage | Result |
|---|---|
| Collector, one pass over both feeds | 8 new events from the hour feed, then 151 changes from the day feed (132 new events and 19 revised events). 159 revisions delivered to Kafka, each confirmed by Kafka. |
| Flink | 418 messages in, 418 out (259 from the day before plus 159), all distinct. 0 in the dead-letter topic. Checkpoints: 22 completed, 0 failed. |
| Spark load | Bronze 418 rows. Versions 418 rows across 399 events. Current 399 rows, one per event. 0 rejected. |

**Compared with USGS itself.** The two live feeds were fetched again independently and normalized with the project's own code. All 230 events in them were present in `earthquakes_current` with the same `record_hash`. Nothing was missing and nothing differed. The lakehouse also holds 169 older events that have left the feeds' windows, which this comparison cannot check.

**The 19 real revisions.** In `earthquake_versions`, 19 events had two versions. For all 19, the current row was the newest version. The fields that changed between versions:

| Field | Revisions that changed it (of 19) |
|---|---|
| review_status | 9 |
| depth_km | 7 |
| location (latitude or longitude) | 7 |
| event_time | 7 |
| magnitude | 5 |
| place | 1 |

Several revisions changed more than one field. A pipeline that watched only the magnitude would have missed most of them, which is why the record hash covers all source fields (D-009).

**Memory with everything running** (Kafka, Flink job and task managers, PostgreSQL, RustFS, after the Spark job): Kafka 509 MB, Flink 543 MB and 503 MB, PostgreSQL 42 MB, RustFS 469 MB of a 512 MB limit. The limit was raised to 768 MB because the margin was too small. Spark ran only while a job was running.

**What this does not show.**
- It covers one day of USGS changes and 19 revisions, a small sample.
- The 169 events outside the feed windows were not compared with USGS.
- No timings were measured: the Flink job and Spark load were run by hand, so end-to-end latency is unmeasured.
- Revisions that USGS makes after an event leaves the all-day feed are not caught until the reconciliation job exists (D-003).
