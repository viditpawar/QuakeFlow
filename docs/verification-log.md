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

## 2026-10-14: historical backfill into the real lakehouse

**What was run.** `python -m batch.backfill --start 2025-01-01 --end 2025-01-08 --min-magnitude 2.5` twice, then `batch/ingest_history.py` twice. Then a second backfill of 2026-10-13 to 2026-10-15 (all magnitudes), which overlaps events the live pipeline had already loaded, loaded twice.

| Step | Result |
|---|---|
| First week, run 1 | 591 events, 591 valid, 0 rejected. Manifest holds the real count and SHA-256. |
| First week, run 2 | Window skipped, nothing downloaded. |
| Load of the first week, twice | Versions 418 to 1009 (+591), current 399 to 990 (+591). The second load changed nothing. |
| Overlapping recent days | 466 events returned. Loaded twice: versions 1009 to 1105 (+96), current 990 to 1058 (+68). The second load changed nothing. |

**Checks across the whole lakehouse after both loads** (all must be zero, and were): current rows that are not the newest version of their event, events with versions but no current row, duplicate event ids in current, and duplicate `(event_id, record_hash)` pairs in versions.

**How the overlap resolved.** 28 events were seen by both the live and the historical path. In all 28 the historical revision was newer than the live one (USGS had revised them after the last live poll), and the current row correctly moved to it. There were no cases of an older historical copy meeting a newer live one, so that rule was not exercised by real data. It is covered by an integration test that seeds a newer live revision, and by a negative control: with the merge changed to always overwrite, the test fails because the old snapshot replaces the live revision.

**What this does not show.** One week and three days of data. The catalog returns the current state of each event, so this does not test recovery of earlier revisions. No timings were measured, and no window over the 20,000-event limit was downloaded for real (splitting is covered by unit tests with a fake catalog).

## 2026-10-14: Gold build and quality gate

**What was run.** `batch/build_gold.py` on the real lakehouse (1,058 current rows across 1,105 versions), then the two integration tests that build Gold from known data.

| Check | Result |
|---|---|
| Real build | 24 checks passed (5 column-order checks, 8 on `earthquakes_current`, 5 on `earthquake_versions`, 6 reconciliations). All 5 Gold tables were replaced. The reconciliations held: 1,044 earthquakes plus 14 other event types equals the 1,058 current rows. |
| Known data | Daily, band, regional, revision and latency numbers matched values worked out by hand from six events with two revisions. A quarry blast was excluded from earthquake counts and counted separately. An event with no magnitude fell in the `unknown` band. |
| Bad data, 1 | A row with latitude 95 blocked the build (exit 1, `latitude_range` failed). Every Gold table was identical to before. |
| Bad data, 2 | A duplicate event id plus a row with no event time blocked the build (`event_id_unique` and `event_timestamp_required` failed). Gold again unchanged. |
| After fixing | With the bad rows removed and one new event added, the build promoted and Gold changed. The build log read promoted, blocked, blocked, promoted. |

**What the real Gold tables show, with their limits.** Latency averaged about 16 hours, but that reflects when the load job was run by hand, not how fast the pipeline is. The magnitude bands are skewed toward 4 to 5 (38 percent) because the lakehouse holds a magnitude 2.5 and above week from January 2025 beside all-magnitude recent days. Regional counts split "CA" and "California". These are properties of the data loaded so far, not of the models.

**A bug found on the way.** The first build failed inside the Spark image, because `datetime.UTC` needs Python 3.11 and the image has 3.10. Unit tests missed it because they run on the host. The image now installs PyYAML (pinned with a hash), ruff is told to leave that code alone, and a unit test fails if a 3.11-only feature appears in code that runs in the image. Reintroducing the original bug makes that test fail.

**What this does not show.** The gate was exercised with three kinds of bad data, not every rule. I did not break the gate itself (for example by disabling it) to see the test fail, though the test asserts both the exit code and that Gold is unchanged. A failure part way through publishing was not simulated. The build time was not measured.
