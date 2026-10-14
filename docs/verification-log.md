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

## 2026-10-14: monitoring and failure drills

**Monitoring stack.** Prometheus scraped all six targets (the collector on the host, Flink's two nodes, the Kafka lag exporter, the Pushgateway, and itself) and Grafana provisioned the dashboard and datasource. Of the 19 dashboard panels, 17 returned data on the first check. The other two ("Invalid records") showed no data because no record had been invalid yet and a labelled counter does not exist until its first increment. The collector now creates every labelled series at zero, and a test covers it.

**Alert rules.** All 11 rules loaded and were healthy. They were also tested with `promtool` against made-up metric histories, including cases that must not fire. As a control, two thresholds were changed in a copy of the rules, and the tests for those two alerts failed.

**Kafka outage, with real metrics.** A first drill that waited for USGS to publish a new revision during a 10 minute outage proved nothing: no new revision arrived, so nothing went pending, although the collector kept polling normally. A second drill used the real publisher, metrics and Prometheus with a stub feed that adds one new event per poll, on a throwaway topic.

| Moment | What Prometheus saw |
|---|---|
| Kafka up, 3 polls | pending 0, published 3 |
| Kafka stopped, 4 more polls | pending 4, published still 3, polls still succeeding, freshness about 76 s |
| Kafka restarted, one more poll | pending 0, published 8 |
| Topic contents | 8 events produced, 8 distinct in the topic, 0 duplicates, none missing |

Recovery took one poll after Kafka reported healthy (about 10 seconds).

**Spark load killed part way.** Three kills at 10, 17 and 23 seconds left the tables in three different states: all empty, then Bronze loaded only, then Bronze and versions but not current. One retry gave 400, 400 and 300 rows with no duplicates and no stale current rows. This shows the tables really can disagree with each other between commits, which is the case the idempotent merges exist for.

**Corrupt input.** Already covered in earlier entries: seven kinds of corrupt message reach the dead-letter topic through Flink, and malformed messages reach `rejected_records` through Spark. Not repeated here.

**Memory with everything running** (Kafka, Flink, PostgreSQL, RustFS, and the monitoring containers): Kafka 443 MB, Flink 470 and 629 MB, RustFS 488 MB, PostgreSQL 44 MB, Grafana 119 MB, Prometheus 67 MB, Pushgateway 23 MB, exporter 19 MB. Spark ran only while a job ran. Host free memory was 3.9 GB of 15.7 GB.

**What this does not show.**
- The alerts were tested against made-up data. One of them later fired on real data (see the entry on the failed Flink job below), but the 10 minute `for` periods were not waited out in a test.
- The real Flink checkpoint-failure and consumer-lag series were present, but a real lag or checkpoint failure was not provoked in this phase.
- Nothing was sent to anyone, because no Alertmanager runs.
- The dashboard queries were run through the APIs and returned data, but the rendering in a browser was not inspected.

## 2026-10-14: continuous integration, checked before pushing

**What was run.** The steps of `ci.yml` in a clean Linux container with Python 3.11, started from a copy of the repository without the local virtual environment: install, `ruff check`, `pytest`, and `scripts/validate_config.py`. The Docker Compose and `promtool` steps had been run directly. The workflow files were linted with `actionlint`. The image checks from `docker-build.yml` were run against the images already built.

| Check | Result |
|---|---|
| First clean install run | Failed. `prometheus_client` was imported by the code but not declared in `pyproject.toml`. It had been installed by hand in the local environment, so every local test had passed. |
| After declaring it | 315 unit tests passed, ruff clean, 6 configuration checks with no problems. |
| `actionlint` | Passed the real workflows. A deliberately broken workflow failed with two errors. |
| Image checks | The Flink image contains the Kafka connector. The Spark image contains the Iceberg, Kafka and PostgreSQL jars and imports `yaml` and `prometheus_client`. |
| New guard | A unit test reads every import in the code and fails if a package is not declared. With the declaration removed it failed and named the two files. |

**Not a real CI run.** None of this ran on GitHub. A container with the same Python version is a close imitation of a hosted runner, and the first real run may still reveal a difference. An early false alarm from the container, every file looking executable, came from mounting a Windows folder, and git records no tracked file as executable, so it does not apply to a real checkout.

**What CI does not cover.** The integration and recovery tests need Kafka, Flink, Spark and PostgreSQL running together and are run locally. The Docker image builds download about a gigabyte and were run once locally, not in CI.

## 2026-10-14: the Flink job failed and stayed failed, and the fix

**What happened.** After the first Kafka outage drill, the real Flink job was in state FAILED and had been since 13:36, about 50 minutes. The cause, from the job's own exception history: *"Recovery is suppressed by FixedDelayRestartBackoffTimeStrategy(maxNumberRestartAttempts=10, backoffTimeMS=10000)"*. The job had been told to retry 10 times, 10 seconds apart. A Kafka outage of about 10 minutes used those up within a few minutes, and the job never came back when Kafka did. It was found when the Flink web UI was looked at. The last lines of my own drill script, which printed the job state, had not been read.

**What the monitoring did.** The `FlinkNoRunningJob` alert fired from 13:41 to 14:27, five minutes after the failure, as its rule says, and stopped when the job was resubmitted. `FlinkCheckpointsFailing` and `CollectorDown` also fired during the drills. The Grafana panel "Kafka consumer lag" was showing 10 (the backlog of 10 messages). So the rules and the dashboard worked, and nobody saw them, because nothing sends alerts anywhere and the alerts page was not being watched.

**Data impact.** None lost. The failed job's checkpoint (chk-722) was kept, the job was resubmitted from it, and the backlog was processed. The topic counts before the fix were 453 raw and 443 validated.

**The fix.** The restart strategy was changed to retry without a limit, with the delay growing from 10 seconds to a cap of 1 minute (D-023). A first choice of a 5 minute cap was changed to 1 minute, because it could leave the job waiting up to 5 minutes after Kafka returned.

**Verification.** Kafka was stopped for 6 minutes with the real job running, which is longer than the old setting survived.

| Moment | Result |
|---|---|
| About 90 s after the stop | Job state RESTARTING |
| 6 minutes after the stop | Still RESTARTING, not FAILED |
| About 15 s after Kafka was healthy | Back to RUNNING, the same job, with no intervention |
| 28 s after Kafka was healthy | Caught up: 456 messages in the raw topic and 456 in the validated topic |

**What this does not show.** It is one drill with one outage length and was not repeated. It is a manual drill and not an automated test, because stopping the shared Kafka disrupts everything else running. An automated unit test only checks that the strategy is configured with no attempt limit. A job that fails for a reason that never fixes itself will now retry forever instead of stopping, and shows only as RESTARTING.

**Also changed.** The Grafana count panels "Events ingested" and "New revisions" were showing in red and with a decimal because of Grafana's default thresholds. They are now neutral blue and whole numbers. Series from the drills (a feed called "drill") remain in the charts until Prometheus's retention removes them.
