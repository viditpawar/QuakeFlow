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

## 2026-10-14: leaked test topics

**What was found.** A check for leftovers after the integration tests found 48 test topics (16 sets named `quakeflow.test.<id>.raw`, `.validated` and `.dlq`) still in Kafka. The Iceberg catalog and the Spark checkpoint volume were clean. No real topic was affected.

**Cause.** The test helper deleted topics with `AdminClient(...).delete_topics(...)` written as one expression. The client object is destroyed as soon as the call returns, which cancels the request before Kafka acts on it. A first explanation (that the helper did not wait for the answer) was only part of it, and a first fix that added the wait did not stop the leak. Reproducing the helper on its own, with no Flink involved, showed that it still left its topics, and keeping the client in a variable fixed it.

**Fix and verification.** The helper keeps the client alive until the deletion finishes. A test now creates a set of topics, deletes them with the helper and checks that they are gone. With the old one-line version restored it fails with "the helper left its topics behind", and with the fix it passes. The 48 leaked topics were deleted by matching only the `quakeflow.test.` prefix, and the three real topics were left alone. A real Flink integration test was then run, and only the three real topics remained afterwards.

**What this does not show.** Only the topics were checked for leaks across all tests. Throwaway namespaces and Spark checkpoints were checked at the end and were clean, but a test that is killed part way can still leave things behind, since cleanup runs when a test finishes.

## 2026-10-15: review follow-up, and a Kafka durability bug it led to

An outside review of the repository asked for four things before further work: precise wording about delivery guarantees, Kafka bound to localhost, a test of what happens after Flink's deduplication state expires, and an honest account of which component keeps data unique. Doing them turned up two further problems.

**Wording.** The README said the platform keeps "every revision" and that output is "exactly-once". It now says every *distinct revision the platform observes* (a revision made and replaced between two polls, or before the platform first saw the event, cannot be recovered, and the USGS catalog does not expose earlier ones). It also scopes "exactly-once" to Flink's transactional Kafka writes, and describes the pipeline as checkpointed, transactional stream processing with idempotent Iceberg ingestion. [docs/architecture.md](architecture.md) has a new section on what each hop guarantees.

**Flink state expiry.** Flink's deduplication state is kept for 7 days. To test what happens after that, the retention was shortened to 20 seconds and the same revision was sent, left for 55 seconds, and sent again.

| Stage | Result |
|---|---|
| Validated topic before the replay | 1 message (a repeat inside the retention was dropped) |
| Validated topic after the replay | 2 messages, so Flink had forgotten the revision and let the replay through |
| Lakehouse after loading both | Bronze 2 rows (every message as received), Silver versions 1, current 1 |

The component that keeps data unique for good is the Iceberg merge on `(event_id, record_hash)`. Flink's filter and the collector's SQLite file keep repeats out of Kafka, and neither is needed for correctness.

**Binding Kafka to localhost.** Kafka and the Flink UI were the only published ports open to the whole network, and Kafka has no authentication. Both are now bound to `127.0.0.1`, and the validator fails if any published port is not. Doing so exposed a weakness: on this machine `localhost` resolves to IPv4 and IPv6, and every connection attempt to the IPv6 address waits 2 seconds before it is refused. Because the broker advertised `localhost:9092`, clients were sent back to the slow path, and a test's availability check failed at random (it made a test skip). Measured: a connection to `127.0.0.1` takes 0.08 s, five in a row. The broker now advertises `127.0.0.1:9092`, every host-side default uses it, and a unit test fails if one goes back to `localhost`.

**Kafka was not storing its data on its volume.** The stack had been shut down with `docker compose down`, and after starting it again the three topics were gone, though the lakehouse, kept in other volumes, was intact (440 live Bronze rows, 1,057 historical, 1,125 versions, 1,074 current events). The cause: the broker's data directory is `/tmp/kafka-logs` inside the container, while the volume `kafka-data` was mounted at a path the broker never used and was empty (4 KB). Data survived a stop and a start, which is all the drills did, and was erased when the container was removed. Earlier statements that `down` keeps the data, and that Kafka's data lives in the `kafka-data` volume, were false for Kafka.

- **Fix:** `KAFKA_LOG_DIRS=/var/lib/kafka/data`. Verified by putting 3 messages in a topic, removing the container with `docker rm -f`, recreating it, and reading the 3 messages back.
- **Hazard found while checking:** Spark's checkpoint recorded read positions of 149, 155 and 145 in the old validated topic, and the new topic was empty. New messages would have started at offset 0, and Spark would have skipped roughly the first 150 per partition without any error, because it is told to tolerate offset gaps. The Spark checkpoint was deleted. This is safe, because the merges are idempotent. The procedure is in the [recovery runbook](recovery-runbook.md).
- **What was lost:** the contents of the old topics. 23 revisions (not the 17 first reported) had been sent to Kafka but not yet loaded into the lakehouse. They were recovered in full, not only as current state as first stated, from the collector's state file, which keeps the content of every revision it sends (see the entry on the collector container below).
- **A guard** now fails the configuration check if Kafka's data directory is not a mounted volume, and was shown to fail with the setting removed.

**Spark service.** `docker compose up` with the batch profile started an idle Spark container that exited at once. It is now excluded from `up` and still works with `run`.

**What this does not show.**
- The earlier failure drills (Kafka outage, Flink worker killed) only stopped and restarted containers. They are valid for that, and say nothing about removing containers, which the persistence drill now covers for Kafka. Flink checkpoints, PostgreSQL and RustFS were confirmed to have survived the removal (the tables, 614 bucket objects and 16 job checkpoint folders were all present).
- Nothing yet checks that a saved read position is not ahead of a topic. After a topic is recreated, resetting the checkpoint is a manual step.
- The state-expiry test shortens the retention to 20 seconds instead of waiting 7 days, and was run once.

## 2026-10-15: Kafka persistence drills, the collector container, and a Bronze bug

**Kafka persistence, with both ways of removing the container.** After the data loss described above, persistence was tested by destroying the Kafka container and comparing the data before and after. It is now an automated test (`tests/recovery/test_kafka_persistence.py`) that runs both paths, because they are different lifecycles:

| Path | What it does |
|---|---|
| `docker rm -f quakeflow-kafka` | Removes the container at once, like losing it in a crash. |
| `docker compose --profile core down kafka` | The documented way to take the service down. It removes the container and keeps the volumes. |

Each run creates a topic with 3 partitions, writes 12 messages, records the end offset of every partition, destroys the container, checks that it is gone and that the volume `quakeflow_kafka-data` still exists, starts Kafka again, and reads everything back.

| Path | End offsets before | End offsets after | Messages before | Messages after |
|---|---|---|---|---|
| `docker rm -f` | {0: 5, 1: 2, 2: 5} | {0: 5, 1: 2, 2: 5} | 12 | 12 |
| `docker compose down kafka` | {0: 5, 1: 2, 2: 5} | {0: 5, 1: 2, 2: 5} | 12 | 12 |

**Control against the original fault.** With `KAFKA_LOG_DIRS` removed, which is the original misconfiguration, the same test failed with `assert {} == {0: 5, 1: 2, 2: 5}`: after the container was recreated the topic was simply gone. The setting was restored, and the real topics became visible again, because the data had been in the volume all along. Not tested: `docker compose down` for the whole stack at once (the Kafka-only form was used so the test does not take the rest of the stack down), and `down -v`, which deletes the volumes by design.

**Collector as a container.** The collector now runs as `quakeflow-collector`. Its state was migrated as follows:

1. The host process was confirmed stopped, and the state file was copied with SQLite's backup command, leaving the original byte-for-byte unchanged (checked by hash). The copy held 463 revisions, all recorded as sent, and passed an integrity check.
2. The Silver versions table was compared with it: **23 revisions** had been sent to Kafka but were not in the lakehouse, published between 13:29 and 14:50 local time on 14 October. They include intermediate revisions that a backfill could not return (for example one event revised three times).
3. Those 23 were marked unsent in the copy, the copy was placed in the new volume with the container user's ownership, and it was read back from inside the volume with the collector's own code: 23 pending.
4. A fresh Flink job was started without a recovery path, then the collector. It sent 24 revisions (the 23 plus 1 new one) and then 184 new events from the day feed, which covered almost exactly the day the collector had been off. Flink passed all 208 on, and the Spark load processed 208 messages, none rejected.
5. Check: the collector's state file lists 648 revisions as sent and none pending, and **0 of them are missing from the lakehouse**.

The container reported healthy, using 34 MiB of its 384 MiB limit, as user 10001, with a read-only root filesystem and no added capabilities. Prometheus scrapes it at `collector:9108` and all six scrape targets are up. This corrects two earlier statements: the number of lost revisions (23, not 17), and that only a backfill could recover them.

**A bug found by checking the counts: Bronze skipped 208 messages.** After the load, Silver had grown by 208 but Bronze was still at 440 rows. Bronze identified messages by topic, partition and offset, and the recreated topic reused offsets that Bronze already held (up to 153 in the old topic, 0 to 79 in the new one), so all 208 were skipped as already loaded, without any error. A test now recreates a topic and writes different messages at the same offsets: against the old merge it failed with Bronze at 3 rows where 6 were expected, and against the fix (the message text is part of the identity, D-026) it passes with 6. The real Bronze table was repaired by resetting the Spark checkpoint and loading again: Bronze 440 to 651 (the 208 skipped plus 3 new), Silver versions 1,333 to 1,336 and current 1,268 to 1,270, which are the same 3 new revisions. Nothing else changed.

**The resend command.** The repair in step 2 is now `python -m ingestion.resend`. It was run through Compose for real, as in the runbook. With the lakehouse's list it reported 1 sent revision missing, which was a revision sent a moment earlier and not yet loaded (so the runbook now says to run a load first). With 5 revisions that the collector had really sent removed from the list, it reported exactly those 5 plus the in-flight one. An empty list was refused with exit code 2, and nothing was applied in any of these runs. An earlier version of this control removed the wrong revisions (ones that only came from a backfill) and proved nothing, so it was redone.

**What this does not show.**
- That only one collector runs is not enforced beyond the fixed container name and the documentation.
- The restart policy and the unhealthy path of the health check were not exercised on the real container. The health logic is unit-tested, including a loop that has stopped.
- The new CI checks for the collector image were run locally and have not run on GitHub.
- The state file is on one volume with no backup.

## 2026-10-16: reconciliation through the outbox

**What was run.** The `reconciliation` DAG against the real USGS catalog, the real collector and Kafka, on the scheduled run and then a manual one.

| Check | Result |
|---|---|
| First run | 1,101 events updated in the window. The collector accepted the batch with 684 new revisions (628 events the live feeds never saw, 56 revisions of events it had) and Kafka confirmed all 684. Every task succeeded in under a minute. |
| Watermark | Equal to the time the batch was queried, set only after the collector's answer was read back. |
| Second run (the overlap) | The same 1,101 events came back, the collector accepted the batch with 0 new revisions, and nothing was sent twice. |
| Collector state | `quakeflow_pending_revisions` 0, no ready or rejected files left, one status file per batch. |
| Unit tests | Damaged files, a crash between committing and answering (repaired without duplicates), one batch ID with different content, accepted versus published, a retried fetch, and a watermark that never moves without an acceptance or backwards. |

**What this does not show.**
- The failure paths of the DAG were not run live: a stopped collector, a rejected batch and a slow Kafka are covered by unit tests only.
- The reconciled revisions travel the live path, so in `gold.ingestion_latency` they appear as live revisions with a long delay (up to two days for the first run) and raise its maximum and 95th percentile.
- A window with more than 20,000 updated events fails the run instead of being split.
- Nothing alerts yet when the outbox stops being read or the watermark stops moving.

## 2026-10-16: historical backfill DAG

**What was run.** `historical_backfill` with 2025-02-01 to 2025-02-08 and a minimum magnitude of 4.5, against the real USGS catalog and lakehouse.

| Check | Result |
|---|---|
| Tasks | Validate, preflight, download, ingest, verify and Gold all succeeded. |
| Download and load | 1 window, 169 features, 169 distinct revisions, 0 rejected. |
| Verification | 169 distinct revisions downloaded, 0 missing from Silver. |
| Gold | Promoted, 24 checks passed, from the Silver snapshots the load recorded. |
| Unit tests | Parameter validation (every problem reported), window selection by range and magnitude, count consistency, the pool and the DAG shape. |

**What this does not show.**
- The same range was not run a second time, so the skip of a complete window was not seen in Airflow (it is unit-tested in `batch/backfill.py`).
- A failed download, a rejected parameter set and a failed verification were not run live.
- The negative control for the verification (deleting a revision from Silver and seeing the DAG fail) was not run.
