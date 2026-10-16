# Architecture

How the pieces fit, what each one guarantees, and where state lives. The reasoning behind each choice is in [decisions.md](../decisions.md), referenced below as D-nnn.

## The idea

An earthquake record is not final when it is published. USGS revises magnitude, location, depth, time and review status, sometimes hours later. The platform treats an event as a sequence of revisions:

- A **revision** is identified by the event id plus a SHA-256 hash of its source content (`record_hash`). The hash covers every field USGS controls and excludes anything the platform adds, such as the time it was received (D-009). The same content always gives the same hash, wherever and however often it is seen.
- **Current state** is one row per event: the revision with the newest `source_updated_at`, and on an exact tie the one with the higher `record_hash`. An older revision never replaces a newer one (D-005).
- Every distinct revision the platform observes is kept, so the history can be analysed. Late and conflicting revisions are kept too and are flagged, because dropping them would lose history. The platform can only keep what it sees: a revision made and replaced between two polls, or before the platform first saw the event, is not recoverable, and the USGS catalog does not expose earlier revisions.

Every stage uses this same rule. That is what makes it safe for data to arrive twice, late, or from two different paths.

## Data flow

| Step | What happens | Guarantee |
|---|---|---|
| Collector | Polls the hour feed every 60 s and the day feed every 15 min. Validates each record with Pydantic (strict types, UTC timestamps, coordinate ranges). Computes the hash. Records each unseen revision in SQLite as pending. | A revision is marked sent only when Kafka confirms it (D-008, D-012). An outage leaves it pending to be retried, never lost. Delivery is at-least-once. |
| Kafka `events.raw` | 3 partitions keyed by event id, so all revisions of one event stay in order. 7 days of retention (D-001). | Durable, replayable input for Flink. |
| Flink job | Reads the raw text, parses and validates it in SQL, drops repeats of the same `(event_id, record_hash)`, and writes with transactions. 10 second checkpoints (D-014). | Transactional (exactly-once) writes to Kafka. Invalid messages go to the dead-letter topic with the reason, and a late older revision is still kept. Its repeat filter forgets after 7 days (see below). |
| Kafka `events.validated`, `events.dlq` | Clean revisions, and rejected messages with their reason and original coordinates. | Readers must use `read_committed`. |
| Spark load | Reads new messages with Structured Streaming (`availableNow`), then merges into Bronze, Silver versions, Silver current and rejected records (D-018). | Every write is a merge on the table's identity, so a replay or a lost checkpoint changes nothing. |
| Backfill | Downloads historical windows, splitting any over the catalog's 20,000-event limit. Normalizes with the same code as the live path. Spark merges the completed windows (D-019). | A window counts as done only when its manifest exists and its download still matches its checksum. Overlapping or repeated loads change nothing. |
| Gold build | Reads the Silver snapshots a finished load recorded, stages five analytics tables as views, audits them, appends them as a new build, and switches the published build in one commit only if every check passes (D-020, D-028). | A failed check publishes nothing. A reader sees one build in full, and an earlier build can be published again. |

## What is guaranteed, end to end

No single component makes the whole pipeline exactly-once. Each hop has its own guarantee, and uniqueness in the lakehouse comes from the last one.

| Hop | Guarantee | What can go wrong |
|---|---|---|
| Collector to Kafka | At-least-once. A revision stays pending until Kafka confirms it. | A crash between Kafka's confirmation and the SQLite write sends it a second time. |
| Flink to Kafka | Transactional writes, so a failed or restarted job does not publish half a batch. | Its repeat filter keeps state for 7 days and then forgets. A revision replayed after that passes again. |
| Kafka to Spark | At-least-once, reading only committed data. | A replay, or a lost Spark checkpoint, reads messages again. |
| Spark to Iceberg | Idempotent. Every write is a merge on the table's identity. | Nothing is duplicated, but the tables are separate commits and can disagree until a load finishes. |

**Which component guarantees persistent uniqueness?** The Iceberg merges do. A revision is identified by `(event_id, record_hash)`, and merging the same revision any number of times, at any distance in time, stores it once. Flink's repeat filter is a first line of defence that keeps repeats out of the validated topic for a week, and the collector's SQLite file stops it sending them. Neither is needed for correctness. This is tested: with the filter's retention shortened to 20 seconds, a replayed revision passes through Flink a second time and the lakehouse still holds it once (`tests/integration/test_state_expiry.py`).

## Where state lives

| State | Where | If it is lost |
|---|---|---|
| Which revisions were seen and sent | SQLite file on the volume `collector-state`, mounted in the collector container. It holds the full content of every revision | The collector would resend revisions. Flink and the merges remove the repeats. Because it keeps the content, it can also resend revisions that Kafka lost, by marking them unsent (D-025). |
| Raw, validated and rejected messages | Kafka topics, stored on the volume `kafka-data` (the broker's data directory is set to it, D-024) | Anything not yet loaded into the lakehouse is gone. The saved read positions of Flink and Spark then point beyond the end of the new, empty topics, and must be reset (see below). |
| Flink offsets and deduplication state | Checkpoints (volume `flink-checkpoints`) | The job restarts from the start of the topic and emits each revision again. |
| Spark offsets | Checkpoint (volume `spark-checkpoints`) | The load rereads the topic. The merges make that harmless. |
| Table metadata | PostgreSQL (volume `postgres-data`) | The tables cannot be found even though their files exist. |
| Table data and metadata files | RustFS (volume `rustfs-data`) | The lakehouse is lost. Rebuild it from Kafka and the backfill. |
| Downloaded historical windows | `data/history/` | Download them again. |

The Iceberg tables are the system of record for analysis. There is no backup of any of these volumes.

**If Kafka's data is lost or the topics are recreated,** the lakehouse keeps everything it had already loaded, but the read positions saved by Spark and Flink refer to offsets that no longer exist. A new message at offset 3 would be skipped without any error by a reader that believes it has already read up to offset 150. So the Spark checkpoint must be deleted and the Flink job started without a recovery path. Both are safe, because the merges are idempotent. The steps are in the [recovery runbook](recovery-runbook.md).

## The lakehouse

| Layer | Tables | Contents |
|---|---|---|
| Bronze | `live_observations`, `historical_observations` | Data as received. Live rows hold the Kafka message text and its coordinates. Historical rows hold the original GeoJSON feature. |
| Silver | `earthquake_versions`, `earthquakes_current`, `rejected_records` | Every revision, one current row per event, and invalid records with the reason. |
| Gold | `daily_seismic_activity`, `regional_activity`, `magnitude_distribution`, `revision_statistics`, `ingestion_latency`, `build_log` | Analytics for earthquakes only (D-004), days in UTC, and a record of every build attempt. |

Metadata is in a PostgreSQL JDBC catalog and files are in S3-compatible RustFS (D-016). Column-level definitions, and what each analytic does and does not mean, are in the [data dictionary](data-dictionary.md).

## Why Flink does not write to Iceberg directly

The design guide offered two routes. Flink writing straight to Iceberg is more elegant but needs closely matched connector versions and a catalog both engines can use. The chosen route, Kafka to Flink to a curated topic to Spark to Iceberg, lets the streaming logic and the table writes be built and proven separately (D-006). The cost is an extra Kafka hop and a Spark job that has to be run.

## Failure behaviour

Each row was tested by injecting the failure and checking the data afterwards. The recovery steps are in the [runbook](recovery-runbook.md), and the measurements are in the [verification log](verification-log.md).

| Failure | Result |
|---|---|
| Kafka down while the collector runs | Revisions stay pending and are sent after recovery. 8 of 8 events arrived once. |
| Kafka down for several minutes | The Flink job keeps retrying and resumes by itself. After a 6 minute outage it was caught up 28 seconds after Kafka was healthy. |
| Flink TaskManager killed mid-stream | The job restores from a checkpoint. No loss, no duplicates. |
| Flink job cancelled and resubmitted | Restoring from the retained checkpoint keeps the deduplication state. Without it, every revision is emitted again. |
| Spark load killed part way | Tables can disagree until the retry, then are exactly right. |
| Same revision sent many times | One accepted current row. |
| Older revision after a newer one | Current state keeps the newer one. |
| Backfill run twice or overlapping | Counts do not change. |
| Corrupt record | Quarantined with the reason. |
| Bad data in Silver | The quality gate blocks the Gold build. |

## Resources

Measured on a 16 GB Windows laptop with Docker Desktop, with the whole stack running and idle: Kafka 443 MB, Flink 470 and 629 MB, RustFS 488 MB, PostgreSQL 44 MB, and the four monitoring containers about 230 MB together. That is about 2.3 GB. A Spark job adds up to its 2 GB limit while it runs. Compose profiles let you run a subset.

## What is not built

- **Restarting Flink containers.** Airflow starts the Spark load and the Gold build (D-027) and submits the Flink job again if it was cancelled or lost (D-031). The containers themselves are restarted by Docker's restart policy, and a JobManager that is down for good fails the `flink_health` run and waits for a person.
- **Complete history of revisions.** The reconciliation DAG (D-029) finds revisions made after an event left the one-day feed, but the catalog returns only an event's current revision, so a revision that is replaced between two runs is never observed.
- **High availability and backups.** One broker, one worker, one JobManager, and no backups.
- **Alert delivery.** Prometheus evaluates the alert rules, but nothing is sent anywhere (D-021).
- **Direct Flink-to-Iceberg writes.** The alternative route in the design guide.
