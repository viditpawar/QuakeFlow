# Decisions Log

A record of the important decisions made on this project: what the situation was, what was chosen, and why.

**Status values:** `Accepted` (decided and in effect), `Planned` (decided, not built yet), `Retrospective` (made earlier and written down afterwards, so the date is when it was logged).

## Template

### D-XXX: Short title
- **Date:** YYYY-MM-DD
- **Status:** Accepted | Planned | Retrospective
- **Situation:** What problem or choice came up?
- **Options considered:** What were the alternatives?
- **Decision:** What was chosen?
- **Reasoning:** What made this the right call?
- **Consequences / trade-offs:** What does this commit us to or rule out?

---

## Decisions

### D-001: Single-node Kafka in KRaft mode, keyed by event_id
- **Date:** 2026-10-13
- **Status:** Retrospective
- **Situation:** The collector needs a durable, replayable buffer between USGS and the processing jobs.
- **Options considered:** Kafka with ZooKeeper; Kafka in KRaft mode; no broker.
- **Decision:** One `apache/kafka:3.9.0` broker in KRaft mode, pinned version, no auto-created topics. Topics: `quakeflow.events.raw` (3 partitions), `quakeflow.events.validated` (3), `quakeflow.events.dlq` (1), replication factor 1. Messages are keyed by `event_id`.
- **Reasoning:** Keying by `event_id` puts every version of one earthquake in one partition, so versions stay in order. KRaft removes ZooKeeper, and the pinned version keeps the setup repeatable.
- **Consequences / trade-offs:** No fault tolerance with one broker, which is acceptable locally. The partition count is a local choice, not benchmark-derived.

### D-002: Docker Compose profiles instead of running everything
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** Development runs on a 16 GB laptop that already uses about 11 GB when idle. Kafka, Flink, Spark, PostgreSQL, object storage and Grafana together will not fit.
- **Options considered:** Everything always on; separate profiles; separate compose files.
- **Decision:** One `docker-compose.yml` with profiles `core`, `streaming`, `batch` and `monitoring`, starting with modest memory limits and measuring real usage before changing them.
- **Reasoning:** Only the services the current phase needs are running.
- **Consequences / trade-offs:** Some end-to-end tests need more than one profile and may have to run in stages.

### D-003: Poll two USGS feeds with independent intervals
- **Date:** 2026-10-13
- **Status:** Planned
- **Situation:** The one-hour feed is quick but misses events and revisions outside its window.
- **Options considered:** `all_hour` only; `all_hour` plus `all_day`; `updatedafter` queries against the catalog API.
- **Decision:** Poll `all_hour` every 60 seconds and `all_day` every 15 minutes, both configurable, with timeouts, exponential backoff and duplicate detection. Add a scheduled longer-lookback or `updatedafter` reconciliation later.
- **Reasoning:** The all-day feed catches missed records, but it cannot catch revisions made after an event leaves its window, so a separate reconciliation job is still needed.
- **Consequences / trade-offs:** The same event appears in both feeds, so deduplication is required to avoid double counting.

### D-004: Keep all event types in Bronze and Silver; filter in Gold
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** USGS reports earthquakes, quarry blasts, explosions and other events in one feed.
- **Options considered:** Drop non-earthquakes at ingestion; keep everything and filter later.
- **Decision:** Preserve all valid event types in Bronze and Silver. Earthquake analytics in Gold use `WHERE event_type = 'earthquake'`.
- **Reasoning:** Keeps the platform flexible and avoids misleading earthquake counts.
- **Consequences / trade-offs:** Every Gold model must state which event types it counts.

### D-005: Event versioning policy
- **Date:** 2026-10-13
- **Status:** Planned
- **Situation:** A source can revise an event after publication, and treating each revision as a new event double counts it.
- **Options considered:** Overwrite in place; store only the latest; store every distinct revision plus a current-state table.
- **Decision:** Identify events by `event_id`. Keep every distinct revision (by content hash) in `silver.earthquake_versions` and one row per event in `silver.earthquakes_current`. The current row changes only when the incoming version has a newer `source_updated_at`. On equal timestamps with different content, the higher `record_hash` wins and the conflict is logged.
- **Reasoning:** Preserves history and gives a correct current view. A late-arriving old revision, or an old historical snapshot, never overwrites a newer live one.
- **Consequences / trade-offs:** The hash tie-break is deterministic, not a claim about which revision is more authoritative, so same-timestamp conflicts are flagged for review.

### D-006: Build the lakehouse path in two stages
- **Date:** 2026-10-13
- **Status:** Planned
- **Situation:** Flink can write to Iceberg directly, but that needs careful connector version matching and checkpoint-aware commits.
- **Options considered:** Approach A: Kafka, Flink, curated Kafka topic, Spark micro-batch, Iceberg. Approach B: Flink writes to Iceberg directly.
- **Decision:** Build Approach A first, and revisit Approach B only if time permits after correctness is proven. No Flink, Spark or Iceberg work starts until ingestion and event-versioning tests pass.
- **Reasoning:** Avoids debugging the Flink-Iceberg connector and the versioning logic at the same time.
- **Consequences / trade-offs:** Adds an extra Kafka hop and a Spark job.

### D-007: Strict normalization with explicit rejection
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** Raw USGS features have nullable fields, epoch-millisecond timestamps and occasional malformed records.
- **Options considered:** Lenient parsing that coerces types; strict typing with explicit rejection.
- **Decision:** A Pydantic model with strict types. Timestamps become timezone-aware UTC, latitude must be -90..90 and longitude -180..180, magnitude and depth are optional, and negative depths are valid. NaN and infinity are rejected. Invalid features are never dropped: they are returned with every reason and the original feature, for the DLQ.
- **Reasoning:** Bad input stays visible and replayable instead of being silently altered.
- **Consequences / trade-offs:** Unusual but legitimate source values may be rejected, and will show up in the rejected records for review.

### D-008: Delivery is confirmed by Kafka, not assumed
- **Date:** 2026-10-13
- **Status:** Planned
- **Situation:** The collector remembers the last hash seen per event in SQLite so unchanged records are not re-published. Saving the hash before Kafka confirms delivery would lose data if the process crashed in between.
- **Options considered:** Record the hash when a change is detected; record it only after Kafka acknowledges the publication.
- **Decision:** A change counts as delivered only after Kafka confirms it. Detected but unconfirmed changes stay pending and are retried.
- **Reasoning:** Protects against silent data loss across crashes and broker outages.
- **Consequences / trade-offs:** Delivery is at-least-once, so downstream steps must tolerate duplicates, which the hash-based deduplication already does.

### D-009: Record hash covers source content only
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** Revision detection and the same-timestamp tie-break both need a fingerprint that is identical whenever USGS says the same thing, and different whenever it says something different.
- **Options considered:** Hash the raw feature JSON; hash a fixed set of normalized fields; hash including the local ingestion time.
- **Decision:** SHA-256 of a canonical JSON of the normalized source fields (`event_id`, `event_time`, `source_updated_at`, `magnitude`, coordinates, `depth_km`, `place`, `review_status`, `event_type`) plus a `hash_version`. Timestamps are integer epoch milliseconds, `-0.0` is treated as `0.0`, and keys are sorted. `ingested_at` is excluded.
- **Reasoning:** Hashing normalized fields, not the raw feature, means unrelated extra fields USGS adds to a feed (counts, URLs, network codes) cannot create false revisions. Excluding `ingested_at` means seeing the same record in both feeds gives the same hash.
- **Consequences / trade-offs:** A real change in a field we do not hash (for example `tsunami` or `felt`) is not detected as a revision. Changing the field list or encoding changes every hash, so `HASH_VERSION` must be bumped. A test pins a known hash and checks that every model field is either hashed or explicitly excluded.

### D-010: Change detector keeps every revision and classifies each new one
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** The collector must not re-publish unchanged records, must not lose a change if it crashes before Kafka confirms delivery, and must still pass late or conflicting revisions downstream so the versions table stays complete (D-005).
- **Options considered:** Store only the last hash per event; store every revision with a delivered flag.
- **Decision:** A SQLite table keyed by `(event_id, record_hash)` holding every revision seen, with its payload and a `published_at` that is set only after Kafka acknowledges it. A revision not seen before is recorded as pending and classified as `new`, `revised` (newer `source_updated_at`), `out_of_order` (older than one already known) or `conflict` (same `source_updated_at`, different content, logged). A revision already seen, pending or published, is ignored. `pending()` returns everything unconfirmed, including after a restart.
- **Reasoning:** Keeping all revisions lets the detector tell a late revision from a new one, and storing the payload lets unconfirmed changes be retried after a crash.
- **Consequences / trade-offs:** Delivery is at-least-once, because a crash between Kafka's acknowledgement and `mark_published` re-sends that revision. The table grows with every revision and needs a retention policy later. Out-of-order and conflicting revisions are still published, since the current-state merge, not the collector, decides which one wins.

### D-011: Poller isolates failures per feed and detects but never publishes
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** Two feeds with different intervals share one process. A USGS outage or slow response on one feed must not stall the other, and the collector must not hammer a feed that is failing.
- **Options considered:** One thread per feed; a single loop with a next-due time per feed; a general-purpose scheduler library.
- **Decision:** A single loop keeps a next-due time per feed. A failed fetch is returned as a result, not raised. Each consecutive failure doubles that feed's delay (60 s becomes 120 s, 240 s and so on), capped at 30 minutes, and a success resets it. Intervals under 60 seconds are refused in the config. The poller only records changes as pending in the change detector; publishing and acknowledgement belong to the Kafka step.
- **Reasoning:** One loop needs no locking around the SQLite connection, and the clock and sleep are injectable, so scheduling and backoff are tested without waiting. Keeping publishing out of the poller means Kafka can be added without changing the ingestion logic.
- **Consequences / trade-offs:** A slow fetch delays the other feed by up to its timeout, since feeds are polled one after another. Backoff has no jitter, which is fine for two feeds. Unexpected exceptions (bugs, database errors) still stop the process on purpose, instead of being swallowed.

### D-012: Mark a revision published only from Kafka's delivery callback
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** The collector must send each new revision to Kafka without losing one if the broker is down, slow, or the process crashes.
- **Options considered:** Mark published when `produce()` returns; mark published after a blocking `flush()`; mark published per message from the delivery callback.
- **Decision:** `produce()` only queues a message locally, so it proves nothing. The detector's `mark_published` is called only from the delivery callback, and only when it reports no error. The producer uses `acks=all`, idempotence on, and a 30 second message timeout so an outage becomes a reported failure instead of an indefinite wait. After every poll, including one whose fetch failed, the publisher sends everything still pending, so earlier failures are retried without waiting for new data.
- **Reasoning:** Only the broker's acknowledgement shows the message is stored. A failed or unacknowledged message stays pending, and an outage costs delay, not data.
- **Consequences / trade-offs:** Delivery is at-least-once: a crash after Kafka acknowledges but before the SQLite write re-sends that revision, which downstream removes by `record_hash`. A retry can arrive after a newer revision of the same event, so consumers order by `source_updated_at`, not arrival. A message Kafka permanently refuses (for example too large) stays pending and is logged every cycle. Rejected records are not yet published to the dead-letter topic: they are not tracked as seen, so sending them every poll would flood it with repeats, and that needs its own dedupe design.

### D-013: Flink 2.2.1 with the matching Kafka connector, SQL-first jobs
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** The Flink job must read Kafka, and the connector has to be built for the same Flink line as the cluster. Flink 2.3.0 is the newest release, but the Kafka connector's newest build, 5.0.0-2.2, targets Flink 2.2.
- **Options considered:** Flink 2.3.0 with an unmatched connector; Flink 1.20 LTS with connector 3.4.0-1.20; Flink 2.2.1 with connector 5.0.0-2.2.
- **Decision:** Flink 2.2.1 (Java 17) with `flink-sql-connector-kafka` 5.0.0-2.2, baked into `docker/flink.Dockerfile` with a SHA-256 checksum. Jobs are written in Flink SQL under `flink/sql/`. One JobManager (1 GB) and one TaskManager (1.5 GB, 2 slots), hashmap state backend, checkpoints on a named volume. Services are in the `streaming` profile and run together with `core`.
- **Reasoning:** Matching the connector to the Flink line avoids runtime class errors, and a checksum makes the image build repeatable and tamper-evident. SQL matches the repository layout and keeps the job easy to read. The hashmap backend is enough for the small state of this dataset.
- **Consequences / trade-offs:** Moving to Flink 2.3 has to wait for a matching connector release. Both containers together use about 1 GB idle and about 1 GB more during a job (measured: Flink 1.0 GB, Kafka 0.4 GB under a query), which fits the 16 GB machine only if little else is running. State is lost if the checkpoint volume is deleted, which is acceptable for local development. Offsets and deduplication state are checked in later steps.

### D-014: Flink validates and deduplicates; it never drops a valid revision
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** The stream job has to reject bad messages, drop repeats, and keep every real revision, including one that arrives after a newer one. Flink SQL treats a stream as an updating table, so an unconstrained `DISTINCT` or an event-time window would either grow forever or silently discard late data.
- **Options considered:** Parse with the JSON format and let bad messages fail or be skipped; dedupe by `event_id` only (keeps just one version); dedupe on `(event_id, record_hash)` and rely on event-time lateness; dedupe on `(event_id, record_hash)` ignoring lateness.
- **Decision:** Read the raw topic as plain text and parse in SQL, so a malformed message becomes a DLQ record with a reason, its source topic, partition and offset, and the original payload. Validation covers: valid JSON object, `event_id`, `event_time`, `source_updated_at`, latitude -90..90, longitude -180..180, and a 64-character `record_hash`. Valid messages are deduplicated by keeping the first sighting of each `(event_id, record_hash)`, with 7 days of state. Late or out-of-order revisions are kept, and the original message text is passed through unchanged with the source update time as the Kafka timestamp. Both sinks are exactly-once, with checkpoints every 10 seconds, fixed-delay restarts (later replaced by unlimited retries with backoff, see D-023) and checkpoints retained on cancel.
- **Reasoning:** Each revision is a real version, so dropping one for arriving late would lose history. Which revision is current is the merge's job (D-005). Passing the text through avoids re-serialization drift and keeps the Flink job independent of the schema's field list. The collector already deduplicates, but at-least-once delivery means repeats are possible, so Flink repeats the check.
- **Consequences / trade-offs:** Records become visible to `read_committed` consumers only when a checkpoint completes, so output lags by up to about 10 seconds, and every downstream consumer must read with `isolation.level=read_committed`. Dedup state is forgotten after 7 days, so a revision replayed after that would pass again, and the hash-based merge handles it. The event-time watermark on `raw_events` is defined for later windowed analytics, but the job itself uses no event-time operator. A validated message that fails rules the SQL does not check (for example a bad `place` type) still passes, because the Python collector has already enforced the full schema.

### D-015: Recover from retained checkpoints, with no JobManager high availability
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** The Flink job must survive a failure without losing or repeating revisions. Running a highly available JobManager needs more containers and memory than the 16 GB machine can spare.
- **Options considered:** JobManager high availability with a second JobManager and shared storage; a single JobManager with checkpoints retained on cancel and resubmission from the latest checkpoint.
- **Decision:** One JobManager and one TaskManager. Checkpoints are kept when a job is cancelled, and `docs/recovery-runbook.md` describes resubmitting from one. A TaskManager failure recovers automatically.
- **Reasoning:** The checkpoint and the exactly-once sinks already give correct results across both failure types, which the recovery tests show from the output data. High availability would only make recovery automatic for a JobManager crash.
- **Consequences / trade-offs:** If the JobManager dies, the job stays stopped until someone resubmits it. Checkpoints live on a single local volume, so deleting it loses the state and forces a restart from the beginning of the topic. The recovery times in the runbook come from one run on one machine.

### D-016: Iceberg on a PostgreSQL JDBC catalog and RustFS, with Spark run per job
- **Date:** 2026-10-14
- **Status:** Accepted
- **Situation:** The lakehouse needs table metadata that survives restarts, S3-compatible storage that runs locally, and a Spark and Iceberg pairing whose versions match, all within about 2 GB of spare RAM. The Iceberg REST test catalog keeps metadata in memory, so it loses everything on restart.
- **Options considered:** An in-memory REST catalog; a Hadoop (filesystem) catalog; a JDBC catalog on PostgreSQL. For storage: MinIO or RustFS. For Spark: a standalone cluster or one container per job.
- **Decision:** Iceberg 1.12.0 on Spark 4.0.4 (Java 17), using a JDBC catalog in PostgreSQL 17 and RustFS 1.0.0 for the files, all version-pinned with checksum-verified jars. Spark runs as a one-off container (`docker compose run --rm spark ...`) in local mode, not as a standing cluster. The catalog is named `local`, with `bronze`, `silver` and `gold` namespaces. Credentials come from `.env` and are rendered into the Spark configuration at start-up, never baked into the image. New service ports bind to localhost only.
- **Reasoning:** The JDBC catalog gives durable, transactional table metadata that the Spark merge needs. RustFS 1.0.0 is the first stable release and is Apache-2.0 licensed. A per-job Spark container uses no memory while idle, which matters on this machine. Iceberg's Spark 4.0 runtime has existed since 1.10, so it is less new than the 4.1 one.
- **Consequences / trade-offs:** Spark 4.1 and RustFS 1.0.1 are available and can be adopted later with a checksum update. The catalog is a single PostgreSQL instance with no backup, and a lost `postgres-data` volume orphans the table files. Spark's start-up costs several seconds per job. The Flink REST catalog route is not available, which matches the choice of Approach A (D-006).

### D-017: Bronze keeps messages as received, Silver separates history from current state
- **Date:** 2026-10-14
- **Status:** Accepted
- **Situation:** The lakehouse must keep what was received, every revision, and one correct current row per event. Two details constrain the design: the collector forwards its normalized event, not the original USGS feature, and Iceberg has no primary keys.
- **Options considered:** Bronze rows with parsed columns only; Bronze with the message text kept; one Silver table with a current flag; separate versions and current tables. For partitioning: fine partitions on `event_time`, or coarse ones.
- **Decision:** `bronze.live_observations` stores the Kafka message text exactly as received, with its topic, partition and offset, and `bronze.historical_observations` stores the original GeoJSON feature. Silver has `earthquake_versions` (every `(event_id, record_hash)`, insert only) and `earthquakes_current` (one row per event, with a `curated_at` column), plus `rejected_records`. Bronze and rejected records are partitioned by day, versions by month of `event_time`, and the current table is not partitioned, because it is small and `event_time` can be revised, which would move rows between partitions. Writes are made repeatable by MERGE on each table's identity, not by relying on exactly-once delivery. All tables use format version 2 and zstd compression. Gold tables wait for the analytics in Phase 6.
- **Reasoning:** Keeping the message text allows replay and parsing fixes without going back to Kafka. A separate current table keeps reads simple, and the versions table keeps full history. The data is a few hundred events a day, so coarse partitions avoid thousands of tiny files.
- **Consequences / trade-offs:** The live Bronze table does not hold the original USGS feature, so any field the collector drops is not recoverable from it (the historical table does keep it). Uniqueness is only as good as the merge logic, and a bad write could create duplicates until the quality checks (Phase 6) run. Changing the partitioning later needs a table rewrite, which is cheap at this size.

### D-018: Spark loads Kafka in availableNow runs, with idempotent MERGEs as the safety net
- **Date:** 2026-10-14
- **Status:** Accepted
- **Situation:** The lakehouse must be fed from Kafka without duplicates or lost revisions, and an old revision must never overwrite a newer one. The memory budget rules out a standing Spark cluster, and Flink's exactly-once output is transactional.
- **Options considered:** A continuously running streaming query; a scheduled batch read that tracks offsets itself; a Structured Streaming query with the `availableNow` trigger and a checkpoint. For correctness: rely on the checkpoint for exactly-once, or make every write idempotent.
- **Decision:** `batch/ingest_live.py` runs a Structured Streaming query with `availableNow`, so each run reads only new data and exits. Offsets are checkpointed on a named volume, but every write is a MERGE on the table's identity (Kafka coordinates for Bronze and rejected records, `event_id` plus `record_hash` for versions, `event_id` for current state), so replaying data or losing the checkpoint changes nothing. The current-state merge replaces a row only when the incoming revision has a newer `source_updated_at`, or the same one with a higher `record_hash`. The job reads with `kafka.isolation.level=read_committed` and `failOnDataLoss=false`, because Flink's transactions leave offset gaps. Bronze keeps every message, including invalid ones, which Silver also records in `rejected_records` with stage `spark`. Flink's dead-letter messages are loaded the same way with stage `flink`. The merge logic is plain SQL in `iceberg/sql/003_*.sql`.
- **Reasoning:** A run-to-completion job uses no memory between runs. Idempotent merges make correctness independent of checkpoint and exactly-once behaviour, which only has to hold for efficiency. Keeping the rules in SQL makes them reviewable without reading Python.
- **Consequences / trade-offs:** Data reaches the lakehouse only when the job is run or scheduled, so freshness equals the schedule. Each run pays several seconds of Spark start-up. The Bronze and versions merges scan their target tables, which is fine at this size and would need partition filters or a different approach for large volumes. `failOnDataLoss=false` also hides genuine loss if Kafka retention deletes unread data, so a run must happen within the 7-day topic retention. The ordering rule and the committed-only read are checked by integration tests, including one that aborts a transaction.

### D-019: Backfill in two stages, with the live normalization code and a manifest written last
- **Date:** 2026-10-14
- **Status:** Accepted
- **Situation:** Historical events must reach the same tables as live ones, using identical validation and hashing, without breaking the USGS catalog's 20,000-event limit, and a repeated or interrupted backfill must be safe. The catalog API returns each event's current state, not past revisions.
- **Options considered:** Re-implement the normalization in Spark SQL; run the Python normalization inside Spark; normalize in Python first and have Spark only merge. For windows: fixed sizes, or sizes chosen from the event count.
- **Decision:** `batch/backfill.py` downloads in windows (7 days by default) and asks the catalog's count endpoint first, splitting any window above 20,000 events in half until it fits. It paces requests and the shared session retries transient errors with backoff. Per window it writes the raw download, the bronze, silver and rejected JSONL files produced by the same `normalize_features` as the live path, and a manifest with the SHA-256 checksum, written last, so a window counts as done only when its manifest says completed and its download still matches. `batch/ingest_history.py` loads only completed windows with idempotent MERGEs. Because two overlapping backfills put the same feature in one load twice, every incoming view is deduplicated first. Historical Bronze rows are identified by `(event_id, payload_sha256)`, historical rejects by stage, event id and text. The current-state ordering rule is repeated in `003_merge_history.sql` and a unit test checks it is identical to the live file.
- **Reasoning:** Using one normalization code path means a historical copy of an event gets the same `record_hash` as its live copy, so it is not a second version, and the rules cannot drift apart. Choosing windows from the count adapts to quiet and busy periods. Writing the manifest last, and checking the checksum on resume, makes interruption and corruption recoverable without a separate state store.
- **Consequences / trade-offs:** Each window costs a count request as well as a download. The files live under `data/history/` on the host, outside git, and take disk space. The rule exists in two SQL files and relies on the test to keep them equal. The catalog only returns the latest version of an event, so a backfill cannot recover revisions that happened earlier, and the protection against old data matters when an old downloaded window is loaded after newer live data. Features that fail validation are kept in `rejected_records` but never retried automatically.

### D-020: Gold is rebuilt in full behind a write-audit-publish quality gate
- **Date:** 2026-10-14
- **Status:** Accepted
- **Situation:** Analytics tables must never be built from, or publish, bad data, and a bad run must not leave readers with half-new results. The data is small, and the analytics questions need revision history, which the current-state table does not hold.
- **Options considered:** Incremental Gold updates; a full rebuild written straight into the Gold tables; a full rebuild staged, audited, then published. For auditing: ad-hoc assertions in code, or a declarative contract.
- **Decision:** `batch/build_gold.py` stages every Gold model as a temporary view, audits, and only then replaces each Gold table with a single `INSERT OVERWRITE`. The audit runs the checks in `configs/quality.yaml` on `earthquakes_current` and `earthquake_versions` (not null, unique, ranges, minimum rows), reconciles each staged result against Silver (for example, daily counts must sum to the number of earthquakes), and checks that each staged view's columns match its table, because `SELECT *` fills columns by position. A check that errors counts as failed. If anything fails, nothing is published, the job exits 1, and the attempt is recorded in `gold.build_log` with the Iceberg snapshots of Silver that were read. Earthquake analytics use `event_type = 'earthquake'` and UTC days, and quarry blasts and other types are counted separately in the daily table. Five tables: daily activity, regional activity, magnitude distribution, revision statistics, and ingestion latency (an addition to the guide's four, to answer the question of how long a source update takes to arrive). Region is the text after the last comma of `place`.
- **Reasoning:** A full rebuild is simple and correct at this size, and an overwrite replaces a table in one atomic commit, so readers see the old or the new table, never a mix. Failing the build on any check means a bad row in Silver cannot reach the analytics. A declarative contract can be read and changed without touching code, and reconciling the output against the input catches a model that loses or doubles events.
- **Consequences / trade-offs:** Overwrites are atomic per table, not across the five tables, so a crash between two of them leaves a mix until the build is run again, which is safe because it is idempotent (the log records `publish_failed`). One bad row anywhere in the checked columns blocks all of Gold, which is deliberate but means someone has to fix or remove it. Rebuilding everything will need to become incremental if Silver grows large. The region grouping is a heuristic and splits "CA" from "California" because USGS writes both. Latency measures the whole path including how often the load job is run. Counts depend on what was loaded: the lakehouse currently holds an M2.5 and above week from January 2025 beside all-magnitude recent days, so magnitude bands and daily counts are not comparable across those periods. Code that runs in the Spark image must work on its Python 3.10, which a unit test now enforces.

### D-021: Monitoring with Prometheus and Grafana, scraped where possible and pushed where not
- **Date:** 2026-10-14
- **Status:** Accepted
- **Situation:** The pipeline needs to show whether it is working, from the collector through Kafka and Flink to the lakehouse. The collector is a host process, the Spark jobs are containers that exist for under a minute, and memory is limited.
- **Options considered:** Scrape everything and containerize the collector; scrape the collector and push batch results to a gateway; log lines only. For alerting: run Alertmanager, or evaluate rules only.
- **Decision:** Prometheus scrapes the collector on the host (through `host.docker.internal`), Flink's built-in reporter, and `kafka-exporter`. Spark jobs push their duration, success and last-success time to a Pushgateway when they finish, using an add-style push so a failed run does not erase the last success. Pushing is off unless `PUSHGATEWAY_URL` is set, and a push failure never fails a job. Collector metrics use only small fixed label sets, never event ids, and every labelled series is created at zero when the collector starts. Source freshness is computed when scraped, so it keeps growing between polls. Eleven alert rules are evaluated by Prometheus and tested with `promtool` against made-up histories, but no Alertmanager is run. The monitoring profile is separate, and all its ports bind to localhost.
- **Reasoning:** Scraping fits long-running processes and pushing fits jobs that exit, so each source uses the mechanism that works for it. Metrics that fail a job would make monitoring a new way for the pipeline to break. Creating series at zero avoids dashboards that say "no data" when they mean "none yet". Rule tests stop alerts from silently never firing.
- **Consequences / trade-offs:** The collector's metrics disappear when it stops, so only `CollectorDown` reports that. Spark metrics describe the last run only, and a job that never runs is detected only by the age of its last success. Nothing is sent to anyone, so someone has to look at the alerts page. Thresholds were chosen without a baseline. Pushgateway keeps the last pushed values forever, even after a job is removed. The collector is not containerized, so monitoring it depends on Docker Desktop's host address.

### D-022: Fast checks on every push, heavy tests kept local, and CI verified before it was pushed
- **Date:** 2026-10-14
- **Status:** Accepted
- **Situation:** The project's strongest tests need Kafka, Flink, Spark and PostgreSQL together, which takes many minutes and several gigabytes of memory. GitHub's hosted runners are free only up to a monthly allowance, and a workflow that has never run is a risk of its own.
- **Options considered:** Run everything in CI; run only unit tests; run fast checks in CI and keep the heavy tests local. For the image builds: every push, or only when image files change.
- **Decision:** `ci.yml` runs on every push and pull request: ruff, the unit tests, a configuration validator (`scripts/validate_config.py`, which loads the feed list and quality contract with the application's own code), Docker Compose validation for every profile, and the `promtool` alert-rule checks and tests. `docker-build.yml` builds the Flink and Spark images and checks their contents, and runs only when files that affect them change, or on demand. The integration and recovery tests are not run in CI and are run locally before pushing. Actions are pinned by commit SHA with a version comment, the workflows use read-only permissions and cancel superseded runs, and Dependabot proposes updates. Before pushing, the CI steps were run in a clean Linux Python 3.11 container, and the workflow files were checked with `actionlint`, including against a deliberately broken file.
- **Reasoning:** The fast checks catch most mistakes in seconds and cost little. The heavy tests are the ones that prove the pipeline, so keeping them local keeps them honest and keeps CI cheap and reliable. Pinning by SHA means a moved tag cannot change what runs. Running the steps in a clean container first found a real fault that every local run had hidden: `prometheus-client` was installed in the developer's environment but missing from `pyproject.toml`, so a clean install failed on import.
- **Consequences / trade-offs:** CI does not prove the stack works end to end, and nothing stops someone from pushing without running the integration tests. The clean-container check is a close imitation of GitHub's runners, not the real thing, so the first real run may still turn up a difference. Pinned SHAs and checksums need regular updates, which Dependabot only helps with. A unit test now fails if the code imports a package that `pyproject.toml` does not declare.

### D-023: The Flink job retries without limit, with a growing but capped delay
- **Date:** 2026-10-14
- **Status:** Accepted
- **Situation:** The job was first configured to restart at most 10 times, 10 seconds apart (D-014). During a Kafka outage drill the job used up those attempts within a few minutes, was marked FAILED, and stayed failed after Kafka came back. It processed nothing for about 50 minutes, and the collector kept publishing into a topic nobody was reading. This was found only when the Flink web UI was looked at, and my own drill script had printed the job state without my reading it.
- **Options considered:** Keep a fixed limit and rely on someone resubmitting the job; retry at a fixed interval with no limit; retry with an exponentially growing delay and no limit.
- **Decision:** `exponential-delay` restart strategy with no limit on attempts: 10 seconds after the first failure, doubling up to a cap of 1 minute, with 10 percent jitter, resetting after 10 minutes without a failure. The 1 minute cap, chosen over 5 minutes, bounds how long the job can wait after Kafka returns. A unit test checks that the strategy is exponential-delay and has no attempt limit.
- **Reasoning:** Failures of the kind that outages cause fix themselves, so a job that gives up converts a temporary outage into a permanent one that someone must notice. Growing the delay keeps a long outage from producing a flood of restarts, and the cap keeps recovery quick. Checkpoints and exactly-once sinks mean that retrying many times is safe.
- **Consequences / trade-offs:** A job that fails for a reason that will never fix itself, such as a bad SQL change, now retries forever, once a minute, instead of stopping. It shows as RESTARTING, and the `FlinkNoRunningJob` alert and the dashboard are what reveal it. The fix was verified by a drill: Kafka down for 6 minutes, the same job went RUNNING, RESTARTING, RUNNING by itself and was caught up 28 seconds after Kafka was healthy, with 456 messages in both topics. This supersedes the fixed-delay setting mentioned in D-014 and D-015.

### D-024: Kafka's data directory is the mounted volume, and recreating topics resets the read positions
- **Date:** 2026-10-15
- **Status:** Accepted
- **Situation:** The compose file mounted a volume `kafka-data` at `/var/lib/kafka/data`, but the broker's own data directory is `/tmp/kafka-logs` by default, so the volume was never used and stayed empty. Kafka's data lived in the container's writable layer. It survived a stop and a start, which is all that the failure drills did, but it was erased the first time the container was removed with `docker compose down`. The three topics and their contents were lost, while the lakehouse, kept in separate volumes that were used correctly, was untouched. The documentation had said that `down` keeps the data.
- **Options considered:** Move the data directory to the volume; keep Kafka's data disposable and rebuild from the lakehouse and backfills; mount the volume at Kafka's default path.
- **Decision:** Set `KAFKA_LOG_DIRS=/var/lib/kafka/data` so the broker writes to the volume. The configuration validator now fails if Kafka's data directory is not a mounted volume. Persistence was proven by putting messages in a topic, removing the container with `docker rm -f`, recreating it, and reading them back. A procedure for the case where topics are lost is in the recovery runbook: delete the Spark checkpoint and start Flink without a recovery path, because their saved offsets would point beyond the end of the new, empty topics and new messages would be skipped without an error.
- **Reasoning:** The raw topic is the replayable input of the whole pipeline, and a 7 day retention means nothing if a routine `down` erases it. Moving the directory keeps the existing volume and compose layout and fixes the cause, where mounting the volume at the default path would have meant tying the setup to an image detail.
- **Consequences / trade-offs:** The data of the old container is not migrated, so the topics started empty. 23 revisions had been sent to Kafka but not yet loaded into the lakehouse, and were lost from Kafka. They were recovered in full, because the collector keeps the content of every revision it sends: they were marked unsent and sent again (see D-025). A deleted or recreated topic still needs a manual reset of the Spark and Flink read positions, because nothing checks that a saved read position is not ahead of the topic. A guard for that is a candidate for the scheduled pipeline. Several drills and tests that reported "no data lost" only ever restarted containers, and were true only for that case. The first report of this decision said that 17 revisions were lost and that only a backfill could recover them. Both were wrong: the count was 23, and the collector's state file held every one of them.

### D-025: The collector runs as one container, and its state file can resend what Kafka lost
- **Date:** 2026-10-15
- **Status:** Accepted
- **Situation:** The collector ran as a process on the host, so `docker compose up` did not start the whole platform, nothing restarted it, and its state file sat on the Windows filesystem. Its SQLite state has a single writer, and a second collector would send every revision twice. The Kafka data loss (D-024) also showed that Kafka cannot be treated as the only copy of what the collector has sent.
- **Options considered:** Keep it on the host; run it as a container sharing the host's state file through a bind mount; run it as a container with the state on a named volume.
- **Decision:** One container, `quakeflow-collector`, in the `core` profile with a restart policy, built from a Python 3.11.17 base image pinned by digest. Its packages are installed from a hash-locked list, and it runs as an unprivileged user (uid 10001) with a read-only root filesystem, no added Linux capabilities and a 384 MB memory cap. It reaches Kafka as `kafka:19092`, never `127.0.0.1`, which inside a container would mean itself. Its state is a SQLite file on the named volume `collector-state`, not a bind mount, because file locking between Windows and Linux is not reliable. A health check passes only if a feed has been polled successfully in the last 20 minutes, so a stuck polling loop is caught. The metrics are published to `127.0.0.1:9108` and Prometheus scrapes `collector:9108`. The existing state file was copied with SQLite's backup command, and the 23 revisions it held that the lakehouse lacked were marked unsent, so they were sent again. That step became a command, `python -m ingestion.resend`, which defaults to a dry run and refuses an empty list or more than 1000 revisions.
- **Reasoning:** A container starts with the rest of the stack, restarts itself, and keeps its state in a volume that can be backed up. Because the state file keeps the full content of every revision, it is the one place that can restore what Kafka lost, with the intermediate revisions that a catalog backfill cannot return.
- **Consequences / trade-offs:** Exactly one collector may run, and nothing enforces that beyond the fixed container name and the documentation. Running one on the host as well would give two writers and two state files. The state file is not backed up. The collector image must be rebuilt for code changes. A host collector for development is still possible and must be stopped first. The reconciliation outbox, a shared volume the collector will read, is still to be added (planned with the Airflow step).

### D-026: Bronze identifies a message by its coordinates and its text
- **Date:** 2026-10-15
- **Status:** Accepted
- **Situation:** Bronze identified a Kafka message by topic, partition and offset (D-017). Those values are unique only while a topic exists. After the topics were recreated (D-024), the 208 new messages reused offsets Bronze already held, the merge treated them as already loaded, and Bronze silently skipped all of them. Silver, which identifies revisions by content, loaded them correctly, so the tables disagreed without any error. It was found because Bronze still showed 440 rows after a load that processed 208 messages.
- **Options considered:** Add a topic generation or epoch to the identity; add the Kafka timestamp; add the message text.
- **Decision:** The merge identity for live Bronze, and for the rejected records that come from Kafka, includes the message text, compared null-safely. A different message at a reused offset is then a different message, and a replay of the same message still matches. A test recreates a topic, produces different messages at the same offsets, and checks that all of them are in Bronze. It was run against the old merge first and failed, with 3 rows where 6 were expected.
- **Reasoning:** The text is already in the table, needs no new column and no new information from Kafka, and answers the real question: is this the same message. Spark does not expose a topic identifier, so an epoch would need a new mechanism.
- **Consequences / trade-offs:** Comparing text makes the merge slightly more expensive, which does not matter at this size. A message that is byte-identical to an older one at the same coordinates is still treated as the same message, which is correct. The 208 messages that had been skipped were added by reading the new topics again.

### D-027: Apache Airflow schedules the batch work, and its scheduler holds the Docker socket
- **Date:** 2026-10-15
- **Status:** Accepted
- **Situation:** Nothing ran the pipeline except a person: ingestion and Gold were commands to type. The platform needed scheduling, dependencies, retries and a record of each run.
- **Options considered:** A small custom orchestrator; cron on the host; Apache Airflow.
- **Decision:** Airflow 3.3.2 with the LocalExecutor, in the `orchestration` profile: API server, scheduler, DAG processor, and a one-shot migration. Its metadata is in the existing PostgreSQL, under a dedicated `airflow` role and database created by `scripts/airflow_setup.py`, with generated secrets in the git-ignored `secrets/` folder. The `incremental_pipeline` DAG runs every 30 minutes, one run at a time: a preflight (Kafka, catalog database, object storage, and the Flink job being RUNNING), then Spark ingestion, then the quality gate and Gold, which run only if ingestion succeeded. Spark tasks use `DockerOperator` with the platform's own Spark image, which now contains the jobs, the SQL, the quality contract and the catalog template, so a container started by Airflow needs no host paths. They join the compose network, mount the checkpoint volume, and receive credentials as private environment. The collector and the Flink job stay long-running services; Airflow checks them and never restarts them.
- **Reasoning:** Airflow gives dependencies, retries with backoff, timeouts, a run history and a UI without writing them. Baking the code into the image is what lets the same job run from Airflow and from `docker compose run`.
- **Consequences / trade-offs:** Starting containers needs the Docker socket, and access to it is equivalent to root on the Docker host. Restrictions: it is mounted into the scheduler only, the one component that executes the DockerOperator tasks; the UI is published on `127.0.0.1` only; the Airflow account has its own generated password; the Docker API is never exposed over TCP. A read-only mount of the socket is not a boundary, because the API stays fully usable. Whoever can edit the DAG files or log in as admin can start any container, so both are trusted. A restricted socket proxy is a possible later hardening. The Simple auth manager is meant for development and has one user; that is acceptable only because of the localhost binding. Airflow adds about 650 MB of memory at rest. Spark containers started by Airflow are not covered by `docker compose down`; a failed one is kept for `docker logs`.

### D-028: Gold is published as a versioned build, built from the Silver snapshots a load recorded
- **Date:** 2026-10-15
- **Status:** Accepted
- **Situation:** Gold replaced each of its five tables with its own commit (D-020). A crash or a reader between two commits saw tables from different builds. The build also read whatever Silver held when it started, so a load running at the same moment could change its input, and an Airflow run gave no way to say which ingestion a Gold build followed.
- **Options considered:** Keep replacing tables and accept the window; write into new tables each time and rename them; append every build under a `build_id` and switch which build is published.
- **Decision:** Each ingestion job ends by writing the Iceberg snapshot IDs of the two Silver tables to `silver.load_log` under the orchestrator's run ID. A Gold build reads exactly those snapshots (Iceberg's `snapshot_id_<id>` table selector), audits them, appends its rows to `<name>_builds` under a new `build_id`, checks the row counts, records the build in `build_manifest`, and replaces the single row of `published_build` in one commit. The public table names are views over the published build. Failed or blocked builds never reach the pointer. The newest 10 builds are kept, an earlier one can be published again with `--publish-existing`, and every attempt is in `build_log`. In the DAG, Gold is given the run ID of the ingestion before it.
- **Reasoning:** The pointer is the only thing readers depend on and it changes in one Iceberg commit, which makes publication atomic across tables without renames, which a JDBC catalog cannot do atomically. Pinning snapshots makes a build reproducible and independent of loads that start later.
- **Consequences / trade-offs:** Storage grows by one small copy of Gold per build until old builds are pruned. The first versioned build drops the old plain tables and creates views with the same names, so there is a moment, once, when the names do not exist. A build that fails after writing rows leaves unclaimed rows that the next build removes. Builds must not run concurrently, which `max_active_runs=1` guarantees in Airflow and nothing guarantees by hand. A snapshot that was expired could not be read, and no snapshot expiry is configured. Pinning is per load, not per transaction: a second job that writes Silver between the load's last write and the recording is included in the recorded state.
