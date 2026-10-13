# Decisions Log

A record of the decisions made on this project: what the situation was, what was chosen, and why.

**Status values:** `Accepted` (decided and in effect), `Planned` (decided, not built yet), `Retrospective` (made earlier and written down afterwards, so the date is when it was logged, not when it was made).

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

### D-001: Project scope is USGS earthquake events
- **Date:** 2026-10-13
- **Status:** Retrospective (the project was first started on GBFS bike-share data, then pivoted; see git history)
- **Situation:** The project needed a real, free, public data source with records that change after first publication.
- **Options considered:** GBFS / Citi Bike bike-share feeds (the first attempt); USGS earthquake feeds.
- **Decision:** Build on USGS earthquake data: live GeoJSON feeds plus the FDSN historical catalog API.
- **Reasoning:** Events are revised after publication (for example magnitude 4.6 revised to 4.8), which exercises event versioning and reconciliation. The data is public and needs no API key.
- **Consequences / trade-offs:** The platform is data infrastructure, not earthquake prediction or warning. USGS must be credited as the source.

### D-002: Single-node Apache Kafka in KRaft mode, keyed by event_id
- **Date:** 2026-10-13
- **Status:** Retrospective
- **Situation:** The collector needs a durable, replayable buffer between USGS and the processing jobs.
- **Options considered:** Kafka with ZooKeeper; Kafka in KRaft mode; processing straight from the poller with no broker.
- **Decision:** One `apache/kafka:3.9.0` broker in KRaft mode, pinned version, no auto-created topics. Topics: `quakeflow.events.raw` (3 partitions), `quakeflow.events.validated` (3), `quakeflow.events.dlq` (1), all with replication factor 1. Messages are keyed by `event_id`.
- **Reasoning:** KRaft removes ZooKeeper. Keying by `event_id` puts every version of one earthquake in one partition, so versions stay in order. A pinned version makes the setup repeatable.
- **Consequences / trade-offs:** No fault tolerance (one broker), which is acceptable for local learning. The 3 partitions are a local choice, not benchmark-derived. Topics are defined in `configs/kafka-topics.yaml`.

### D-003: Docker Compose with separate profiles
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** Development runs on a 16 GB laptop that already uses about 11 GB when idle (Task Manager reading on 2026-10-13). Kafka, Flink, Spark, PostgreSQL, object storage, Prometheus and Grafana together will not fit.
- **Options considered:** One compose file with everything always on; separate profiles; separate compose files.
- **Decision:** One `docker-compose.yml` with profiles `core`, `streaming`, `batch` and `monitoring`. Start with modest memory limits and measure real usage before changing them.
- **Reasoning:** Only the services needed for the current phase run at one time.
- **Consequences / trade-offs:** Some end-to-end tests will need more than one profile, so they may only run on a stronger machine or in stages. Kafka is currently in the `core` profile.

### D-004: Keep the flat repository layout
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** The design guide proposed `src/quakeflow/...`, but the repo already had top-level `ingestion/`, `configs/` and `infrastructure/`.
- **Options considered:** Migrate to `src/quakeflow/` (the guide's layout); keep the flat layout.
- **Decision:** Keep the flat layout and add top-level packages as needed (`schemas/`, `processing/`, later `batch/` and others). Each package is listed explicitly under `[tool.setuptools.packages.find]` in `pyproject.toml`, and the project is installed with `pip install -e .`.
- **Reasoning:** Avoids refactoring history that already works. Setuptools refuses to auto-discover several top-level packages, so an explicit list is required.
- **Consequences / trade-offs:** Package names are generic (`schemas`, `processing`) and import as top-level modules, so they could clash with other installed packages. Each new package must be added to `pyproject.toml`, and Dockerfiles must install the project the same way. The repository and Python distribution are named QuakeFlow and `quakeflow`; the longer descriptive title is used only in the README.

### D-005: Python 3.11 baseline
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** The machine also has a newer Python. PySpark, Flink and Iceberg tooling often lag behind new Python releases.
- **Options considered:** Use the newest installed Python; standardize on 3.11.
- **Decision:** Use 3.11 (`.python-version`, `.venv`), with `requires-python = ">=3.11,<3.13"`.
- **Reasoning:** Avoids compatibility problems when the Spark and Flink phases arrive.
- **Consequences / trade-offs:** The upper bound has to be raised deliberately later. Local commands are Windows PowerShell.

### D-006: Poll two USGS feeds with independent, configurable intervals
- **Date:** 2026-10-13
- **Status:** Planned
- **Situation:** The one-hour feed is quick, but misses events and revisions that appear outside its window.
- **Options considered:** `all_hour` only; `all_hour` plus `all_day`; `updatedafter` queries against the catalog API.
- **Decision:** Poll `all_hour` every 60 seconds and `all_day` every 15 minutes, both configurable, with HTTP timeouts, exponential backoff and duplicate detection. Add a scheduled longer-lookback or `updatedafter` reconciliation later.
- **Reasoning:** The all-day feed catches records missed by the short window, but it cannot catch revisions made after an event has left that window, so a separate reconciliation job is still needed.
- **Consequences / trade-offs:** The same event will appear in both feeds, so deduplication is required to avoid double counting. The feeds refresh about once a minute, so polling faster is wasteful.

### D-007: Keep all event types in Bronze and Silver; filter in Gold
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** USGS reports earthquakes, quarry blasts, explosions and other seismic events in one feed.
- **Options considered:** Drop non-earthquakes at ingestion; keep everything and filter later.
- **Decision:** Preserve all valid event types in Bronze and Silver. Earthquake-specific Gold analytics filter with `WHERE event_type = 'earthquake'`.
- **Reasoning:** Keeps the platform flexible and avoids misleading earthquake counts.
- **Consequences / trade-offs:** Every Gold model must state which event types it counts.

### D-008: Event versioning policy
- **Date:** 2026-10-13
- **Status:** Planned
- **Situation:** A source can revise an event after publication. Treating each revision as a new event double counts it.
- **Options considered:** Overwrite the record in place; store only the latest; store every distinct revision plus a separate current-state table.
- **Decision:** Identify events by `event_id`. Store every distinct revision (distinct by content hash) in `silver.earthquake_versions`, and keep one row per event in `silver.earthquakes_current`. The current row changes only when the incoming version has a newer `source_updated_at`. On equal timestamps with different content, the higher `record_hash` wins deterministically and the conflict is logged.
- **Reasoning:** Keeps history and a correct current view. A late-arriving old revision, or an old historical snapshot, must not overwrite a newer live one.
- **Consequences / trade-offs:** The hash tie-break is deterministic, not a claim about which revision is more authoritative, so same-timestamp conflicts are flagged for review.

### D-009: Build the lakehouse path in two stages (Approach A first)
- **Date:** 2026-10-13
- **Status:** Planned
- **Situation:** Flink can write to Iceberg directly, but that needs careful connector version matching and checkpoint-aware commits.
- **Options considered:** Approach A: USGS to Kafka to Flink to a curated Kafka topic to Spark micro-batch to Iceberg. Approach B: Flink writes to Iceberg directly.
- **Decision:** Build Approach A first. Revisit Approach B only if time permits after correctness is proven.
- **Reasoning:** Avoids debugging the Flink-Iceberg connector and the event-versioning logic at the same time.
- **Consequences / trade-offs:** Adds an extra Kafka hop and a Spark job. No Flink, Spark or Iceberg work starts until ingestion and event-versioning tests pass.

### D-010: Normalization rules for incoming records
- **Date:** 2026-10-13
- **Status:** Accepted
- **Situation:** Raw USGS features have nullable fields, epoch-millisecond timestamps and occasional malformed records.
- **Options considered:** Lenient parsing that coerces types; strict typing with explicit rejection.
- **Decision:** A Pydantic model with strict types. Timestamps become timezone-aware UTC. Latitude must be in -90..90 and longitude in -180..180. Magnitude and depth are optional, and negative depths are valid (shallow or above-reference depths exist in seismic catalogs). NaN and infinity are rejected. Invalid features are never dropped: they come back with every reason and the original feature, so they can go to the DLQ.
- **Reasoning:** Bad input is visible and replayable instead of silently altered.
- **Consequences / trade-offs:** Strictness may reject unusual but legitimate source values, which then appear in the rejected records for review. The record hash is added separately in the next step.

### D-011: Change detector separates "detected" from "published"
- **Date:** 2026-10-13
- **Status:** Planned
- **Situation:** The collector remembers the last hash seen per event in SQLite so unchanged records are not re-published. Saving the hash before Kafka confirms delivery would lose data if the process crashed in between.
- **Options considered:** Record the hash when a change is detected; record it only after Kafka acknowledges the publication.
- **Decision:** A change counts as delivered only after Kafka confirms it. Detected-but-unconfirmed changes stay pending and are retried.
- **Reasoning:** Protects against silent data loss across crashes and broker outages.
- **Consequences / trade-offs:** Delivery is at-least-once, so downstream steps must tolerate duplicates (the hash-based deduplication already does).
