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
