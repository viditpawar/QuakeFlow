# Data dictionary

Tables in the Iceberg catalog `local`. Definitions live in `iceberg/sql/002_create_tables.sql`. All timestamps are UTC.
Gold tables are not defined yet. They arrive with the analytics in Phase 6.

Iceberg has no primary keys. Each table's "identity" below is enforced by the MERGE that writes it, so repeating a write is safe.

## Bronze: data as received

### `bronze.live_observations`
Messages from the `quakeflow.events.validated` Kafka topic. Identity: `(kafka_topic, kafka_partition, kafka_offset, payload)`. The message text is part of the identity because Kafka offsets are reused if a topic is recreated, and matching on the coordinates alone would skip the new messages as already loaded (D-026). Partitioned by day of `loaded_at`.

| Column | Meaning |
|---|---|
| kafka_topic, kafka_partition, kafka_offset | Where the message came from |
| kafka_timestamp | Kafka record time, set to the source update time |
| event_id | Kafka key, the USGS event id |
| record_hash | Fingerprint of the source content (see D-009) |
| source_updated_at | USGS last-updated time |
| payload | The message text exactly as received. This is the collector's normalized event, not the original USGS feature. |
| loaded_at | When the row was written to the lakehouse |

### `bronze.historical_observations`
Events from the USGS FDSN catalog API. Identity: `(event_id, payload_sha256)`. Partitioned by day of `loaded_at`.

| Column | Meaning |
|---|---|
| event_id, source_updated_at | USGS id and last-updated time |
| payload, payload_sha256 | Original GeoJSON feature text and its SHA-256 |
| window_start, window_end, min_magnitude | The catalog request that returned it |
| source_checksum | SHA-256 of the downloaded file, from its manifest |
| loaded_at | When the row was written |

## Silver: validated and deduplicated

### `silver.earthquake_versions`
Every distinct revision of every event. Identity: `(event_id, record_hash)`. Insert only. Partitioned by month of `event_time`.

Columns: `event_id`, `record_hash`, `event_time`, `source_updated_at`, `magnitude`, `latitude`, `longitude`, `depth_km`, `place`, `review_status`, `event_type`, `ingested_at`, plus `source` (`live` or `historical`) and `loaded_at`.

### `silver.earthquakes_current`
One accepted row per event. Identity: `event_id`. Not partitioned.

Columns: the same event fields as above, plus `curated_at`, the time the row last changed in the lakehouse (used to measure how long a source update takes to reach the curated table).

The accepted revision is the one with the newest `source_updated_at`. On an exact tie the higher `record_hash` wins (D-005).

### `silver.rejected_records`
Records that failed validation. Columns: `rejected_at`, `stage` (flink, spark or quality), `reason`, `source`, the Kafka coordinates, `event_id` (null when unusable) and `payload`. Partitioned by day of `rejected_at`.

## Field notes

- `magnitude` can be null: USGS does not always assign one.
- `depth_km` can be negative: depth is relative to a reference surface, so shallow events can sit above it.
- `event_type` includes more than earthquakes (quarry blasts, explosions and others). Earthquake analytics filter on `event_type = 'earthquake'` (D-004).
- `review_status` is `automatic` until USGS reviews the event.

## How the tables are loaded

`batch/ingest_live.py` fills the live tables from Kafka, using the statements in `iceberg/sql/003_merge_current.sql` and `003_merge_flink_rejected.sql`:

- Every message goes to `bronze.live_observations`, valid or not.
- Valid messages add a row to `silver.earthquake_versions` if that `(event_id, record_hash)` is new.
- `silver.earthquakes_current` takes the newest revision of each event. A later-arriving older revision does not replace it.
- Messages that fail validation go to `silver.rejected_records` with `stage = 'spark'`. Records Flink rejected are read from the dead-letter topic with `stage = 'flink'`.

## How historical data is loaded

`batch/backfill.py` downloads the USGS catalog in windows and `batch/ingest_history.py` merges the completed windows (statements in `iceberg/sql/003_merge_history.sql`):

- Every feature goes to `bronze.historical_observations` as the original GeoJSON text. A row is identified by `(event_id, payload_sha256)`.
- Valid features add a `source = 'historical'` row to `silver.earthquake_versions` if that `(event_id, record_hash)` is new. A historical copy of an event that live already loaded, with the same content, has the same `record_hash` and adds nothing.
- `silver.earthquakes_current` follows the same rule as the live load: a historical revision replaces the stored one only if it is newer.
- Features that fail validation go to `silver.rejected_records` with `stage = 'normalize'` and `source = 'historical'`, identified by their event id and text.

The catalog returns the current state of each event, not its past revisions, so a backfill cannot recover earlier versions.

# Gold: analytics

Built by `batch/build_gold.py` from Silver, behind the quality gate in `configs/quality.yaml`. Each table is replaced as a whole on every successful build, and a failed build leaves all of them as they were.

**Conventions.** Days and months are UTC. Earthquake analytics count only `event_type = 'earthquake'`, so quarry blasts and other types do not inflate the numbers (D-004). Counts describe what USGS had published and this platform had loaded when the table was built. They are not a complete catalog of every earthquake, and the lakehouse currently holds a magnitude 2.5 and above week from January 2025 beside all-magnitude recent days, so compare periods only if they were loaded the same way.

| Table | One row per | Answers |
|---|---|---|
| `gold.daily_seismic_activity` | UTC day of `event_time` | How many earthquakes per day, with magnitude statistics, how many were reviewed, and how many other event types were left out. |
| `gold.regional_activity` | UTC month and region | Which regions have the most activity. The region is the text after the last comma of `place`, so it is a convenient grouping and not an authoritative geography. USGS writes both "CA" and "California", which show up as separate regions. |
| `gold.magnitude_distribution` | whole-magnitude band | How earthquake counts vary by magnitude. Bands are `<0`, `0-1` up to `6-7`, `7+`, and `unknown` for events with no magnitude. `share` is the fraction of all earthquakes. |
| `gold.revision_statistics` | UTC day of the latest revision's `event_time` | How many events were revised, how long between revisions, and what changed (magnitude, depth, location, review status), plus the largest magnitude change and same-timestamp conflicts. It only sees revisions this platform observed, so changes between two polls are invisible. |
| `gold.ingestion_latency` | UTC day of `loaded_at` | How long a live revision waited between its USGS update and being written to Silver (average, median, 95th percentile, maximum, in minutes). The wait includes how often the load job is run, so it measures the whole path and not Kafka or Flink alone. Historical backfills are excluded. |
| `gold.build_log` | build attempt | Whether each attempt was `promoted`, `blocked` by a failed check, or `publish_failed`, which checks failed, and which Iceberg snapshots of Silver were read. |

## Quality contract

`configs/quality.yaml` holds the checks. A check passes when it finds no violations, and a check that cannot run counts as failed.

- **On `silver.earthquakes_current`:** at least one row, `event_id` present and unique, `event_time`, `source_updated_at` and `record_hash` present, latitude within -90 to 90, and longitude within -180 to 180.
- **On `silver.earthquake_versions`:** `(event_id, record_hash)` unique, `event_id` and `source_updated_at` present, and the same coordinate ranges.
- **Reconciliations:** each staged Gold result must account for exactly the events in Silver, for example the daily counts must sum to the number of earthquakes in `earthquakes_current`.
- **Column order:** each staged result must have the same columns, in the same order, as the table it replaces.
