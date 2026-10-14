# Data dictionary

Tables in the Iceberg catalog `local`. Definitions live in `iceberg/sql/002_create_tables.sql`. All timestamps are UTC.
Gold tables are not defined yet. They arrive with the analytics in Phase 6.

Iceberg has no primary keys. Each table's "identity" below is enforced by the MERGE that writes it, so repeating a write is safe.

## Bronze: data as received

### `bronze.live_observations`
Messages from the `quakeflow.events.validated` Kafka topic. Identity: `(kafka_topic, kafka_partition, kafka_offset)`. Partitioned by day of `loaded_at`.

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
