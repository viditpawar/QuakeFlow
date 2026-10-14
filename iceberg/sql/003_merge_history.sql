-- Statements run by batch/ingest_history.py for the files that batch/backfill.py wrote.
-- Python registers three temporary views first, each with one column called line (a JSON text):
--   raw_history_bronze    lines of the *.bronze.jsonl files of completed windows
--   raw_history_silver    lines of the *.silver.jsonl files, already normalized by the live code
--   raw_history_rejected  lines of the *.rejected.jsonl files
-- Every write is a MERGE on the identity of the target table, and every incoming view is
-- deduplicated first. Two overlapping backfills can contain the same feature twice in one load,
-- and a MERGE that inserts unmatched rows would otherwise insert both copies.
-- Statements are split on semicolons, so avoid semicolons inside comments and strings.

CREATE OR REPLACE TEMP VIEW incoming_hist_bronze AS
SELECT
    event_id, payload_sha256, source_updated_at, payload, window_start, window_end,
    min_magnitude, source_checksum, loaded_at
FROM (
    SELECT
        j.event_id AS event_id,
        j.payload_sha256 AS payload_sha256,
        timestamp_millis(j.source_updated_at) AS source_updated_at,
        j.payload AS payload,
        CAST(j.window_start AS DATE) AS window_start,
        CAST(j.window_end AS DATE) AS window_end,
        j.min_magnitude AS min_magnitude,
        j.source_checksum AS source_checksum,
        current_timestamp() AS loaded_at,
        row_number() OVER (
            PARTITION BY j.event_id, j.payload_sha256 ORDER BY j.source_checksum
        ) AS copy_number
    FROM (
        SELECT from_json(line,
            'event_id string, payload_sha256 string, source_updated_at long, payload string, window_start string, window_end string, min_magnitude double, source_checksum string'
        ) AS j
        FROM raw_history_bronze
    )
    WHERE j IS NOT NULL
)
WHERE copy_number = 1;

-- One row per distinct revision, in the column order of earthquake_versions.
CREATE OR REPLACE TEMP VIEW incoming_hist_versions AS
SELECT
    event_id, record_hash, event_time, source_updated_at, magnitude, latitude, longitude,
    depth_km, place, review_status, event_type, ingested_at, source, loaded_at
FROM (
    SELECT
        j.event_id AS event_id,
        j.record_hash AS record_hash,
        timestamp_millis(j.event_time) AS event_time,
        timestamp_millis(j.source_updated_at) AS source_updated_at,
        j.magnitude AS magnitude,
        j.latitude AS latitude,
        j.longitude AS longitude,
        j.depth_km AS depth_km,
        j.place AS place,
        j.review_status AS review_status,
        j.event_type AS event_type,
        to_timestamp(j.ingested_at) AS ingested_at,
        'historical' AS source,
        current_timestamp() AS loaded_at,
        row_number() OVER (
            PARTITION BY j.event_id, j.record_hash ORDER BY j.ingested_at
        ) AS copy_number
    FROM (
        SELECT from_json(line,
            'event_id string, event_time long, source_updated_at long, magnitude double, latitude double, longitude double, depth_km double, place string, review_status string, event_type string, ingested_at string, record_hash string'
        ) AS j
        FROM raw_history_silver
    )
    WHERE j IS NOT NULL AND j.event_id IS NOT NULL AND j.record_hash IS NOT NULL
)
WHERE copy_number = 1;

-- The winning revision of each event in this load, in the column order of earthquakes_current.
-- Ordering rule: newest source_updated_at, then the higher record_hash on an exact tie.
CREATE OR REPLACE TEMP VIEW incoming_hist_latest AS
SELECT
    event_id, event_time, source_updated_at, magnitude, latitude, longitude, depth_km, place,
    review_status, event_type, record_hash, ingested_at, curated_at
FROM (
    SELECT
        event_id, event_time, source_updated_at, magnitude, latitude, longitude, depth_km, place,
        review_status, event_type, record_hash, ingested_at,
        current_timestamp() AS curated_at,
        row_number() OVER (
            PARTITION BY event_id ORDER BY source_updated_at DESC, record_hash DESC
        ) AS rank_in_load
    FROM incoming_hist_versions
)
WHERE rank_in_load = 1;

-- Features that failed validation, in the column order of rejected_records. They have no Kafka
-- coordinates, so their identity is the stage, the event id and the rejected text itself.
CREATE OR REPLACE TEMP VIEW incoming_hist_rejected AS
SELECT
    rejected_at, stage, reason, source, kafka_topic, kafka_partition, kafka_offset, event_id, payload
FROM (
    SELECT
        current_timestamp() AS rejected_at,
        'normalize' AS stage,
        j.reason AS reason,
        'historical' AS source,
        CAST(NULL AS STRING) AS kafka_topic,
        CAST(NULL AS INT) AS kafka_partition,
        CAST(NULL AS BIGINT) AS kafka_offset,
        j.event_id AS event_id,
        j.payload AS payload,
        row_number() OVER (PARTITION BY j.event_id, j.payload ORDER BY j.reason) AS copy_number
    FROM (
        SELECT from_json(line, 'event_id string, reason string, payload string') AS j
        FROM raw_history_rejected
    )
    WHERE j IS NOT NULL
)
WHERE copy_number = 1;

MERGE INTO local.bronze.historical_observations AS t
USING incoming_hist_bronze AS s
ON t.event_id <=> s.event_id AND t.payload_sha256 = s.payload_sha256
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO local.silver.earthquake_versions AS t
USING incoming_hist_versions AS s
ON t.event_id = s.event_id
   AND t.record_hash = s.record_hash
   AND t.event_time = s.event_time
WHEN NOT MATCHED THEN INSERT *;

-- An old snapshot must never overwrite a newer revision, so the stored row is replaced only if
-- the incoming one is newer, or has the same source_updated_at and a higher record_hash.
MERGE INTO local.silver.earthquakes_current AS t
USING incoming_hist_latest AS s
ON t.event_id = s.event_id
WHEN MATCHED AND (
    s.source_updated_at > t.source_updated_at
    OR (s.source_updated_at = t.source_updated_at AND s.record_hash > t.record_hash)
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO local.silver.rejected_records AS t
USING incoming_hist_rejected AS s
ON t.stage = s.stage
   AND t.source = s.source
   AND t.event_id <=> s.event_id
   AND t.payload = s.payload
WHEN NOT MATCHED THEN INSERT *;
