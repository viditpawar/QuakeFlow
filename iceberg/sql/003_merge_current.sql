-- Statements run by batch/ingest_live.py for every micro-batch read from Kafka.
-- Python registers two temporary views first:
--   raw_messages      one row per message from quakeflow.events.validated
-- Records that Flink rejected are handled separately, in 003_merge_flink_rejected.sql.
-- Every write is a MERGE on the identity of the target table, so replaying a batch (after a
-- failure, or after losing the checkpoint) changes nothing. Statements are split on semicolons,
-- so avoid semicolons inside comments and strings.

-- Parse each message. from_json gives NULL for text that is not JSON, and NULL fields for
-- fields that are missing or have the wrong type, so every problem becomes a reject reason.
CREATE OR REPLACE TEMP VIEW parsed_messages AS
SELECT
    kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, kafka_key, payload,
    from_json(payload,
        'event_id string, event_time long, source_updated_at long, magnitude double, latitude double, longitude double, depth_km double, place string, review_status string, event_type string, ingested_at string, record_hash string'
    ) AS j
FROM raw_messages;

CREATE OR REPLACE TEMP VIEW checked_messages AS
SELECT
    *,
    CASE
        WHEN payload IS NULL THEN 'empty message'
        WHEN j IS NULL THEN 'not valid JSON'
        WHEN j.event_id IS NULL OR j.event_id = '' THEN 'event_id missing'
        WHEN j.event_time IS NULL THEN 'event_time missing or not epoch milliseconds'
        WHEN j.source_updated_at IS NULL THEN 'source_updated_at missing or not epoch milliseconds'
        WHEN j.latitude IS NULL OR j.latitude < -90 OR j.latitude > 90 THEN 'latitude missing or outside -90..90'
        WHEN j.longitude IS NULL OR j.longitude < -180 OR j.longitude > 180 THEN 'longitude missing or outside -180..180'
        WHEN j.record_hash IS NULL OR length(j.record_hash) <> 64 THEN 'record_hash missing or not 64 characters'
        WHEN try_to_timestamp(j.ingested_at) IS NULL THEN 'ingested_at missing or not a timestamp'
        ELSE CAST(NULL AS STRING)
    END AS reject_reason
FROM parsed_messages;

-- Bronze keeps every message, valid or not, in the column order of the table.
CREATE OR REPLACE TEMP VIEW incoming_bronze AS
SELECT
    kafka_topic, kafka_partition, kafka_offset, kafka_timestamp,
    coalesce(kafka_key, j.event_id) AS event_id,
    j.record_hash AS record_hash,
    timestamp_millis(j.source_updated_at) AS source_updated_at,
    payload,
    current_timestamp() AS loaded_at
FROM checked_messages;

-- One row per distinct revision in this batch, in the column order of earthquake_versions.
CREATE OR REPLACE TEMP VIEW incoming_versions AS
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
        'live' AS source,
        current_timestamp() AS loaded_at,
        row_number() OVER (
            PARTITION BY j.event_id, j.record_hash ORDER BY kafka_partition, kafka_offset
        ) AS copy_number
    FROM checked_messages
    WHERE reject_reason IS NULL
)
WHERE copy_number = 1;

-- The winning revision of each event within this batch, in the column order of earthquakes_current.
-- Ordering rule: newest source_updated_at, then the higher record_hash on an exact tie.
CREATE OR REPLACE TEMP VIEW incoming_latest AS
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
        ) AS rank_in_batch
    FROM incoming_versions
)
WHERE rank_in_batch = 1;

-- Messages this job rejects, in the column order of rejected_records.
CREATE OR REPLACE TEMP VIEW incoming_rejected AS
SELECT
    current_timestamp() AS rejected_at,
    'spark' AS stage,
    reject_reason AS reason,
    'live' AS source,
    kafka_topic, kafka_partition, kafka_offset,
    coalesce(kafka_key, j.event_id) AS event_id,
    payload
FROM checked_messages
WHERE reject_reason IS NOT NULL;

MERGE INTO local.bronze.live_observations AS t
USING incoming_bronze AS s
ON t.kafka_topic = s.kafka_topic
   AND t.kafka_partition = s.kafka_partition
   AND t.kafka_offset = s.kafka_offset
WHEN NOT MATCHED THEN INSERT *;

-- A revision is identified by (event_id, record_hash). The hash covers event_time, so the
-- event_time condition changes nothing logically but lets Iceberg skip other months.
MERGE INTO local.silver.earthquake_versions AS t
USING incoming_versions AS s
ON t.event_id = s.event_id
   AND t.record_hash = s.record_hash
   AND t.event_time = s.event_time
WHEN NOT MATCHED THEN INSERT *;

-- Current state. An incoming revision replaces the stored one only if it is newer, or has the
-- same source_updated_at and a higher record_hash. Anything older, or identical, changes nothing.
MERGE INTO local.silver.earthquakes_current AS t
USING incoming_latest AS s
ON t.event_id = s.event_id
WHEN MATCHED AND (
    s.source_updated_at > t.source_updated_at
    OR (s.source_updated_at = t.source_updated_at AND s.record_hash > t.record_hash)
) THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO local.silver.rejected_records AS t
USING incoming_rejected AS s
ON t.stage = s.stage
   AND t.kafka_topic = s.kafka_topic
   AND t.kafka_partition = s.kafka_partition
   AND t.kafka_offset = s.kafka_offset
WHEN NOT MATCHED THEN INSERT *;
