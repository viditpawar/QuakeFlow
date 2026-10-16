-- Parse and validate raw messages, and define where good and bad records go.
-- Run after kafka_sources.sql. Avoid semicolons inside comments.

-- One row per raw message with its fields parsed. JSON_VALUE returns text and TRY_CAST turns
-- it into a number or NULL, so a missing or wrongly typed field is never an error: it
-- becomes a reject reason instead.
CREATE VIEW parsed_messages AS
SELECT
    raw_key,
    payload,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    proc_time,
    event_id,
    source_updated_at,
    record_hash,
    CASE
        WHEN payload IS NULL THEN 'empty message'
        WHEN NOT (payload IS JSON OBJECT) THEN 'not a JSON object'
        WHEN event_id IS NULL OR event_id = '' THEN 'event_id missing'
        WHEN event_time IS NULL THEN 'event_time missing or not epoch milliseconds'
        WHEN source_updated_at IS NULL THEN 'source_updated_at missing or not epoch milliseconds'
        WHEN latitude IS NULL OR latitude < -90 OR latitude > 90 THEN 'latitude missing or outside -90..90'
        WHEN longitude IS NULL OR longitude < -180 OR longitude > 180 THEN 'longitude missing or outside -180..180'
        WHEN record_hash IS NULL OR CHAR_LENGTH(record_hash) <> 64 THEN 'record_hash missing or not 64 characters'
        ELSE CAST(NULL AS STRING)
    END AS reject_reason
FROM (
    SELECT
        raw_key,
        payload,
        kafka_topic,
        kafka_partition,
        kafka_offset,
        proc_time,
        JSON_VALUE(payload, '$.event_id') AS event_id,
        TRY_CAST(JSON_VALUE(payload, '$.event_time') AS BIGINT) AS event_time,
        TRY_CAST(JSON_VALUE(payload, '$.source_updated_at') AS BIGINT) AS source_updated_at,
        TRY_CAST(JSON_VALUE(payload, '$.latitude') AS DOUBLE) AS latitude,
        TRY_CAST(JSON_VALUE(payload, '$.longitude') AS DOUBLE) AS longitude,
        JSON_VALUE(payload, '$.record_hash') AS record_hash
    FROM raw_messages
);

-- Valid revisions. The original message text is passed through unchanged. The record timestamp is written
-- as the source update time, but the topic's LogAppendTime setting replaces it with the time the broker
-- received the record, so that retention counts from arrival (D-032).
CREATE TABLE IF NOT EXISTS validated_events_sink (
    event_id STRING,
    payload  STRING,
    kafka_ts TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
    'connector' = 'kafka',
    'topic' = 'quakeflow.events.validated',
    'properties.bootstrap.servers' = 'kafka:19092',
    'key.format' = 'raw',
    'key.fields' = 'event_id',
    'value.format' = 'raw',
    'value.fields-include' = 'EXCEPT_KEY',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = 'quakeflow-validated',
    'properties.transaction.timeout.ms' = '600000'
);

-- Rejected messages, as JSON with the reason and where the message came from.
CREATE TABLE IF NOT EXISTS rejected_events_sink (
    payload STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'quakeflow.events.dlq',
    'properties.bootstrap.servers' = 'kafka:19092',
    'value.format' = 'raw',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = 'quakeflow-rejected',
    'properties.transaction.timeout.ms' = '600000'
);
