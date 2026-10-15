-- Statements run by batch/ingest_live.py for every micro-batch read from the dead-letter topic.
-- Python registers one temporary view first:
--   raw_dlq_messages  one row per message from quakeflow.events.dlq
-- The write is a MERGE on the original message's coordinates, so replaying a batch changes
-- nothing. Statements are split on semicolons, so avoid semicolons inside comments and strings.

-- Records that Flink rejected, read back from the dead-letter topic. The identity is the
-- original message's coordinates, taken from the JSON that Flink wrote.
CREATE OR REPLACE TEMP VIEW incoming_flink_rejected AS
SELECT
    timestamp_millis(unix_millis(kafka_timestamp)) AS rejected_at,
    'flink' AS stage,
    get_json_object(payload, '$.reason') AS reason,
    'live' AS source,
    get_json_object(payload, '$.source_topic') AS kafka_topic,
    CAST(get_json_object(payload, '$.source_partition') AS INT) AS kafka_partition,
    CAST(get_json_object(payload, '$.source_offset') AS BIGINT) AS kafka_offset,
    get_json_object(get_json_object(payload, '$.payload'), '$.event_id') AS event_id,
    get_json_object(payload, '$.payload') AS payload
FROM raw_dlq_messages;

MERGE INTO local.silver.rejected_records AS t
USING incoming_flink_rejected AS s
ON t.stage = s.stage
   AND t.kafka_topic = s.kafka_topic
   AND t.kafka_partition = s.kafka_partition
   AND t.kafka_offset = s.kafka_offset
   AND t.payload <=> s.payload
WHEN NOT MATCHED THEN INSERT *;
