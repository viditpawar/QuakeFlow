-- Source tables over the raw topic written by the collector.
-- Kafka is reached through its INTERNAL listener, kafka:19092, from inside the compose network.
-- Avoid semicolons inside comments: the SQL client splits statements on them.

-- Typed view of the events, for ad-hoc queries. Event time is the source's own
-- source_updated_at, with a 10 minute watermark for downstream windowed analytics.
CREATE TABLE IF NOT EXISTS raw_events (
    event_id          STRING,
    event_time        BIGINT,        -- USGS epoch milliseconds
    source_updated_at BIGINT,        -- USGS epoch milliseconds
    magnitude         DOUBLE,
    latitude          DOUBLE,
    longitude         DOUBLE,
    depth_km          DOUBLE,
    place             STRING,
    review_status     STRING,
    event_type        STRING,
    ingested_at       STRING,        -- ISO-8601 UTC, set by the collector
    record_hash       STRING,
    kafka_partition   INT    METADATA FROM 'partition' VIRTUAL,
    kafka_offset      BIGINT METADATA FROM 'offset' VIRTUAL,
    updated_ts AS TO_TIMESTAMP_LTZ(source_updated_at, 3),
    WATERMARK FOR updated_ts AS updated_ts - INTERVAL '10' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'quakeflow.events.raw',
    'properties.bootstrap.servers' = 'kafka:19092',
    'properties.group.id' = 'quakeflow-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- The same topic as untouched text, so malformed messages can be rejected instead of
-- crashing the job. This is the table the validation job reads.
CREATE TABLE IF NOT EXISTS raw_messages (
    raw_key         STRING,
    payload         STRING,
    kafka_topic     STRING METADATA FROM 'topic' VIRTUAL,
    kafka_partition INT    METADATA FROM 'partition' VIRTUAL,
    kafka_offset    BIGINT METADATA FROM 'offset' VIRTUAL,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'quakeflow.events.raw',
    'properties.bootstrap.servers' = 'kafka:19092',
    'properties.group.id' = 'quakeflow-flink-validate',
    'scan.startup.mode' = 'earliest-offset',
    'key.format' = 'raw',
    'key.fields' = 'raw_key',
    'value.format' = 'raw',
    'value.fields-include' = 'EXCEPT_KEY'
);
