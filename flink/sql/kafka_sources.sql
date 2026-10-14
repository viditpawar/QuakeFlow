-- Source table over the raw topic written by the collector.
-- Run it first in any Flink SQL session: sql-client.sh -i /opt/flink/sql/kafka_sources.sql
-- Kafka is reached through its INTERNAL listener, kafka:19092, from inside the compose network.
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
    kafka_offset      BIGINT METADATA FROM 'offset' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = 'quakeflow.events.raw',
    'properties.bootstrap.servers' = 'kafka:19092',
    'properties.group.id' = 'quakeflow-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
