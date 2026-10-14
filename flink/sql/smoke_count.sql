-- Connectivity check: count what is currently in the raw topic, then finish.
-- docker exec quakeflow-flink-jobmanager ./bin/sql-client.sh \
--     -i /opt/flink/sql/kafka_sources.sql -f /opt/flink/sql/smoke_count.sql
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

SELECT
    COUNT(*)                    AS messages,
    COUNT(DISTINCT event_id)    AS distinct_events,
    COUNT(DISTINCT kafka_partition) AS partitions
FROM raw_events /*+ OPTIONS('scan.bounded.mode' = 'latest-offset') */;
