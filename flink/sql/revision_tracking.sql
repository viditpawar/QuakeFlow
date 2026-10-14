-- Distinct revisions and the streaming job. Run after kafka_sources.sql and validated_events.sql.
-- Avoid semicolons inside comments.

SET 'pipeline.name' = 'quakeflow-validate-and-dedupe';
SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';

-- Checkpoints every 10 seconds make the Kafka sinks exactly-once. Records become visible to
-- read_committed consumers when a checkpoint completes. Checkpoints are kept if the job is cancelled.
SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';

-- If the job fails, for example because Kafka is down, retry for as long as it takes: 10 seconds
-- after the first failure, doubling each time up to 1 minute between tries, with no limit on the
-- number of tries. A fixed limit made the job give up for good after a few minutes of outage
-- and stay failed even once Kafka was back. The 1 minute cap means that once Kafka returns the
-- job notices within about a minute. The delay resets after 10 minutes without a failure.
SET 'restart-strategy.type' = 'exponential-delay';
SET 'restart-strategy.exponential-delay.initial-backoff' = '10 s';
SET 'restart-strategy.exponential-delay.max-backoff' = '1 min';
SET 'restart-strategy.exponential-delay.backoff-multiplier' = '2.0';
SET 'restart-strategy.exponential-delay.reset-backoff-threshold' = '10 min';
SET 'restart-strategy.exponential-delay.jitter-factor' = '0.1';

-- Remember which revisions were seen for 7 days, the same as the raw topic's retention.
SET 'table.exec.state.ttl' = '7 d';
-- A partition with no traffic must not hold back the others.
SET 'table.exec.source.idle-timeout' = '30 s';

-- Keep the first sighting of each (event_id, record_hash). A repeat of the same revision is
-- dropped, and a revised event has a new record_hash, so it passes through. Late or
-- out-of-order revisions are still kept: which one is current is decided later by the merge.
CREATE VIEW distinct_revisions AS
SELECT event_id, payload, source_updated_at
FROM (
    SELECT
        event_id,
        payload,
        source_updated_at,
        ROW_NUMBER() OVER (PARTITION BY event_id, record_hash ORDER BY proc_time ASC) AS row_num
    FROM parsed_messages
    WHERE reject_reason IS NULL
)
WHERE row_num = 1;

EXECUTE STATEMENT SET
BEGIN
    INSERT INTO validated_events_sink
    SELECT event_id, payload, TO_TIMESTAMP_LTZ(source_updated_at, 3)
    FROM distinct_revisions;

    INSERT INTO rejected_events_sink
    SELECT JSON_OBJECT(
        'reason' VALUE reject_reason,
        'source_topic' VALUE kafka_topic,
        'source_partition' VALUE kafka_partition,
        'source_offset' VALUE kafka_offset,
        'payload' VALUE payload
    )
    FROM parsed_messages
    WHERE reject_reason IS NOT NULL;
END;
