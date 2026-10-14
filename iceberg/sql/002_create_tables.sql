-- Bronze and Silver tables. Safe to run repeatedly (IF NOT EXISTS leaves existing tables alone).
-- Gold tables are created later, in 004_gold_models.sql, once the analytics are defined.
--
-- All timestamps are UTC (Spark runs with spark.sql.session.timeZone=UTC). Iceberg has no
-- primary keys, so uniqueness is enforced by the MERGE statements that write these tables,
-- and every write is designed to be safe to repeat.

-- ---------------------------------------------------------------------------------------------
-- Bronze: data as received, plus where it came from. Append-only.
-- ---------------------------------------------------------------------------------------------

-- Messages from quakeflow.events.validated. The payload is the Kafka message text exactly as
-- received. It is the collector's normalized event, not the original USGS feature, which the
-- collector does not forward. A message is identified by its Kafka topic, partition and offset.
CREATE TABLE IF NOT EXISTS local.bronze.live_observations (
    kafka_topic       STRING    COMMENT 'Source topic',
    kafka_partition   INT       COMMENT 'Source partition',
    kafka_offset      BIGINT    COMMENT 'Offset within the partition',
    kafka_timestamp   TIMESTAMP COMMENT 'Kafka record timestamp, set to the source update time',
    event_id          STRING    COMMENT 'Kafka key, the USGS event id',
    record_hash       STRING    COMMENT 'SHA-256 fingerprint of the source content',
    source_updated_at TIMESTAMP COMMENT 'USGS last-updated time, from the payload',
    payload           STRING    COMMENT 'Message text exactly as received',
    loaded_at         TIMESTAMP COMMENT 'When this row was written to the lakehouse'
)
USING iceberg
PARTITIONED BY (days(loaded_at))
COMMENT 'Live observations as received from Kafka'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);

-- Events from the USGS FDSN catalog API. The payload is the original GeoJSON feature. A row is
-- identified by event id plus the SHA-256 of that payload, so reloading a window adds nothing.
CREATE TABLE IF NOT EXISTS local.bronze.historical_observations (
    event_id            STRING    COMMENT 'USGS event id',
    payload_sha256      STRING    COMMENT 'SHA-256 of the original feature text',
    source_updated_at   TIMESTAMP COMMENT 'USGS last-updated time, from the payload',
    payload             STRING    COMMENT 'Original GeoJSON feature text',
    window_start        DATE      COMMENT 'Start of the catalog request window',
    window_end          DATE      COMMENT 'End of the catalog request window',
    min_magnitude       DOUBLE    COMMENT 'Minimum magnitude filter of the request',
    source_checksum     STRING    COMMENT 'SHA-256 of the downloaded file, from its manifest',
    loaded_at           TIMESTAMP COMMENT 'When this row was written to the lakehouse'
)
USING iceberg
PARTITIONED BY (days(loaded_at))
COMMENT 'Historical observations from the USGS catalog API'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);

-- ---------------------------------------------------------------------------------------------
-- Silver: validated, normalized, deduplicated.
-- ---------------------------------------------------------------------------------------------

-- Every distinct revision of every event, from live and historical sources. A revision is
-- identified by (event_id, record_hash). The table is never updated, only inserted into.
-- Partitioned by month of the event because the data is small: finer partitions would only
-- produce tiny files.
CREATE TABLE IF NOT EXISTS local.silver.earthquake_versions (
    event_id          STRING    COMMENT 'USGS event id',
    record_hash       STRING    COMMENT 'SHA-256 fingerprint of the source content',
    event_time        TIMESTAMP COMMENT 'When the event occurred',
    source_updated_at TIMESTAMP COMMENT 'USGS last-updated time of this revision',
    magnitude         DOUBLE    COMMENT 'Null when USGS has not assigned one',
    latitude          DOUBLE,
    longitude         DOUBLE,
    depth_km          DOUBLE    COMMENT 'Can be negative: depth is relative to a reference surface',
    place             STRING,
    review_status     STRING    COMMENT 'automatic or reviewed',
    event_type        STRING    COMMENT 'earthquake, quarry blast, explosion and so on',
    ingested_at       TIMESTAMP COMMENT 'When the collector or backfill first received it',
    source            STRING    COMMENT 'live or historical',
    loaded_at         TIMESTAMP COMMENT 'When this revision was first written to Silver'
)
USING iceberg
PARTITIONED BY (months(event_time))
COMMENT 'Every distinct revision of every event'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);

-- One accepted row per event: the revision that wins the ordering rule (newest
-- source_updated_at, with the higher record_hash breaking an exact tie). Not partitioned: the
-- table is small, and event_time can itself be revised, which would move rows between
-- partitions on every merge.
CREATE TABLE IF NOT EXISTS local.silver.earthquakes_current (
    event_id          STRING    COMMENT 'USGS event id, unique in this table',
    event_time        TIMESTAMP COMMENT 'When the event occurred',
    source_updated_at TIMESTAMP COMMENT 'USGS last-updated time of the accepted revision',
    magnitude         DOUBLE,
    latitude          DOUBLE,
    longitude         DOUBLE,
    depth_km          DOUBLE,
    place             STRING,
    review_status     STRING,
    event_type        STRING,
    record_hash       STRING    COMMENT 'Fingerprint of the accepted revision',
    ingested_at       TIMESTAMP COMMENT 'When the accepted revision was first received',
    curated_at        TIMESTAMP COMMENT 'When this row was last changed in the lakehouse'
)
USING iceberg
COMMENT 'Current accepted state of each event'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);

-- Messages that failed validation, kept so they can be inspected and replayed.
CREATE TABLE IF NOT EXISTS local.silver.rejected_records (
    rejected_at       TIMESTAMP COMMENT 'When the record was rejected',
    stage             STRING    COMMENT 'Where it failed: flink, spark or quality',
    reason            STRING    COMMENT 'Why it was rejected',
    source            STRING    COMMENT 'live or historical',
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      BIGINT,
    event_id          STRING    COMMENT 'Null when the record had no usable id',
    payload           STRING    COMMENT 'The rejected message text'
)
USING iceberg
PARTITIONED BY (days(rejected_at))
COMMENT 'Invalid records with the reason they were rejected'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '20'
);
