-- Gold tables: analytics built from Silver by batch/build_gold.py. Safe to run repeatedly.
--
-- Conventions for every Gold table:
--   * Days and months are UTC.
--   * Earthquake analytics count only event_type = 'earthquake'. Quarry blasts, explosions and the
--     other event types stay in Silver and are reported separately where it helps (decision D-004).
--   * Counts reflect what USGS had published and this platform had loaded when the table was
--     built. They are not a complete catalog of every earthquake.
--   * Every build appends its rows to the <name>_builds tables under a new build_id, and only if the
--     quality checks pass. Readers use the views called <name> (created by batch/build_gold.py), which show
--     the rows of the one published build. Publishing is a single commit to published_build, so a reader
--     sees the previous build or the new one in full, never a mix of tables from different builds.

-- Events per UTC day of the event's own time.
CREATE TABLE IF NOT EXISTS local.gold.daily_seismic_activity_builds (
    build_id             STRING COMMENT 'The Gold build that wrote this row',
    event_date           DATE   COMMENT 'UTC day of event_time',
    event_count          BIGINT COMMENT 'Earthquakes that day',
    magnitude_count      BIGINT COMMENT 'Earthquakes with a magnitude, the denominator of the averages',
    avg_magnitude        DOUBLE,
    min_magnitude        DOUBLE,
    max_magnitude        DOUBLE,
    avg_depth_km         DOUBLE,
    reviewed_count       BIGINT COMMENT 'Earthquakes USGS has reviewed',
    non_earthquake_count BIGINT COMMENT 'Other event types that day, not counted in event_count'
)
USING iceberg
COMMENT 'Earthquake counts and magnitude statistics per UTC day'
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');

-- Events per month and region. The region is the text after the last comma of USGS place
-- (usually a state or country). Place is free text, so this is a convenient grouping and
-- not an authoritative geographic assignment.
CREATE TABLE IF NOT EXISTS local.gold.regional_activity_builds (
    build_id             STRING COMMENT 'The Gold build that wrote this row',
    event_month   DATE   COMMENT 'First day of the UTC month of event_time',
    region        STRING COMMENT 'Text after the last comma of place, or all of place when it has none',
    event_count   BIGINT,
    avg_magnitude DOUBLE,
    max_magnitude DOUBLE
)
USING iceberg
COMMENT 'Earthquake counts per UTC month and region'
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');

-- How many earthquakes fall in each whole-magnitude band.
CREATE TABLE IF NOT EXISTS local.gold.magnitude_distribution_builds (
    build_id             STRING COMMENT 'The Gold build that wrote this row',
    magnitude_band STRING COMMENT 'Label such as 2-3. Below 0 and 7 or above are grouped, and no magnitude is unknown',
    band_min       INT    COMMENT 'Lower edge of the band, -1 for below 0, null for unknown',
    event_count    BIGINT,
    share          DOUBLE COMMENT 'Fraction of all earthquakes, between 0 and 1'
)
USING iceberg
COMMENT 'Earthquake frequency by magnitude band'
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');

-- How often, and in what ways, the platform has seen events revised. Built from the revisions
-- this platform observed, so a revision made between two polls, or before the platform first saw
-- the event, is not visible here.
CREATE TABLE IF NOT EXISTS local.gold.revision_statistics_builds (
    build_id             STRING COMMENT 'The Gold build that wrote this row',
    event_date                      DATE   COMMENT 'UTC day of event_time of the latest revision',
    event_count                     BIGINT COMMENT 'Earthquakes that day',
    revised_event_count             BIGINT COMMENT 'Earthquakes with more than one observed revision',
    revision_count                  BIGINT COMMENT 'Revisions after the first, summed over events',
    avg_minutes_between_revisions   DOUBLE COMMENT 'Mean gap between consecutive source_updated_at values',
    magnitude_changed_event_count   BIGINT COMMENT 'Earthquakes whose magnitude changed at least once',
    depth_changed_event_count       BIGINT,
    location_changed_event_count    BIGINT,
    status_changed_event_count      BIGINT COMMENT 'Earthquakes whose review_status changed at least once',
    max_abs_magnitude_change        DOUBLE COMMENT 'Largest single magnitude change that day',
    same_timestamp_conflict_count   BIGINT COMMENT 'Revisions with the same source_updated_at as the one before but different content'
)
USING iceberg
COMMENT 'Revision behaviour of earthquake events per UTC day'
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');

-- How long a revision seen by the live pipeline waited before it was written to Silver. The wait
-- includes how often the load job is run, so it measures the whole path and not Kafka or Flink alone.
CREATE TABLE IF NOT EXISTS local.gold.ingestion_latency_builds (
    build_id             STRING COMMENT 'The Gold build that wrote this row',
    load_date           DATE   COMMENT 'UTC day the revisions were written to Silver',
    revision_count      BIGINT COMMENT 'Live revisions written that day',
    avg_latency_minutes DOUBLE COMMENT 'loaded_at minus source_updated_at',
    p50_latency_minutes DOUBLE,
    p95_latency_minutes DOUBLE,
    max_latency_minutes DOUBLE
)
USING iceberg
COMMENT 'Delay between a USGS update and its arrival in Silver, live revisions only'
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');

-- One row per attempt to build Gold, whether or not it was allowed to publish.
CREATE TABLE IF NOT EXISTS local.gold.build_log (
    run_id                      STRING,
    built_at                    TIMESTAMP,
    status                      STRING COMMENT 'promoted, blocked, publish_failed or republished',
    checks_passed               INT,
    checks_failed               INT,
    failed_checks               STRING COMMENT 'Names of the failed checks, separated by semicolons',
    load_run_id                 STRING COMMENT 'The load whose Silver snapshots were read',
    source_current_snapshot_id  BIGINT COMMENT 'Iceberg snapshot of silver.earthquakes_current that was read',
    source_versions_snapshot_id BIGINT COMMENT 'Iceberg snapshot of silver.earthquake_versions that was read'
)
USING iceberg
COMMENT 'History of Gold build attempts and quality gate outcomes'
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');

-- The builds that were written in full and passed the quality gate, with the Silver snapshots they read.
CREATE TABLE IF NOT EXISTS local.gold.build_manifest (
    build_id                    STRING,
    completed_at                TIMESTAMP,
    load_run_id                 STRING COMMENT 'The load whose Silver snapshots this build read',
    source_current_snapshot_id  BIGINT,
    source_versions_snapshot_id BIGINT,
    checks_passed               INT,
    table_row_counts            STRING COMMENT 'JSON object: Gold table name to number of rows written'
)
USING iceberg
COMMENT 'Complete Gold builds, available to be published or rolled back to'
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');

-- Always zero or one row: the build the views show. Replaced in one commit.
CREATE TABLE IF NOT EXISTS local.gold.published_build (
    build_id     STRING,
    published_at TIMESTAMP
)
USING iceberg
COMMENT 'The Gold build that readers currently see'
TBLPROPERTIES ('format-version' = '2', 'write.parquet.compression-codec' = 'zstd');
