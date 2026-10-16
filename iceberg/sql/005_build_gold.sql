-- Built by batch/build_gold.py in two phases.
-- Phase 1 runs every CREATE OR REPLACE TEMP VIEW below. These stage the results without touching
--   the Gold tables, so the quality gate can audit them first. They read the Silver snapshots that
--   the load recorded, not whatever Silver holds when the build starts.
-- Phase 2 publishes, only if every check passed. Each INSERT OVERWRITE below names a Gold table and the
--   staged view that fills it, and the staged views have the same columns, in the same order, as the
--   table. The build does not run these statements as written. It appends the staged rows to the
--   table's <name>_builds table under a new build_id, and then makes that build the published one
--   in a single commit, so the INSERT OVERWRITE form is only the declaration of what fills what.
-- Statements are split on semicolons, so avoid semicolons inside comments and strings.

CREATE OR REPLACE TEMP VIEW staged_daily_seismic_activity AS
SELECT
    CAST(event_time AS DATE) AS event_date,
    sum(CASE WHEN event_type = 'earthquake' THEN 1 ELSE 0 END) AS event_count,
    sum(CASE WHEN event_type = 'earthquake' AND magnitude IS NOT NULL THEN 1 ELSE 0 END) AS magnitude_count,
    avg(CASE WHEN event_type = 'earthquake' THEN magnitude END) AS avg_magnitude,
    min(CASE WHEN event_type = 'earthquake' THEN magnitude END) AS min_magnitude,
    max(CASE WHEN event_type = 'earthquake' THEN magnitude END) AS max_magnitude,
    avg(CASE WHEN event_type = 'earthquake' THEN depth_km END) AS avg_depth_km,
    sum(CASE WHEN event_type = 'earthquake' AND review_status = 'reviewed' THEN 1 ELSE 0 END) AS reviewed_count,
    sum(CASE WHEN event_type = 'earthquake' THEN 0 ELSE 1 END) AS non_earthquake_count
FROM local.silver.earthquakes_current
GROUP BY CAST(event_time AS DATE);

CREATE OR REPLACE TEMP VIEW staged_regional_activity AS
SELECT
    CAST(date_trunc('month', event_time) AS DATE) AS event_month,
    CASE
        WHEN place IS NULL OR trim(place) = '' THEN 'unknown'
        WHEN instr(place, ',') > 0 THEN trim(element_at(split(place, ','), -1))
        ELSE trim(place)
    END AS region,
    count(*) AS event_count,
    avg(magnitude) AS avg_magnitude,
    max(magnitude) AS max_magnitude
FROM local.silver.earthquakes_current
WHERE event_type = 'earthquake'
GROUP BY
    CAST(date_trunc('month', event_time) AS DATE),
    CASE
        WHEN place IS NULL OR trim(place) = '' THEN 'unknown'
        WHEN instr(place, ',') > 0 THEN trim(element_at(split(place, ','), -1))
        ELSE trim(place)
    END;

CREATE OR REPLACE TEMP VIEW staged_magnitude_distribution AS
SELECT
    CASE
        WHEN band_min IS NULL THEN 'unknown'
        WHEN band_min = -1 THEN '<0'
        WHEN band_min = 7 THEN '7+'
        ELSE concat(CAST(band_min AS STRING), '-', CAST(band_min + 1 AS STRING))
    END AS magnitude_band,
    band_min,
    event_count,
    event_count / sum(event_count) OVER () AS share
FROM (
    SELECT
        CASE
            WHEN magnitude IS NULL THEN CAST(NULL AS INT)
            WHEN magnitude < 0 THEN -1
            WHEN magnitude >= 7 THEN 7
            ELSE CAST(floor(magnitude) AS INT)
        END AS band_min,
        count(*) AS event_count
    FROM local.silver.earthquakes_current
    WHERE event_type = 'earthquake'
    GROUP BY 1
);

-- One row per observed revision with the previous revision's values beside it, then one row per
-- event, then one row per day. The order within an event is by source_updated_at, then record_hash.
CREATE OR REPLACE TEMP VIEW staged_revision_statistics AS
SELECT
    event_date,
    count(*) AS event_count,
    sum(CASE WHEN revisions > 0 THEN 1 ELSE 0 END) AS revised_event_count,
    sum(revisions) AS revision_count,
    sum(gap_minutes) / nullif(sum(revisions), 0) AS avg_minutes_between_revisions,
    sum(CASE WHEN magnitude_changes > 0 THEN 1 ELSE 0 END) AS magnitude_changed_event_count,
    sum(CASE WHEN depth_changes > 0 THEN 1 ELSE 0 END) AS depth_changed_event_count,
    sum(CASE WHEN location_changes > 0 THEN 1 ELSE 0 END) AS location_changed_event_count,
    sum(CASE WHEN status_changes > 0 THEN 1 ELSE 0 END) AS status_changed_event_count,
    max(max_abs_magnitude_change) AS max_abs_magnitude_change,
    sum(conflicts) AS same_timestamp_conflict_count
FROM (
    SELECT
        event_id,
        CAST(max_by(event_time, source_updated_at) AS DATE) AS event_date,
        sum(CASE WHEN prev_updated IS NOT NULL THEN 1 ELSE 0 END) AS revisions,
        sum(CASE WHEN prev_updated IS NOT NULL
                 THEN (unix_millis(source_updated_at) - unix_millis(prev_updated)) / 60000.0
                 ELSE 0 END) AS gap_minutes,
        sum(CASE WHEN prev_updated IS NOT NULL AND NOT (magnitude <=> prev_magnitude) THEN 1 ELSE 0 END) AS magnitude_changes,
        sum(CASE WHEN prev_updated IS NOT NULL AND NOT (depth_km <=> prev_depth_km) THEN 1 ELSE 0 END) AS depth_changes,
        sum(CASE WHEN prev_updated IS NOT NULL
                  AND (NOT (latitude <=> prev_latitude) OR NOT (longitude <=> prev_longitude))
                 THEN 1 ELSE 0 END) AS location_changes,
        sum(CASE WHEN prev_updated IS NOT NULL AND NOT (review_status <=> prev_review_status) THEN 1 ELSE 0 END) AS status_changes,
        max(CASE WHEN prev_updated IS NOT NULL AND magnitude IS NOT NULL AND prev_magnitude IS NOT NULL
                 THEN abs(magnitude - prev_magnitude) END) AS max_abs_magnitude_change,
        sum(CASE WHEN prev_updated IS NOT NULL AND source_updated_at = prev_updated THEN 1 ELSE 0 END) AS conflicts
    FROM (
        SELECT
            event_id, event_time, source_updated_at, magnitude, depth_km, latitude, longitude, review_status,
            lag(source_updated_at) OVER w AS prev_updated,
            lag(magnitude) OVER w AS prev_magnitude,
            lag(depth_km) OVER w AS prev_depth_km,
            lag(latitude) OVER w AS prev_latitude,
            lag(longitude) OVER w AS prev_longitude,
            lag(review_status) OVER w AS prev_review_status
        FROM local.silver.earthquake_versions
        WHERE event_type = 'earthquake'
        WINDOW w AS (PARTITION BY event_id ORDER BY source_updated_at, record_hash)
    )
    GROUP BY event_id
)
GROUP BY event_date;

CREATE OR REPLACE TEMP VIEW staged_ingestion_latency AS
SELECT
    CAST(loaded_at AS DATE) AS load_date,
    count(*) AS revision_count,
    avg(latency_minutes) AS avg_latency_minutes,
    percentile_approx(latency_minutes, 0.5) AS p50_latency_minutes,
    percentile_approx(latency_minutes, 0.95) AS p95_latency_minutes,
    max(latency_minutes) AS max_latency_minutes
FROM (
    SELECT
        loaded_at,
        (unix_millis(loaded_at) - unix_millis(source_updated_at)) / 60000.0 AS latency_minutes
    FROM local.silver.earthquake_versions
    WHERE source = 'live'
)
GROUP BY CAST(loaded_at AS DATE);

INSERT OVERWRITE local.gold.daily_seismic_activity SELECT * FROM staged_daily_seismic_activity;

INSERT OVERWRITE local.gold.regional_activity SELECT * FROM staged_regional_activity;

INSERT OVERWRITE local.gold.magnitude_distribution SELECT * FROM staged_magnitude_distribution;

INSERT OVERWRITE local.gold.revision_statistics SELECT * FROM staged_revision_statistics;

INSERT OVERWRITE local.gold.ingestion_latency SELECT * FROM staged_ingestion_latency;
