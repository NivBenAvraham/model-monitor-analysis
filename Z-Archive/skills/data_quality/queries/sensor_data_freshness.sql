-- Name: sensor_data_freshness
-- Domain: data_quality
-- Description: Check data freshness across all key pipeline tables for a given group.
--              Shows the most recent data timestamp in each table to identify where
--              data flow stopped.
-- Created: 2026-03-17
--
-- Usage: Replace {{group_id}} with the group to investigate.
--        Each section shows the latest data available for the group in that table.
--        A gap between tables indicates where data flow stopped.
--
-- Context: When investigating "why has group X not been updated", this query shows
--          exactly which stage of the pipeline has the most recent data, revealing
--          where the break is.
--
-- Sample output:
-- table_name                | latest_date | row_count_last_3d | notes
-- sensor_daily_snapshot     | 2026-03-17  | 156               | 52 sensors x 3 days
-- unified_bee_frames        | 2026-03-17  | 156               | predictions present
-- hive_updates_metadata     | 2026-03-17  | 156               | BEE_FRAMES entries
-- preprocess                | 2026-03-17  | 52                | today's preprocess
-- daily_latest_hive_update  | 2026-03-17  | 3                 | recency check

-- ============================================================
-- Table 1: sensor_daily_snapshot (sensor-to-group mapping)
-- ============================================================

SELECT
    'sensor_daily_snapshot' AS table_name,
    MAX(date) AS latest_date,
    COUNT(*) AS rows_last_3d,
    COUNT(DISTINCT mac) AS distinct_sensors
FROM data_lake_curated_data.sensor_daily_snapshot
WHERE group_id = {{group_id}}
  AND date >= CURRENT_DATE - INTERVAL '3' DAY;


-- ============================================================
-- Table 2: unified_bee_frames (predictions)
-- ============================================================

SELECT
    'unified_bee_frames' AS table_name,
    MAX(input_date) AS latest_date,
    COUNT(*) AS rows_last_3d,
    COUNT(DISTINCT sensor_mac_address) AS distinct_sensors
FROM data_lake_raw_data.unified_bee_frames
WHERE group_id = {{group_id}}
  AND input_date >= CURRENT_DATE - INTERVAL '3' DAY;


-- ============================================================
-- Table 3: hive_updates_metadata (BEE_FRAMES entries via MAC join)
-- ============================================================

SELECT
    'hive_updates_metadata' AS table_name,
    MAX(DATE(hum.created)) AS latest_date,
    COUNT(*) AS rows_last_3d,
    COUNT(DISTINCT hum.sensor_mac_address) AS distinct_sensors
FROM data_lake_curated_data.hive_updates_metadata hum
JOIN data_lake_curated_data.sensor_daily_snapshot sds
    ON sds.mac = hum.sensor_mac_address
    AND sds.date = DATE(hum.created)
WHERE sds.group_id = {{group_id}}
  AND hum.model = 'BEE_FRAMES'
  AND hum.created >= CURRENT_DATE - INTERVAL '3' DAY;


-- ============================================================
-- Table 4: preprocess (monitoring preprocess)
-- ============================================================

SELECT
    'preprocess' AS table_name,
    MAX(date) AS latest_date,
    COUNT(*) AS rows_last_3d,
    COUNT(DISTINCT date) AS days_present
FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
WHERE group_id = {{group_id}}
  AND date >= CURRENT_DATE - INTERVAL '3' DAY;


-- ============================================================
-- Table 5: daily_latest_hive_update (pre-aggregated recency)
-- ============================================================

SELECT
    'daily_latest_hive_update' AS table_name,
    MAX(date) AS latest_date,
    COUNT(*) AS rows_last_7d
FROM data_lake_curated_data.daily_latest_hive_update
WHERE group_id = {{group_id}}
  AND date >= CURRENT_DATE - INTERVAL '7' DAY;


-- ============================================================
-- Table 6: model_deployments (deployment status)
-- ============================================================

WITH latest AS (
    SELECT
        status,
        timestamp AS calibration_date,
        ROW_NUMBER() OVER (ORDER BY timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments
    WHERE group_id = {{group_id}}
)
SELECT
    'model_deployments' AS table_name,
    MAX(calibration_date) AS latest_calibration,
    MAX(status) AS current_status,
    DATE_DIFF('day', CAST(MAX(calibration_date) AS DATE), CURRENT_DATE) AS days_since_calibration
FROM latest
WHERE rn = 1;
