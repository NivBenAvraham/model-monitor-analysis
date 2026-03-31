-- Name: cross_table_consistency
-- Domain: data_quality
-- Description: Compare sensor/prediction counts across pipeline tables for a group
--              to identify where data is being lost between pipeline stages.
--              Shows counts at each stage side-by-side for a given date.
-- Created: 2026-03-17
--
-- Usage: Replace {{group_id}} with the group to investigate.
--        Replace {{date}} with the date to check (use CURRENT_DATE for today).
--        A drop in count between stages indicates data loss at that join.
--
-- Context: The preprocess model INNER JOINs unified_bee_frames, which can silently
--          drop sensors. This query reveals exactly where counts diverge.
--
-- Sample output:
-- stage                    | sensor_count | notes
-- sensor_daily_snapshot    | 52           | All sensors mapped to group
-- unified_bee_frames       | 50           | 2 sensors missing predictions
-- hive_updates_metadata    | 48           | 2 sensors missing BEE_FRAMES updates
-- preprocess_output        | 47           | 1 sensor lost in model_name join

-- ============================================================
-- All stages in a single comparison view
-- ============================================================

WITH sds_count AS (
    SELECT COUNT(DISTINCT mac) AS cnt
    FROM data_lake_curated_data.sensor_daily_snapshot
    WHERE group_id = {{group_id}}
      AND date = {{date}}
),
ubf_count AS (
    SELECT COUNT(DISTINCT sensor_mac_address) AS cnt
    FROM data_lake_raw_data.unified_bee_frames
    WHERE group_id = {{group_id}}
      AND input_date = {{date}}
),
hum_count AS (
    SELECT COUNT(DISTINCT hum.sensor_mac_address) AS cnt
    FROM data_lake_curated_data.hive_updates_metadata hum
    JOIN data_lake_curated_data.sensor_daily_snapshot sds
        ON sds.mac = hum.sensor_mac_address
        AND sds.date = DATE(hum.created)
    WHERE sds.group_id = {{group_id}}
      AND hum.model = 'BEE_FRAMES'
      AND DATE(hum.created) = {{date}}
),
preprocess_count AS (
    SELECT COUNT(*) AS cnt
    FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
    WHERE group_id = {{group_id}}
      AND date = {{date}}
)
SELECT 'sensor_daily_snapshot' AS stage, 1 AS stage_order, cnt AS sensor_count FROM sds_count
UNION ALL
SELECT 'unified_bee_frames', 2, cnt FROM ubf_count
UNION ALL
SELECT 'hive_updates_metadata', 3, cnt FROM hum_count
UNION ALL
SELECT 'preprocess_output', 4, cnt FROM preprocess_count
ORDER BY stage_order;


-- ============================================================
-- Sensors in snapshot but MISSING from unified_bee_frames
-- (identifies sensors that should have predictions but don't)
-- ============================================================

SELECT
    sds.mac AS sensor_mac_missing_prediction,
    sds.status AS sensor_status,
    sds.hive_id
FROM data_lake_curated_data.sensor_daily_snapshot sds
LEFT JOIN data_lake_raw_data.unified_bee_frames ubf
    ON ubf.sensor_mac_address = sds.mac
    AND ubf.input_date = {{date}}
    AND ubf.group_id = {{group_id}}
WHERE sds.group_id = {{group_id}}
  AND sds.date = {{date}}
  AND ubf.sensor_mac_address IS NULL
LIMIT 50;
