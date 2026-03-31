-- Name: preprocess_source_check
-- Domain: dbt_gold_layer
-- Description: Verify that all 6 source tables feeding the preprocess model have data
--              for a given group and date. Helps identify which source table is causing
--              a group to be missing from the final preprocess output.
-- Created: 2026-03-17
--
-- Usage: Replace {{group_id}} with the group to investigate.
--        Replace {{date}} with the date to check (use CURRENT_DATE for today).
--
-- Context: The preprocess model joins 6 tables. If any required table is missing data
--          for a group, that group will be missing or incomplete in the output.
--          This query checks each source independently.
--
-- Related runbook: runbooks/debug_preprocess_missing_group.md (Step 2)
--
-- Sample output:
-- source_table             | has_data | row_count | notes
-- hive_updates_metadata    | true     | 47        | BEE_FRAMES entries present
-- unified_bee_frames       | true     | 47        | Predictions present
-- sensor_daily_snapshot    | true     | 52        | 52 active sensors
-- seasonal_activities      | true     | 1         | season_id = 90
-- model_deployments        | true     | 1         | PRODUCTION, calibrated 2026-03-01
-- daily_beekeeper_metrics  | true     | 1         | Avg available

-- ============================================================
-- Source 1: hive_updates_metadata (BEE_FRAMES entries)
-- ============================================================

SELECT
    'hive_updates_metadata' AS source_table,
    CASE WHEN COUNT(*) > 0 THEN true ELSE false END AS has_data,
    COUNT(*) AS row_count,
    COUNT(DISTINCT sensor_mac_address) AS distinct_sensors
FROM data_lake_curated_data.hive_updates_metadata hum
WHERE hum.model = 'BEE_FRAMES'
  AND DATE(hum.created) = {{date}}
  AND hum.sensor_mac_address IN (
      SELECT mac FROM data_lake_curated_data.sensor_daily_snapshot
      WHERE group_id = {{group_id}} AND date = {{date}}
  );


-- ============================================================
-- Source 2: unified_bee_frames (predictions)
-- ============================================================

SELECT
    'unified_bee_frames' AS source_table,
    CASE WHEN COUNT(*) > 0 THEN true ELSE false END AS has_data,
    COUNT(*) AS row_count,
    COUNT(DISTINCT sensor_mac_address) AS distinct_sensors,
    ROUND(AVG(pred_rounded), 2) AS avg_prediction
FROM data_lake_raw_data.unified_bee_frames
WHERE group_id = {{group_id}}
  AND input_date = {{date}};


-- ============================================================
-- Source 3: sensor_daily_snapshot (sensor-to-group mapping)
-- ============================================================

SELECT
    'sensor_daily_snapshot' AS source_table,
    CASE WHEN COUNT(*) > 0 THEN true ELSE false END AS has_data,
    COUNT(*) AS row_count,
    COUNT(DISTINCT mac) AS distinct_sensors
FROM data_lake_curated_data.sensor_daily_snapshot
WHERE group_id = {{group_id}}
  AND date = {{date}};


-- ============================================================
-- Source 4: group_to_seasonal_activities (season membership)
-- ============================================================

SELECT
    'group_to_seasonal_activities' AS source_table,
    CASE WHEN COUNT(*) > 0 THEN true ELSE false END AS has_data,
    COUNT(*) AS row_count,
    MAX(seasonal_activities_id) AS season_id,
    CASE WHEN MAX(seasonal_activities_id) = 90 THEN 'current season' ELSE 'NOT current season' END AS season_status
FROM data_lake_raw_data.group_to_seasonal_activities
WHERE group_id = {{group_id}};


-- ============================================================
-- Source 5: model_deployments (latest deployment)
-- ============================================================

WITH latest AS (
    SELECT
        group_id,
        status,
        timestamp AS calibration_date,
        ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments
    WHERE group_id = {{group_id}}
)
SELECT
    'model_deployments' AS source_table,
    CASE WHEN COUNT(*) > 0 THEN true ELSE false END AS has_data,
    MAX(status) AS deployment_status,
    MAX(calibration_date) AS calibration_date,
    DATE_DIFF('day', CAST(MAX(calibration_date) AS DATE), CURRENT_DATE) AS days_since_calibration,
    CASE
        WHEN MAX(status) = 'PRODUCTION'
             AND MAX(calibration_date) <= CURRENT_DATE - INTERVAL '3' DAY
        THEN 'ready'
        WHEN MAX(status) = 'PRODUCTION'
        THEN 'warming up (< 3 days since calibration)'
        ELSE COALESCE(MAX(status), 'no deployment record')
    END AS readiness
FROM latest
WHERE rn = 1;


-- ============================================================
-- Source 6: daily_beekeeper_metrics (user-visible averages)
-- ============================================================

SELECT
    'daily_beekeeper_metrics' AS source_table,
    CASE WHEN COUNT(*) > 0 THEN true ELSE false END AS has_data,
    COUNT(*) AS row_count
FROM data_lake_curated_data.daily_beekeeper_metrics
WHERE group_id = {{group_id}}
  AND date = {{date}}
LIMIT 1;
