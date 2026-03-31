-- Name: sbs_group_eligibility
-- Domain: sbs_pipeline
-- Description: Check whether a specific group meets all criteria for SBS predictions.
--              Returns a single row with boolean flags for each eligibility criterion.
-- Created: 2026-03-17
--
-- Usage: Replace {{group_id}} with the group to investigate.
--        All flags should be True for the group to receive predictions.
--
-- Related runbook: runbooks/debug_missing_sbs_predictions.md (Step 1)
--
-- Sample output:
-- group_id | in_season | has_production_deployment | calibration_mature | has_active_sensors | has_predictions_today | eligible
-- 2794     | true      | true                     | true               | true               | true                  | true

WITH latest_deployment AS (
    SELECT
        group_id,
        status,
        timestamp AS calibration_date,
        ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments
    WHERE group_id = {{group_id}}
),
deployment_info AS (
    SELECT * FROM latest_deployment WHERE rn = 1
),
season_info AS (
    SELECT
        group_id,
        seasonal_activities_id
    FROM data_lake_raw_data.group_to_seasonal_activities
    WHERE group_id = {{group_id}}
),
sensor_info AS (
    SELECT
        group_id,
        COUNT(DISTINCT mac) AS active_sensors
    FROM data_lake_curated_data.sensor_daily_snapshot
    WHERE group_id = {{group_id}}
      AND date = CURRENT_DATE
    GROUP BY group_id
),
prediction_info AS (
    SELECT
        group_id,
        COUNT(*) AS prediction_count
    FROM data_lake_raw_data.unified_bee_frames
    WHERE group_id = {{group_id}}
      AND input_date = CURRENT_DATE
    GROUP BY group_id
)
SELECT
    {{group_id}} AS group_id,
    -- Criterion 1: In current season
    CASE WHEN si.seasonal_activities_id = 90 THEN true ELSE false END AS in_season,
    -- Criterion 2: Latest deployment is PRODUCTION
    CASE WHEN di.status = 'PRODUCTION' THEN true ELSE false END AS has_production_deployment,
    di.status AS deployment_status,
    di.calibration_date,
    -- Criterion 3: Calibration > 3 days old
    CASE
        WHEN di.calibration_date <= CURRENT_DATE - INTERVAL '3' DAY THEN true
        ELSE false
    END AS calibration_mature,
    DATE_DIFF('day', CAST(di.calibration_date AS DATE), CURRENT_DATE) AS days_since_calibration,
    -- Criterion 4: Active sensors
    CASE WHEN COALESCE(sn.active_sensors, 0) > 0 THEN true ELSE false END AS has_active_sensors,
    COALESCE(sn.active_sensors, 0) AS active_sensor_count,
    -- Criterion 5: Predictions today
    CASE WHEN COALESCE(pi.prediction_count, 0) > 0 THEN true ELSE false END AS has_predictions_today,
    COALESCE(pi.prediction_count, 0) AS prediction_count,
    -- Overall eligibility
    CASE
        WHEN si.seasonal_activities_id = 90
             AND di.status = 'PRODUCTION'
             AND di.calibration_date <= CURRENT_DATE - INTERVAL '3' DAY
             AND COALESCE(sn.active_sensors, 0) > 0
        THEN true
        ELSE false
    END AS eligible
FROM (SELECT 1 AS dummy) base
LEFT JOIN season_info si ON true
LEFT JOIN deployment_info di ON true
LEFT JOIN sensor_info sn ON true
LEFT JOIN prediction_info pi ON true;
