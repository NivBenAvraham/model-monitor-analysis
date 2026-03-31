-- Name: sbs_daily_execution_summary
-- Domain: sbs_pipeline
-- Description: Daily summary of SBS pipeline execution: how many production groups
--              received predictions, total sensor count, and comparison with yesterday.
--              Identifies groups that are eligible but missing predictions.
-- Created: 2026-03-17
--
-- Usage: Run daily after 06:30 UTC to verify pipeline completed successfully.
--        Part 1 shows the aggregate summary.
--        Part 2 shows per-group detail with today vs yesterday comparison.
--        Part 3 shows eligible groups that are MISSING predictions today.
--
-- Expected: All production groups should have predictions. Missing groups need investigation.

-- ============================================================
-- Part 1: Aggregate summary
-- ============================================================

WITH latest_deployment AS (
    SELECT
        group_id,
        status,
        timestamp AS calibration_date,
        ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments
),
production_groups AS (
    SELECT DISTINCT ld.group_id
    FROM latest_deployment ld
    JOIN data_lake_raw_data.group_to_seasonal_activities gtsa
        ON gtsa.group_id = ld.group_id
    WHERE ld.status = 'PRODUCTION'
      AND ld.rn = 1
      AND gtsa.seasonal_activities_id = 90
      AND ld.calibration_date <= CURRENT_DATE - INTERVAL '3' DAY
),
today_counts AS (
    SELECT
        group_id,
        COUNT(*) AS sensors
    FROM data_lake_raw_data.unified_bee_frames
    WHERE input_date = CURRENT_DATE
      AND group_id IN (SELECT group_id FROM production_groups)
    GROUP BY group_id
),
yesterday_counts AS (
    SELECT
        group_id,
        COUNT(*) AS sensors
    FROM data_lake_raw_data.unified_bee_frames
    WHERE input_date = CURRENT_DATE - INTERVAL '1' DAY
      AND group_id IN (SELECT group_id FROM production_groups)
    GROUP BY group_id
)
SELECT
    (SELECT COUNT(*) FROM production_groups) AS total_eligible_groups,
    (SELECT COUNT(DISTINCT group_id) FROM today_counts) AS groups_with_predictions_today,
    (SELECT COUNT(DISTINCT group_id) FROM yesterday_counts) AS groups_with_predictions_yesterday,
    (SELECT COALESCE(SUM(sensors), 0) FROM today_counts) AS total_sensors_today,
    (SELECT COALESCE(SUM(sensors), 0) FROM yesterday_counts) AS total_sensors_yesterday,
    (SELECT COUNT(*) FROM production_groups)
        - (SELECT COUNT(DISTINCT group_id) FROM today_counts) AS groups_missing_today;


-- ============================================================
-- Part 2: Per-group detail (today vs yesterday sensor counts)
-- ============================================================

WITH latest_deployment AS (
    SELECT
        group_id,
        status,
        timestamp AS calibration_date,
        ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments
),
production_groups AS (
    SELECT DISTINCT ld.group_id, ld.calibration_date
    FROM latest_deployment ld
    JOIN data_lake_raw_data.group_to_seasonal_activities gtsa
        ON gtsa.group_id = ld.group_id
    WHERE ld.status = 'PRODUCTION'
      AND ld.rn = 1
      AND gtsa.seasonal_activities_id = 90
      AND ld.calibration_date <= CURRENT_DATE - INTERVAL '3' DAY
),
today_counts AS (
    SELECT group_id, COUNT(*) AS sensors_today
    FROM data_lake_raw_data.unified_bee_frames
    WHERE input_date = CURRENT_DATE
    GROUP BY group_id
),
yesterday_counts AS (
    SELECT group_id, COUNT(*) AS sensors_yesterday
    FROM data_lake_raw_data.unified_bee_frames
    WHERE input_date = CURRENT_DATE - INTERVAL '1' DAY
    GROUP BY group_id
)
SELECT
    pg.group_id,
    pg.calibration_date,
    COALESCE(tc.sensors_today, 0) AS sensors_today,
    COALESCE(yc.sensors_yesterday, 0) AS sensors_yesterday,
    CASE
        WHEN tc.sensors_today IS NULL OR tc.sensors_today = 0 THEN 'MISSING'
        WHEN yc.sensors_yesterday > 0
             AND tc.sensors_today < yc.sensors_yesterday * 0.8 THEN 'DROP >20%'
        ELSE 'OK'
    END AS status
FROM production_groups pg
LEFT JOIN today_counts tc ON tc.group_id = pg.group_id
LEFT JOIN yesterday_counts yc ON yc.group_id = pg.group_id
ORDER BY status DESC, pg.group_id
LIMIT 200;


-- ============================================================
-- Part 3: Missing groups only (for quick alerting)
-- ============================================================

WITH latest_deployment AS (
    SELECT
        group_id,
        status,
        timestamp AS calibration_date,
        ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments
),
production_groups AS (
    SELECT DISTINCT ld.group_id
    FROM latest_deployment ld
    JOIN data_lake_raw_data.group_to_seasonal_activities gtsa
        ON gtsa.group_id = ld.group_id
    WHERE ld.status = 'PRODUCTION'
      AND ld.rn = 1
      AND gtsa.seasonal_activities_id = 90
      AND ld.calibration_date <= CURRENT_DATE - INTERVAL '3' DAY
)
SELECT pg.group_id
FROM production_groups pg
LEFT JOIN (
    SELECT DISTINCT group_id
    FROM data_lake_raw_data.unified_bee_frames
    WHERE input_date = CURRENT_DATE
) today ON today.group_id = pg.group_id
WHERE today.group_id IS NULL
ORDER BY pg.group_id;
