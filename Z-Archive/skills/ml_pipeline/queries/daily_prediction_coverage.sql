-- Name: daily_prediction_coverage
-- Domain: ml_pipeline
-- Description: Check how many production groups got predictions today vs yesterday vs expected.
--              Joins unified_bee_frames with model_deployments and group_to_seasonal_activities
--              to identify production groups and whether they have today's predictions.
-- Created: 2026-02-18
--
-- Usage: Run daily to verify that all production groups received predictions.
--        Part 1 shows a summary: today's count, yesterday's count, expected count per group.
--        Part 2 shows which production groups are missing predictions today.
--        Part 3 shows volume comparison (sensor count per group today vs yesterday).
--
-- Context: Production groups (status=PRODUCTION, season=90, calibrated 2+ days ago)
--          should have predictions in unified_bee_frames each day. Missing groups
--          indicate a lambda chain or deployment issue.
--
-- Related: ../beeframes_monitoring/queries/daily_validation_coverage.sql (downstream validation check)
--
-- Diagram (mermaid):
--
-- ```mermaid
-- graph TD
--     MD[model_deployments<br/>latest per group_id<br/>DEDUP: ROW_NUMBER by group_id<br/>ORDER BY timestamp DESC] -->|status = PRODUCTION<br/>calibrated 2+ days ago| PG[production_groups]
--     GTSA[group_to_seasonal_activities<br/>seasonal_activities_id = 90] -->|INNER JOIN on group_id| PG
--
--     UBF_TODAY[unified_bee_frames<br/>input_date = CURRENT_DATE] -->|LEFT JOIN on group_id| RESULT
--     UBF_YESTERDAY[unified_bee_frames<br/>input_date = CURRENT_DATE - 1] -->|LEFT JOIN on group_id| RESULT
--     PG --> RESULT[OUTPUT: per-group coverage<br/>today_sensors, yesterday_sensors<br/>has_predictions_today]
--
--     RESULT --> MISSING{Missing today?}
--     MISSING -- today_sensors = 0 --> GAP[GAP: investigate]
--     MISSING -- today_sensors > 0 --> OK[Predictions present]
-- ```
--

-- ============================================================
-- Part 1: Per-group prediction coverage summary (today vs yesterday)
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
    SELECT
        ld.group_id,
        ld.calibration_date
    FROM latest_deployment ld
    JOIN data_lake_raw_data.group_to_seasonal_activities gtsa
        ON gtsa.group_id = ld.group_id
    WHERE ld.status = 'PRODUCTION'
      AND ld.rn = 1
      AND gtsa.seasonal_activities_id = 90
      AND ld.calibration_date <= CURRENT_DATE - interval '2' day
),
today_predictions AS (
    SELECT
        group_id,
        COUNT(*) AS sensor_count_today
    FROM data_lake_raw_data.unified_bee_frames
    WHERE input_date = CURRENT_DATE
    GROUP BY group_id
),
yesterday_predictions AS (
    SELECT
        group_id,
        COUNT(*) AS sensor_count_yesterday
    FROM data_lake_raw_data.unified_bee_frames
    WHERE input_date = CURRENT_DATE - interval '1' day
    GROUP BY group_id
)
SELECT
    pg.group_id,
    pg.calibration_date,
    COALESCE(tp.sensor_count_today, 0) AS sensors_today,
    COALESCE(yp.sensor_count_yesterday, 0) AS sensors_yesterday,
    CASE
        WHEN tp.sensor_count_today IS NOT NULL AND tp.sensor_count_today > 0 THEN true
        ELSE false
    END AS has_predictions_today,
    CASE
        WHEN yp.sensor_count_yesterday > 0 AND tp.sensor_count_today IS NULL THEN 'MISSING'
        WHEN yp.sensor_count_yesterday > 0
             AND tp.sensor_count_today < yp.sensor_count_yesterday * 0.8 THEN 'DROP'
        ELSE 'OK'
    END AS status
FROM production_groups pg
LEFT JOIN today_predictions tp ON tp.group_id = pg.group_id
LEFT JOIN yesterday_predictions yp ON yp.group_id = pg.group_id
ORDER BY has_predictions_today ASC, pg.group_id;


-- ============================================================
-- Part 2: Production groups with NO predictions today (the gaps)
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
      AND ld.calibration_date <= CURRENT_DATE - interval '2' day
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


-- ============================================================
-- Part 3: Aggregate totals (quick health check)
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
      AND ld.calibration_date <= CURRENT_DATE - interval '2' day
),
today_with_predictions AS (
    SELECT DISTINCT group_id
    FROM data_lake_raw_data.unified_bee_frames
    WHERE input_date = CURRENT_DATE
      AND group_id IN (SELECT group_id FROM production_groups)
)
SELECT
    (SELECT COUNT(*) FROM production_groups) AS total_production_groups,
    (SELECT COUNT(*) FROM today_with_predictions) AS groups_with_predictions_today,
    (SELECT COUNT(*) FROM production_groups)
        - (SELECT COUNT(*) FROM today_with_predictions) AS groups_missing_predictions;
