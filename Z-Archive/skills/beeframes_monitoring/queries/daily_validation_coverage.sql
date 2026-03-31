-- Name: daily_validation_coverage
-- Domain: beeframes_monitoring
-- Description: Check which production groups have been validated today vs which should have been.
--              Identifies monitoring gaps for groups with models in production calibrated 2+ days ago.
-- Created: 2026-02-05
--
-- Usage: Run daily to verify that all production groups with recent model deployments
--        (seasonal_activities_id=90) appear in the monitoring validations table.
--        Part 1 shows validated groups with all columns. Part 2 shows the gaps.
--
-- Context: Groups with a production model deployment calibrated at least 2 days ago
--          should appear in the monitoring validations table each day. Missing groups
--          indicate a monitoring pipeline gap that needs investigation.
--

-- ============================================================
-- Part 1: Production groups that WERE validated today (all columns)
-- ============================================================

WITH latest_md AS (
    SELECT
        md.*,
        ROW_NUMBER() OVER (PARTITION BY md.group_id ORDER BY md.timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments md
),
production_groups AS (
    SELECT DISTINCT lm.group_id
    FROM latest_md lm
    JOIN data_lake_raw_data.group_to_seasonal_activities gtsa
        ON gtsa.group_id = lm.group_id
    WHERE gtsa.seasonal_activities_id = 90
      AND lm.status = 'PRODUCTION'
      AND lm.rn = 1
      AND lm.timestamp <= CURRENT_DATE - interval '2' day
)
SELECT v.*
FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_validations v
JOIN production_groups pg ON pg.group_id = v.group_id
WHERE CAST(v.timestamp AS DATE) = CURRENT_DATE;


-- ============================================================
-- Part 2: Production groups NOT validated today (the gaps)
-- ============================================================

WITH latest_md AS (
    SELECT
        md.*,
        ROW_NUMBER() OVER (PARTITION BY md.group_id ORDER BY md.timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments md
),
production_groups AS (
    SELECT DISTINCT lm.group_id
    FROM latest_md lm
    JOIN data_lake_raw_data.group_to_seasonal_activities gtsa
        ON gtsa.group_id = lm.group_id
    WHERE gtsa.seasonal_activities_id = 90
      AND lm.status = 'PRODUCTION'
      AND lm.rn = 1
      AND lm.timestamp <= CURRENT_DATE - interval '2' day
)
SELECT pg.group_id
FROM production_groups pg
LEFT JOIN (
    SELECT DISTINCT v.group_id
    FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_validations v
    WHERE CAST(v.timestamp AS DATE) = CURRENT_DATE
) validated ON validated.group_id = pg.group_id
WHERE validated.group_id IS NULL
ORDER BY pg.group_id;
