-- Name: model_deployment_history
-- Domain: ml_pipeline
-- Description: Show full deployment history for a given group: all status changes,
--              calibration dates, and model versions. Includes the current (latest)
--              deployment plus all historical records.
-- Created: 2026-02-18
--
-- Usage: Replace {{group_id}} with the group to investigate.
--        Shows all deployment records ordered by timestamp (newest first).
--        The row with rn=1 is the current active deployment.
--
-- Context: When debugging deployment issues (wrong status, failed calibration,
--          unexpected rollback), this query provides the full timeline of changes
--          for a group to understand what happened and when.
--
-- Related runbook: runbooks/debug_missing_predictions.md (Step 2)
--
-- Diagram (mermaid):
--
-- ```mermaid
-- graph TD
--     MD[model_deployments<br/>WHERE group_id = input<br/>all records] --> RN[ROW_NUMBER<br/>PARTITION BY group_id<br/>ORDER BY timestamp DESC]
--     GTSA[group_to_seasonal_activities] -->|LEFT JOIN on group_id| OUT
--     RN --> OUT[OUTPUT: deployment history<br/>rn=1 is current<br/>status, calibration_date<br/>season info]
-- ```
--

-- ============================================================
-- Full deployment history for a group
-- ============================================================

WITH deployment_history AS (
    SELECT
        md.group_id,
        md.status,
        md.timestamp AS calibration_date,
        ROW_NUMBER() OVER (PARTITION BY md.group_id ORDER BY md.timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments md
    WHERE md.group_id = {{group_id}}
)
SELECT
    dh.group_id,
    dh.status,
    dh.calibration_date,
    dh.rn,
    CASE WHEN dh.rn = 1 THEN true ELSE false END AS is_current,
    gtsa.seasonal_activities_id,
    CASE
        WHEN gtsa.seasonal_activities_id = 90 THEN true
        ELSE false
    END AS is_current_season,
    CASE
        WHEN dh.rn = 1
             AND dh.status = 'PRODUCTION'
             AND gtsa.seasonal_activities_id = 90
             AND dh.calibration_date <= CURRENT_DATE - interval '2' day
        THEN true
        ELSE false
    END AS meets_production_criteria
FROM deployment_history dh
LEFT JOIN data_lake_raw_data.group_to_seasonal_activities gtsa
    ON gtsa.group_id = dh.group_id
ORDER BY dh.calibration_date DESC;


-- ============================================================
-- Current deployment summary (single row, quick check)
-- ============================================================

WITH latest_deployment AS (
    SELECT
        md.group_id,
        md.status,
        md.timestamp AS calibration_date,
        ROW_NUMBER() OVER (PARTITION BY md.group_id ORDER BY md.timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments md
    WHERE md.group_id = {{group_id}}
)
SELECT
    ld.group_id,
    ld.status AS current_status,
    ld.calibration_date,
    DATE_DIFF('day', CAST(ld.calibration_date AS DATE), CURRENT_DATE) AS days_since_calibration,
    gtsa.seasonal_activities_id,
    CASE
        WHEN ld.status = 'PRODUCTION'
             AND gtsa.seasonal_activities_id = 90
             AND ld.calibration_date <= CURRENT_DATE - interval '2' day
        THEN 'ACTIVE_PRODUCTION'
        WHEN ld.status = 'PRODUCTION'
             AND ld.calibration_date > CURRENT_DATE - interval '2' day
        THEN 'PRODUCTION_WARMING_UP'
        WHEN ld.status = 'PRODUCTION'
             AND (gtsa.seasonal_activities_id IS NULL OR gtsa.seasonal_activities_id != 90)
        THEN 'PRODUCTION_OUT_OF_SEASON'
        ELSE ld.status
    END AS effective_status
FROM latest_deployment ld
LEFT JOIN data_lake_raw_data.group_to_seasonal_activities gtsa
    ON gtsa.group_id = ld.group_id
WHERE ld.rn = 1;
