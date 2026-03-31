-- Name: preprocess_gate_flag_audit
-- Domain: dbt_gold_layer
-- Description: Audit gate flags across all in-season groups for a given date.
--              Shows how many groups pass/fail each gate flag, helping identify
--              systemic issues vs individual group problems.
-- Created: 2026-03-17
--
-- Usage: Replace {{date}} with the date to audit (use CURRENT_DATE for today).
--        Part 1 shows aggregate counts per flag combination.
--        Part 2 shows groups that are in-season but NOT ready for review, with reasons.
--
-- Context: If many groups suddenly fail the same gate flag, it suggests a systemic
--          issue (e.g., dbt model bug, season_id change, pipeline failure) rather than
--          individual group problems.
--
-- Sample output (Part 1):
-- group_in_season | with_hive_updates | ready_for_review | group_count
-- true            | true              | true             | 42
-- true            | true              | false            | 3
-- true            | false             | false            | 2

-- ============================================================
-- Part 1: Gate flag distribution across all groups
-- ============================================================

WITH preprocess_deduped AS (
    SELECT
        group_id,
        group_in_season,
        groups_in_season_with_hive_updates,
        groups_in_season_ready_for_review,
        is_production_model,
        deployment_status,
        ROW_NUMBER() OVER (
            PARTITION BY group_id
            ORDER BY groups_in_season_ready_for_review DESC
        ) AS rn
    FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
    WHERE date = {{date}}
)
SELECT
    group_in_season,
    groups_in_season_with_hive_updates,
    groups_in_season_ready_for_review,
    is_production_model,
    COUNT(DISTINCT group_id) AS group_count
FROM preprocess_deduped
WHERE rn = 1
GROUP BY
    group_in_season,
    groups_in_season_with_hive_updates,
    groups_in_season_ready_for_review,
    is_production_model
ORDER BY group_count DESC;


-- ============================================================
-- Part 2: In-season groups NOT ready for review (with diagnosis)
-- ============================================================

WITH preprocess_deduped AS (
    SELECT
        group_id,
        group_in_season,
        groups_in_season_with_hive_updates,
        groups_in_season_ready_for_review,
        is_production_model,
        deployment_status,
        model_status,
        ROW_NUMBER() OVER (
            PARTITION BY group_id
            ORDER BY groups_in_season_ready_for_review DESC
        ) AS rn
    FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
    WHERE date = {{date}}
),
latest_deployment AS (
    SELECT
        group_id,
        status,
        timestamp AS calibration_date,
        ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments
)
SELECT
    p.group_id,
    p.groups_in_season_with_hive_updates,
    p.groups_in_season_ready_for_review,
    p.is_production_model,
    p.deployment_status,
    d.calibration_date,
    DATE_DIFF('day', CAST(d.calibration_date AS DATE), CURRENT_DATE) AS days_since_calibration,
    CASE
        WHEN p.groups_in_season_with_hive_updates = false
            THEN 'NO HIVE UPDATES - check sensor health and SBS pipeline'
        WHEN p.is_production_model = false OR p.deployment_status != 'PRODUCTION'
            THEN 'NOT IN PRODUCTION - check model_deployments'
        WHEN d.calibration_date > CURRENT_DATE - INTERVAL '3' DAY
            THEN 'CALIBRATION TOO RECENT - wait ' || CAST(3 - DATE_DIFF('day', CAST(d.calibration_date AS DATE), CURRENT_DATE) AS VARCHAR) || ' more days'
        ELSE 'UNKNOWN - investigate manually'
    END AS diagnosis
FROM preprocess_deduped p
LEFT JOIN latest_deployment d ON d.group_id = p.group_id AND d.rn = 1
WHERE p.rn = 1
  AND p.group_in_season = true
  AND p.groups_in_season_ready_for_review = false
ORDER BY p.group_id
LIMIT 100;
