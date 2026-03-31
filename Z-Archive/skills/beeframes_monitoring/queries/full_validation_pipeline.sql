-- Name: full_validation_pipeline
-- Domain: beeframes_monitoring
-- Description: Full validation pipeline view per group for a given day.
--              Combines preprocess, validations, and model_metric_test to detect gaps
--              between actual metric results and expected results per the Confluence spec.
-- Created: 2026-02-07
--
-- Usage: Run daily to find gaps. If expected_passed = metric_passed for all rows,
--        the pipeline ran as planned. Rows where is_gap = true need investigation.
--
-- Tables:
--   - data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess (base, T-1)
--   - data_lake_curated_data.beekeeper_beeframe_model_monitoring_validations (LEFT JOIN, T-1)
--   - data_lake_curated_data.model_metric_test (LEFT JOIN, metric_name LIKE '%post%', T)
--   - data_lake_raw_data.model_deployments (LEFT JOIN, latest per group)
--
-- Timing:
--   - Preprocess runs at ~17:00 on day T-1
--   - Manual validations happen ~17:30-00:00 on day T-1
--   - Post-validation metric runs at ~03:00 on day T (evaluates T-1 data)
--   - Therefore: preprocess & validations use CURRENT_DATE - 1, metric uses CURRENT_DATE
--
-- Notes:
--   - Preprocess: filtered to group_in_season = true, deduped via SELECT DISTINCT
--   - Validations: deduped by group_id, latest timestamp on T-1
--   - Metric: filtered to validator = true, deduped by entity_id + metric_name, latest run_time on T
--   - For historical queries, replace CURRENT_DATE with DATE 'YYYY-MM-DD' (metric date)
--     and CURRENT_DATE - interval '1' day with DATE 'YYYY-MM-DD' - interval '1' day
--
-- Diagram (mermaid):
--
-- ```mermaid
-- graph TD
--     subgraph "T-1 ~17:00"
--         P[preprocess<br/>date = T-1<br/>group_in_season = true<br/>DEDUP: ROW_NUMBER by group_id<br/>ORDER BY ready_for_review DESC]
--     end
--     subgraph "T-1 ~17:30-00:00"
--         V[validations<br/>timestamp = T-1<br/>DEDUP: ROW_NUMBER by group_id<br/>ORDER BY timestamp DESC]
--     end
--     subgraph "T ~03:00"
--         M[model_metric_test<br/>DATE run_time = T<br/>metric_name LIKE '%post%'<br/>validator = true<br/>DEDUP: ROW_NUMBER by entity_id, metric_name<br/>ORDER BY run_time DESC]
--     end
--
--     D[model_deployments<br/>latest per group_id<br/>DEDUP: ROW_NUMBER by group_id<br/>ORDER BY timestamp DESC]
--
--     P -- LEFT JOIN ON group_id --> V
--     P -- LEFT JOIN ON group_id --> M
--     P -- LEFT JOIN ON group_id --> D
--
--     P --> O[OUTPUT: 1 row per group]
--     V --> O
--     M --> O
--     D --> O
--
--     O --> G{is_gap?<br/>expected_passed != passed}
--     G -- true --> GAP[GAP: investigate]
--     G -- false --> OK[Pipeline matched spec]
-- ```
--
-- Gatekeeper logic (from Confluence spec — Post Validation Metric page):
--
--   deploy_status  | ready_for_review | tier1    | tier2    | metric | meaning
--   ---------------+-----------------+----------+----------+--------+--------------------------------------
--   !=PRODUCTION   | (any)           | (any)    | (any)    | NULL   | not deployed, not a concern
--   PRODUCTION     | (any)           | (any)    | !=valid  | BLOCK  | tier2 invalid always blocks
--   PRODUCTION     | false           | (any)    | NA/valid | PASS   | not yet eligible, let data flow
--   PRODUCTION     | true            | valid    | valid    | PASS   | both tiers approved
--   PRODUCTION     | true            | valid    | NA       | PASS   | tier1 approved, no tier2 yet
--   PRODUCTION     | true            | !=valid  | NA       | BLOCK  | tier1 rejected, no tier2
--   PRODUCTION     | true            | NA       | NA       | BLOCK  | not reviewed at all
--
--   Gap detection: is_gap = true when metric result is NULL OR disagrees with expected
--

WITH manual_validation AS (
    SELECT
        group_id,
        tier1_status,
        tier2_status,
        ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC) AS rn
    FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_validations
    WHERE timestamp = CURRENT_DATE - interval '1' day
),

manual_validation_deduped AS (
    SELECT * FROM manual_validation WHERE rn = 1
),

post_metric AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY entity_id, metric_name ORDER BY run_time DESC) AS rn
    FROM data_lake_curated_data.model_metric_test
    WHERE metric_name LIKE '%post%'
      AND DATE(run_time) = CURRENT_DATE
),

post_metric_deduped AS (
    SELECT * FROM post_metric WHERE rn = 1
),

deployment AS (
    SELECT
        group_id,
        status AS deployment_status,
        timestamp AS calibration_date,
        ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments
),

deployment_deduped AS (
    SELECT * FROM deployment WHERE rn = 1
),

preprocess AS (
    SELECT
        group_id,
        group_in_season,
        groups_in_season_ready_for_review,
        ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY groups_in_season_ready_for_review DESC) AS rn
    FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
    WHERE date = CURRENT_DATE - interval '1' day
      AND group_in_season = true
),
preprocess_deduped AS (
    SELECT * FROM preprocess WHERE rn = 1
)

SELECT DISTINCT
    p.group_id,
    d.deployment_status,
    d.calibration_date,
    p.group_in_season,
    p.groups_in_season_ready_for_review,
    mv.tier1_status,
    mv.tier2_status,
    pm.metric_name,
    pm.result,
    -- Expected result per Confluence spec gatekeeper logic
    -- Non-PRODUCTION groups are NULL (not a concern)
    CASE
        WHEN d.deployment_status IS NULL OR d.deployment_status != 'PRODUCTION' THEN NULL
        WHEN mv.tier2_status IS NOT NULL AND mv.tier2_status != 'valid' THEN false
        WHEN p.groups_in_season_ready_for_review = false THEN true
        WHEN mv.tier2_status = 'valid' THEN true
        WHEN mv.tier1_status = 'valid' AND mv.tier2_status IS NULL THEN true
        WHEN mv.tier1_status IS NOT NULL AND mv.tier1_status != 'valid' AND mv.tier2_status IS NULL THEN false
        WHEN mv.tier1_status IS NULL AND mv.tier2_status IS NULL THEN false
    END AS expected_passed,
    -- Gap flag: true when actual metric disagrees with expected, or metric missing for PRODUCTION groups
    CASE
        WHEN d.deployment_status IS NULL OR d.deployment_status != 'PRODUCTION' THEN NULL
        WHEN pm.passed IS NULL THEN true
        WHEN pm.passed != CASE
            WHEN mv.tier2_status IS NOT NULL AND mv.tier2_status != 'valid' THEN false
            WHEN p.groups_in_season_ready_for_review = false THEN true
            WHEN mv.tier2_status = 'valid' THEN true
            WHEN mv.tier1_status = 'valid' AND mv.tier2_status IS NULL THEN true
            WHEN mv.tier1_status IS NOT NULL AND mv.tier1_status != 'valid' AND mv.tier2_status IS NULL THEN false
            WHEN mv.tier1_status IS NULL AND mv.tier2_status IS NULL THEN false
        END THEN true
        ELSE false
    END AS is_gap
FROM preprocess_deduped AS p
LEFT JOIN manual_validation_deduped AS mv ON p.group_id = mv.group_id
LEFT JOIN post_metric_deduped AS pm ON CAST(p.group_id AS VARCHAR) = pm.entity_id
LEFT JOIN deployment_deduped AS d ON p.group_id = d.group_id
ORDER BY is_gap DESC, p.group_id
