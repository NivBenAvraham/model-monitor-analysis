-- Name: post_validation_hive_updates
-- Domain: beeframes_monitoring
-- Description: For groups validated on a given date, check whether they received hive updates
--              downstream by joining validations with hive_updates via sensors,
--              plus post-validation metric results from model_metric_test.
-- Created: 2026-02-06
-- Updated: 2026-02-11
--
-- Usage: Run after validations to verify which groups actually got hive updates pushed.
--        Useful for confirming the full pipeline completed:
--        lambda chain → preprocess → manual validations → auto validations → hive_updates
--
-- Parameters:
--   Uses dynamic dates: validations from yesterday, hive updates with T-1 lag
--   (hive_updates stamped T-1 represent today's run)
--
-- Tables:
--   - data_lake_curated_data.beekeeper_beeframe_model_monitoring_validations (validation status)
--   - data_lake_raw_data.hive_updates (actual hive updates pushed)
--   - data_lake_raw_data.sensors (hive_id → group_id mapping)
--   - data_lake_curated_data.model_metric_test (post-validation metric, validator flag)
--
-- Diagram (mermaid):
--   graph LR
--     V[validations<br/>yesterday] -->|group_id| JOIN((LEFT JOIN))
--     HU[hive_updates] -->|hive_id| S[sensors]
--     S -->|group_id| JOIN
--     MMT[model_metric_test<br/>post_manual_validation<br/>today] -->|entity_id = group_id| JOIN
--     JOIN --> OUT[group_id<br/>tier1/tier2 status<br/>has_hive_updates_today<br/>post_validation_metric<br/>validator]
--

WITH recent_validations AS (
    SELECT DISTINCT group_id, tier1_status, tier2_status
    FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_validations
    WHERE CAST(timestamp AS DATE) = CURRENT_DATE - INTERVAL '1' DAY
),
latest_hive_updates AS (
    SELECT
        s.group_id,
        MAX(hu.created) AS max_created_hive_update
    FROM data_lake_raw_data.hive_updates hu
    JOIN data_lake_raw_data.sensors s ON s.hive_id = hu.hive_id
    WHERE s.group_id IN (SELECT group_id FROM recent_validations)
    GROUP BY s.group_id
),
post_metric AS (
    SELECT
        entity_id,
        result AS post_validation_metric,
        passed,
        validator,
        ROW_NUMBER() OVER (PARTITION BY entity_id ORDER BY validator DESC, run_time DESC) AS rn
    FROM data_lake_curated_data.model_metric_test
    WHERE metric_name = 'post_manual_validation'
      AND DATE(run_time) = CURRENT_DATE
),
post_metric_deduped AS (
    SELECT * FROM post_metric WHERE rn = 1
)
SELECT
    v.group_id,
    v.tier1_status,
    v.tier2_status,
    pm.post_validation_metric,
    pm.validator,
    CASE WHEN CAST(lhu.max_created_hive_update AS DATE) = CURRENT_DATE - INTERVAL '1' DAY THEN true ELSE false END AS has_hive_updates_today,
    lhu.max_created_hive_update
FROM recent_validations v
LEFT JOIN latest_hive_updates lhu ON lhu.group_id = v.group_id
LEFT JOIN post_metric_deduped pm ON CAST(v.group_id AS VARCHAR) = pm.entity_id
ORDER BY v.group_id;
