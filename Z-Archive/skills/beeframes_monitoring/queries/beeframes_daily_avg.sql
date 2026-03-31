-- Name: beeframes_daily_avg
-- Domain: beeframes_monitoring
-- Description: user_visible_avg per in-season group over the last 7 days.
--              Used to identify groups with declining beeframe counts over time.
-- Created: 2026-02-23
--
-- Tables:
--   - data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
--
-- Diagram (mermaid):
--
-- ```mermaid
-- graph TD
--     DR[date_range\nCURRENT_DATE − 7d → CURRENT_DATE − 1d]
--     P[preprocess\ngroup_in_season = true\ndate ∈ date_range\nDEDUP: ROW_NUMBER per group_id + date]
--     PD[preprocess_deduped rn=1]
--     OUT[OUTPUT\ngroup_id, date, user_visible_avg, calibration_average]
--     DR --> P --> PD --> OUT
-- ```

WITH date_range AS (
    SELECT
        CURRENT_DATE - INTERVAL '7' DAY AS start_date,
        CURRENT_DATE - INTERVAL '1' DAY AS end_date
),

preprocess_in_season AS (
    SELECT
        p.group_id,
        p.date AS report_date,
        p.user_visible_avg,
        TRY_CAST(p.calibration_average AS DOUBLE) AS calibration_average,
        ROW_NUMBER() OVER (
            PARTITION BY p.group_id, p.date
            ORDER BY p.groups_in_season_ready_for_review DESC
        ) AS rn
    FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess p
    WHERE p.group_in_season = true
      AND p.date >= (SELECT start_date FROM date_range)
      AND p.date <= (SELECT end_date FROM date_range)
),

preprocess_deduped AS (
    SELECT * FROM preprocess_in_season WHERE rn = 1
)

SELECT
    report_date,
    group_id,
    user_visible_avg,
    calibration_average
FROM preprocess_deduped
ORDER BY group_id, report_date
