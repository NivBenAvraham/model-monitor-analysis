-- Name: prediction_drift_check
-- Domain: ml_pipeline
-- Description: Compare today's prediction statistics (mean, stddev, min, max of pred_rounded)
--              against the recent 7-day rolling averages per group to detect drift.
--              Flags groups where today's mean deviates by more than 2 standard deviations
--              from the 7-day average.
-- Created: 2026-02-18
--
-- Usage: Run daily after predictions are written. Shows all production groups with
--        today's stats alongside 7-day baselines. Groups with potential drift are
--        flagged (drift_flag = true) and sorted to the top.
--
-- Context: Prediction drift can indicate model degradation, calibration issues,
--          or data distribution changes. Early detection prevents bad predictions
--          from reaching downstream consumers.
--
-- Diagram (mermaid):
--
-- ```mermaid
-- graph TD
--     subgraph "Production Groups"
--         MD[model_deployments<br/>latest per group_id<br/>status = PRODUCTION] -->|INNER JOIN| PG[production_groups]
--         GTSA[group_to_seasonal_activities<br/>season = 90] -->|INNER JOIN| PG
--     end
--
--     subgraph "Today's Stats"
--         UBF_T[unified_bee_frames<br/>input_date = CURRENT_DATE] --> AGG_T[AGG per group_id<br/>mean, stddev, min, max<br/>sensor_count]
--     end
--
--     subgraph "7-Day Baseline"
--         UBF_7[unified_bee_frames<br/>input_date BETWEEN T-8 AND T-1] --> AGG_7[AGG per group_id<br/>avg_mean, avg_stddev<br/>stddev_of_means]
--     end
--
--     PG --> JOIN((JOIN))
--     AGG_T --> JOIN
--     AGG_7 --> JOIN
--     JOIN --> OUT[OUTPUT: per-group comparison<br/>today vs 7-day baseline]
--     OUT --> DRIFT{drift_flag?<br/>abs diff > 2 * stddev_of_means}
--     DRIFT -- true --> ALERT[Potential drift: investigate]
--     DRIFT -- false --> OK[Within normal range]
-- ```
--

-- ============================================================
-- Prediction drift detection: today vs 7-day baseline
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
today_stats AS (
    SELECT
        group_id,
        COUNT(*) AS sensor_count_today,
        AVG(pred_rounded) AS mean_today,
        STDDEV(pred_rounded) AS stddev_today,
        MIN(pred_rounded) AS min_today,
        MAX(pred_rounded) AS max_today
    FROM data_lake_raw_data.unified_bee_frames
    WHERE input_date = CURRENT_DATE
      AND group_id IN (SELECT group_id FROM production_groups)
    GROUP BY group_id
),
baseline_daily AS (
    -- Per-group, per-day stats for the last 7 days (excluding today)
    SELECT
        group_id,
        input_date,
        AVG(pred_rounded) AS daily_mean,
        STDDEV(pred_rounded) AS daily_stddev,
        COUNT(*) AS daily_sensor_count
    FROM data_lake_raw_data.unified_bee_frames
    WHERE input_date BETWEEN CURRENT_DATE - interval '8' day AND CURRENT_DATE - interval '1' day
      AND group_id IN (SELECT group_id FROM production_groups)
    GROUP BY group_id, input_date
),
baseline_stats AS (
    -- Aggregate the daily stats into a 7-day baseline per group
    SELECT
        group_id,
        COUNT(*) AS days_with_data,
        AVG(daily_mean) AS baseline_mean,
        AVG(daily_stddev) AS baseline_avg_stddev,
        STDDEV(daily_mean) AS baseline_stddev_of_means,
        AVG(daily_sensor_count) AS baseline_avg_sensor_count
    FROM baseline_daily
    GROUP BY group_id
)
SELECT
    pg.group_id,
    -- Today's stats
    ts.sensor_count_today,
    ROUND(ts.mean_today, 2) AS mean_today,
    ROUND(ts.stddev_today, 2) AS stddev_today,
    ts.min_today,
    ts.max_today,
    -- 7-day baseline
    bs.days_with_data AS baseline_days,
    ROUND(bs.baseline_mean, 2) AS baseline_mean,
    ROUND(bs.baseline_avg_stddev, 2) AS baseline_avg_stddev,
    ROUND(bs.baseline_stddev_of_means, 2) AS baseline_stddev_of_means,
    ROUND(bs.baseline_avg_sensor_count, 0) AS baseline_avg_sensors,
    -- Drift indicators
    ROUND(ts.mean_today - bs.baseline_mean, 2) AS mean_diff,
    CASE
        WHEN bs.baseline_stddev_of_means > 0
        THEN ROUND(ABS(ts.mean_today - bs.baseline_mean) / bs.baseline_stddev_of_means, 2)
        ELSE NULL
    END AS z_score,
    -- Drift flag: true if today's mean deviates > 2 stddevs from baseline
    CASE
        WHEN ts.mean_today IS NULL THEN NULL  -- no predictions today
        WHEN bs.baseline_mean IS NULL THEN NULL  -- no baseline available
        WHEN bs.baseline_stddev_of_means = 0 THEN
            CASE WHEN ts.mean_today != bs.baseline_mean THEN true ELSE false END
        WHEN ABS(ts.mean_today - bs.baseline_mean) > 2 * bs.baseline_stddev_of_means THEN true
        ELSE false
    END AS drift_flag,
    -- Sensor count drop flag
    CASE
        WHEN ts.sensor_count_today < bs.baseline_avg_sensor_count * 0.8 THEN true
        ELSE false
    END AS sensor_drop_flag
FROM production_groups pg
LEFT JOIN today_stats ts ON ts.group_id = pg.group_id
LEFT JOIN baseline_stats bs ON bs.group_id = pg.group_id
ORDER BY drift_flag DESC NULLS LAST, pg.group_id;
