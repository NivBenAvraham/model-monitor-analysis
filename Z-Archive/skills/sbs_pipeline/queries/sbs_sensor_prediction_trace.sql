-- Name: sbs_sensor_prediction_trace
-- Domain: sbs_pipeline
-- Description: Trace a single sensor's data from sensor_daily_snapshot through to
--              unified_bee_frames prediction and hive_updates_metadata, for a given date.
--              Useful for end-to-end debugging of why a sensor has or lacks a prediction.
-- Created: 2026-03-17
--
-- Usage: Replace {{sensor_mac}} with the sensor MAC address to trace.
--        Replace {{date}} with the date to check (use CURRENT_DATE for today).
--
-- Context: When a sensor is expected to have a prediction but doesn't (or has a
--          suspicious value), this query shows every stage of the pipeline for that
--          sensor on the given date.
--
-- Sample output:
-- stage              | sensor_mac   | group_id | date       | value / status
-- sensor_snapshot    | AA:BB:CC:... | 2794     | 2026-03-17 | active
-- unified_bee_frames | AA:BB:CC:... | 2794     | 2026-03-17 | pred_rounded = 7
-- hive_updates_meta  | AA:BB:CC:... | (null)   | 2026-03-17 | BEE_FRAMES entry present

-- ============================================================
-- Stage 1: sensor_daily_snapshot (is the sensor mapped to a group?)
-- ============================================================

SELECT
    'sensor_daily_snapshot' AS stage,
    mac AS sensor_mac,
    group_id,
    date,
    status,
    hive_id,
    yard_id
FROM data_lake_curated_data.sensor_daily_snapshot
WHERE mac = '{{sensor_mac}}'
  AND date = {{date}}
LIMIT 5;


-- ============================================================
-- Stage 2: unified_bee_frames (did the sensor get a prediction?)
-- ============================================================

SELECT
    'unified_bee_frames' AS stage,
    sensor_mac_address,
    group_id,
    input_date,
    pred_raw,
    pred_clipped,
    pred_rounded,
    pred_base,
    model_name,
    deployment_status
FROM data_lake_raw_data.unified_bee_frames
WHERE sensor_mac_address = '{{sensor_mac}}'
  AND input_date = {{date}}
LIMIT 5;


-- ============================================================
-- Stage 3: hive_updates_metadata (did BEE_FRAMES update flow?)
-- ============================================================

SELECT
    'hive_updates_metadata' AS stage,
    sensor_mac_address,
    model,
    router_s3_pkl_file AS model_name,
    created,
    value
FROM data_lake_curated_data.hive_updates_metadata
WHERE sensor_mac_address = '{{sensor_mac}}'
  AND model = 'BEE_FRAMES'
  AND DATE(created) = {{date}}
LIMIT 5;


-- ============================================================
-- Stage 4: model_deployments (what is the group's deployment status?)
-- ============================================================

WITH sensor_group AS (
    SELECT group_id
    FROM data_lake_curated_data.sensor_daily_snapshot
    WHERE mac = '{{sensor_mac}}'
      AND date = {{date}}
    LIMIT 1
)
SELECT
    'model_deployments' AS stage,
    md.group_id,
    md.status,
    md.timestamp AS calibration_date,
    ROW_NUMBER() OVER (PARTITION BY md.group_id ORDER BY md.timestamp DESC) AS rn
FROM data_lake_raw_data.model_deployments md
WHERE md.group_id = (SELECT group_id FROM sensor_group)
ORDER BY md.timestamp DESC
LIMIT 5;
