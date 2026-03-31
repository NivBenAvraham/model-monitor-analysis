-- Name: join_path_validation
-- Domain: data_quality
-- Description: Validate that the CORRECT join path (sensor_daily_snapshot MAC-based)
--              returns expected row counts for a group, compared to the WRONG join path
--              (sensors.hive_id). Use this to prove join path correctness when debugging.
-- Created: 2026-03-17
--
-- Usage: Replace {{group_id}} with the group to validate.
--        The correct path should return more recent data than the incorrect path
--        (especially for groups where sensors have been moved between hives).
--
-- Context: Learned from group 2794 investigation (March 2026). The sensors.hive_id
--          join showed data only through March 9, while the MAC-based join correctly
--          showed data through March 11. This query proves the difference.
--
-- Expected: correct_path rows >= incorrect_path rows, and correct_path latest_date >= incorrect_path latest_date

-- ============================================================
-- CORRECT path: sensor_daily_snapshot (MAC-based)
-- ============================================================

SELECT
    'CORRECT: sensor_daily_snapshot (MAC)' AS join_path,
    COUNT(*) AS total_hive_update_rows,
    COUNT(DISTINCT hum.sensor_mac_address) AS distinct_sensors,
    MAX(DATE(hum.created)) AS latest_date,
    MIN(DATE(hum.created)) AS earliest_date
FROM data_lake_curated_data.hive_updates_metadata hum
JOIN data_lake_curated_data.sensor_daily_snapshot sds
    ON sds.mac = hum.sensor_mac_address
    AND sds.date = DATE(hum.created)
WHERE sds.group_id = {{group_id}}
  AND hum.model = 'BEE_FRAMES'
  AND hum.created >= CURRENT_DATE - INTERVAL '14' DAY;


-- ============================================================
-- WRONG path: sensors.hive_id (goes stale)
-- WARNING: This is shown for comparison ONLY. Do not use this pattern.
-- ============================================================

SELECT
    'WRONG: sensors.hive_id (STALE)' AS join_path,
    COUNT(*) AS total_hive_update_rows,
    COUNT(DISTINCT hu.hive_id) AS distinct_hives,
    MAX(DATE(hu.created)) AS latest_date,
    MIN(DATE(hu.created)) AS earliest_date
FROM data_lake_raw_data.hive_updates hu
JOIN data_lake_raw_data.sensors s
    ON s.hive_id = hu.hive_id
WHERE s.group_id = {{group_id}}
  AND hu.created >= CURRENT_DATE - INTERVAL '14' DAY;
