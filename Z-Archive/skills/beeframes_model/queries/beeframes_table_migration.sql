-- Name: beeframes_table_migration
-- Domain: beeframes_model
-- Description: Compare prediction rows between legacy (supervised_beeframes) and new (unified_bee_frames) tables during migration
-- Created: 2026-02-03
--
-- Usage: Use this query to validate data consistency during the migration from
--        supervised_beeframes to unified_bee_frames. Compares predictions for
--        the same sensor/timestamp to ensure values match.
--
-- Context: Part of the unified_bee_frames migration (2026-02-03). The new table
--          adds group_id, deployment_status, and other fields while removing
--          approved_sensor and pred_smoothed.
--

SELECT
    o.sensor_mac_address,
    o.log_timestamp,
    o.pred_raw as old_pred_raw,
    n.pred_raw as new_pred_raw,
    n.group_id,
    n.deployment_status
FROM data_lake_raw_data.supervised_beeframes o
JOIN data_lake_raw_data.unified_bee_frames n
    ON o.sensor_mac_address = n.sensor_mac_address
    AND o.log_timestamp = n.log_timestamp
WHERE o.log_timestamp >= current_timestamp - interval '1' day
LIMIT 100
