-- Name: get_validation_results
-- Domain: beeframes_monitoring
-- Description: Get all validation results for today with full details.
-- Created: 2026-02-07
--
-- Usage: Run to see today's validation results including scores, reviewer info, and statuses.
--        Filters by timestamp (cast to date) = CURRENT_DATE.
--

SELECT
    filtered_sensors_count,
    group_id,
    model_name,
    num_hive_updates,
    num_sensors,
    review_date,
    reviewer,
    reviewer_notes,
    sampled_sensors_count,
    season_id,
    tier1_status,
    tier2_status,
    timestamp,
    total_sensors_count,
    with_savgol_score,
    without_savgol_score
FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_validations
WHERE CAST(timestamp AS DATE) = CURRENT_DATE
ORDER BY group_id
