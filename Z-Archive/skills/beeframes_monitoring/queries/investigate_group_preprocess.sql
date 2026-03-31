-- Name: investigate_group_preprocess
-- Domain: beeframes_monitoring
-- Description: Look up a specific group's preprocess data for a given date.
--              Used when investigating why a group is missing from validations.
-- Created: 2026-02-05
--
-- Usage: Replace {{group_id}} with the group to investigate.
--        Replace {{date}} with the date to check (use CURRENT_DATE for today).
--
-- Related runbook: runbooks/debug_missing_validation.md (Steps 2-3)
--

-- Full row inspection
SELECT *
FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
WHERE group_id = {{group_id}}
  AND date = {{date}}
LIMIT 10;


-- Gate flag summary (quick view of why a group may be filtered)
SELECT
    group_id,
    date,
    group_in_season,
    groups_in_season_with_hive_updates,
    groups_in_season_ready_for_review,
    is_production_model,
    model_status,
    deployment_status
FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
WHERE group_id = {{group_id}}
  AND date = {{date}}
LIMIT 10;

-- Sample output (gate flag summary):
-- group_id | date       | group_in_season | groups_in_season_with_hive_updates | groups_in_season_ready_for_review | is_production_model | model_status | deployment_status
-- 1155     | 2026-02-05 | False           | False                              | False                             | True                | active       | PRODUCTION
