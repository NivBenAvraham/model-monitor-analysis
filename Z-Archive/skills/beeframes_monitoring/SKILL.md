# Beeframes Monitoring

Daily monitoring pipeline for beeframe model predictions: preprocess snapshots, manual/automatic validations, gap detection, and downstream hive updates.

For the model itself (predictions, deployment lifecycle, table schemas), see [beeframes_model](../beeframes_model/SKILL.md).

## Pipeline Flow

`lambda chain (ML model)` -> `preprocess` (daily ~17:00) -> `manual validations` (~17:30-00:00) -> `auto validations` (~03:00 T+1) -> `hive_updates`

## Quick Reference

| Item | Value |
|------|-------|
| Preprocess Table | `data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess` |
| Validations Table | `data_lake_curated_data.beekeeper_beeframe_model_monitoring_validations` |
| Metric Table | `data_lake_curated_data.model_metric_test` |
| Hive Updates (raw) | `data_lake_raw_data.hive_updates` (needs sensors join for group_id) |
| Hive Updates (curated) | `data_lake_curated_data.hive_updates` (has group_id) |
| Sensors Mapping | `data_lake_raw_data.sensors` (hive_id -> group_id) |
| Daily Automation | `scripts/daily_validation_pipeline.py` (cron 6 AM) |

## Gate Flags (Preprocess Table)

These flags determine whether a group reaches human reviewers. If any required flag is False, the group is filtered out before validation.

| Flag | Meaning | If False |
|------|---------|----------|
| `group_in_season` | Group is in the current active season | Filtered out (seasonal gap, usually expected) |
| `groups_in_season_with_hive_updates` | Group has recent hive update data | Filtered out (check sensor health) |
| `groups_in_season_ready_for_review` | Group meets all readiness criteria | Filtered out (may be timing or data issue) |
| `is_production_model` | Model is in production status | Filtered out (unexpected if in deployments) |

## Validation Statuses

| `tier1_status` | Meaning |
|-----------------|---------|
| `valid` | Model output passed tier 1 review |
| `invalid` | Model output failed tier 1 review |
| `needs_calibration` | Model needs recalibration |

## Reviewers

Human reviewers who fill the validations table: `camila.rosero`, `kim.k`

## Gatekeeper Logic (Post-Validation Metric)

| deploy_status | ready_for_review | tier1 | tier2 | metric | meaning |
|---------------|-----------------|-------|-------|--------|---------|
| !=PRODUCTION | (any) | (any) | (any) | NULL | not deployed, not a concern |
| PRODUCTION | (any) | (any) | !=valid | BLOCK | tier2 invalid always blocks |
| PRODUCTION | false | (any) | NA/valid | PASS | not yet eligible, let data flow |
| PRODUCTION | true | valid | valid | PASS | both tiers approved |
| PRODUCTION | true | valid | NA | PASS | tier1 approved, no tier2 yet |
| PRODUCTION | true | !=valid | NA | BLOCK | tier1 rejected, no tier2 |
| PRODUCTION | true | NA | NA | BLOCK | not reviewed at all |

Gap detection: `is_gap = true` when metric result is NULL or disagrees with expected.

## Table Schemas

### Preprocess Table (29 columns)

**Partition column:** `date` (date type -- use `date = CURRENT_DATE`, not string comparison)

Key columns: `group_id`, `date`, `group_in_season`, `groups_in_season_with_hive_updates`, `groups_in_season_ready_for_review`, `is_production_model`, `model_status`, `deployment_status`, `pred_raw`, `pred_clipped`, `pred_rounded`, `pred_base`, `model_name`, `calibration_average`

### Validations Table

Columns: `filtered_sensors_count`, `group_id`, `model_name`, `num_hive_updates`, `num_sensors`, `review_date`, `reviewer`, `reviewer_notes`, `sampled_sensors_count`, `season_id`, `tier1_status`, `tier2_status`, `timestamp`, `total_sensors_count`, `with_savgol_score`, `without_savgol_score`

## Common Failure Modes

| Symptom | Likely Cause | Resolution |
|---------|--------------|------------|
| Group missing from validations | Gate flags False (out-of-season, no hive updates) | Check preprocess gate flags -- see [runbook](runbooks/debug_missing_validation.md) |
| Group in validations but not in coverage query | Not in current season (sa_id != 90) or not latest deployment | Check `group_to_seasonal_activities` and `model_deployments` |
| Preprocess has no rows for a group | Pipeline ingestion issue | Check Airflow/orchestration logs |
| All flags True but group not validated | Reviewer workload or process gap | Contact reviewers (camila.rosero, kim.k) |

## Knowledge Index

### Queries
- [daily_validation_coverage.sql](queries/daily_validation_coverage.sql) - **Daily recurring** -- Check production groups validated today vs expected; find monitoring gaps
- [investigate_group_preprocess.sql](queries/investigate_group_preprocess.sql) - Look up a specific group's preprocess data and gate flags for debugging
- [get_validation_results.sql](queries/get_validation_results.sql) - Get all validation results for today with full details (scores, reviewer, statuses)
- [post_validation_hive_updates.sql](queries/post_validation_hive_updates.sql) - Check which validated groups received hive updates downstream + post_manual_validation metric and validator flag from model_metric_test
- [full_validation_pipeline.sql](queries/full_validation_pipeline.sql) - **Full pipeline view** -- Joins preprocess + validations + model_metric_test; shows readiness, validation status, and post-validation gatekeeper metric
- [beeframes_daily_avg.sql](queries/beeframes_daily_avg.sql) — Daily average beeframes prediction for in-season groups with hive updates (default: last 7 days). Two result sets: daily aggregate + per-group detail.

### Runbooks
- [debug_missing_validation.md](runbooks/debug_missing_validation.md) - Step-by-step guide for investigating groups missing from daily validations

## Related Skills
- [beeframes_model](../beeframes_model/SKILL.md) — The ML model itself: predictions, deployment lifecycle, calibration pipeline, and table schemas

---
*Last updated: 2026-03-17*
