# Runbook: Debug Preprocess Missing Group

## When to Use

A group that should appear in `beekeeper_beeframe_model_monitoring_preprocess` is missing for today's date. This means the group will not reach the validation pipeline and will not be reviewed.

## Prerequisites

- Athena query access
- Know the `group_id` of the missing group
- Know the expected date (usually CURRENT_DATE)

## Steps

### 1. Confirm the group is missing from preprocess

```sql
SELECT *
FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
WHERE group_id = {{group_id}}
  AND date = CURRENT_DATE
LIMIT 10;
```

**Expected:** Zero rows for the missing date.

- **If rows exist** -- The group IS in the preprocess table. The issue is likely a gate flag filtering it from downstream views. See Step 3.
- **If zero rows** -- Confirmed missing. Continue to Step 2.

### 2. Check all 6 source tables

Run `queries/preprocess_source_check.sql` with the group_id and date. This checks each source table independently.

**Decision tree based on results:**

| Missing Source | Impact | Action |
|---------------|--------|--------|
| `hive_updates_metadata` | Base table has no BEE_FRAMES entries | Check SBS pipeline. Did predictions get written? Use `sbs_group_eligibility.sql`. |
| `unified_bee_frames` | INNER JOIN will drop all rows | Check SBS pipeline. The Lambda chain may not have produced predictions for this group. |
| `sensor_daily_snapshot` | No sensor-to-group mapping | Check if sensors are assigned to this group in the operations system. |
| `group_to_seasonal_activities` | Group not in any season | Group is correctly excluded if not in season 90. Verify with operations. |
| `model_deployments` | No deployment record | Group has no model deployed. Check with ML team. |
| `daily_beekeeper_metrics` | LEFT JOIN -- won't cause missing rows | This alone won't cause the group to be missing. Look at other sources. |

**If all sources have data but the group is still missing:**
- The dbt model itself may have failed. Check dbt run logs.
- There may be a model name mismatch between `hive_updates_metadata.router_s3_pkl_file` and `unified_bee_frames.model_name` (see dbt_gold_layer SKILL.md, Gotcha #4).

### 3. Check gate flags (if group is present but filtered)

If Step 1 found rows, check the gate flags:

```sql
SELECT
    group_id,
    date,
    group_in_season,
    groups_in_season_with_hive_updates,
    groups_in_season_ready_for_review,
    is_production_model,
    deployment_status
FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
WHERE group_id = {{group_id}}
  AND date = CURRENT_DATE
LIMIT 10;
```

| Flag | Value | Meaning | Action |
|------|-------|---------|--------|
| `group_in_season = false` | Not in season 90 | Check `group_to_seasonal_activities`. May need season_id update. |
| `groups_in_season_with_hive_updates = false` | In season but no BEE_FRAMES hive updates | Check `hive_updates_metadata` for BEE_FRAMES entries. |
| `groups_in_season_ready_for_review = false` | Has data but not ready | Check deployment status and calibration age (3-day threshold). |
| `is_production_model = false` | Model not in PRODUCTION | Check `model_deployments` for latest status. |

### 4. Check for systemic issues

If multiple groups are missing, this is likely a systemic issue rather than a single-group problem.

Run `queries/preprocess_gate_flag_audit.sql` to see the gate flag distribution across all groups.

**Signs of systemic issues:**
- Many groups suddenly have `group_in_season = false` -- Season ID may have changed
- Many groups have `groups_in_season_with_hive_updates = false` -- SBS pipeline may not have run
- Preprocess table is empty for today -- dbt model did not run at all

### 5. Check dbt execution

If source data looks correct but the preprocess table is missing/incomplete:

- Check Airflow for the dbt run task status
- Check dbt run logs for errors
- The dbt model runs after the SBS Lambda chain (~06:20 UTC). If the Lambda was delayed, dbt may have run on stale data.

### 6. Determine resolution

| Root Cause | Action |
|------------|--------|
| Source table missing data | Investigate upstream (SBS pipeline, sensor health). Use `sbs_pipeline` skill. |
| Season ID changed | Update the hardcoded season_id in the dbt model. |
| Calibration too recent (< 3 days) | Wait. Group will appear after 3-day warm-up. |
| Model name mismatch | Investigate version divergence. May need redeployment. |
| dbt model failed | Check dbt logs. May need manual re-run. |
| Multiple groups affected | Systemic issue. Check pipeline orchestration (Airflow/Step Functions). |

## Success Criteria

- Root cause identified for why the group is missing from preprocess
- If source data is the issue, the appropriate upstream runbook is followed
- If the dbt model itself failed, it is re-run and the group appears in preprocess
- If the cause is expected (seasonal, warming up), documented for future reference

## Real-World Example: Group 2794 (March 2026)

- **Symptom:** Group 2794 missing from preprocess table since March 12
- **Investigation:** Used sensor_daily_snapshot (MAC-based) join instead of stale sensors.hive_id join
- **Key finding:** The sensors.hive_id join path showed data only through March 9 (stale mapping), while MAC-based join showed data through March 11
- **Root cause:** Hive update data gap from March 12 onward (upstream pipeline issue, not a join problem)
- **Lesson learned:** Always use `sensor_daily_snapshot.mac` for sensor-to-group mapping, never `sensors.hive_id`

---
*Created: 2026-03-17*
