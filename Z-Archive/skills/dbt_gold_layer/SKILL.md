# dbt Gold Layer (Preprocess Model)

The dbt preprocess model that produces `beekeeper_beeframe_model_monitoring_preprocess` -- the central table used by the monitoring dashboard, validation pipeline, and daily operations. This skill covers the 7-step join logic, source tables, gate flags, and known gotchas.

For the upstream prediction pipeline, see [sbs_pipeline](../sbs_pipeline/SKILL.md).
For downstream monitoring validations, see [beeframes_monitoring](../beeframes_monitoring/SKILL.md).

## Quick Reference

| Item | Value |
|------|-------|
| Output Table | `data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess` |
| Partition Column | `date` (date type -- use `date = CURRENT_DATE`, not string comparison) |
| Runs After | SBS Lambda chain (~06:20 UTC daily) |
| dbt Project | `beehero-algorythms` repo, dbt models directory |
| Row Grain | One row per sensor per group per date |

## Common Questions This Skill Answers

- Why is a group missing from the preprocess table today?
- What are the gate flags and how do they filter groups for validation?
- Which source tables feed into the preprocess model?
- Why does a group show `groups_in_season_ready_for_review = False`?
- What is the 3-day calibration threshold and why does it exist?
- Why is season_id hardcoded to 90?

## The 7-Step Join

The preprocess model joins 6 source tables in a specific order to produce the final output. Understanding this join is critical for debugging missing or incorrect data.

```
Step 1: hive_updates_metadata (base)
    BF model predictions per sensor (model = 'BEE_FRAMES')
    Join key: sensor_mac_address, date
    |
Step 2: unified_bee_frames (INNER JOIN)
    Raw + clipped + rounded predictions, model metadata
    Join key: sensor_mac_address + model_name match
    NOTE: This is an INNER JOIN -- sensors without predictions are dropped
    |
Step 3: sensor_daily_snapshot (LEFT JOIN)
    Sensor location, status, group assignment (MAC-based)
    Join key: sensor_mac_address (MAC address), date
    IMPORTANT: This is the authoritative sensor-to-group mapping.
    Do NOT use sensors.hive_id -- it goes stale.
    |
Step 4: group_to_seasonal_activities (LEFT JOIN)
    Season membership per group
    Join key: group_id
    Filter: seasonal_activities_id = 90 (hardcoded)
    |
Step 5: model_deployments (LEFT JOIN)
    Calibration date, deployment status per group
    Join key: group_id
    Pattern: Latest deployment via ROW_NUMBER
    |
Step 6: daily_beekeeper_metrics (LEFT JOIN)
    User-visible averages (the values beekeepers see in the app)
    Join key: group_id, date
    |
Step 7: Compute gate flags
    Derived boolean columns that determine validation eligibility
```

## Source Tables

| Table | Schema | Join Key | Role |
|-------|--------|----------|------|
| `hive_updates_metadata` | `data_lake_curated_data` | `sensor_mac_address` | Base table -- BF predictions per sensor |
| `unified_bee_frames` | `data_lake_raw_data` | `sensor_mac_address`, model_name | Raw/clipped/rounded predictions |
| `sensor_daily_snapshot` | `data_lake_curated_data` | `mac` (MAC address), `date` | Sensor-to-group mapping (authoritative) |
| `group_to_seasonal_activities` | `data_lake_raw_data` | `group_id` | Season membership |
| `model_deployments` | `data_lake_raw_data` | `group_id` | Calibration date and deploy status |
| `daily_beekeeper_metrics` | `data_lake_curated_data` | `group_id`, `date` | User-visible averages |

## Gate Flags

These computed boolean columns determine whether a group reaches human reviewers in the validation pipeline. They are evaluated in order -- if an earlier flag is False, later flags are also False.

| Flag | Logic | When False |
|------|-------|------------|
| `group_in_season` | `group_to_seasonal_activities.seasonal_activities_id = 90` | Group is not in the current pollination season. Expected for off-season groups. |
| `groups_in_season_with_hive_updates` | `group_in_season = True` AND hive_updates_metadata has rows for this group today | Group is in-season but no BF predictions were generated. Check sensor health and SBS pipeline. |
| `groups_in_season_ready_for_review` | `groups_in_season_with_hive_updates = True` AND latest deployment `status = 'PRODUCTION'` AND calibration `timestamp <= CURRENT_DATE - interval '3' day` | Group has predictions but either: (a) not in PRODUCTION status, (b) calibrated too recently (< 3 days). |

## Decision Logic: Why Is a Group Not Ready for Review?

```
groups_in_season_ready_for_review = False
    |
    +-- Is group_in_season = True?
    |   NO -> Not in season 90. Check group_to_seasonal_activities.
    |   YES -> Continue
    |
    +-- Is groups_in_season_with_hive_updates = True?
    |   NO -> No hive updates today. Check:
    |         - Did SBS pipeline run? (unified_bee_frames for today)
    |         - Does group have active sensors? (sensor_daily_snapshot)
    |         - Did hive_updates_metadata receive BEE_FRAMES entries?
    |   YES -> Continue
    |
    +-- Is latest deployment status = 'PRODUCTION'?
    |   NO -> Model not deployed to production. Check model_deployments.
    |   YES -> Continue
    |
    +-- Was calibration > 3 days ago?
        NO -> Calibration too recent. Wait 3 days after calibration.
        YES -> Should be ready. Check for data quality issues in source tables.
```

## Known Gotchas

### 1. Hardcoded season_id (90)

The `group_to_seasonal_activities` filter uses `seasonal_activities_id = 90`, which is the current pollination season. This value changes each season and must be updated manually in the dbt model. If you see groups suddenly disappearing from the preprocess table at a season boundary, check whether the season_id needs updating.

**Where to verify current season:**
```sql
SELECT DISTINCT seasonal_activities_id
FROM data_lake_raw_data.group_to_seasonal_activities
ORDER BY seasonal_activities_id DESC
LIMIT 5;
```

### 2. 3-Day Calibration Threshold

Groups are not marked `ready_for_review` until 3 days after their latest calibration. This exists because:
- Newly calibrated models need a burn-in period to stabilize
- Prevents reviewers from evaluating predictions made with incomplete calibration data
- The threshold is hardcoded in the dbt model (not configurable)

If a group was recalibrated yesterday and seems "missing" from reviews, this is expected behavior.

### 3. INNER JOIN on unified_bee_frames

Step 2 uses an INNER JOIN between `hive_updates_metadata` and `unified_bee_frames`. This means any sensor that has hive updates but NO prediction in `unified_bee_frames` will be silently dropped. This is the most common cause of "fewer sensors than expected" in the preprocess output.

### 4. Model Name Matching

The join between `hive_updates_metadata` and `unified_bee_frames` matches on model name. The `hive_updates_metadata` table stores it as `router_s3_pkl_file`, while `unified_bee_frames` derives it as `substring(lambda_name, 17)`. A mismatch between these fields causes sensors to be dropped from the join.

### 5. sensor_daily_snapshot Uses MAC, Not hive_id

The preprocess model correctly uses `sensor_daily_snapshot.mac` for sensor-to-group mapping. Do NOT attempt to replicate this join using `sensors.hive_id` -- hive-to-sensor mappings go stale when sensors are moved between hives (learned from group 2794 investigation, March 2026).

### 6. Duplicate Rows per Group

The preprocess table can have multiple rows per group per date (one per sensor). When querying at the group level, deduplicate using:
```sql
ROW_NUMBER() OVER (PARTITION BY group_id, date ORDER BY groups_in_season_ready_for_review DESC) = 1
```

## Common Failure Modes

| Symptom | Likely Cause | Resolution |
|---------|--------------|------------|
| Group missing from preprocess entirely | Not in season 90, or no sensors in sensor_daily_snapshot | Run `queries/preprocess_source_check.sql` |
| Group present but `group_in_season = False` | Season ID mismatch or group not in `group_to_seasonal_activities` | Verify season_id is current |
| Group present but `groups_in_season_with_hive_updates = False` | No BEE_FRAMES entries in hive_updates_metadata | Check SBS pipeline ran; check sensor health |
| Group present but `ready_for_review = False` | Calibration < 3 days old OR deployment not PRODUCTION | Check `model_deployments` latest record |
| Fewer sensors than expected for a group | INNER JOIN dropped sensors with no unified_bee_frames match | Check for model_name mismatch (gotcha #4) |
| Preprocess table empty for today | dbt model did not run | Check dbt run logs, Airflow scheduler |

## Queries

- `queries/preprocess_source_check.sql` -- Verify all 6 source tables have data for a given group and date
- `queries/preprocess_gate_flag_audit.sql` -- Audit gate flags across all in-season groups to find anomalies

## Runbooks

- `runbooks/debug_preprocess_missing_group.md` -- Step-by-step guide when a group is missing from preprocess

## Related Skills

- [sbs_pipeline](../sbs_pipeline/SKILL.md) -- Upstream: the Lambda chain that produces unified_bee_frames
- [beeframes_monitoring](../beeframes_monitoring/SKILL.md) -- Downstream: validation pipeline that consumes preprocess
- [beeframes_model](../beeframes_model/SKILL.md) -- Model schemas, prediction fields, calibration function
- [data_quality](../data_quality/SKILL.md) -- Join patterns and data quality patterns

---
*Last updated: 2026-03-17*
