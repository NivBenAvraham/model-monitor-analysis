# SBS Prediction Pipeline

The automated side-by-side (SBS) prediction pipeline that runs daily to produce bee frame predictions for all production groups. Covers the Lambda chain execution, data flow through tables, daily schedule, and failure debugging.

For the model itself (calibration, V2 auto-cal), see [beeframes_model](../beeframes_model/SKILL.md).
For downstream monitoring and validation, see [beeframes_monitoring](../beeframes_monitoring/SKILL.md).

## Quick Reference

| Item | Value |
|------|-------|
| Trigger | Daily at 06:00 UTC |
| Orchestration | AWS Lambda chain (Step Functions) |
| Input | Sensor data from IoT fleet + calibration params from `model_deployments` |
| Output Table | `data_lake_raw_data.unified_bee_frames` (partition: `input_date`) |
| Legacy Output | `data_lake_raw_data.supervised_beeframes` -- **DEPRECATED, do not use** |
| Downstream | dbt preprocess -> monitoring validations -> hive_updates |
| Season Filter | `group_to_seasonal_activities.seasonal_activities_id = 90` |

## Common Questions This Skill Answers

- Why did the SBS pipeline not produce predictions for group X today?
- What is the daily Lambda chain execution flow?
- How do sensor readings become bee frame predictions?
- When should I expect predictions to be available in unified_bee_frames?
- What tables are written at each stage of the pipeline?
- How do I tell if the pipeline ran but a specific group was skipped?

## Pipeline Architecture

```
06:00 UTC - Lambda Chain Start
    |
    v
[1] Sensor Ingestion Lambda
    Read sensor_samples (temp, humidity, ambient) for all active sensors
    |
    v
[2] V1 Ranker (MLP)
    Compute relative hive strength scores (inter-hive ranking)
    Input: sensor features  |  Output: ranker scores per sensor
    |
    v
[3] Calibration Lambda
    Apply per-group calibration function: CF(v) = clip(slope * v + bias, 0, saturation)
    Reads calibration params from model_deployments (latest PRODUCTION deployment)
    |
    v
[4] Prediction Writer Lambda
    Write final predictions to unified_bee_frames
    Columns: pred_raw, pred_clipped, pred_rounded, pred_base
    Partition: input_date = CURRENT_DATE
    |
    v
[5] Downstream Trigger
    Signals dbt to run preprocess model (~06:20 UTC)
```

## Daily Schedule

| Time (UTC) | Event | Table Written |
|------------|-------|---------------|
| 06:00 | Lambda chain starts | -- |
| ~06:05 | Ranker completes | (intermediate, not persisted) |
| ~06:10 | Calibration + prediction write | `unified_bee_frames` |
| ~06:20 | dbt preprocess runs | `beekeeper_beeframe_model_monitoring_preprocess` |
| ~06:30 | Data available for validation | -- |
| ~17:00 | Manual validation window opens | -- |
| ~03:00+1 | Auto-validation runs | `model_metric_test` |

## Data Sources

- **Table:** `data_lake_raw_data.unified_bee_frames` -- Final prediction output (one row per sensor per day)
- **Table:** `data_lake_raw_data.model_deployments` -- Calibration parameters and deployment status per group
- **Table:** `data_lake_raw_data.group_to_seasonal_activities` -- Season membership (current = 90)
- **Table:** `data_lake_curated_data.sensor_daily_snapshot` -- Sensor-to-group mapping (MAC-based, authoritative)
- **Table:** `data_lake_raw_data.sensor_samples` -- Raw sensor readings (temp, humidity)

## Which Groups Get Predictions?

A group receives SBS predictions when ALL conditions are met:

1. **In current season:** `group_to_seasonal_activities.seasonal_activities_id = 90`
2. **Production deployment:** Latest `model_deployments` record has `status = 'PRODUCTION'`
3. **Calibration mature:** Deployment `timestamp <= CURRENT_DATE - interval '3' day`
4. **Active sensors:** Group has sensors reporting data in `sensor_daily_snapshot`

Use the `ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC) = 1` pattern on `model_deployments` to get the latest deployment.

## Decision Logic: Is a Group Missing or Correctly Excluded?

```
Group has no predictions today
    |
    +-- Is group in season 90?
    |   NO -> Correctly excluded (seasonal gap)
    |   YES -> Continue
    |
    +-- Does group have latest deployment status = 'PRODUCTION'?
    |   NO -> Correctly excluded (not deployed / disabled / staging)
    |   YES -> Continue
    |
    +-- Was calibration > 3 days ago?
    |   NO -> Warming up period, predictions will start after 3 days
    |   YES -> Continue
    |
    +-- Does group have sensors in sensor_daily_snapshot today?
    |   NO -> Sensor issue (hardware/connectivity). Check sensor_samples.
    |   YES -> Pipeline failure. Check Lambda execution logs.
```

## Common Failure Modes

| Symptom | Likely Cause | Resolution |
|---------|--------------|------------|
| No predictions for any group today | Lambda chain did not start or failed early | Check AWS Step Functions execution history, CloudWatch logs |
| One group missing predictions | Group excluded by eligibility criteria or sensor issue | Run decision tree above; use `queries/sbs_group_eligibility.sql` |
| Predictions present but all pred_rounded = 0 | Calibration parameters corrupt (slope=0 or bias set wrong) | Check `model_deployments` for the group's calibration params |
| Prediction count dropped >20% for a group | Sensors went offline or were removed | Compare sensor count in `sensor_daily_snapshot` today vs yesterday |
| Predictions delayed past 07:00 UTC | Lambda execution slow or retry in progress | Check Step Functions; pipeline typically completes by 06:15 |
| `supervised_beeframes` has data but `unified_bee_frames` does not | Migration artifact -- `supervised_beeframes` is deprecated | Always check `unified_bee_frames`; the legacy table may still receive writes but is not authoritative |

## Key Partition Columns

| Table | Partition Column | Type | Example |
|-------|-----------------|------|---------|
| `unified_bee_frames` | `input_date` | date | `input_date = DATE '2026-03-17'` |
| `sensor_daily_snapshot` | `date` | date | `date = CURRENT_DATE` |
| `model_deployments` | (none) | -- | Use ROW_NUMBER pattern |
| `group_to_seasonal_activities` | (none) | -- | Filter by `seasonal_activities_id` |

Always filter on partition columns first to avoid full table scans.

## Queries

- `queries/sbs_group_eligibility.sql` -- Check if a group meets all criteria for SBS predictions
- `queries/sbs_daily_execution_summary.sql` -- Daily summary of pipeline execution: groups processed, sensor counts, timing
- `queries/sbs_sensor_prediction_trace.sql` -- Trace a single sensor's data from raw readings through to final prediction

## Runbooks

- `runbooks/debug_missing_sbs_predictions.md` -- Step-by-step guide when a group is missing from today's SBS run

## Related Skills

- [beeframes_model](../beeframes_model/SKILL.md) -- Model architecture, calibration function, V2 auto-cal
- [beeframes_monitoring](../beeframes_monitoring/SKILL.md) -- Downstream validation pipeline
- [dbt_gold_layer](../dbt_gold_layer/SKILL.md) -- The dbt preprocess model that runs after SBS
- [ml_pipeline](../ml_pipeline/SKILL.md) -- Deployment lifecycle and drift detection

---
*Last updated: 2026-03-17*
