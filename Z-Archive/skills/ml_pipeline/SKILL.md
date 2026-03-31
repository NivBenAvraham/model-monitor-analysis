# ML Pipeline

The end-to-end ML model lifecycle: training, deployment, daily inference, and pipeline health monitoring. Covers debugging scenarios when models fail to produce predictions or produce anomalous results.

For model-specific schemas and prediction fields, see [beeframes_model](../beeframes_model/SKILL.md).
For daily monitoring validations and gap detection, see [beeframes_monitoring](../beeframes_monitoring/SKILL.md).

## Pipeline Flow

`MLOps Service (training)` -> `calibration` -> `deployment (staging -> production)` -> `lambda chain (daily inference)` -> `predictions written to unified_bee_frames`

## Quick Reference

| Item | Value |
|------|-------|
| Predictions Table | `data_lake_raw_data.unified_bee_frames` (partition: `input_date`) |
| Deployments Table | `data_lake_raw_data.model_deployments` |
| Seasonal Activities | `data_lake_raw_data.group_to_seasonal_activities` |
| Preprocess Table | `data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess` (partition: `date`) |
| MLOps Service | GitHub: `Bee-Hero/mlops-service` |

## Model Lifecycle

### Training
- Models are trained via the MLOps service (`mlops-service` repo)
- Training produces model artifacts (weights, config, calibration parameters)
- Each model version is tracked with a `model_name` identifier

> Note: Training details (frequency, tracking, artifact storage) are managed by the ML team, not the data team.

### Deployment Workflow
1. **Calibration** -- Model is calibrated against group-specific data
2. **Staging** -- Deployed to staging for validation
3. **Production** -- Promoted to production status in `model_deployments`
4. **Rollback** -- If issues detected, status changed from `PRODUCTION` to `DISABLED`

### Production Group Criteria

> See beeframes_model/SKILL.md § Production Group Criteria

## Inference Pipeline (Daily Predictions)

### Lambda Chain Execution
- Runs daily, producing predictions for all production groups
- Input: sensor data + calibration parameters from deployment
- Output: rows written to `unified_bee_frames` with `input_date = CURRENT_DATE`
- Each sensor in a production group gets a prediction row

### Key Prediction Fields

> See beeframes_model/SKILL.md § Key Prediction Fields

## Pipeline Health Monitoring

### Daily Checks
1. **Prediction coverage** -- Do all production groups have today's predictions?
2. **Volume comparison** -- Is today's prediction count consistent with recent days?
3. **Drift detection** -- Are prediction statistics (mean, stddev) within normal range?

### Key Metrics to Track

| Metric | How to Check | Alarm Threshold |
|--------|-------------|-----------------|
| Groups with predictions | Count distinct group_id in unified_bee_frames for today | Fewer than yesterday |
| Sensors per group | Count sensors per group_id today vs 7-day average | Drop > 20% |
| Prediction mean per group | AVG(pred_rounded) per group today vs 7-day avg | Deviation > 2 stddev |
| Missing groups | Production groups with zero predictions today | Any missing group |

## Common Debugging Scenarios

### 1. Model Not Producing Predictions for a Group

| Check | Query/Action | Expected |
|-------|-------------|----------|
| Deployment status | Check `model_deployments` for latest status | `PRODUCTION` |
| Calibration age | Check `timestamp` in deployment | At least 3 days old |
| Season membership | Check `group_to_seasonal_activities` | `seasonal_activities_id = 90` |
| Predictions exist | Check `unified_bee_frames` for `input_date = CURRENT_DATE` | Rows present |
| Preprocess data | Check preprocess table for `date = CURRENT_DATE` | Rows present |

See [runbook: debug_missing_predictions](runbooks/debug_missing_predictions.md) for step-by-step guide.

### 2. Predictions Look Wrong (Drift/Outliers)

- Compare today's `pred_rounded` stats against 7-day rolling average per group
- Look for sudden jumps in mean or increase in stddev
- Check if model version changed recently (new `model_name` in unified_bee_frames)
- Check calibration date -- recent recalibration may shift predictions

Use [prediction_drift_check.sql](queries/prediction_drift_check.sql) to detect anomalies.

### 3. Deployment Status Issues

- Group stuck in non-PRODUCTION status when it should be active
- Multiple deployments for the same group (use ROW_NUMBER pattern to get latest)
- Calibration failed -- check if `timestamp` is recent but status is not `PRODUCTION`

Use [model_deployment_history.sql](queries/model_deployment_history.sql) to inspect the full history.

### 4. Calibration Failures

- Calibration is a prerequisite for production deployment
- If calibration fails, the group will not reach `PRODUCTION` status
- Check `model_deployments` for the group's latest record -- status will indicate the failure
- Groups need at least 3 days post-calibration before entering monitoring pipeline

## Table Schemas

### unified_bee_frames (Predictions)

> See beeframes_model/SKILL.md § unified_bee_frames Schema

### model_deployments

| Column | Description |
|--------|-------------|
| `group_id` | Beekeeper group |
| `status` | e.g., `'PRODUCTION'`, `'DISABLED'`, `'STAGING'` |
| `timestamp` | Calibration/deployment date |

**Pattern:** Use `ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC)` to get the latest deployment per group.

### group_to_seasonal_activities

| Column | Description |
|--------|-------------|
| `group_id` | Beekeeper group |
| `seasonal_activities_id` | Current season = `90` |

## Knowledge Index

### Queries
- [daily_prediction_coverage.sql](queries/daily_prediction_coverage.sql) -- **Daily recurring** -- Check how many production groups got predictions today vs yesterday vs expected
- [model_deployment_history.sql](queries/model_deployment_history.sql) -- Show full deployment history for a group: status changes, calibration dates, model versions
- [prediction_drift_check.sql](queries/prediction_drift_check.sql) -- Compare today's prediction statistics against 7-day rolling averages to detect drift

### Runbooks
- [debug_missing_predictions.md](runbooks/debug_missing_predictions.md) -- Step-by-step guide for investigating groups missing from daily predictions

## Related Skills
- [beeframes_model](../beeframes_model/SKILL.md) — Model-specific schemas, prediction fields, calibration pipeline, and V2 automated calibration
- [beeframes_monitoring](../beeframes_monitoring/SKILL.md) — Daily monitoring validations, gap detection, and reviewer workflow

---
*Last updated: 2026-03-17*
