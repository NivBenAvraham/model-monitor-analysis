# Runbook: Debug Missing Predictions

## When to Use

A production group appears in the "gaps" output of `daily_prediction_coverage.sql` (Part 2) -- meaning it has a production deployment but no predictions in `unified_bee_frames` for today.

## Prerequisites

- Athena query access
- Know the `group_id` of the missing group (from the coverage query output)

## Steps

### 1. Confirm the group is actually missing predictions

Run the following to verify no predictions exist for the group today:

```sql
SELECT COUNT(*) AS prediction_count
FROM data_lake_raw_data.unified_bee_frames
WHERE group_id = {{group_id}}
  AND input_date = CURRENT_DATE;
```

**Expected:** Zero rows. If rows appear, the predictions were written after the coverage query ran -- no issue.

### 2. Check deployment status

Use [model_deployment_history.sql](../queries/model_deployment_history.sql) or run:

```sql
WITH latest AS (
    SELECT
        group_id,
        status,
        timestamp AS calibration_date,
        ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC) AS rn
    FROM data_lake_raw_data.model_deployments
    WHERE group_id = {{group_id}}
)
SELECT *
FROM latest
WHERE rn = 1;
```

**Decision tree:**

| Status | Calibration Age | Meaning | Action |
|--------|----------------|---------|--------|
| `PRODUCTION` | >= 2 days | Should be producing predictions | Continue to Step 3 |
| `PRODUCTION` | < 2 days | Recently calibrated, still warming up | Wait -- predictions will start after 2-day buffer |
| `DISABLED` | (any) | Model was rolled back | Check why it was disabled (talk to ML team) |
| `STAGING` | (any) | Not yet promoted to production | Not expected to have predictions -- no action |
| No rows | -- | Group has no deployment record | Group was never deployed -- check if this is expected |

### 3. Check if predictions exist for recent days

If the deployment looks correct, check whether this is a new issue or ongoing:

```sql
SELECT
    input_date,
    COUNT(*) AS sensor_count
FROM data_lake_raw_data.unified_bee_frames
WHERE group_id = {{group_id}}
  AND input_date BETWEEN CURRENT_DATE - interval '7' day AND CURRENT_DATE
GROUP BY input_date
ORDER BY input_date DESC;
```

**Expected:** One row per day with consistent sensor counts.

- **If recent days also missing** -- This is a longer-term issue. The lambda chain may have stopped processing this group. Escalate to the ML ops team.
- **If only today is missing** -- Likely a transient pipeline issue. Check if the daily lambda chain completed (see Step 4).
- **If sensor count is dropping** -- Possible sensor health issue. Check sensor data availability.

### 4. Check preprocess data availability

The monitoring preprocess runs after the lambda chain. If preprocess data exists but predictions don't, the issue is in the inference step:

```sql
SELECT
    group_id,
    date,
    group_in_season,
    is_production_model,
    model_status,
    deployment_status,
    pred_rounded
FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
WHERE group_id = {{group_id}}
  AND date = CURRENT_DATE
LIMIT 10;
```

**Decision tree:**

| Preprocess Data | Predictions in unified_bee_frames | Meaning |
|----------------|-----------------------------------|---------|
| Present, has pred_rounded values | Missing | Preprocess ran but predictions not in unified_bee_frames -- possible data ingestion lag |
| Present, pred_rounded is NULL | Missing | Lambda chain did not produce predictions for this group |
| Not present | Missing | Upstream issue -- lambda chain may not have run at all |

### 5. Check season membership

Verify the group is in the current season:

```sql
SELECT *
FROM data_lake_raw_data.group_to_seasonal_activities
WHERE group_id = {{group_id}};
```

**Expected:** `seasonal_activities_id = 90`. If the group is not in season 90, it is correctly excluded from production predictions.

### 6. Determine resolution

| Root Cause | Action |
|------------|--------|
| Recently calibrated (< 2 days) | No action. Wait for the 2-day buffer period to pass. |
| Deployment status not PRODUCTION | Check with ML team why the group was disabled/not promoted. |
| Out of season (sa_id != 90) | No action. Group is correctly excluded. |
| Lambda chain didn't run | Check pipeline orchestration logs (Airflow/scheduler). May need manual trigger. |
| Predictions exist in preprocess but not unified_bee_frames | Data ingestion lag. Wait and re-check, or investigate ETL pipeline. |
| Persistent multi-day gap | Escalate: create a ClickUp ticket with the group_id, date range of missing data, and deployment status. |
| Sensor count dropping | Cross-check with sensor health data. May be hardware issue in the field. |

## Escalation Path

1. **First check:** Run this runbook to identify root cause
2. **If pipeline issue:** Check Airflow/orchestration logs, notify ML ops team
3. **If deployment issue:** Contact ML team lead for status change investigation
4. **If persistent (3+ days):** Create ClickUp ticket in Data Quality epic (list ID: `901519943134`)
5. **If sensor health issue:** Flag to field operations team

## Success Criteria

- Root cause identified for why the group is missing predictions
- If the cause is a real gap (not just warming up or out-of-season), an action item is created
- If the cause is expected (warming up, seasonal), documented to avoid re-investigation

---
*Created: 2026-02-18*
