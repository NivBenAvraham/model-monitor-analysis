# Runbook: Debug Missing SBS Predictions

## When to Use

A production group should have predictions in `unified_bee_frames` for today but does not. This can be detected via `sbs_daily_execution_summary.sql` (Part 3) or manually checking the table.

## Prerequisites

- Athena query access
- Know the `group_id` of the missing group

## Steps

### 1. Check group eligibility

Run `queries/sbs_group_eligibility.sql` with the group_id. This returns a single row with boolean flags for each criterion.

**Decision tree based on results:**

| Flag | Value | Meaning | Action |
|------|-------|---------|--------|
| `in_season` | false | Group not in season 90 | No action. Correctly excluded. Verify with `group_to_seasonal_activities`. |
| `has_production_deployment` | false | Latest deployment is not PRODUCTION | Check `model_deployments` for status. May be DISABLED, STAGING, or missing. |
| `calibration_mature` | false | Calibrated < 3 days ago | No action. Wait for 3-day warm-up period. |
| `has_active_sensors` | false | No sensors in `sensor_daily_snapshot` today | Sensor issue. Continue to Step 2. |
| All flags true, `has_predictions_today` false | -- | Pipeline failure | Continue to Step 3. |

### 2. Investigate sensor health

If the group has no active sensors, check raw sensor data:

```sql
-- Check sensor_daily_snapshot for recent days
SELECT date, COUNT(DISTINCT mac) AS sensor_count
FROM data_lake_curated_data.sensor_daily_snapshot
WHERE group_id = {{group_id}}
  AND date >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY date
ORDER BY date DESC;
```

**If sensor count dropped to zero recently:**
- Check with field operations for hardware issues
- Sensors may have been reassigned to another group
- Gateway connectivity issues can cause all sensors in a yard to go offline

**If sensors were never present:**
- Group may not have physical sensors deployed yet
- Check group setup in the operations system

### 3. Check if the Lambda chain ran

If the group is eligible and has sensors, the issue is in the pipeline itself.

```sql
-- Check if ANY group got predictions today
-- (distinguishes "pipeline didn't run" from "one group skipped")
SELECT COUNT(DISTINCT group_id) AS groups_with_predictions
FROM data_lake_raw_data.unified_bee_frames
WHERE input_date = CURRENT_DATE;
```

**If zero groups have predictions:**
- The Lambda chain did not run. Check AWS Step Functions execution history.
- Check CloudWatch logs for the Lambda functions.
- Common causes: IAM permission changes, Lambda timeout, Step Functions schedule disabled.

**If other groups have predictions but this one doesn't:**
- The pipeline ran but skipped this group. Continue to Step 4.

### 4. Trace the sensor-level data

Run `queries/sbs_sensor_prediction_trace.sql` for a known sensor in the group to trace data through each stage.

```sql
-- Find a sensor MAC for this group
SELECT mac
FROM data_lake_curated_data.sensor_daily_snapshot
WHERE group_id = {{group_id}}
  AND date = CURRENT_DATE
LIMIT 1;
```

Then trace that sensor through the pipeline stages. Look for where data stops:

| Stage | Has Data | Likely Issue |
|-------|----------|-------------|
| sensor_daily_snapshot: yes, unified_bee_frames: no | Ranker or calibration Lambda failed for this group | Check Lambda logs for errors with this group_id |
| unified_bee_frames: yes but pred_rounded = 0 | Calibration parameters corrupt | Check `model_deployments` calibration params |
| All stages have data | Data is present but was not counted in your initial check | Re-verify with correct date/partition filters |

### 5. Check for model name mismatch

If `hive_updates_metadata` has data but the preprocess join drops it:

```sql
-- Check model name alignment between tables
SELECT DISTINCT
    hum.router_s3_pkl_file AS hu_model_name,
    ubf.model_name AS ubf_model_name,
    CASE WHEN hum.router_s3_pkl_file = ubf.model_name THEN 'MATCH' ELSE 'MISMATCH' END AS status
FROM data_lake_curated_data.hive_updates_metadata hum
JOIN data_lake_curated_data.sensor_daily_snapshot sds
    ON sds.mac = hum.sensor_mac_address AND sds.date = DATE(hum.created)
LEFT JOIN data_lake_raw_data.unified_bee_frames ubf
    ON ubf.sensor_mac_address = hum.sensor_mac_address
    AND ubf.input_date = DATE(hum.created)
WHERE sds.group_id = {{group_id}}
  AND DATE(hum.created) = CURRENT_DATE
  AND hum.model = 'BEE_FRAMES'
LIMIT 20;
```

A MISMATCH means the model version in hive_updates doesn't match unified_bee_frames, causing the INNER JOIN in the preprocess model to drop rows.

### 6. Determine resolution

| Root Cause | Action |
|------------|--------|
| Out of season | No action needed. |
| Not PRODUCTION status | Contact ML team to investigate deployment status. |
| Calibration < 3 days old | Wait. Predictions will start after warm-up period. |
| No sensors in snapshot | Check with field ops for hardware/connectivity issues. |
| Lambda chain didn't run | Check Step Functions and CloudWatch. May need manual trigger. |
| Lambda ran but skipped this group | Check Lambda logs for error specific to this group_id. |
| Model name mismatch | Investigate why model versions diverged. May need redeployment. |
| Calibration params corrupt | May need manual recalibration by domain expert. |

## Escalation Path

1. **First check:** Run this runbook to identify root cause
2. **If Lambda issue:** Check CloudWatch logs, notify ML ops team
3. **If deployment issue:** Contact ML team lead
4. **If persistent (3+ days):** Create ClickUp ticket in Data Quality epic (list ID: `901519943134`)
5. **If sensor hardware:** Flag to field operations team

## Success Criteria

- Root cause identified and documented
- If the cause is a real gap, action item created (ClickUp ticket)
- If the cause is expected (warming up, off-season), documented to avoid re-investigation

---
*Created: 2026-03-17*
