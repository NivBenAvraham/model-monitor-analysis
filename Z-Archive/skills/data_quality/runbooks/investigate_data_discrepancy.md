# Runbook: Investigate Data Discrepancy

## When to Use

Data for a group looks wrong, incomplete, or inconsistent across pipeline tables. This covers scenarios like:
- Prediction count doesn't match sensor count
- Preprocess shows different numbers than raw tables
- A group's data is present in one table but missing in another
- Values look suspicious (all zeros, sudden jumps, unexpected NULLs)

## Prerequisites

- Athena query access
- Know the `group_id` and `date` of the discrepancy
- Know which table(s) show the issue

## Steps

### 1. Establish the baseline: what data exists where?

Run `queries/sensor_data_freshness.sql` with the group_id. This shows the latest data available in each pipeline table.

**Look for the "break point"** -- where is the most recent data in each table?

| Pattern | Meaning |
|---------|---------|
| All tables have today's data | Data is flowing, issue may be in values not availability |
| sensor_daily_snapshot current, unified_bee_frames stale | SBS pipeline not producing predictions |
| unified_bee_frames current, preprocess stale | dbt model not running or failing |
| Everything stale (same cutoff date) | Upstream data source stopped |

### 2. Compare counts across pipeline stages

Run `queries/cross_table_consistency.sql` with the group_id and date. This shows sensor counts at each stage.

**Look for count drops between stages:**

| Drop Location | Likely Cause | Investigation |
|--------------|-------------|---------------|
| snapshot -> unified_bee_frames | Some sensors missing predictions | Check if those sensors are in a valid state. May be dead/new sensors. |
| unified_bee_frames -> hive_updates_metadata | Predictions exist but no hive updates written | Pipeline timing issue, or hive update writer failed |
| hive_updates_metadata -> preprocess | INNER JOIN dropped rows | Model name mismatch between `router_s3_pkl_file` and `lambda_name`. See Step 4. |

### 3. Verify the join path

If counts don't match expectations, verify you're using the correct join path.

Run `queries/join_path_validation.sql` with the group_id. This compares:
- CORRECT: `sensor_daily_snapshot.mac` -> `hive_updates_metadata.sensor_mac_address`
- WRONG: `sensors.hive_id` -> `hive_updates.hive_id`

**If the wrong path returns fewer/older rows than the correct path:**
Your investigation was using a stale join. Switch to the MAC-based join and re-run your analysis.

**If both paths return the same results:**
The join path is not the issue. Continue to Step 4.

### 4. Check for model name mismatch

The INNER JOIN in the preprocess model matches on model name between `hive_updates_metadata` and `unified_bee_frames`. A mismatch silently drops sensors.

```sql
SELECT DISTINCT
    hum.router_s3_pkl_file AS hu_model_name,
    ubf.model_name AS ubf_model_name,
    CASE
        WHEN hum.router_s3_pkl_file = ubf.model_name THEN 'MATCH'
        WHEN ubf.model_name IS NULL THEN 'MISSING_IN_UBF'
        ELSE 'MISMATCH'
    END AS status,
    COUNT(*) AS sensor_count
FROM data_lake_curated_data.hive_updates_metadata hum
JOIN data_lake_curated_data.sensor_daily_snapshot sds
    ON sds.mac = hum.sensor_mac_address
    AND sds.date = DATE(hum.created)
LEFT JOIN data_lake_raw_data.unified_bee_frames ubf
    ON ubf.sensor_mac_address = hum.sensor_mac_address
    AND ubf.input_date = DATE(hum.created)
WHERE sds.group_id = {{group_id}}
  AND DATE(hum.created) = {{date}}
  AND hum.model = 'BEE_FRAMES'
GROUP BY hum.router_s3_pkl_file, ubf.model_name
ORDER BY sensor_count DESC
LIMIT 20;
```

**If MISMATCH or MISSING_IN_UBF rows appear:**
- Check if the group was recently redeployed with a new model version
- The hive_updates may still reference the old model while predictions use the new one
- Resolution: wait for the next cycle to align, or investigate the deployment

### 5. Check for value anomalies

If data is present but values look wrong:

```sql
SELECT
    group_id,
    input_date,
    COUNT(*) AS sensor_count,
    ROUND(AVG(pred_rounded), 2) AS avg_pred,
    ROUND(STDDEV(pred_rounded), 2) AS stddev_pred,
    MIN(pred_rounded) AS min_pred,
    MAX(pred_rounded) AS max_pred,
    -- Check for suspicious patterns
    SUM(CASE WHEN pred_rounded = 0 THEN 1 ELSE 0 END) AS zero_count,
    SUM(CASE WHEN pred_rounded IS NULL THEN 1 ELSE 0 END) AS null_count
FROM data_lake_raw_data.unified_bee_frames
WHERE group_id = {{group_id}}
  AND input_date BETWEEN {{date}} - INTERVAL '7' DAY AND {{date}}
GROUP BY group_id, input_date
ORDER BY input_date DESC;
```

| Pattern | Likely Cause | Action |
|---------|-------------|--------|
| All zeros | Calibration slope = 0 or bias issue | Check calibration params in model_deployments |
| All same value | Saturation hit for all sensors | Check if saturation parameter is too low |
| Sudden jump from prior days | Recent recalibration | Check model_deployments timestamp; may be expected |
| High stddev vs prior days | Mixed sensor population | Check if new/dead sensors were added/removed |
| Many NULLs | Pipeline partial failure | Check Lambda logs for errors on specific sensors |

### 6. Check date and type consistency

Common query bugs that cause apparent discrepancies:

```sql
-- WRONG: string comparison on date column
WHERE date = '2026-03-17'  -- May not match date type

-- CORRECT: use date type
WHERE date = DATE '2026-03-17'
WHERE date = CURRENT_DATE

-- WRONG: missing partition filter (causes slow/incomplete results)
SELECT * FROM unified_bee_frames WHERE group_id = 2794
-- This scans ALL partitions

-- CORRECT: always include partition
SELECT * FROM unified_bee_frames WHERE group_id = 2794 AND input_date = CURRENT_DATE

-- WRONG: comparing group_id across tables with different types
WHERE v.group_id = m.entity_id  -- group_id is INT, entity_id is VARCHAR

-- CORRECT: cast to same type
WHERE CAST(v.group_id AS VARCHAR) = m.entity_id
```

### 7. Determine resolution

| Root Cause | Action |
|------------|--------|
| Using stale join path (sensors.hive_id) | Switch to sensor_daily_snapshot MAC-based join |
| Model name mismatch | Wait for next cycle or investigate deployment version |
| Upstream data gap | Follow sbs_pipeline runbook to debug Lambda chain |
| dbt model failure | Check dbt logs, re-run if needed |
| Calibration issue | Flag for domain expert review |
| Query bug (wrong date type, missing partition) | Fix the query |
| Actual data quality problem (corrupt values) | Create ClickUp ticket for data team investigation |

## Escalation Path

1. **First check:** Run this runbook to identify root cause
2. **If join path issue:** Fix queries, no escalation needed
3. **If pipeline issue:** Follow relevant pipeline runbook (sbs_pipeline, dbt_gold_layer)
4. **If value corruption:** Create ClickUp ticket in Data Quality epic (list ID: `901519943134`)
5. **If persistent across multiple groups:** Escalate to ML ops and data engineering

## Success Criteria

- Root cause of the discrepancy identified
- If a query/join issue: corrected query documented
- If a pipeline issue: upstream fix applied or ticket created
- If a data quality issue: documented with affected date range, group_ids, and tables

---
*Created: 2026-03-17*
