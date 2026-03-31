# Data Quality

Common data quality patterns, investigation techniques, and correct join paths across BeeHero data pipelines. This skill is a cross-cutting reference for avoiding known pitfalls and running reliable investigations.

For domain-specific debugging, see the relevant skill:
- [sbs_pipeline](../sbs_pipeline/SKILL.md) -- Prediction pipeline failures
- [dbt_gold_layer](../dbt_gold_layer/SKILL.md) -- Preprocess model issues
- [beeframes_monitoring](../beeframes_monitoring/SKILL.md) -- Validation gaps

## Quick Reference

| Item | Value |
|------|-------|
| Authoritative sensor-to-group mapping | `data_lake_curated_data.sensor_daily_snapshot` (MAC-based) |
| Hive update recency (pre-aggregated) | `data_lake_curated_data.daily_latest_hive_update` |
| Hive updates with model info | `data_lake_curated_data.hive_updates_metadata` |
| Raw hive updates (no group_id) | `data_lake_raw_data.hive_updates` |
| Curated hive updates (has group_id) | `data_lake_curated_data.hive_updates` |
| Production predictions | `data_lake_raw_data.unified_bee_frames` |
| Deprecated predictions | `data_lake_raw_data.supervised_beeframes` -- **DO NOT USE** |

## Common Questions This Skill Answers

- What is the correct way to join hive_updates to a group?
- Why does my query show stale data for a group?
- How do I check if sensors are reporting for a group?
- What are the known data quality pitfalls in BeeHero tables?
- How do I trace a data issue from raw sensor data to final prediction?
- What partition columns should I always filter on?

## Correct Join Patterns

### Getting Hive Updates for a Group (CORRECT)

Use `sensor_daily_snapshot` (MAC-based) to map sensors to groups, then join to `hive_updates_metadata`:

```sql
-- CORRECT: MAC-based join via sensor_daily_snapshot
SELECT
    sds.group_id,
    DATE(hum.created) AS update_date,
    COUNT(DISTINCT hum.sensor_mac_address) AS sensors_with_updates,
    COUNT(*) AS total_updates
FROM data_lake_curated_data.hive_updates_metadata hum
JOIN data_lake_curated_data.sensor_daily_snapshot sds
    ON sds.mac = hum.sensor_mac_address
    AND sds.date = DATE(hum.created)
WHERE sds.group_id = {{group_id}}
  AND hum.model = 'BEE_FRAMES'
  AND hum.created >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY sds.group_id, DATE(hum.created)
ORDER BY update_date DESC;
```

### Getting Hive Updates for a Group (WRONG -- DO NOT USE)

Do NOT join through `sensors.hive_id`. The hive-to-sensor mapping goes stale when sensors are moved between hives, causing queries to show outdated or missing data.

```sql
-- WRONG: hive_id-based join goes stale
-- This showed data only through March 9 for group 2794
-- when data actually existed through March 11
SELECT s.group_id, COUNT(*)
FROM data_lake_raw_data.hive_updates hu
JOIN data_lake_raw_data.sensors s ON s.hive_id = hu.hive_id  -- STALE!
WHERE s.group_id = {{group_id}}
GROUP BY s.group_id;
```

**Why:** Sensors are physical devices that get moved between hives. The `sensors` table tracks the current hive assignment, but `hive_updates` records were written with the hive_id at the time of the update. Once a sensor moves, old records can no longer be joined correctly. `sensor_daily_snapshot` uses MAC address which is immutable.

### Quick Hive Update Recency Check

For checking when a group last received hive updates, use the pre-aggregated table:

```sql
-- Fast: pre-aggregated hive update recency
SELECT *
FROM data_lake_curated_data.daily_latest_hive_update
WHERE group_id = {{group_id}}
ORDER BY date DESC
LIMIT 7;
```

## Partition Column Reference

Always filter on partition columns first. Forgetting this causes full table scans that are slow and expensive.

| Table | Partition Column | Type | Filter Example |
|-------|-----------------|------|----------------|
| `unified_bee_frames` | `input_date` | date | `input_date = DATE '2026-03-17'` |
| `sensor_daily_snapshot` | `date` | date | `date = CURRENT_DATE` |
| `beekeeper_beeframe_model_monitoring_preprocess` | `date` | date | `date = CURRENT_DATE` |
| `hive_updates_metadata` | `created` | timestamp | `created >= CURRENT_DATE - INTERVAL '1' DAY` |
| `sensor_samples` | `log_timestamp` | timestamp | `log_timestamp >= CURRENT_DATE - INTERVAL '1' DAY` |
| `hive_updates` (raw) | `created` | timestamp | `created >= CURRENT_DATE - INTERVAL '1' DAY` |
| `model_deployments` | (none) | -- | Use ROW_NUMBER pattern to get latest |
| `group_to_seasonal_activities` | (none) | -- | Small table, no partition needed |

## Investigation Decision Trees

### "Group X has no data today"

```
1. Check preprocess table for group today
   |
   +-- Has rows -> Check gate flags (see dbt_gold_layer skill)
   +-- No rows -> Continue
   |
2. Check sensor_daily_snapshot for group today
   |
   +-- Has sensors -> Sensors exist but no predictions. Check unified_bee_frames.
   +-- No sensors -> Continue
   |
3. Check group_to_seasonal_activities
   |
   +-- seasonal_activities_id = 90 -> In season but no sensors. Check sensor_samples for raw data.
   +-- seasonal_activities_id != 90 -> Not in current season. Correctly excluded.
   +-- No rows -> Group not registered for any season.
```

### "Prediction count dropped for group X"

```
1. Compare sensor_daily_snapshot count today vs yesterday
   |
   +-- Count dropped -> Sensors went offline. Check:
   |   - sensor_samples for last data timestamp per sensor
   |   - Field operations for hardware issues
   |
   +-- Count same -> Sensors present but fewer predictions. Check:
   |   - unified_bee_frames for today's count
   |   - Model name mismatch in hive_updates_metadata vs unified_bee_frames
   |   - INNER JOIN in preprocess dropping sensors
```

### "Data looks wrong / suspicious values"

```
1. Check pred_rounded distribution for the group
   |
   +-- All zeros -> Calibration issue (slope=0 or corrupt params)
   +-- All same value -> Saturation hit or bias dominating
   +-- Sudden jump from previous days -> Recent recalibration (check model_deployments timestamp)
   +-- Normal distribution but shifted -> Seasonal change or sensor population change
   |
2. Compare with calibration_average in preprocess
   |
   +-- Large deviation from calibration_average -> Model drift. Check prediction_drift_check.sql
   +-- Close to calibration_average -> Values may be correct; compare with field observations
```

## Anomaly Detection Thresholds

| Metric | Normal Range | Warning | Critical |
|--------|-------------|---------|----------|
| Sensor count per group (day-over-day) | < 5% change | 5-20% drop | > 20% drop |
| Prediction mean per group (vs 7-day avg) | < 1 stddev | 1-2 stddev | > 2 stddev |
| Prediction stddev per group | < 2x baseline | 2-3x baseline | > 3x baseline |
| Groups with predictions (total) | All production groups | 1-2 missing | 3+ missing |
| Pipeline completion time | < 06:15 UTC | 06:15-07:00 UTC | > 07:00 UTC |
| Hive update recency per group | < 24 hours | 24-48 hours | > 48 hours |

## Deprecated Tables and Fields

| Deprecated | Replacement | Notes |
|-----------|-------------|-------|
| `data_lake_raw_data.supervised_beeframes` | `data_lake_raw_data.unified_bee_frames` | Migrated Feb 2026. Legacy table may still receive writes but is not authoritative. |
| `sensors.hive_id` for group lookups | `sensor_daily_snapshot.mac` | hive_id mappings go stale. Always use MAC-based joins. |
| `pred_smoothed` (old column) | (removed) | Was in supervised_beeframes, not in unified_bee_frames. |
| `approved_sensor` (old column) | (removed) | Was in supervised_beeframes, not in unified_bee_frames. |

## Common Failure Modes

| Symptom | Likely Cause | Resolution |
|---------|--------------|------------|
| Query returns stale data (old dates only) | Using `sensors.hive_id` join instead of `sensor_daily_snapshot.mac` | Switch to MAC-based join pattern |
| Query returns zero rows unexpectedly | Missing partition filter or wrong date type | Add partition column filter; use `date` type not string |
| Full table scan (slow query) | No partition predicate | Add `WHERE input_date = ...` or `WHERE date = ...` |
| Duplicate rows in aggregation | Missing deduplication | Add `ROW_NUMBER()` or `DISTINCT` as appropriate |
| Group appears twice with different flags | Multiple sensors per group in preprocess | Deduplicate at group level with ROW_NUMBER |
| joined table returns NULLs for known-good groups | Wrong join key (hive_id vs MAC, group_id type mismatch) | Verify join keys match; CAST group_id types if needed |

## SQL Best Practices for BeeHero Tables

1. **Always filter on partition columns** -- Every query on large tables must include a partition predicate
2. **Always include LIMIT** -- Especially in exploratory queries. Default to `LIMIT 100`
3. **Use ROW_NUMBER for dedup** -- `model_deployments`, `validations`, and `preprocess` all need deduplication
4. **Cast group_id consistently** -- Some tables store it as INT, others as VARCHAR. Use `CAST(group_id AS VARCHAR)` for cross-table joins
5. **Use MAC address for sensor identity** -- Never use hive_id for sensor-to-group lookups
6. **Date type matters** -- Use `date = CURRENT_DATE` not `date = '2026-03-17'` (string comparison may not match date type)

## Queries

- `queries/sensor_data_freshness.sql` -- Check data freshness across all key tables for a group
- `queries/join_path_validation.sql` -- Validate that the correct join path returns expected row counts for a group
- `queries/cross_table_consistency.sql` -- Compare row counts across pipeline tables to find data loss points

## Runbooks

- `runbooks/investigate_data_discrepancy.md` -- Step-by-step guide for tracing a data issue across the pipeline

## Related Skills

- [sbs_pipeline](../sbs_pipeline/SKILL.md) -- Upstream prediction pipeline
- [dbt_gold_layer](../dbt_gold_layer/SKILL.md) -- Preprocess model join logic
- [beeframes_monitoring](../beeframes_monitoring/SKILL.md) -- Validation and gap detection
- [beeframes_model](../beeframes_model/SKILL.md) -- Model schemas and prediction fields

---
*Last updated: 2026-03-17*
