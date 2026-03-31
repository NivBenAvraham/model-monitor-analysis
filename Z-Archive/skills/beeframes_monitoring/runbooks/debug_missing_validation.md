# Runbook: Debug Missing Validation

## When to Use

A production group appears in the "gaps" output of `daily_validation_coverage.sql` (Part 2) -- meaning it has a production deployment but no validation record for today.

## Prerequisites

- Athena query access
- Know the `group_id` of the missing group (from the coverage query output)

## Steps

### 1. Confirm the group is actually missing from validations

Run the following to verify no validation exists for the group today:

```sql
SELECT *
FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_validations
WHERE group_id = {{group_id}}
  AND CAST(timestamp AS DATE) = CURRENT_DATE;
```

**Expected:** Zero rows. If rows appear, the group was validated after the coverage query ran -- no issue.

### 2. Check if the group exists in today's preprocess table

```sql
SELECT *
FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
WHERE group_id = {{group_id}}
  AND date = CURRENT_DATE
LIMIT 10;
```

**Expected:** One or more rows with today's date.

- **If rows exist** -- proceed to Step 3 (check gate flags).
- **If zero rows** -- the preprocess pipeline did not generate data for this group today. Investigate upstream: check if the group has sensor data, check pipeline logs. This is a pipeline ingestion issue, not a validation issue.

### 3. Check gate flags in preprocess

```sql
SELECT
    group_id,
    group_in_season,
    groups_in_season_with_hive_updates,
    groups_in_season_ready_for_review,
    is_production_model,
    model_status
FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
WHERE group_id = {{group_id}}
  AND date = CURRENT_DATE
LIMIT 10;
```

**Decision tree based on results:**

| Flag | Value | Meaning |
|------|-------|---------|
| `group_in_season` | `False` | Group is out-of-season. It will NOT reach reviewers. This is expected for seasonal gaps. |
| `groups_in_season_with_hive_updates` | `False` | Group is in-season but has no recent hive updates. It will NOT reach reviewers. Check if sensors are reporting. |
| `groups_in_season_ready_for_review` | `False` | Group has hive updates but other readiness criteria not met. Will NOT reach reviewers. |
| `is_production_model` | `False` | Group's model is not in production status. Unexpected if it appeared in the coverage query. Check `model_deployments` table for status discrepancy. |
| All flags `True` | -- | Group should have been reviewed. Escalate to the validation team (camila.rosero, kim.k) -- they may have skipped it. |

### 4. Determine resolution

| Root Cause | Action |
|------------|--------|
| Out-of-season (`group_in_season=False`) | No action needed. Group is correctly filtered. Consider updating the coverage query to exclude out-of-season groups if this is common. |
| No hive updates | Check sensor health for the group. May need sensor team investigation. |
| Not ready for review | Check what additional criteria are missing. May be a timing issue (data arrives later in the day). |
| Flags all True but not validated | Contact reviewers. May be a workload or process issue. |
| Not in preprocess at all | Pipeline issue. Check Airflow/orchestration logs for the preprocess job. |

## Success Criteria

- Root cause identified for why the group is missing from validations.
- If the cause is a real gap (not just seasonal filtering), an action item is created (e.g., ClickUp ticket).
- If the cause is seasonal/expected, document it for future reference to avoid re-investigation.

## Real-World Example: Group 1155 (2026-02-05)

- **Symptom:** Group 1155 appeared in daily validation coverage gaps.
- **Step 2 result:** Present in preprocess with today's date.
- **Step 3 result:** `group_in_season=False`, `groups_in_season_ready_for_review=False`.
- **Resolution:** Group is correctly filtered out as out-of-season. No action needed.

---
*Created: 2026-02-05*
