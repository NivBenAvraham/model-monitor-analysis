# Queries → Metrics Mapping

This file maps each query function in `scripts/queries.py` to the metric or
decision step that consumes it.

All query functions accept `timestamp: str` (YYYY-MM-DD) — the day whose data
is being examined — plus `group_ids: list[int]` where relevant.

---

## Query map

| Query function | Used by | Layer | Purpose |
|---|---|---|---|
| `candidates_query(timestamp)` | Decision engine | Ingestion | Find all stale PRODUCTION beekeeper groups active on `timestamp` |
| `validation_history_query(group_ids, timestamp)` | `must_review` check + prior-invalid blocker + HU stats | Ingestion | tier2 review history per group up to `timestamp` |
| `clipping_diff_query(group_ids, timestamp)` | `clipping_diff` (Signal A) | Ingestion | Latest `pred_raw` + `pred_clipped` per sensor on `timestamp` |
| `inspection_signal_query(group_ids, timestamp)` | `inspection_discrepancy` (Signal B) | Ingestion | Yard inspections (last 14 days) + same-day model outputs |
| `thermoreg_dipping_query(group_ids, timestamp)` | `thermoreg_dipping` (Signal C) | Ingestion | Daily per-yard temperature stats (last 14 days) |
| `auto_review_score_query(group_ids, timestamp)` | `auto_review_score` (Signal D) | Ingestion | 21-day UBF window, earliest per (group, sensor, date) |
| `hu_stats_query(group_ids, timestamp)` | HU stats algorithm | Ingestion | Latest hive update per group for anchor date + ordering |
| `ubf_presence_query(group_ids, timestamp)` | Auto-valid blocker | Ingestion | Groups that have at least one row on `timestamp` |

---

## Per-metric detail

### Signal A — Clipping Diff
**Metric file:** `src/model_monitor/metrics/triage_rules/clipping_diff.py`

```
clipping_diff_query(group_ids, timestamp)
    ↓
clipping_diff(ubf_df)
```

**Query output columns used:**

| Column | Role |
|---|---|
| `group_id` | Group identifier |
| `sensor_mac_address` | One row per sensor |
| `pred_raw` | Raw model prediction |
| `pred_clipped` | Clipped model prediction |

**Metric computation:** `avg(abs(pred_raw - pred_clipped))` across all sensors.
Fails when result > `CLIPPING_DIFF_THRESHOLD = 1.0`.

---

### Signal B — Inspection Discrepancy
**Metric file:** `src/model_monitor/metrics/triage_rules/inspection_discrepancy.py`

```
inspection_signal_query(group_ids, timestamp)
    ↓  split by source column
    ├─ source='inspection'  →  inspections_df
    └─ source='model'       →  model_df
         ↓
inspection_discrepancy(inspections_df, model_df)
```

**Query output columns used:**

| Column | Role |
|---|---|
| `source` | `'inspection'` or `'model'` — split before passing to metric |
| `group_id` | Group identifier |
| `bee_frames_distribution` | JSON string parsed into avg bee_frames (inspection rows) |
| `numerical_model_result` | Per-sensor model output (model rows) |

**Metric computation:** `abs(mean(inspection_avg) - mean(model_avg))`.
Fails when discrepancy > `INSPECTION_GAP_THRESHOLD = 1.5`.

---

### Signal C — Thermoregulation Dipping
**Metric file:** `src/model_monitor/metrics/triage_rules/thermoreg_dipping.py`

```
thermoreg_dipping_query(group_ids, timestamp)
    ↓
thermoreg_dipping(yard_daily_df)
```

**Query output columns used:**

| Column | Role |
|---|---|
| `group_id` | Group identifier |
| `yard_id` | Yard identifier |
| `yard_name` | Human-readable yard label |
| `date` | Calendar day |
| `temp_std` | Average temperature std across sensors in that yard on that day |

**Metric computation:** classify each yard's `temp_std` trend over 14 days
(`dipping` / `recovering` / `volatile` / `stable`).
Fails when `(dipping yards / all classified yards) × 100 > DIPPING_YARD_PCT_THRESHOLD = 15.0 %`.

---

### Signal D — Auto Review Score
**Metric file:** `src/model_monitor/metrics/triage_rules/auto_review_score.py`

```
auto_review_score_query(group_ids, timestamp)
    ↓
auto_review_score(ubf_df, timestamp)
```

**Query output columns used:**

| Column | Role |
|---|---|
| `group_id` | Group identifier |
| `sensor_mac_address` | Sensor identifier |
| `input_date` | Calendar day of the prediction |
| `pred_raw` | Raw bee_frames prediction |

**Metric computation:** 7 features derived from the most-recent 7-day sub-window,
scored with the SPECS.md weighted formula. `pass_metric=None` when data is
insufficient (< 50 rows, < 3 daily aggregates, or any day has < 10 readings).
Fails when `score ≥ AUTO_REVIEW_THRESHOLD = 2.4`.

---

## Decision-engine queries (not consumed by a metric function)

### `candidates_query`
Called once at the start of each run to build the candidate set.
Returns `group_id`, `group_name`, `deployment_timestamp`, `days_since_deployment`.

### `validation_history_query`
Called for:
1. **`must_review` check** — filter rows where `review_date == yesterday` and `tier2_status == 'invalid'`.
2. **Prior-invalid blocker** — find latest invalid date per group, then compare against HU anchor.
3. **HU stats `compute_anchor_from_reviews`** — full history per group to walk back the invalid streak.

### `hu_stats_query`
Called for:
1. **`must_review` row ordering** — sort by latest valid HU date ascending.
2. **Prior-invalid blocker** — check whether a newer valid HU date exists after the latest invalid review.

### `ubf_presence_query`
Called once after signals are applied to check whether auto-valid candidates
have same-day data. Groups absent from the result → `needs_review (no_data)`.

---

## Calling pattern (pseudocode)

```python
from skills.Calibration_Review_Triage.scripts.queries import (
    candidates_query,
    validation_history_query,
    clipping_diff_query,
    inspection_signal_query,
    thermoreg_dipping_query,
    auto_review_score_query,
    hu_stats_query,
    ubf_presence_query,
)
from skills.data_lake.scripts.connection import read_curated

timestamp   = "2026-05-03"
candidates  = read_curated(candidates_query(timestamp))
group_ids   = candidates["group_id"].tolist()

# Signal A
ubf_df       = read_curated(clipping_diff_query(group_ids, timestamp))
# → clipping_diff(ubf_df) per group

# Signal B
insp_raw     = read_curated(inspection_signal_query(group_ids, timestamp))
insp_df      = insp_raw[insp_raw["source"] == "inspection"]
model_df     = insp_raw[insp_raw["source"] == "model"]
# → inspection_discrepancy(insp_df, model_df) per group

# Signal C
yard_df      = read_curated(thermoreg_dipping_query(group_ids, timestamp))
# → thermoreg_dipping(yard_df) per group

# Signal D
ubf_long_df  = read_curated(auto_review_score_query(group_ids, timestamp))
# → auto_review_score(ubf_long_df, timestamp) per group

# Blockers
ubf_present  = read_curated(ubf_presence_query(group_ids, timestamp))
val_history  = read_curated(validation_history_query(group_ids, timestamp))
hu_stats     = read_curated(hu_stats_query(group_ids, timestamp))
```
