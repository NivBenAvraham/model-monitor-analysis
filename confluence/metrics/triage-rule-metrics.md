# Calibration Review Triage — Rule & Metrics

**Repo:** `model-monitor-analysis`  
**Layer:** Calibration Review Triage  
**Decision file:** `src/model_monitor/decision/triage_rule.py`  
**Metrics dir:** `src/model_monitor/metrics/triage_rules/`  
**Queries:** `skills/Calibration Review Triage/scripts/queries.py`  
**Spec:** `skills/Calibration Review Triage/spec/SPECS.md`  
**Last updated:** 2026-05-13

---

## Overview

The Calibration Review Triage decides which stale production beekeeper groups need a human reviewer and which can be automatically approved.

A group is **stale** when it has been in `PRODUCTION` status for at least `STALE_DAYS_THRESHOLD = 3` days without a review.

The system runs daily against all stale production groups and produces one output row per group:

| Review bucket | Status | Meaning |
|---|---|---|
| **1 — must_review** | needs_review | Yesterday's tier2 review was `invalid` — escalate immediately |
| **2 — needs_review** | needs_review | At least one risk signal fired, or an auto-valid blocker is active |
| **3 — auto_valid** | valid | All 4 signals had data, none fired, and yesterday's status was `valid` |

The triage rule implemented in this repo (`triage_rule.py`) covers the **auto_valid vs. needs_review** decision (buckets 2 and 3). `must_review` (bucket 1) is resolved upstream by the caller before invoking the rule.

---

## Decision Flow

```
Input: 4 signal results + last_status (from yesterday)

STEP 1 — Count signal coverage
  passed_metrics = signals where pass_metric is not None
  (True and False both count — only None / insufficient data is excluded)
  Maximum = 4

STEP 2 — Decide
  auto_valid   when:  passed_metrics == 4  AND  last_status == 'valid'
  needs_review otherwise

Output: auto_valid | needs_review  +  reason string
```

### Why coverage counts, not pass/fail

A signal returning `False` means it fired — the group has a problem, and the caller should handle it as `needs_review` via the signal itself. A signal returning `None` means there was not enough data to evaluate. The decision step only checks that all 4 signals had enough data to run and that the prior status was clean. The individual signal values determine which `needs_review` reason is reported.

### last_status

`last_status` is the `tier2_status` from **exactly yesterday** (`reference_date − 1 day`), returned by the validation-history query. Only `'valid'` enables `auto_valid`. Any other value (`'invalid'`, `'needs_recalibration'`, `None`) keeps the group in `needs_review`.

---

## Auto-Valid Blockers

Even when all 4 signals return a result and `last_status == 'valid'`, two additional blockers can prevent `auto_valid`:

| Blocker | Trigger | Output reason |
|---|---|---|
| **No same-day UBF data** | Group has no Unified Bee Frames row on `reference_date` | `no_data` |
| **Prior invalid not superseded** | Group has a previous tier2 `invalid` review and no hive update after that date | `prior_tier2_invalid(<date>); latest_hu=<date>` |

These blockers are applied by the caller after receiving the triage rule output, not inside `triage_rule.py` itself.

---

## Part 1 — Signal Reference

All 4 signals feed directly into the decision step. Each signal returns `pass_metric=True` (no issue), `pass_metric=False` (issue found), or `pass_metric=None` (insufficient data).

---

### Signal A — Clipping Diff

**File:** `src/model_monitor/metrics/triage_rules/clipping_diff.py`  
**Query:** `clipping_diff_query()` in `queries.py`

**Business target:** Catch groups where the model's raw prediction has drifted far from the clipped "safe zone" output. A large average gap between `pred_raw` and `pred_clipped` means the model is pushing against its clipping boundaries — a sign of calibration drift before it becomes visible to users.

**What it checks:** Average absolute difference between raw and clipped prediction per sensor, on the examination date only (same-day, no lookback).

**Algorithm:**
1. Receive one row per sensor: latest `pred_raw` and `pred_clipped` on `reference_date`.
2. Compute `avg(abs(pred_raw - pred_clipped))` across all sensors.
3. `pass_metric=False` when `avg_clip_diff > CLIPPING_DIFF_THRESHOLD`.

**Threshold:** `CLIPPING_DIFF_THRESHOLD = 1.0 bee_frames`

**Data window:** Same-day only (`reference_date`)



---

### Signal B — Inspection Discrepancy

**File:** `src/model_monitor/metrics/triage_rules/inspection_discrepancy.py`  
**Query:** `inspection_signal_query()` in `queries.py`

**Business target:** Detect groups where field inspectors' physical bee_frames counts disagree significantly with the model's same-day output. Inspectors physically count bees — when they disagree with the model by more than 1.5 frames on average, the model is likely mis-calibrated for that group.

**What it checks:** The gap between the mean of recent manual inspection bee_frames averages and the mean of same-day model outputs.

**Algorithm:**
1. Receive inspection rows from the last `INSPECTION_LOOKBACK_DAYS = 14` days with a parsed `avg_bee_frames` per inspection.
2. Receive same-day model output per sensor (`numerical_model_result`).
3. Compute `inspection_avg = mean(parsed averages)` and `model_avg = mean(model results)`.
4. `pass_metric=False` when `abs(inspection_avg - model_avg) > INSPECTION_GAP_THRESHOLD`.
5. If no inspections exist in the window, `pass_metric=True` (no signal — cannot fire).

**Threshold:** `INSPECTION_GAP_THRESHOLD = 1.5 bee_frames`

**Note:** The standalone inspection monitor flags at > 1.0, but the triage system uses a higher threshold of 1.5 to reduce noise.

**Data window:** Last 14 days for inspections (`INSPECTION_LOOKBACK_DAYS`), same-day for model output.

---

### Signal C — Thermoregulation Dipping

**File:** `src/model_monitor/metrics/triage_rules/thermoreg_dipping.py`  
**Query:** `thermoreg_dipping_query()` in `queries.py`

**Business target:** Catch groups where too many yards are showing increasing temperature dispersion over time — a sign that hive thermoregulation is breaking down. A healthy colony holds its temperature stable; a declining or mis-calibrated colony shows rising sensor spread.

**What it checks:** The percentage of a group's yards classified as "dipping" (temperature standard deviation trending upward).

**Algorithm:**
1. Receive yard-level daily temperature stats over the last `THERMOREG_LOOKBACK_DAYS = 14` days.
2. For each yard, classify the dispersion trend:

| Class | Condition |
|---|---|
| `recovering` | slope < −0.03 AND peak before 60% of window |
| `dipping` | slope > 0.03 AND trough before 60% of window |
| `volatile` | \|slope\| > 0.02 AND std of stds > 0.15 |
| `stable` | everything else |
| `insufficient_data` | fewer than 4 data points |

3. `dip_pct = 100 × (dipping yards) / (all classified yards)`.
4. `pass_metric=False` when `dip_pct > DIPPING_YARD_PCT_THRESHOLD`.
5. If all yards are `insufficient_data`, `pass_metric=None`.

**Threshold:** `DIPPING_YARD_PCT_THRESHOLD = 15.0 %`

**Data window:** Last 14 days (`THERMOREG_LOOKBACK_DAYS`)

**Why 14 days matters:** Trend detection requires enough historical points per yard. With only 7 days, borderline yards that previously had insufficient data (returning `None`) can resolve to a misleading `stable` classification — producing 4 additional False Positives in empirical testing. 14 days is the minimum reliable window.

---

### Signal D — Auto Review Score

**File:** `src/model_monitor/metrics/triage_rules/auto_review_score.py`  
**Query:** `auto_review_score_query()` in `queries.py`

**Business target:** Detect groups with unstable or anomalous raw prediction behaviour using a composite score across 7 stability features. Groups whose model output is erratic, trending, or inconsistent across sensors over the past weeks are at higher risk of calibration problems.

**What it checks:** A composite score (0 to ~7) computed from 7 features derived from the last 21 days of Unified Bee Frames data. A score ≥ 2.4 means the model's prediction behaviour is unstable.

**Algorithm:**

Full window: `AUTO_REVIEW_LOOKBACK_DAYS = 21` days → used for `sensor_temporal_cv` only.  
Feature window: last `AUTO_REVIEW_RECENT_DAYS = 7` days → used for all other features.

Minimum requirements for the recent window:
- ≥ 50 rows
- ≥ 3 daily aggregates
- each usable day must have ≥ 10 rows

**7 features:**

| Feature | Description |
|---|---|
| `detrended_vol` | Range of residuals after removing the linear trend from daily means |
| `median_tail` | Median of (daily median − daily p5) — captures tail skew |
| `cv_floor` | Minimum daily coefficient of variation |
| `cv_trend` | Slope of daily CV over time |
| `cv_range` | Max daily CV − min daily CV |
| `sensor_temporal_cv` | Median per-sensor CV over the **full 21-day window** |
| `cv_volatility` | Standard deviation of daily CVs |

**Scoring formula:**

```
score = (
    min(max(cv_floor − 0.20, 0) / 0.09, 2.5)
    + min(max(detrended_vol − 0.5, 0) / 2.5, 1.0)
    + min(max(median_tail − 4.5, 0) / 3.0, 1.0)
    + min(max(cv_trend − (−0.003), 0) / 0.008, 1.0) × 0.8
    + min(max(cv_range − 0.03, 0) / 0.09, 1.0) × 0.8
    + min(max(sensor_temporal_cv − 0.09, 0) / 0.03, 1.0) × 0.30
    + min(max(cv_volatility − 0.025, 0) / 0.02, 0.5)
)
```

`pass_metric=False` when `score ≥ AUTO_REVIEW_THRESHOLD = 2.4`.  
`pass_metric=None` when insufficient data.

**Thresholds:** `AUTO_REVIEW_THRESHOLD = 2.4`, `AUTO_REVIEW_LOOKBACK_DAYS = 21`, `AUTO_REVIEW_RECENT_DAYS = 7`

**Why the full 21-day window for `sensor_temporal_cv`:** Per-sensor CV is a measure of how consistently each sensor behaves over time. A 7-day window is too short and produces a noisy, typically lower estimate — empirically, cutting to 7 days flipped 18 groups from `False → True` (score dropped below threshold), weakening the signal's ability to catch unstable groups.

---

## Part 2 — Decision Rule

**File:** `src/model_monitor/decision/triage_rule.py`

### How it works

```
Input:  list of 4 metric result dicts  +  last_status (str | None)

Step 1 — Count signal coverage
  passed_metrics = count(signals where pass_metric is not None)
  True counts. False counts. Only None does not count.

Step 2 — Decide
  auto_valid   when:  passed_metrics == 4  AND  last_status == "valid"
  needs_review otherwise

Output dict:
  prediction      : "auto_valid" | "needs_review"
  passed_metrics  : int 0–4
  last_status     : echoed back
  reason          : None | str
  signals         : {signal_name: bool | None}
```

### Reason strings

| Condition | reason |
|---|---|
| All 4 signals have data, last_status is valid | `None` (auto_valid) |
| last_status not valid, signals incomplete | `insufficient_signal_data; last_status_not_valid` |
| Signals incomplete only | `insufficient_signal_data` |
| last_status not valid only | `last_status_not_valid` |

---

## Performance

Evaluated on the 2026 California season ground-truth dataset (643 labelled dates, 51 groups). `needs_recalibration` is treated as `invalid`.

| Metric | Value |
|---|---|
| TP (valid → auto_valid) | 142 |
| FP (invalid → auto_valid) | 69 |
| TN (invalid → needs_review) | 89 |
| FN (valid → needs_review) | 103 |
| **Precision** | **0.673** |
| Recall | 0.580 |

### On the 69 False Positives

All 69 FPs were also checked against the `temperature_health_rule`. The temperature rule is a separate, orthogonal layer and would catch many of these independently — the two rules complement each other.

### Lookback window sensitivity (7d vs. 14/21d)

Reducing all signal lookback windows to 7 days was evaluated as a scenario. Results:

| Scenario | TP | FP | TN | FN | Precision |
|---|---|---|---|---|---|
| **Original (14/21d)** | **142** | **69** | **89** | **103** | **0.673** |
| 7-day cap | 143 | 73 | 85 | 102 | 0.662 |

The 7-day scenario adds 4 FPs with zero offsetting gain. All 4 additional FPs are caused by `thermoreg_dipping` flipping from `None → True` (insufficient 14-day trend data → just enough 7-day data to produce a misleading "stable" classification). The 14-day window for thermoreg and 21-day window for auto_review_score are the right choices.

---

## Threshold Reference

All thresholds are defined as constants inside their respective metric files.

| Signal | Threshold | Lookback window |
|---|---|---|
| Signal A — clipping_diff | `CLIPPING_DIFF_THRESHOLD = 1.0 bee_frames` | Same-day only |
| Signal B — inspection_discrepancy | `INSPECTION_GAP_THRESHOLD = 1.5 bee_frames` | 14 days (inspections) |
| Signal C — thermoreg_dipping | `DIPPING_YARD_PCT_THRESHOLD = 15.0 %` | 14 days |
| Signal D — auto_review_score | `AUTO_REVIEW_THRESHOLD = 2.4` | 21-day full / 7-day recent |
| Rule | `passed_metrics > 3` AND `last_status == 'valid'` | yesterday |
| Candidate staleness | `STALE_DAYS_THRESHOLD = 3 days` | — |
| Historical clipping exemption | `HIST_VALID_WINDOW_DAYS = 14 days` | — |

---

## File Map

| Purpose | File |
|---|---|
| Decision logic | `src/model_monitor/decision/triage_rule.py` |
| Signal A | `src/model_monitor/metrics/triage_rules/clipping_diff.py` |
| Signal B | `src/model_monitor/metrics/triage_rules/inspection_discrepancy.py` |
| Signal C | `src/model_monitor/metrics/triage_rules/thermoreg_dipping.py` |
| Signal D | `src/model_monitor/metrics/triage_rules/auto_review_score.py` |
| Signal C — 7d variant | `src/model_monitor/metrics/triage_rules/thermoreg_dipping_7d.py` |
| Signal D — 7d variant | `src/model_monitor/metrics/triage_rules/auto_review_score_7d.py` |
| Athena SQL queries | `skills/Calibration Review Triage/scripts/queries.py` |
| Query-to-metric mapping | `skills/Calibration Review Triage/spec/queries-to-metrics.md` |
| Full spec | `skills/Calibration Review Triage/spec/SPECS.md` |
| Unit tests | `tests/test_triage_rules.py`, `tests/test_triage_rule.py` |
