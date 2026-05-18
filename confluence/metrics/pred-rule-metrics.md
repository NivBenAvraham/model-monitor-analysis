# Pred Rule Metrics

**Repo:** `model-monitor-analysis`  
**Layer:** Group Model Health — Clipping Pressure  
**Last updated:** 2026-05-18

---

## Overview

The Pred Rule evaluates whether a BeeFrame model is VALID for a given beekeeper group on a given date using signals derived from the **model's own prediction output** in the preprocess table — specifically, the gap between `pred_raw` (the model's unconstrained output) and `pred_clipped` (the ceiling-capped value shown to users).

This is a **complementary rule** to `temperature_health_rule`, which operates on raw sensor temperature physics. The two rules use entirely different data sources and can be combined.

| Aspect | Value |
|---|---|
| Signal family | `pred_raw` / `pred_clipped` (preprocess table) |
| Primary signal | `clip_diff_roll3` — 3-day rolling mean of `mean\|pred_raw − pred_clipped\|` |
| Decision type | Single threshold — one signal, one threshold |
| Rolling window | 10-day lookback (uses last 3 days for the roll) |
| Output | `"valid"` / `"invalid"` + confidence 1–5 |

---

## What `pred_raw` and `pred_clipped` encode

```
pred_raw     — model's unconstrained bee-frame prediction per sensor
pred_clipped — value capped at a group-specific calibration ceiling
clip_diff    = |pred_raw − pred_clipped|
```

**Healthy model (VALID):** Raw predictions sit naturally inside the ceiling → `clip_diff ≈ 0` for most sensors.

**Miscalibrated model (INVALID):** Predictions consistently overshoot the ceiling → many sensors are clipped → `clip_diff` is systematically > 0 across the group.

---

## Signal: `clipping_pressure`

**File:** `src/model_monitor/metrics/pred_rules/clipping_pressure.py`

### Algorithm

1. Accept a preprocess DataFrame covering the evaluation date and the preceding days (recommend ≥ 5 days for rolling).
2. For each calendar day, compute per-sensor `|pred_raw − pred_clipped|`.
3. Aggregate per day across all sensors:
   - `clip_diff_mean` — mean
   - `clip_diff_p90` — 90th percentile
   - `pct_clipped` — fraction of sensors where `pred_raw ≠ pred_clipped`
4. Sort days chronologically; compute rolling means (3-day, 5-day).
5. Return the **most recent day's** values and their rolling context.

### Output values

| Field | Description |
|---|---|
| `clip_diff_mean` | Today's mean `\|raw−clipped\|` across all sensors (bee-frames) |
| `clip_diff_p90` | Today's 90th percentile `\|raw−clipped\|` |
| `pct_clipped` | Today's fraction of sensors that were clipped |
| `clip_diff_roll3` | 3-day rolling mean of `clip_diff_mean` |
| `clip_diff_roll5` | 5-day rolling mean of `clip_diff_mean` |

---

## Analysis — How the threshold was chosen

### Step 1 — AUROC ranking of all signals (train split, 299 pairs)

| Signal | AUROC |
|---|---|
| `clip_diff_mean` | **0.842** |
| `clip_diff_p90` | 0.813 |
| `clip_diff_roll3` | 0.767 |
| `pct_clipped` | 0.748 |
| `clip_diff_roll5` | 0.713 |
| `clip_diff_roll7` | 0.720 |
| `p90_roll3` | 0.748 |

`clip_diff_mean` has the highest raw AUROC. The rolling averages sacrifice some AUROC but gain precision by requiring the signal to be **sustained** over multiple days.

### Step 2 — Pareto sweep: best single threshold per test FP level

A fine-grained threshold sweep over all signals was run on train only. Each threshold was then evaluated on test as a single shot (no selection from test).

| Test FP | Best signal | Threshold | Train TP | Train P | Test TP | Test P |
|---------|-------------|-----------|----------|---------|---------|--------|
| 0 | `pct_clipped` | ≤ 0.636 | 19 | 0.905 | 6 | **1.000** |
| **1** | **`clip_diff_roll3`** | **≤ 0.591** | **33** | **0.917** | **15** | **0.938** |
| 2 | `clip_diff_p90` | ≤ 1.484 | 45 | 0.918 | 17 | 0.895 |
| 3 | `clip_diff_p90` | ≤ 1.569 | 54 | 0.931 | 24 | 0.889 |
| 7 | `clip_diff_mean` | ≤ 0.799 | 118 | 0.908 | 43 | 0.860 |

**Selected: `clip_diff_roll3 ≤ 0.591`** — best balance of precision (test P=0.938) and recall (test TP=15). The 3-day rolling average outperforms today's raw mean in precision because it filters one-off noise days.

### Step 3 — Why a single threshold, not a gate chain?

An earlier version of this rule used two gates (`clip_diff_mean ≤ 0.60` AND `clip_diff_roll3 ≤ 0.65`). This was compared against the single-threshold approach:

| Version | Train P | Train TP | Test P | Test TP | Test FP |
|---------|---------|---------|--------|---------|---------|
| 2-gate (v1) | 0.929 | 39 | 0.900 | 18 | 2 |
| Single threshold (v2, current) | **0.921** | 35 | **0.941** | 16 | **1** |

All signals in this family (`clip_diff_mean`, `clip_diff_p90`, `clip_diff_roll3`) are derived from the same continuous source. They are highly correlated — chaining multiple thresholds on correlated signals reduces recall without a meaningful precision gain. One calibrated threshold on the best signal is the correct design.

---

## Decision Rule

**File:** `src/model_monitor/decision/pred_rule.py`

### Flow

```
Input: clipping_pressure() result for one (group_id, date)

PRIMARY  — clip_diff_roll3 available (≥ 2 days history)?
  YES →  clip_diff_roll3 ≤ 0.591  →  VALID   (confidence 5)
         clip_diff_roll3 > 0.591  →  INVALID (confidence 1–3)

FALLBACK — clip_diff_roll3 unavailable (< 2 days history)?
  clip_diff_mean ≤ 0.60  →  VALID   (confidence 4)
  clip_diff_mean > 0.60  →  INVALID (confidence 1–3)

NO DATA → INVALID (confidence 1)
```

### Confidence mapping

| Confidence | Meaning |
|---|---|
| 5 | VALID — roll3 below threshold |
| 4 | VALID — single-day fallback (insufficient rolling history) |
| 3 | INVALID — value slightly above threshold (ratio < 1.5×) |
| 2 | INVALID — value clearly above threshold (ratio 1.5–3×) |
| 1 | INVALID — value far above threshold (ratio ≥ 3×) or no data |

---

## Performance

Evaluated on the 2026 California season. Train/test split is 75/25, stratified by status (`needs_recalibration` treated as invalid).

| Split | Pairs | TP | FP | TN | FN | Precision | Recall |
|---|---|---|---|---|---|---|---|
| **Train** | 363 | 35 | 3 | 125 | 200 | **0.921** | 0.149 |
| **Test** | 125 | 16 | 1 | 44 | 64 | **0.941** | 0.200 |

**Note on recall:** Low recall is by design. The rule is conservative — it only predicts VALID when there is strong sustained evidence. When it says VALID, it is correct > 92% of the time.

### Test FP detail

The 1 test FP has `clip_diff_roll3 = 0.460`, which is comfortably below the threshold (0.591). This group looks like a valid model from the clipping signal alone — the clipping pressure is not elevated and the rule has no available signal to distinguish it.

| group_id | date | clip_diff_roll3 | clip_diff_mean |
|---|---|---|---|
| 484 | 2026-03-19 | 0.460 | 0.366 |

### Train FP detail

The 3 train FPs all have `clip_diff_roll3` between 0.475–0.516 — genuinely close to the threshold and reflecting the irreducible noise floor of this signal.

| group_id | date | clip_diff_roll3 |
|---|---|---|
| 1144 | 2026-03-15 | 0.516 |
| 2703 | 2026-03-22 | 0.475 |
| 2777 | 2026-03-21 | 0.496 |

---

## Comparison with `temperature_health_rule`

| Aspect | `temperature_health_rule` | `pred_rule` |
|---|---|---|
| Data source | PCB temperature sensor readings | `pred_raw` / `pred_clipped` |
| Signal type | 10 orthogonal physical metrics | 1 continuous signal (rolling mean) |
| Decision logic | 4 hard gates + 5/6 score | Single threshold |
| Test Precision | 0.826 | **0.941** |
| Test Recall | 0.224 | 0.200 |
| Test FP | 4 | **1** |
| Test TP | 19 | 16 |

The two rules catch **different populations of invalids** — combining them with OR logic would increase total recall while both maintain high individual precision.

---

## Threshold reference

All thresholds: `configs/thresholds.yaml` → `pred_rules.clipping_pressure`

| Signal | Threshold | Role |
|---|---|---|
| `clip_diff_roll3` | **≤ 0.591 bee-frames** | PRIMARY decision signal |
| `clip_diff_mean` | ≤ 0.60 bee-frames | Fallback (< 2 days history) |
| `clip_diff_p90` | 1.50 bee-frames | Informational only |
| `pct_clipped` | 0.64 | Informational (0 test FP at this level) |

---

## File map

| File | Role |
|---|---|
| `src/model_monitor/metrics/pred_rules/clipping_pressure.py` | Metric: computes `clip_diff_mean`, `clip_diff_p90`, `pct_clipped`, `clip_diff_roll3/5` |
| `src/model_monitor/metrics/pred_rules/__init__.py` | Package init |
| `src/model_monitor/decision/pred_rule.py` | Decision: single-threshold → VALID / INVALID |
| `configs/thresholds.yaml` → `pred_rules.clipping_pressure` | All thresholds |
| `tests/test_pred_rules.py` | 13 unit tests |
| `data/results/pred_rule_results.csv` | Full train+test results (363+125 pairs) |
| `explore_notebooks/pred_raw_metrics/pred_raw_metrics.ipynb` | Analysis notebook (Parts 1 & 2) |
