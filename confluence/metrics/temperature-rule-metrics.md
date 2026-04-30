# Temperature Rule Metrics

**Repo:** `model-monitor-analysis`  
**Layer:** Group Model Temperature Health (Layer 2)  
**Last updated:** 2026-04-29

---

## Overview

The temperature metrics evaluate whether a BeeFrame model prediction is reliable for a given beekeeper group on a given date. They operate on 2-day windows of raw sensor (hive) and gateway (ambient) temperature data.

There are **10 metrics** in total, split into two roles inside the decision rule:

| Role | Metrics | Behaviour |
|---|---|---|
| **Hard gate** (4) | ATV, R3, R5, R7 | Any failure → INVALID immediately |
| **Scored** (6) | R1, R2, R4, R6a, R6b, R6c | Need ≥ 5/6 to pass for VALID |

All thresholds live in `configs/thresholds.yaml` — never hardcoded.

---

## Hive Size Buckets

The metrics operate per bucket. Bucket assignment comes from `bee_frames`:

| Bucket | bee_frames | Thermal behaviour |
|---|---|---|
| `small` | < 6 | Tracks ambient — little thermoregulation |
| `medium` | 6 – 9 | Partial regulation — moderately warm |
| `large` | ≥ 10 | Active thermoregulation — stable ~34 °C |

---

## Part 1 — Metric Reference

---

### ATV — Ambient Temperature Volatility *(Hard Gate)*

**Business target:** Detect days where the weather shifted so drastically between nights that no temperature-based metric can be trusted.

**What it checks:** The coldest nightly temperature for each day in the window. If the jump between any two consecutive nights exceeds the threshold, the ambient environment is considered too unstable to evaluate the model.

**Algorithm:**
1. For each gateway and clock-hour, compute the minimum temperature.
2. Per calendar day, find the coldest hour across all gateways.
3. For each consecutive day pair, compute the absolute difference.
4. If any pair's delta ≥ threshold → `volatile=True` → `pass_metric=False`.

**Threshold:** `MIN_DAILY_DELTA_CELSIUS = 5.0 °C`

**Why it matters:** Even a perfectly healthy large hive will look unstable if the outside temperature swings 15 °C overnight. This gate prevents evaluating the model under physically meaningless conditions.

---

### R1 — Ambient Stability *(Scored)*

**Business target:** Confirm that the ambient (gateway) signal is smooth enough to serve as a meaningful reference baseline.

**What it checks:** The Coefficient of Variation (CV = std / |mean|) of all hourly ambient readings in the window. High CV means the gateway signal is erratic — not a normal diurnal pattern.

**Algorithm:**
1. Resample gateway readings to 1-hour means.
2. Compute CV across all hourly values.
3. `pass_metric=False` if CV > 0.55 (primary threshold).

**Threshold:** CV ≤ 0.55

**Why it matters:** R1 and R2 are sanity checks on the ambient data quality rather than on the model prediction itself. They carry low discriminating power between valid and invalid groups (invalid groups pass at ≈ 100%) and are scored with equal weight alongside stronger metrics.

---

### R2 — Ambient Range *(Scored)*

**Business target:** Reject evaluation windows where the ambient temperature is outside a physically plausible range.

**What it checks:** Whether the minimum and maximum hourly ambient readings fall within [5 °C, 50 °C].

**Algorithm:**
1. Resample gateway readings to 1-hour means.
2. `pass_metric=False` if min < 5 °C or max > 50 °C.

**Thresholds:** `min_celsius = 5.0`, `max_celsius = 50.0`

**Why it matters:** Below 5 °C bees are largely inactive; above 50 °C is almost certainly a sensor error. Either extreme makes model evaluation meaningless. Like R1, this is an ambient data quality gate — invalid groups pass it at 100%, so it only affects recall, not precision.

---

### R3 — Bucket Reference Adherence *(Hard Gate)*

**Business target:** Verify that each hive-size bucket's mean internal temperature stays inside the expected thermal band for that class.

**What it checks:** Per-bucket mean temperature vs. per-bucket reference bands calibrated on analyst-labelled anchor examples.

**Algorithm:**
1. Resample sensor readings to 1-hour means.
2. For each bucket, compute the mean temperature across all sensors and hours.
3. `pass_metric=False` if any bucket mean falls outside its `[low, high]` band.

**Thresholds (from `configs/thresholds.yaml`):**

| Bucket | low | high |
|---|---|---|
| small | 17.4 °C | 29.0 °C |
| medium | 27.3 °C | 32.0 °C |
| large | 33.9 °C | 35.0 °C |

**Why it's a gate:** The large bucket band (33.9–35.0 °C) provides a clean gap between all perfect-valid anchors (≥ 33.95 °C) and all perfect-invalid anchors (≤ 33.45 °C). It is the single strongest discriminator in the full metric set, catching 28 invalids that no other gate catches alone.

---

### R4 — Sensor Spread Within Bucket *(Scored)*

**Business target:** Detect when sensors in the same size class have wildly different temperatures, indicating wrong bucket assignments or corrupted data.

**What it checks:** The standard deviation of per-sensor mean temperatures within each bucket.

**Algorithm:**
1. Resample sensor readings to 1-hour means.
2. For each bucket, compute the mean temperature per sensor (across hours).
3. Compute std across sensors within the bucket.
4. `pass_metric=False` if std > per-bucket cap.

**Thresholds:**

| Bucket | cap |
|---|---|
| small | 8.0 °C |
| medium | 8.0 °C |
| large | 1.05 °C |

**Why the large cap is tight:** A genuine large hive has sensors reading very similar temperatures because they are all inside the brood cluster. A spread above 1.05 °C in the large bucket was a clean separator on the anchor set.

---

### R5 — Bucket Temporal Stability *(Hard Gate)*

**Business target:** Ensure that each bucket's temperature profile is consistent day-over-day — not drifting or collapsing across the evaluation window.

**What it checks:** The standard deviation of the per-bucket daily mean temperature across calendar days.

**Algorithm:**
1. Resample sensor readings to 1-hour means.
2. For each bucket, compute one daily mean (mean across all sensors and hours for that day).
3. Compute std of daily means across days.
4. `pass_metric=False` if std > per-bucket cap.

**Thresholds:**

| Bucket | std_max |
|---|---|
| small | 2.0 °C |
| medium | 0.95 °C |
| large | 0.20 °C |

**Why it's a gate:** R5 is complementary to R7 — R5 catches multi-day drift while R7 catches within-day swings. Together they close a blind spot: a hive swinging 20 °C every day in a perfectly repeating cycle has R5 ≈ 0 (no day-to-day drift) but fails R7.

---

### R7 — Bucket Diurnal Amplitude *(Hard Gate)*

**Business target:** Detect "fake large" hives — groups predicted as large but whose temperatures swing wildly within each day, proving they are not actually thermoregulating.

**What it checks:** The mean of (max − min) temperature within each calendar day, per bucket.

**Algorithm:**
1. Resample sensor readings to 1-hour means.
2. For each bucket and calendar day, compute (max − min) across all sensor-hour readings.
3. Take the mean of those daily amplitudes.
4. `pass_metric=False` if mean amplitude > per-bucket cap.

**Thresholds:**

| Bucket | cap |
|---|---|
| small | 40.0 °C (loose — small hives naturally track ambient) |
| medium | 25.0 °C |
| large | 14.0 °C |

**Calibration evidence:** Perfect-valid anchors had large bucket amplitude 2–10 °C; perfect-invalid anchors had 17–35 °C. The clean gap at 14 °C catches 5/5 perfect-invalid anchors with zero valid loss.

**Why R5 doesn't catch this:** R5 computes a daily mean then takes std across days — a hive swinging 20 °C every day in a repeating pattern has near-zero R5 but fails R7 immediately.

---

### R6a — Small Hive Ambient Tracking *(Scored)*

**Business target:** Confirm that sensors labelled "small" actually behave like small hives — following the outdoor temperature rather than maintaining a stable warm interior.

**What it checks:** Pearson correlation between the hourly small-bucket mean and the ambient (gateway) hourly series.

**Algorithm:**
1. Resample sensor and gateway readings to 1-hour means.
2. Compute Pearson r between the small-bucket mean series and the ambient series.
3. `pass_metric=False` if r < 0.30. If correlation cannot be computed, pass by default.

**Threshold:** r ≥ 0.30

---

### R6b — Large Hive Thermoregulation *(Scored)*

**Business target:** Confirm that sensors labelled "large" are actually thermoregulating — maintaining a stable internal temperature independently of the outdoor temperature.

**What it checks:** Pearson correlation between the hourly large-bucket mean and the ambient series. A strong positive correlation means the large hive is tracking ambient like a small hive — a red flag.

**Algorithm:**
1. Resample sensor and gateway readings to 1-hour means.
2. Compute Pearson r between the large-bucket mean series and the ambient series.
3. `pass_metric=False` if r > 0.85. If correlation cannot be computed, pass by default.

**Threshold:** r ≤ 0.85

---

### R6c — Bucket Temperature Ordering *(Scored)*

**Business target:** Verify the fundamental physical ordering: small hives should be cooler than medium hives, which should be cooler than large hives.

**What it checks:** Whether mean(small) < mean(medium) < mean(large) with at least a minimum gap between adjacent buckets.

**Algorithm:**
1. Resample sensor readings to 1-hour means.
2. Compute mean temperature per bucket.
3. For each adjacent pair (small→medium, medium→large), check that gap ≥ 1.5 °C.
4. `pass_metric=False` if any gap is violated.

**Threshold:** min gap = 1.5 °C

---

## Part 2 — Decision Rule

**File:** `src/model_monitor/decision/temperature_health_rule.py`

### How it works

```
Input: 10 metric results for one (group_id, date)

STEP 1 — 4 Hard Gates
  ATV + R3 + R5 + R7 must ALL pass
  Any failure → INVALID (confidence 1) — stop, no scoring

STEP 2 — Score 6 remaining metrics
  R1, R2, R4, R6a, R6b, R6c
  valid_score = pass_count / 6
  ≥ 5/6 (score ≥ 0.833) → VALID
  ≤ 4/6 (score < 0.833) → INVALID

Output: VALID or INVALID
```

The **effective threshold is 5/6** — both confidence 4 (5/6) and confidence 5 (6/6) output VALID. Confidence is informational only.

### Why 5/6 and not 6/6

The 6 scored metrics have near-zero discriminating power on the remaining False Positives — all 11 train FPs pass every scored metric at 100%. Requiring all 6 gives 72 TPs; allowing one miss (5/6) recovers 8 TPs at zero precision cost.

### Gate selection rationale

The 4 gates were chosen via exhaustive anchor-constrained search across all 847 gate combinations (1–7 gates from 10 metrics). Only combinations perfectly separating all 19 analyst anchor pairs (14 valid, 5 invalid) were kept (438). Best result on train:

- `ATV + R3 + R5 + R7` with 5/6 scored → **TP=80, FP=11, Precision=0.879, Specificity=0.929**

---

### Performance

Evaluated on the 2026 California season. Train/test split is 75/25, stratified by status.

| Split | Pairs | TP | FP | TN | FN | Precision | Specificity | Recall |
|---|---|---|---|---|---|---|---|---|
| **Train** | 423 | 80 | 11 | 143 | 189 | **0.879** | **0.929** | 0.297 |
| **Test** | 136 | 19 | 4 | 47 | 66 | **0.826** | **0.922** | 0.224 |

**Note on recall:** The low recall is by design. The goal is maximum precision when predicting VALID — we accept many False Negatives in exchange for near-certainty on True Positives. When the rule says VALID, it is correct 88% of the time on train and 83% on unseen test data.

### Test split — False Positives detail

All 4 test FPs scored a **perfect 1.0** across all 6 scored metrics and passed all 4 gates — the rule has no available signal to distinguish them from truly valid groups.

| group_id | date | confidence | score |
|---|---|---|---|
| 484 | 2026-02-26 | 5 | 1.000 |
| 1618 | 2026-02-22 | 5 | 1.000 |
| 1730 | 2026-02-26 | 5 | 1.000 |
| 1730 | 2026-03-12 | 5 | 1.000 |

### Irreducible floor

11 train FPs and 4 test FPs pass **all 10 metrics** — they are completely invisible to the current metric set. No threshold change or gate rearrangement can remove them without losing True Positives. Future improvement requires either new data signals or analyst review of those specific groups.

---

## Threshold reference

All thresholds: `configs/thresholds.yaml`  
History of changes: `configs/thresholds_history/`

| Metric | Key threshold |
|---|---|
| ATV | `MIN_DAILY_DELTA_CELSIUS = 5.0 °C` |
| R1 | `cv_threshold = 0.55` |
| R2 | `min_celsius = 5.0`, `max_celsius = 50.0` |
| R3 | large: `[33.9, 35.0]`, medium: `[27.3, 32.0]`, small: `[17.4, 29.0]` |
| R4 | large: `1.05 °C`, medium: `8.0 °C`, small: `8.0 °C` |
| R5 | large: `0.20 °C`, medium: `0.95 °C`, small: `2.0 °C` |
| R7 | large: `14.0 °C`, medium: `25.0 °C`, small: `40.0 °C` |
| R6a | `min_correlation = 0.30` |
| R6b | `max_correlation = 0.85` |
| R6c | `min_gap_celsius = 1.5 °C` |
| Rule | `l1_min_pass_rate = 0.85`, `score_confidence_4_min = 0.833` |
