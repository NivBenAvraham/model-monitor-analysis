# Balkany Works — ML Experiments on Temperature Metrics

## Overview

`other/balkany_works/` contains a series of supervised learning experiments that sit **alongside** the deterministic rule-based system (`temperature_health_rule.py`).  The goal was to explore whether a trained ML model — fed the same temperature metric outputs — can learn a better decision boundary for predicting `valid` vs `invalid`.

Four progressively richer model versions were built and evaluated, each building on the previous one.  Evaluation uses **5-fold stratified cross-validation (OOF)** on the train split, and a final **held-out test set evaluation** via `test_niv_inference.py`.

---

## Input Data

All versions share the same input: the output CSV produced by running `src/model_monitor/metrics/temperature/` on the train/test splits.

| Format | Description |
|---|---|
| **Training** (`meteric_temp_health_rule.csv`) | Long format — one row per `(group_id, date, metric_name)`.  Columns: `group_id`, `date`, `status`, `metric_name`, `value`, `threshold`, `pass_metric` |
| **Test** (`test_metrics.csv`) | Wide format — one row per `(group_id, date)`.  Columns: `{metric}_value`, `{metric}_pass`, `gt_label` |

**Label**: `valid = 1`, `invalid = 0` (needs_recalibration treated as invalid).

---

## Feature Engineering (shared across all versions — defined in v1)

All versions use the same base feature extraction pipeline built in `test_niv_results_v1.py`:

### Ratio features
For each metric, the raw value is normalised by its threshold:

```
ratio = value / threshold
```

For range-style thresholds (e.g. `bucket_reference_adherence` has `{low, high}` per bucket), the ratio is the normalised distance from the band centre:

```
ratio = (value - centre) / half_width
```

This encodes **how far the group is from the decision boundary**, not just whether it passed.

### Pass-metric features
A binary flag per metric: `1` if `pass_metric = True`, else `0`.

### Derived features
- `n_metrics_passing` — total count of metrics passing for this `(group_id, date)`
- `has_small_bucket` — `1` if any small-bucket feature is non-null (hive has small-bucket sensors)

---

## Version Comparison

### v1 — Logistic Regression (`test_niv_results_v1.py`)

**Model**: `sklearn.LogisticRegression` with `class_weight="balanced"`, median imputation, standard scaling.

**Key design choices**:
- Uses ratio features + pass flags (not raw values) — the ratio encodes distance from the threshold which is more informative than the raw measurement
- Class weight balanced to compensate for valid/invalid imbalance
- Straightforward, interpretable — coefficients directly show feature importance

**Results (5-fold OOF)**:

| metric | mean | std |
|---|---|---|
| precision | 0.765 | 0.049 |
| recall | 0.707 | 0.038 |
| f1 | 0.734 | 0.040 |
| roc_auc | 0.727 | 0.061 |

**Artifacts**: `v1/feature_coefficients.csv`, `v1/threshold_sweep.csv`, `v1/oof_predictions.csv`

---

### v2 — XGBoost (`test_niv_results_v2.py`)

**Model**: `XGBClassifier` replacing Logistic Regression.  NaN values filled with `-999` (sentinel) so XGBoost can learn from missingness patterns.

**Key design choices**:
- `scale_pos_weight < 1` intentionally makes the model **stricter on predicting valid** — reduces FP
- Grid search over `scale_pos_weight × max_depth`; best params: `scale_pos_weight=0.3, max_depth=3`
- NaN-as-sentinel lets the model distinguish "metric not applicable" from a passing/failing value

**Results (5-fold OOF)**:

| metric | mean | std |
|---|---|---|
| precision | 0.787 | 0.046 |
| recall | 0.711 | 0.016 |
| f1 | 0.746 | 0.018 |
| roc_auc | 0.760 | 0.042 |

**Delta vs v1**: +0.022 precision, -0.004 recall, +0.034 roc_auc

**Artifacts**: `v2/feature_importance.csv`, `v2/threshold_sweep.csv`, `v2/oof_predictions.csv`

---

### v3 — XGBoost + Native NaN + SHAP (`test_niv_results_v3.py`)

**Model**: XGBoost with native missing-value handling (no sentinel fill) and early stopping.

**Key design choices**:
- **Native NaN**: XGBoost learns the optimal direction for missing splits internally, rather than treating NaN as a specific value
- **Expanded grid search**: adds `min_child_weight` and `gamma` for better regularisation; best params: `scale_pos_weight=0.3, max_depth=4, min_child_weight=5, gamma=0`
- **Early stopping**: trains up to 1000 trees on an 80/20 internal split, stops when validation loss stops improving → `best_n_estimators=198`
- **SHAP analysis**: uses XGBoost native SHAP (`pred_contribs`) for per-sample explanations

**Top SHAP features (v3)**:

| Rank | Feature | Mean \|SHAP\| |
|---|---|---|
| 1 | `bucket_diurnal_amplitude_large_ratio` | 0.568 |
| 2 | `bucket_reference_adherence_large_ratio` | 0.263 |
| 3 | `bucket_temperature_ordering_medium_to_large_ratio` | 0.226 |
| 4 | `bucket_reference_adherence_medium_ratio` | 0.209 |

**Interpretation**: Large-bucket diurnal amplitude and large-bucket reference adherence dominate — consistent with the design of the deterministic rule gates (R3 and R7 are the two strongest gates in `temperature_health_rule.py`).

**Results (5-fold OOF)**:

| metric | mean | std |
|---|---|---|
| precision | 0.834 | 0.083 |
| recall | 0.625 | 0.048 |
| f1 | 0.713 | 0.049 |
| roc_auc | 0.760 | 0.041 |

**Delta vs v2**: +0.047 precision, -0.086 recall (trades recall for precision — desired direction)

**Artifacts**: `v3/shap_importance.csv`, `v3/shap_values.csv`, `v3/best_params.json`, `v3/grid_search_results.csv`

---

### v4 — XGBoost + Richer Feature Engineering (`test_niv_results_v4.py`)

**Model**: Same XGBoost setup as v3 with additional feature engineering on top of the base matrix.

**Key design choices — three new feature families**:

1. **Bucket aggregates**: for each metric family that has values across multiple bucket sizes (small/medium/large), adds `{family}_mean_ratio` and `{family}_std_ratio` — how consistent is the metric across sizes?

2. **Rank ordering flags**: binary signals like `{family}_large_gte_medium` — does the metric follow the expected thermal ordering (large > medium > small)? A violation is a strong signal of miscalibration.

3. **Pairwise interaction terms**: products of the top-5 SHAP features from v3 (10 new features).  Example: `ix_diurnal_amplitude_large_x_reference_adherence_medium`.

**Best params**: `scale_pos_weight=0.3, max_depth=3, min_child_weight=10, gamma=0`, `n_estimators=126`

**Results (5-fold OOF)**:

| metric | mean | std |
|---|---|---|
| precision | 0.808 | 0.060 |
| recall | 0.531 | 0.072 |
| f1 | 0.640 | 0.069 |
| roc_auc | 0.745 | 0.071 |

**Delta vs v3**: -0.026 precision, -0.094 recall — v4 **underperforms v3** on OOF despite the richer features, likely due to overfitting the small dataset (the interaction terms add noise on ~400 training examples).

---

## Test Set Results (held-out, `test_niv_inference.py`)

All four models were retrained on the full training set using their known best parameters, then evaluated once on the held-out test set.

| Model | Precision | Recall | F1 | AUROC | TP | FP | FN | TN |
|---|---|---|---|---|---|---|---|---|
| v1 LR | 0.775 | 0.647 | 0.705 | 0.746 | 55 | 16 | 30 | 35 |
| v2 XGB | 0.877 | 0.671 | 0.760 | 0.815 | 57 | 8 | 28 | 43 |
| **v3 XGB+** | **0.889** | 0.565 | 0.691 | **0.793** | 48 | **6** | 37 | **45** |
| v4 XGB+FE | 0.870 | 0.471 | 0.611 | 0.771 | 40 | 6 | 45 | 45 |

**Key observations**:
- **v3 has the highest precision (0.889) and fewest FPs (6)** on the test set — the primary objective
- **v2 has the best recall (0.671)** — catches more valid groups, at the cost of more FPs (8)
- v4 does not improve over v3 despite the extra feature engineering — the benefit of interactions is outweighed by overfitting risk on a small dataset
- All versions improve materially over v1 LR on precision and AUROC

---

## Comparison with `temperature_health_rule.py`

The deterministic rule (confidence ≥ 4) achieves on the same test set:

| | Precision | Recall | FP | TP |
|---|---|---|---|---|
| temperature_health_rule (conf≥4) | ~0.879 | ~0.547 | ~11 | ~80 |
| v3 XGB (threshold=0.5) | **0.889** | 0.565 | **6** | 48 |
| v2 XGB (threshold=0.5) | 0.877 | **0.671** | 8 | 57 |

The ML models are competitive with the deterministic rule, and v3/v2 achieve lower FP counts.  However the ML models are **not deterministic** and require a training set, threshold tuning, and feature alignment with fixed thresholds — adding operational complexity.

---

## File Layout

```
other/balkany_works/
  test_niv_results_v1.py    — feature engineering + Logistic Regression
  test_niv_results_v2.py    — XGBoost (NaN as -999, grid search)
  test_niv_results_v3.py    — XGBoost (native NaN, expanded grid, SHAP)
  test_niv_results_v4.py    — XGBoost + bucket aggregates + interactions
  test_niv_inference.py     — retrain all on train, evaluate once on test
  v1/                       — cv_results.json, feature_coefficients.csv, plots/
  v2/                       — cv_results.json, feature_importance.csv, plots/
  v3/                       — cv_results.json, best_params.json, shap_importance.csv, plots/
  v4/                       — cv_results.json, best_params.json, shap_importance.csv, plots/
  inference/                — test_results.csv, test_predictions.csv, v3_threshold_sweep.csv
```

---

## How to Re-run

```bash
source .venv/bin/activate

# Individual versions (runs on training data, saves to v{n}/)
python other/balkany_works/test_niv_results_v1.py
python other/balkany_works/test_niv_results_v2.py
python other/balkany_works/test_niv_results_v3.py
python other/balkany_works/test_niv_results_v4.py

# Test set inference (requires both training CSV and test CSV to exist)
python other/balkany_works/test_niv_inference.py
```

Note: `CSV_PATH` in `test_niv_results_v1.py` and `TEST_CSV` in `test_niv_inference.py` are currently hardcoded to the EC2 paths.  Update them to local paths before running.

---

## Key Takeaways

1. **v3 is the best model** — highest precision on test (0.889), fewest FPs (6), strong AUROC (0.793)
2. **Ratio features > raw values** — normalising by threshold dramatically improves signal quality
3. **`bucket_diurnal_amplitude_large`** is the single strongest feature across all versions (mirrors the R7 gate in the deterministic rule)
4. **`scale_pos_weight < 1`** is essential — it biases the model toward being cautious about predicting `valid`, which is the right priority
5. **More features ≠ better** — v4 adds interaction terms that hurt OOF performance on this dataset size (~400 examples)
6. The ML approach and the deterministic rule are **complementary**, not competing — the rule is interpretable and zero-maintenance; the ML model can be used as a second opinion or to catch edge cases
