"""
Evaluate v1–v4 final models on the held-out test set (test_metrics.csv).

Key differences between training and test format:
  - Training: long format (1 row per metric per key), has threshold column
  - Test:     wide format (1 row per key, {metric}_value / {metric}_pass columns)
  - Thresholds are global constants — extracted from training and hard-coded here.
  - Label column: gt_label instead of status.

Each model is retrained on the full training set with its known best params,
then evaluated once on the test set.
"""
import json
import os
import sys

import numpy as np
import pandas as pd
from sklearn.metrics import (
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_niv_results_v1 import (
    CSV_PATH,
    SMALL_BUCKET_FEATURES,
    build_feature_matrix,
    make_pipeline,
    parse_value,
    sanitize_key,
    threshold_sweep,
)
from test_niv_results_v3 import make_xgb
from test_niv_results_v4 import (
    add_bucket_aggregates,
    add_interactions,
    add_rank_ordering_flags,
    build_feature_matrix_v4,
)

TEST_CSV = "/home/ec2-user/repositories/ds_auto-calibration/test_metrics.csv"
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "inference")

# Best params from CV grid search for each version
V2_PARAMS = {"scale_pos_weight": 0.3, "max_depth": 3}
V3_PARAMS = {"scale_pos_weight": 0.3, "max_depth": 4, "min_child_weight": 5, "gamma": 0.0}
V4_PARAMS = {"scale_pos_weight": 0.3, "max_depth": 3, "min_child_weight": 10, "gamma": 0.0}

# n_estimators from early stopping runs
V3_N_EST = 198
V4_N_EST = 126

# CV OOF baselines (for comparison)
CV_BASELINES = {
    "v1": {"precision": 0.782, "recall": 0.715, "f1": 0.747, "roc_auc": 0.742},
    "v2": {"precision": 0.787, "recall": 0.711, "f1": 0.746, "roc_auc": 0.760},
    "v3": {"precision": 0.834, "recall": 0.625, "f1": 0.713, "roc_auc": 0.760},
    "v4": {"precision": 0.808, "recall": 0.531, "f1": 0.640, "roc_auc": 0.745},
}

# Global thresholds — verified as constants (1 unique value per metric across all training rows)
THRESHOLDS = {
    "ambient_range":                  {"min": 5.0, "max": 50.0},
    "ambient_stability":              0.55,
    "ambient_temperature_volatility": 5.0,
    "bucket_diurnal_amplitude":       {"small": 40.0, "medium": 25.0, "large": 14.0},
    "bucket_reference_adherence":     {
        "small":  {"low": 17.4,  "high": 29.0},
        "medium": {"low": 27.3,  "high": 32.0},
        "large":  {"low": 33.9,  "high": 35.0},
    },
    "bucket_temperature_ordering":    1.5,
    "bucket_temporal_stability":      {"small": 2.0, "medium": 0.95, "large": 0.2},
    "large_hive_thermoregulation":    0.85,
    "sensor_spread_within_bucket":    {"small": 8.0, "medium": 8.0, "large": 1.05},
    "small_hive_ambient_tracking":    0.3,
}


# ---------------------------------------------------------------------------
# Test feature engineering
# ---------------------------------------------------------------------------

def _apply_ratio_formula(metric_name, val, thr):
    """
    Mirror of expand_ratio_row logic.
    Given a parsed value and threshold, returns {col_name: float}.
    """
    result = {}
    if not isinstance(val, dict):
        try:
            v_f, t_f = float(val), float(thr)
            if t_f != 0:
                result[f"{metric_name}_ratio"] = v_f / t_f
        except (TypeError, ValueError):
            pass
        return result

    for k, v in val.items():
        col = f"{metric_name}_{sanitize_key(k)}_ratio"
        try:
            v_f = float(v)
        except (TypeError, ValueError):
            continue

        if isinstance(thr, dict):
            t_val = thr.get(k)
            if t_val is None:
                continue
            if isinstance(t_val, dict):
                try:
                    low, high = float(t_val["low"]), float(t_val["high"])
                    half = (high - low) / 2
                    if half != 0:
                        result[col] = (v_f - (low + high) / 2) / half
                except (KeyError, TypeError, ValueError):
                    pass
            else:
                try:
                    t_f = float(t_val)
                    if t_f != 0:
                        result[col] = v_f / t_f
                except (TypeError, ValueError):
                    pass
        else:
            try:
                t_f = float(thr)
                if t_f != 0:
                    result[col] = v_f / t_f
            except (TypeError, ValueError):
                pass
    return result


def build_test_features(df_test, train_feature_cols):
    """
    Convert wide-format test CSV → feature matrix aligned to train_feature_cols.
    Any training column absent in the test data is filled with NaN.
    """
    rows = []
    for _, row in df_test.iterrows():
        entry = {}
        for metric, thr in THRESHOLDS.items():
            val_col  = f"{metric}_value"
            pass_col = f"{metric}_pass"

            raw = row.get(val_col, np.nan)
            if pd.isna(raw):
                val = np.nan
            else:
                val = parse_value(raw)

            if not (isinstance(val, float) and np.isnan(val)) and val is not None:
                entry.update(_apply_ratio_formula(metric, val, thr))

            if pass_col in df_test.columns:
                entry[f"{metric}_pass"] = int(bool(row[pass_col]))

        rows.append(entry)

    feat_df = pd.DataFrame(rows)

    # n_metrics_passing — sum of pass columns
    pass_cols = [c for c in feat_df.columns if c.endswith("_pass")]
    feat_df["n_metrics_passing"] = feat_df[pass_cols].sum(axis=1)

    # has_small_bucket — mirrors training logic exactly
    small_present = [c for c in SMALL_BUCKET_FEATURES if c in feat_df.columns]
    feat_df["has_small_bucket"] = (
        feat_df[small_present].notna().any(axis=1).astype(float)
        if small_present else 0.0
    )

    # Align to training column set (fill missing with NaN, preserve order)
    for col in train_feature_cols:
        if col not in feat_df.columns:
            feat_df[col] = np.nan

    return feat_df[train_feature_cols].astype(float)


def build_test_features_v4(df_test, feature_cols_base, feature_cols_v4):
    """Build test features for v4: base → aggregates → ordering → interactions."""
    X = build_test_features(df_test, feature_cols_base).copy()
    X, _ = add_bucket_aggregates(X, feature_cols_base)
    X, _ = add_rank_ordering_flags(X, feature_cols_base)
    X, _ = add_interactions(X, feature_cols_base)
    # Align to training v4 column order (fills any gaps with NaN)
    for col in feature_cols_v4:
        if col not in X.columns:
            X[col] = np.nan
    return X[feature_cols_v4].astype(float)


# ---------------------------------------------------------------------------
# Evaluation helper
# ---------------------------------------------------------------------------

def evaluate(y_true, y_prob, threshold=0.5, label=""):
    y_pred = (y_prob >= threshold).astype(int)
    prec = precision_score(y_true, y_pred, pos_label=1, zero_division=0)
    rec  = recall_score(y_true, y_pred, pos_label=1, zero_division=0)
    f1   = f1_score(y_true, y_pred, pos_label=1, zero_division=0)
    auc  = roc_auc_score(y_true, y_prob)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred).ravel()
    if label:
        print(f"  {label:14s}  prec={prec:.3f}  rec={rec:.3f}  f1={f1:.3f}  auc={auc:.3f}"
              f"  TP={tp} FP={fp} FN={fn} TN={tn}")
    return {"precision": prec, "recall": rec, "f1": f1, "roc_auc": auc,
            "tp": int(tp), "fp": int(fp), "fn": int(fn), "tn": int(tn)}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(OUT_DIR, exist_ok=True)

    # ---- Load data ----
    print(f"Loading training: {CSV_PATH}")
    df_train = pd.read_csv(CSV_PATH)
    print(f"  {len(df_train)} rows")

    print(f"Loading test:     {TEST_CSV}")
    df_test = pd.read_csv(TEST_CSV)
    print(f"  {len(df_test)} rows  |  {df_test['group_id'].nunique()} unique groups")

    y_test = (df_test["gt_label"] == "valid").astype(int)
    print(f"  Test labels — valid: {y_test.sum()}  invalid: {(y_test == 0).sum()}")

    # ---- Build feature matrices ----
    print("\nBuilding training features (base) ...")
    X_train, y_train, feature_cols, _ = build_feature_matrix(df_train)
    print(f"  Train shape: {X_train.shape}")

    print("Building test features (base) ...")
    X_test = build_test_features(df_test, feature_cols)
    print(f"  Test shape:  {X_test.shape}")

    nan_test = X_test.isna().sum()
    nan_nonzero = nan_test[nan_test > 0]
    if len(nan_nonzero):
        print(f"  NaN columns in test ({len(nan_nonzero)}):")
        for col, n in nan_nonzero.items():
            print(f"    {col}: {n}/{len(X_test)}")

    print("Building v4 feature matrices ...")
    X_train_v4, y_train_v4, feature_cols_v4, _ = build_feature_matrix_v4(df_train)
    X_test_v4 = build_test_features_v4(df_test, feature_cols, feature_cols_v4)
    print(f"  Train v4: {X_train_v4.shape}  Test v4: {X_test_v4.shape}")

    # ---- Train & evaluate each model ----
    print("\n=== Test Set Results (threshold=0.5) ===")
    results = {}

    # v1 — Logistic Regression
    pipe_v1 = make_pipeline()
    pipe_v1.fit(X_train, y_train)
    probs_v1 = pipe_v1.predict_proba(X_test)[:, 1]
    results["v1"] = evaluate(y_test, probs_v1, label="v1 LR")

    # v2 — XGBoost, NaN as -999
    model_v2 = make_xgb(**V2_PARAMS, n_estimators=300)
    model_v2.fit(X_train.fillna(-999), y_train)
    probs_v2 = model_v2.predict_proba(X_test.fillna(-999))[:, 1]
    results["v2"] = evaluate(y_test, probs_v2, label="v2 XGB")

    # v3 — XGBoost, native NaN
    model_v3 = make_xgb(**V3_PARAMS, n_estimators=V3_N_EST)
    model_v3.fit(X_train, y_train)
    probs_v3 = model_v3.predict_proba(X_test)[:, 1]
    results["v3"] = evaluate(y_test, probs_v3, label="v3 XGB+")

    # v4 — XGBoost + feature engineering
    model_v4 = make_xgb(**V4_PARAMS, n_estimators=V4_N_EST)
    model_v4.fit(X_train_v4, y_train_v4)
    probs_v4 = model_v4.predict_proba(X_test_v4)[:, 1]
    results["v4"] = evaluate(y_test, probs_v4, label="v4 XGB+FE")

    # ---- Threshold sweep on best CV model (v3) ----
    sweep_df = threshold_sweep(y_test, probs_v3)
    print(f"\n=== Threshold Sweep — v3 (test set) ===\n{sweep_df.to_string(index=False)}")

    # ---- CV vs test comparison ----
    print("\n=== CV (OOF) vs Test Set — metric by metric ===")
    print(f"  {'model':6s}  {'metric':12s}  {'CV':>7}  {'test':>7}  {'delta':>7}")
    for v, res in results.items():
        for m in ["precision", "recall", "f1", "roc_auc"]:
            cv_val   = CV_BASELINES[v][m]
            test_val = res[m]
            diff     = test_val - cv_val
            sign     = "+" if diff >= 0 else ""
            print(f"  {v:6s}  {m:12s}  {cv_val:>7.3f}  {test_val:>7.3f}  {sign}{diff:>6.3f}")

    # ---- Save outputs ----
    results_df = pd.DataFrame(results).T
    results_df.index.name = "model"
    results_df.to_csv(os.path.join(OUT_DIR, "test_results.csv"))

    pred_df = df_test[["group_id", "date", "gt_label"]].copy()
    pred_df["y_true"]  = y_test.values
    pred_df["v1_prob"] = probs_v1.round(4)
    pred_df["v2_prob"] = probs_v2.round(4)
    pred_df["v3_prob"] = probs_v3.round(4)
    pred_df["v4_prob"] = probs_v4.round(4)
    pred_df["v3_pred"] = (probs_v3 >= 0.5).astype(int)
    pred_df["v3_error"] = ""
    pred_df.loc[(pred_df["v3_pred"] == 1) & (pred_df["y_true"] == 0), "v3_error"] = "FP"
    pred_df.loc[(pred_df["v3_pred"] == 0) & (pred_df["y_true"] == 1), "v3_error"] = "FN"
    pred_df.to_csv(os.path.join(OUT_DIR, "test_predictions.csv"), index=False)

    sweep_df.to_csv(os.path.join(OUT_DIR, "v3_threshold_sweep.csv"), index=False)

    print(f"\nAll results saved to {OUT_DIR}/")


if __name__ == "__main__":
    main()
