"""
v4 improvements over v3:
  1. Aggregate features — mean/std across bucket sizes per metric family
  2. Rank ordering flags — is large >= medium >= small per metric?
  3. Cross-metric interactions — pairwise products of top-5 SHAP features from v3
"""
import json
import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import shap
import xgboost as xgb
from sklearn.metrics import (
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold, train_test_split
from xgboost import XGBClassifier

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_niv_results_v1 import CSV_PATH, build_feature_matrix, threshold_sweep
from test_niv_results_v3 import make_xgb, run_cv, fit_final_model, grid_search

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "v4")
PLOTS_DIR = os.path.join(OUT_DIR, "plots")

V1_BASELINE = {"precision": 0.782, "recall": 0.715, "f1": 0.747, "roc_auc": 0.742}
V2_BASELINE = {"precision": 0.787, "recall": 0.711, "f1": 0.746, "roc_auc": 0.760}
V3_BASELINE = {"precision": 0.834, "recall": 0.625, "f1": 0.713, "roc_auc": 0.760}

BUCKET_SIZES = ["large", "medium", "small"]

# Top-5 features from v3 SHAP used to generate pairwise interaction terms
V3_SHAP_TOP5 = [
    "bucket_diurnal_amplitude_large_ratio",
    "bucket_reference_adherence_large_ratio",
    "bucket_temperature_ordering_medium_to_large_ratio",
    "bucket_reference_adherence_medium_ratio",
    "bucket_temporal_stability_medium_ratio",
]


# ---------------------------------------------------------------------------
# Feature engineering
# ---------------------------------------------------------------------------

def _get_bucket_metric_groups(feature_cols):
    """
    Parse ratio feature names → {metric_family: {size: col_name}}.
    Excludes compound names like bucket_temperature_ordering_medium_to_large
    (a bucket size word appears inside the family name after stripping the suffix).
    Only returns families with >= 2 sizes present.
    """
    groups = {}
    for col in feature_cols:
        for size in BUCKET_SIZES:
            suffix = f"_{size}_ratio"
            if col.endswith(suffix):
                family = col[: -len(suffix)]
                if any(s in family for s in BUCKET_SIZES):
                    continue  # compound name — skip
                groups.setdefault(family, {})[size] = col
    return {k: v for k, v in groups.items() if len(v) >= 2}


def add_bucket_aggregates(X, feature_cols):
    """Mean and std of ratio values across bucket sizes for each metric family."""
    groups = _get_bucket_metric_groups(feature_cols)
    new_cols = []
    for family, size_cols in sorted(groups.items()):
        cols = list(size_cols.values())
        X[f"{family}_mean_ratio"] = X[cols].mean(axis=1)
        X[f"{family}_std_ratio"] = X[cols].std(axis=1)
        new_cols += [f"{family}_mean_ratio", f"{family}_std_ratio"]
    return X, new_cols


def add_rank_ordering_flags(X, feature_cols):
    """
    Binary flags (NaN when either operand missing):
      large >= medium, medium >= small, and all three ordered.
    """
    groups = _get_bucket_metric_groups(feature_cols)
    new_cols = []
    for family, size_cols in sorted(groups.items()):
        if "large" in size_cols and "medium" in size_cols:
            c_l, c_m = size_cols["large"], size_cols["medium"]
            name = f"{family}_large_gte_medium"
            X[name] = (X[c_l] >= X[c_m]).astype(float).where(
                ~(X[c_l].isna() | X[c_m].isna())
            )
            new_cols.append(name)

        if "medium" in size_cols and "small" in size_cols:
            c_m, c_s = size_cols["medium"], size_cols["small"]
            name = f"{family}_medium_gte_small"
            X[name] = (X[c_m] >= X[c_s]).astype(float).where(
                ~(X[c_m].isna() | X[c_s].isna())
            )
            new_cols.append(name)

        if all(s in size_cols for s in BUCKET_SIZES):
            c_l, c_m, c_s = size_cols["large"], size_cols["medium"], size_cols["small"]
            name = f"{family}_fully_ordered"
            X[name] = (
                ((X[c_l] >= X[c_m]) & (X[c_m] >= X[c_s])).astype(float).where(
                    ~(X[c_l].isna() | X[c_m].isna() | X[c_s].isna())
                )
            )
            new_cols.append(name)
    return X, new_cols


def _short_feat_name(feat):
    return (
        feat.replace("bucket_", "")
            .replace("sensor_spread_within_", "spread_")
            .replace("_ratio", "")
    )


def add_interactions(X, feature_cols):
    """Pairwise products of top-5 SHAP features from v3 (10 new features)."""
    present = [f for f in V3_SHAP_TOP5 if f in feature_cols]
    new_cols = []
    for i in range(len(present)):
        for j in range(i + 1, len(present)):
            f1, f2 = present[i], present[j]
            name = f"ix_{_short_feat_name(f1)}_x_{_short_feat_name(f2)}"
            X[name] = X[f1] * X[f2]  # NaN propagates naturally
            new_cols.append(name)
    return X, new_cols


def build_feature_matrix_v4(df):
    X, y, feature_cols, W_df = build_feature_matrix(df)
    X = X.copy()

    X, agg_cols = add_bucket_aggregates(X, feature_cols)
    X, ord_cols = add_rank_ordering_flags(X, feature_cols)
    X, ix_cols  = add_interactions(X, feature_cols)

    feature_cols_v4 = feature_cols + agg_cols + ord_cols + ix_cols

    print(f"  base features       : {len(feature_cols)}")
    print(f"  + aggregates        : {len(agg_cols)}")
    print(f"  + ordering flags    : {len(ord_cols)}")
    print(f"  + interactions      : {len(ix_cols)}")
    print(f"  total               : {len(feature_cols_v4)}")

    return X[feature_cols_v4], y, feature_cols_v4, W_df


# ---------------------------------------------------------------------------
# SHAP (saves to v4 OUT_DIR / PLOTS_DIR)
# ---------------------------------------------------------------------------

def compute_and_plot_shap(model, X, feature_cols):
    print("\n  Computing SHAP values ...")
    dmatrix = xgb.DMatrix(X, feature_names=feature_cols)
    shap_vals = model.get_booster().predict(dmatrix, pred_contribs=True)[:, :-1]

    pd.DataFrame(shap_vals, columns=feature_cols).to_csv(
        os.path.join(OUT_DIR, "shap_values.csv"), index=False
    )

    mean_shap = (
        pd.DataFrame({"feature": feature_cols, "mean_abs_shap": np.abs(shap_vals).mean(axis=0)})
        .sort_values("mean_abs_shap", ascending=False)
        .reset_index(drop=True)
    )
    mean_shap.to_csv(os.path.join(OUT_DIR, "shap_importance.csv"), index=False)

    plt.figure(figsize=(10, max(8, len(feature_cols) * 0.28)))
    shap.summary_plot(shap_vals, X, feature_names=feature_cols, show=False)
    plt.title("SHAP Beeswarm — v4")
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, "shap_beeswarm.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved: shap_beeswarm.png")

    plt.figure(figsize=(9, max(7, len(feature_cols) * 0.28)))
    shap.summary_plot(shap_vals, X, feature_names=feature_cols, plot_type="bar", show=False)
    plt.title("SHAP Feature Importance (mean |SHAP|) — v4")
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, "shap_bar.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved: shap_bar.png")

    return mean_shap


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------

def plot_precision_recall_curve(y, oof_probs):
    precision, recall, thresholds = precision_recall_curve(y, oof_probs)
    baseline = y.mean()
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(recall, precision, color="steelblue", lw=2)
    ax.axhline(baseline, color="gray", linestyle="--", lw=1, label=f"Baseline = {baseline:.2f}")
    idx = np.argmin(np.abs(thresholds - 0.5))
    ax.scatter(recall[idx], precision[idx], color="tomato", zorder=5, s=80, label="threshold=0.50")
    ax.set_xlabel("Recall"); ax.set_ylabel("Precision")
    ax.set_title("Precision-Recall Curve (OOF) — v4")
    ax.legend(); ax.set_xlim(0, 1); ax.set_ylim(0, 1)
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS_DIR, "precision_recall_curve.png"), dpi=150)
    plt.close(fig)
    print("  Saved: precision_recall_curve.png")


def plot_cv_metrics(metrics_df):
    melted = metrics_df[["precision", "recall", "f1", "roc_auc"]].melt(var_name="metric", value_name="score")
    fig, ax = plt.subplots(figsize=(7, 4))
    sns.boxplot(data=melted, x="metric", y="score", hue="metric", ax=ax,
                palette="Set2", width=0.4, legend=False)
    sns.stripplot(data=melted, x="metric", y="score", ax=ax, color="black", size=5, jitter=False)
    ax.set_ylim(0, 1); ax.set_xlabel(""); ax.set_ylabel("Score")
    ax.set_title("CV Metric Distribution (5 folds) — v4")
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS_DIR, "cv_metrics_boxplot.png"), dpi=150)
    plt.close(fig)
    print("  Saved: cv_metrics_boxplot.png")


def plot_confusion_matrix(fold_cms):
    agg_cm = np.sum(fold_cms, axis=0)
    tn, fp, fn, tp = agg_cm.ravel()
    fig, ax = plt.subplots(figsize=(5, 4))
    sns.heatmap(agg_cm, annot=True, fmt="d", cmap="Blues",
                xticklabels=["pred invalid", "pred valid"],
                yticklabels=["true invalid", "true valid"], ax=ax)
    ax.set_title(f"Confusion Matrix (aggregated OOF)\nTP={tp}  TN={tn}  FP={fp}  FN={fn}")
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS_DIR, "confusion_matrix.png"), dpi=150)
    plt.close(fig)
    print("  Saved: confusion_matrix.png")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    os.makedirs(PLOTS_DIR, exist_ok=True)

    print(f"Loading {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    print(f"Loaded {len(df)} rows")

    X, y, feature_cols, W_df = build_feature_matrix_v4(df)
    print(f"\nMatrix shape : {X.shape}  (keys x features)")
    print(f"Label counts : {y.value_counts().to_dict()}  (1=valid, 0=invalid)")

    best_params, grid_df = grid_search(X, y)
    grid_df.to_csv(os.path.join(OUT_DIR, "grid_search_results.csv"), index=False)

    metrics_df, fold_cms, oof_probs, oof_preds = run_cv(X, y, best_params)

    final_model = fit_final_model(X, y, best_params)

    sweep_df = threshold_sweep(y, oof_probs)
    print(f"\n=== Threshold Sweep ===\n{sweep_df.to_string(index=False)}")

    oof_df = W_df[["group_id", "date", "status"]].copy()
    oof_df["y_true"]     = y.values
    oof_df["y_pred"]     = oof_preds
    oof_df["y_prob"]     = oof_probs.round(4)
    oof_df["correct"]    = (oof_df["y_true"] == oof_df["y_pred"])
    oof_df["error_type"] = ""
    oof_df.loc[(oof_df["y_pred"] == 1) & (oof_df["y_true"] == 0), "error_type"] = "FP"
    oof_df.loc[(oof_df["y_pred"] == 0) & (oof_df["y_true"] == 1), "error_type"] = "FN"

    print("\n=== SHAP Analysis ===")
    mean_shap = compute_and_plot_shap(final_model, X, feature_cols)
    print("\n  Top 15 features by mean |SHAP|:")
    print(mean_shap.head(15).to_string(index=False))

    sweep_df.to_csv(os.path.join(OUT_DIR, "threshold_sweep.csv"), index=False)
    oof_df.to_csv(os.path.join(OUT_DIR, "oof_predictions.csv"), index=False)
    cv_summary = {
        m: {"mean": round(float(metrics_df[m].mean()), 4), "std": round(float(metrics_df[m].std()), 4)}
        for m in ["precision", "recall", "f1", "roc_auc"]
    }
    with open(os.path.join(OUT_DIR, "cv_results.json"), "w") as f:
        json.dump(cv_summary, f, indent=2)
    with open(os.path.join(OUT_DIR, "best_params.json"), "w") as f:
        json.dump(best_params, f, indent=2)

    print("\n=== Saving plots ===")
    plot_precision_recall_curve(y, oof_probs)
    plot_cv_metrics(metrics_df)
    plot_confusion_matrix(fold_cms)

    print("\n=== v1 LR  vs  v2 XGB  vs  v3 XGB+  vs  v4 XGB+FE ===")
    print(f"  {'metric':12s}  {'v1 LR':>8}  {'v2 XGB':>8}  {'v3 XGB+':>8}  {'v4 XGB+FE':>10}  {'Δv3→v4':>8}")
    for m in ["precision", "recall", "f1", "roc_auc"]:
        v4_val = float(metrics_df[m].mean())
        dv3 = v4_val - V3_BASELINE[m]
        sign = "+" if dv3 >= 0 else ""
        print(f"  {m:12s}  {V1_BASELINE[m]:>8.3f}  {V2_BASELINE[m]:>8.3f}  "
              f"{V3_BASELINE[m]:>8.3f}  {v4_val:>10.3f}  {sign}{dv3:>7.3f}")

    print(f"\nAll results saved to {OUT_DIR}/")


if __name__ == "__main__":
    main()
