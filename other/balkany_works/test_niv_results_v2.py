"""
v2: XGBoost model with ratio + pass_metric features.
Imports feature engineering from v1; saves outputs to test_niv/v2/.
"""
import copy
import json
import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import (
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold
from xgboost import XGBClassifier

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from test_niv_results_v1 import (
    CSV_PATH,
    build_feature_matrix,
    threshold_sweep,
)

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "v2")
PLOTS_DIR = os.path.join(OUT_DIR, "plots")

# Baseline from v1 (raw values, LR) for comparison
V1_BASELINE = {"precision": 0.782, "recall": 0.715, "f1": 0.747, "roc_auc": 0.742}


# ---------------------------------------------------------------------------
# XGBoost pipeline
# ---------------------------------------------------------------------------

def make_xgb(scale_pos_weight=0.5, max_depth=4):
    return XGBClassifier(
        n_estimators=300,
        max_depth=max_depth,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=scale_pos_weight,  # < 1 → stricter on predicting valid → less FP
        eval_metric="logloss",
        random_state=42,
        verbosity=0,
    )


def run_cv(X, y, scale_pos_weight=0.5, max_depth=4, verbose=True):
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    fold_metrics = []
    fold_cms = []
    oof_probs = np.zeros(len(y))
    oof_preds = np.zeros(len(y), dtype=int)

    if verbose:
        print(f"\n=== 5-Fold CV — XGBoost (scale_pos_weight={scale_pos_weight}, max_depth={max_depth}) ===")
    for fold, (train_idx, val_idx) in enumerate(cv.split(X, y)):
        model = make_xgb(scale_pos_weight, max_depth)
        model.fit(X.iloc[train_idx], y.iloc[train_idx])

        y_prob = model.predict_proba(X.iloc[val_idx])[:, 1]
        y_pred = model.predict(X.iloc[val_idx])
        y_val  = y.iloc[val_idx]

        oof_probs[val_idx] = y_prob
        oof_preds[val_idx] = y_pred

        prec = precision_score(y_val, y_pred, pos_label=1, zero_division=0)
        rec  = recall_score(y_val, y_pred, pos_label=1, zero_division=0)
        f1   = f1_score(y_val, y_pred, pos_label=1, zero_division=0)
        auc  = roc_auc_score(y_val, y_prob)
        cm   = confusion_matrix(y_val, y_pred)

        fold_metrics.append({"fold": fold + 1, "precision": prec, "recall": rec, "f1": f1, "roc_auc": auc})
        fold_cms.append(cm)
        if verbose:
            print(f"  Fold {fold+1}: precision={prec:.3f}  recall={rec:.3f}  f1={f1:.3f}  auc={auc:.3f}")

    metrics_df = pd.DataFrame(fold_metrics)
    if verbose:
        print(f"\n  {'mean':>6}   precision={metrics_df['precision'].mean():.3f}  "
              f"recall={metrics_df['recall'].mean():.3f}  "
              f"f1={metrics_df['f1'].mean():.3f}  "
              f"auc={metrics_df['roc_auc'].mean():.3f}")

    full_model = make_xgb(scale_pos_weight, max_depth)
    full_model.fit(X, y)

    return full_model, metrics_df, fold_cms, oof_probs, oof_preds


def grid_search(X, y):
    """Sweep scale_pos_weight × max_depth, return results sorted by precision."""
    scale_pos_weights = [0.3, 0.4, 0.5]
    max_depths = [3, 4]

    print("\n=== Grid Search: scale_pos_weight × max_depth ===")
    print(f"  {'spw':>5}  {'depth':>5}  {'prec':>6}  {'rec':>6}  {'f1':>6}  {'auc':>6}")

    rows = []
    for spw in scale_pos_weights:
        for depth in max_depths:
            _, metrics_df, _, _, _ = run_cv(X, y, scale_pos_weight=spw, max_depth=depth, verbose=False)
            row = {
                "scale_pos_weight": spw,
                "max_depth": depth,
                "precision": metrics_df["precision"].mean(),
                "recall":    metrics_df["recall"].mean(),
                "f1":        metrics_df["f1"].mean(),
                "roc_auc":   metrics_df["roc_auc"].mean(),
            }
            rows.append(row)
            print(f"  {spw:>5.1f}  {depth:>5}  "
                  f"{row['precision']:>6.3f}  {row['recall']:>6.3f}  "
                  f"{row['f1']:>6.3f}  {row['roc_auc']:>6.3f}")

    grid_df = pd.DataFrame(rows).sort_values("precision", ascending=False)
    best = grid_df.iloc[0]
    print(f"\n  Best by precision → scale_pos_weight={best['scale_pos_weight']}, max_depth={int(best['max_depth'])}")
    return float(best["scale_pos_weight"]), int(best["max_depth"])


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------

def plot_precision_recall_curve(y, oof_probs, label="XGBoost"):
    precision, recall, thresholds = precision_recall_curve(y, oof_probs)
    baseline = y.mean()

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(recall, precision, color="steelblue", lw=2, label=label)
    ax.axhline(baseline, color="gray", linestyle="--", lw=1, label=f"Baseline = {baseline:.2f}")

    idx = np.argmin(np.abs(thresholds - 0.5))
    ax.scatter(recall[idx], precision[idx], color="tomato", zorder=5, s=80, label="threshold=0.50")

    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title("Precision-Recall Curve (OOF) — v2 XGBoost")
    ax.legend()
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS_DIR, "precision_recall_curve.png"), dpi=150)
    plt.close(fig)
    print("  Saved: precision_recall_curve.png")


def plot_feature_importance(model, feature_cols):
    scores = model.get_booster().get_score(importance_type="gain")
    importance = (
        pd.DataFrame({"feature": list(scores.keys()), "gain": list(scores.values())})
        .sort_values("gain", ascending=True)
    )

    fig, ax = plt.subplots(figsize=(8, max(5, len(importance) * 0.35)))
    ax.barh(importance["feature"], importance["gain"], color="steelblue")
    ax.set_xlabel("Gain")
    ax.set_title("XGBoost Feature Importance (by gain)")
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS_DIR, "feature_importance.png"), dpi=150)
    plt.close(fig)
    print("  Saved: feature_importance.png")

    return importance.sort_values("gain", ascending=False).reset_index(drop=True)


def plot_cv_metrics(metrics_df):
    metric_cols = ["precision", "recall", "f1", "roc_auc"]
    melted = metrics_df[metric_cols].melt(var_name="metric", value_name="score")

    fig, ax = plt.subplots(figsize=(7, 4))
    sns.boxplot(data=melted, x="metric", y="score", hue="metric", ax=ax,
                palette="Set2", width=0.4, legend=False)
    sns.stripplot(data=melted, x="metric", y="score", ax=ax, color="black", size=5, jitter=False)
    ax.set_ylim(0, 1)
    ax.set_xlabel("")
    ax.set_ylabel("Score")
    ax.set_title("CV Metric Distribution (5 folds) — v2 XGBoost")
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS_DIR, "cv_metrics_boxplot.png"), dpi=150)
    plt.close(fig)
    print("  Saved: cv_metrics_boxplot.png")


def plot_confusion_matrix(fold_cms):
    agg_cm = np.sum(fold_cms, axis=0)
    tn, fp, fn, tp = agg_cm.ravel()

    fig, ax = plt.subplots(figsize=(5, 4))
    sns.heatmap(
        agg_cm,
        annot=True, fmt="d", cmap="Blues",
        xticklabels=["pred invalid", "pred valid"],
        yticklabels=["true invalid", "true valid"],
        ax=ax,
    )
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

    X, y, feature_cols, W_df = build_feature_matrix(df)
    print(f"\nMatrix shape : {X.shape}  (keys x features)")
    print(f"Label counts : {y.value_counts().to_dict()}  (1=valid, 0=invalid)")

    # XGBoost NaN-safe: fill NaN with -999 so the model can learn missingness
    X_xgb = X.fillna(-999)

    best_spw, best_depth = grid_search(X_xgb, y)
    full_model, metrics_df, fold_cms, oof_probs, oof_preds = run_cv(
        X_xgb, y, scale_pos_weight=best_spw, max_depth=best_depth
    )

    # Feature importance
    importance_df = plot_feature_importance(full_model, feature_cols)
    print("\n=== Feature Importance (top 15, by gain) ===")
    print(importance_df.head(15).to_string(index=False))

    # Threshold sweep
    sweep_df = threshold_sweep(y, oof_probs)
    print(f"\n=== Threshold Sweep ===\n{sweep_df.to_string(index=False)}")

    # OOF predictions
    oof_df = W_df[["group_id", "date", "status"]].copy()
    oof_df["y_true"]     = y.values
    oof_df["y_pred"]     = oof_preds
    oof_df["y_prob"]     = oof_probs.round(4)
    oof_df["correct"]    = (oof_df["y_true"] == oof_df["y_pred"])
    oof_df["error_type"] = ""
    oof_df.loc[(oof_df["y_pred"] == 1) & (oof_df["y_true"] == 0), "error_type"] = "FP"
    oof_df.loc[(oof_df["y_pred"] == 0) & (oof_df["y_true"] == 1), "error_type"] = "FN"

    # Save outputs
    importance_df.to_csv(os.path.join(OUT_DIR, "feature_importance.csv"), index=False)
    sweep_df.to_csv(os.path.join(OUT_DIR, "threshold_sweep.csv"), index=False)
    oof_df.to_csv(os.path.join(OUT_DIR, "oof_predictions.csv"), index=False)
    cv_summary = {
        m: {"mean": round(float(metrics_df[m].mean()), 4), "std": round(float(metrics_df[m].std()), 4)}
        for m in ["precision", "recall", "f1", "roc_auc"]
    }
    with open(os.path.join(OUT_DIR, "cv_results.json"), "w") as f:
        json.dump(cv_summary, f, indent=2)

    # Plots
    print("\n=== Saving plots ===")
    plot_precision_recall_curve(y, oof_probs)
    plot_cv_metrics(metrics_df)
    plot_confusion_matrix(fold_cms)

    # Before / after comparison vs v1 LR baseline
    print("\n=== v1 LR baseline vs v2 XGBoost ===")
    print(f"  {'metric':12s}  {'v1 LR':>8}  {'v2 XGB':>8}  {'delta':>8}")
    for m in ["precision", "recall", "f1", "roc_auc"]:
        new_val = float(metrics_df[m].mean())
        delta = new_val - V1_BASELINE[m]
        sign = "+" if delta >= 0 else ""
        print(f"  {m:12s}  {V1_BASELINE[m]:>8.3f}  {new_val:>8.3f}  {sign}{delta:>7.3f}")

    print(f"\nAll results saved to {OUT_DIR}/")


if __name__ == "__main__":
    main()
