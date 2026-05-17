"""
v3 improvements over v2:
  1. Native NaN handling — XGBoost learns optimal split direction for missing values
  2. Expanded hyperparameter grid — adds min_child_weight + gamma; early stopping on final model
  3. SHAP values — per-sample explanations, beeswarm + bar plots + CSV
"""
import itertools
import json
import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import shap
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

OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "v3")
PLOTS_DIR = os.path.join(OUT_DIR, "plots")

V1_BASELINE = {"precision": 0.782, "recall": 0.715, "f1": 0.747, "roc_auc": 0.742}
V2_BASELINE = {"precision": 0.787, "recall": 0.711, "f1": 0.746, "roc_auc": 0.760}

PARAM_GRID = {
    "scale_pos_weight": [0.3, 0.4],
    "max_depth":        [3, 4],
    "min_child_weight": [1, 5, 10],
    "gamma":            [0, 0.1],
}


# ---------------------------------------------------------------------------
# Model factory
# ---------------------------------------------------------------------------

def make_xgb(scale_pos_weight=0.3, max_depth=3, min_child_weight=1,
             gamma=0, n_estimators=300, early_stopping_rounds=None):
    return XGBClassifier(
        n_estimators=n_estimators,
        max_depth=max_depth,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=min_child_weight,
        gamma=gamma,
        scale_pos_weight=scale_pos_weight,
        eval_metric="logloss",
        early_stopping_rounds=early_stopping_rounds,
        random_state=42,
        verbosity=0,
    )


# ---------------------------------------------------------------------------
# CV
# ---------------------------------------------------------------------------

def run_cv(X, y, params, verbose=True):
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    fold_metrics, fold_cms = [], []
    oof_probs = np.zeros(len(y))
    oof_preds = np.zeros(len(y), dtype=int)

    if verbose:
        print(f"\n=== 5-Fold CV — XGBoost {params} ===")

    for fold, (train_idx, val_idx) in enumerate(cv.split(X, y)):
        model = make_xgb(**params)
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

        fold_metrics.append({"fold": fold + 1, "precision": prec, "recall": rec, "f1": f1, "roc_auc": auc})
        fold_cms.append(confusion_matrix(y_val, y_pred))
        if verbose:
            print(f"  Fold {fold+1}: precision={prec:.3f}  recall={rec:.3f}  f1={f1:.3f}  auc={auc:.3f}")

    metrics_df = pd.DataFrame(fold_metrics)
    if verbose:
        print(f"\n  mean   precision={metrics_df['precision'].mean():.3f}  "
              f"recall={metrics_df['recall'].mean():.3f}  "
              f"f1={metrics_df['f1'].mean():.3f}  "
              f"auc={metrics_df['roc_auc'].mean():.3f}")

    return metrics_df, fold_cms, oof_probs, oof_preds


# ---------------------------------------------------------------------------
# Grid search
# ---------------------------------------------------------------------------

def grid_search(X, y):
    keys = list(PARAM_GRID.keys())
    combos = list(itertools.product(*PARAM_GRID.values()))

    print(f"\n=== Grid Search ({len(combos)} combos) ===")
    header = f"  {'spw':>4}  {'dep':>3}  {'mcw':>3}  {'gam':>4}  {'prec':>6}  {'rec':>6}  {'f1':>6}  {'auc':>6}"
    print(header)

    rows = []
    for combo in combos:
        params = dict(zip(keys, combo))
        metrics_df, _, _, _ = run_cv(X, y, params, verbose=False)
        row = {**params,
               "precision": metrics_df["precision"].mean(),
               "recall":    metrics_df["recall"].mean(),
               "f1":        metrics_df["f1"].mean(),
               "roc_auc":   metrics_df["roc_auc"].mean()}
        rows.append(row)
        print(f"  {params['scale_pos_weight']:>4.1f}  {params['max_depth']:>3}  "
              f"{params['min_child_weight']:>3}  {params['gamma']:>4.1f}  "
              f"{row['precision']:>6.3f}  {row['recall']:>6.3f}  "
              f"{row['f1']:>6.3f}  {row['roc_auc']:>6.3f}")

    grid_df = pd.DataFrame(rows).sort_values("precision", ascending=False)
    best = grid_df.iloc[0]
    best_params = {k: (int(best[k]) if k in ("max_depth", "min_child_weight") else float(best[k]))
                   for k in keys}
    print(f"\n  Best by precision → {best_params}")
    return best_params, grid_df


# ---------------------------------------------------------------------------
# Final model with early stopping
# ---------------------------------------------------------------------------

def fit_final_model(X, y, params):
    # Use 20% of data to find optimal n_estimators via early stopping
    X_tr, X_es, y_tr, y_es = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )
    probe = make_xgb(**params, n_estimators=1000, early_stopping_rounds=20)
    probe.fit(X_tr, y_tr, eval_set=[(X_es, y_es)], verbose=False)
    best_n = probe.best_iteration + 1
    print(f"\n  Early stopping → best n_estimators = {best_n}")

    # Retrain on full data with that n_estimators
    final = make_xgb(**params, n_estimators=best_n)
    final.fit(X, y)
    return final


# ---------------------------------------------------------------------------
# SHAP
# ---------------------------------------------------------------------------

def compute_and_plot_shap(model, X, feature_cols):
    print("\n  Computing SHAP values ...")
    import xgboost as xgb
    # Use XGBoost native SHAP (avoids shap 0.49 / XGBoost 3.x compatibility bug)
    dmatrix = xgb.DMatrix(X, feature_names=feature_cols)
    shap_vals = model.get_booster().predict(dmatrix, pred_contribs=True)[:, :-1]  # drop bias col

    shap_df = pd.DataFrame(shap_vals, columns=feature_cols)
    shap_df.to_csv(os.path.join(OUT_DIR, "shap_values.csv"), index=False)

    # Mean |SHAP| per feature — overall importance
    mean_shap = (
        pd.DataFrame({"feature": feature_cols, "mean_abs_shap": np.abs(shap_vals).mean(axis=0)})
        .sort_values("mean_abs_shap", ascending=False)
        .reset_index(drop=True)
    )
    mean_shap.to_csv(os.path.join(OUT_DIR, "shap_importance.csv"), index=False)

    # Beeswarm plot — shap.summary_plot owns the figure, save via plt
    plt.figure(figsize=(9, max(6, len(feature_cols) * 0.35)))
    shap.summary_plot(shap_vals, X, feature_names=feature_cols, show=False)
    plt.title("SHAP Beeswarm — feature impact on valid prediction")
    plt.tight_layout()
    plt.savefig(os.path.join(PLOTS_DIR, "shap_beeswarm.png"), dpi=150, bbox_inches="tight")
    plt.close()
    print("  Saved: shap_beeswarm.png")

    # Bar plot
    plt.figure(figsize=(8, max(5, len(feature_cols) * 0.35)))
    shap.summary_plot(shap_vals, X, feature_names=feature_cols, plot_type="bar", show=False)
    plt.title("SHAP Feature Importance (mean |SHAP|)")
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
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title("Precision-Recall Curve (OOF) — v3")
    ax.legend()
    ax.set_xlim(0, 1); ax.set_ylim(0, 1)
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
    ax.set_title("CV Metric Distribution (5 folds) — v3")
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

    # Native NaN — no fillna, XGBoost handles missing values directly
    X, y, feature_cols, W_df = build_feature_matrix(df)
    print(f"\nMatrix shape : {X.shape}  (keys x features)")
    print(f"Label counts : {y.value_counts().to_dict()}  (1=valid, 0=invalid)")

    best_params, grid_df = grid_search(X, y)
    grid_df.to_csv(os.path.join(OUT_DIR, "grid_search_results.csv"), index=False)

    metrics_df, fold_cms, oof_probs, oof_preds = run_cv(X, y, best_params)

    final_model = fit_final_model(X, y, best_params)

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

    # SHAP
    print("\n=== SHAP Analysis ===")
    mean_shap = compute_and_plot_shap(final_model, X, feature_cols)
    print("\n  Top 10 features by mean |SHAP|:")
    print(mean_shap.head(10).to_string(index=False))

    # Save data outputs
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

    # Plots
    print("\n=== Saving plots ===")
    plot_precision_recall_curve(y, oof_probs)
    plot_cv_metrics(metrics_df)
    plot_confusion_matrix(fold_cms)

    # Comparison table
    print("\n=== v1 LR  vs  v2 XGB  vs  v3 XGB+ ===")
    print(f"  {'metric':12s}  {'v1 LR':>8}  {'v2 XGB':>8}  {'v3 XGB+':>8}  {'Δv1→v3':>8}")
    for m in ["precision", "recall", "f1", "roc_auc"]:
        v3  = float(metrics_df[m].mean())
        dv1 = v3 - V1_BASELINE[m]
        sign = "+" if dv1 >= 0 else ""
        print(f"  {m:12s}  {V1_BASELINE[m]:>8.3f}  {V2_BASELINE[m]:>8.3f}  {v3:>8.3f}  {sign}{dv1:>7.3f}")

    print(f"\nAll results saved to {OUT_DIR}/")


if __name__ == "__main__":
    main()
