import ast
import copy
import json
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import StratifiedKFold
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

CSV_PATH = "/home/ec2-user/repositories/ds_auto-calibration/meteric_temp_health_rule.csv"
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "v1")
PLOTS_DIR = os.path.join(OUT_DIR, "plots")

SMALL_BUCKET_FEATURES = [
    "bucket_reference_adherence_small",
    "bucket_temporal_stability_small",
    "bucket_diurnal_amplitude_small",
    "small_hive_ambient_tracking",
    "bucket_temperature_ordering_small_to_medium",
    "sensor_spread_within_bucket_small",
]


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_value(s):
    if isinstance(s, (int, float)):
        return s
    s = str(s).strip()
    if s.startswith("{"):
        try:
            return ast.literal_eval(s)
        except Exception:
            pass
    try:
        return float(s)
    except ValueError:
        return np.nan


def sanitize_key(k):
    return str(k).replace("→", "_to_").replace(" ", "_").replace("-", "_")


def flatten_value(val, prefix=""):
    if isinstance(val, dict):
        result = {}
        for k, v in val.items():
            new_key = f"{prefix}_{sanitize_key(k)}" if prefix else sanitize_key(k)
            result.update(flatten_value(v, new_key))
        return result
    try:
        return {prefix: float(val)} if prefix else {}
    except (TypeError, ValueError):
        return {prefix: np.nan} if prefix else {}


def expand_row(row):
    val = parse_value(row["value"])
    name = row["metric_name"]
    if isinstance(val, dict):
        flat = flatten_value(val)
        return {f"{name}_{k}": v for k, v in flat.items()}
    try:
        return {name: float(val)}
    except (TypeError, ValueError):
        return {name: np.nan}


def expand_ratio_row(row):
    """value / threshold per feature — encodes distance from decision boundary."""
    val = parse_value(row["value"])
    thr = parse_value(row["threshold"])
    name = row["metric_name"]
    result = {}

    if not isinstance(val, dict):
        try:
            v, t = float(val), float(thr)
            if t != 0:
                result[f"{name}_ratio"] = v / t
        except (TypeError, ValueError):
            pass
        return result

    for k, v in val.items():
        col = f"{name}_{sanitize_key(k)}_ratio"
        try:
            v_float = float(v)
        except (TypeError, ValueError):
            continue

        if isinstance(thr, dict):
            t_val = thr.get(k)
            if t_val is None:
                continue
            if isinstance(t_val, dict):
                # range threshold {low, high} → normalized distance from center
                try:
                    low, high = float(t_val["low"]), float(t_val["high"])
                    half = (high - low) / 2
                    if half != 0:
                        result[col] = (v_float - (low + high) / 2) / half
                except (KeyError, TypeError, ValueError):
                    pass
            else:
                try:
                    t_float = float(t_val)
                    if t_float != 0:
                        result[col] = v_float / t_float
                except (TypeError, ValueError):
                    pass
        else:
            # scalar threshold applies to all dict keys (e.g. bucket_temperature_ordering)
            try:
                t_float = float(thr)
                if t_float != 0:
                    result[col] = v_float / t_float
            except (TypeError, ValueError):
                pass

    return result


def expand_pass_row(row):
    return {f"{row['metric_name']}_pass": int(bool(row["pass_metric"]))}


# ---------------------------------------------------------------------------
# Feature matrix construction
# ---------------------------------------------------------------------------

def build_feature_matrix(df):
    expanded = []
    for _, row in df.iterrows():
        entry = {
            "group_id": row["group_id"],
            "date": row["date"],
            "status": row["status"],
        }
        entry.update(expand_ratio_row(row))   # ratio replaces raw value
        entry.update(expand_pass_row(row))
        expanded.append(entry)

    feat_df = pd.DataFrame(expanded)

    # n_metrics_passing: count of pass columns that are 1 per key
    pass_cols = [c for c in feat_df.columns if c.endswith("_pass")]
    feat_df["n_metrics_passing"] = feat_df[pass_cols].sum(axis=1)

    agg_dict = {col: "first" for col in feat_df.columns if col not in ["group_id", "date"]}
    # n_metrics_passing: sum across metrics for the same key
    agg_dict["n_metrics_passing"] = "sum"
    W_df = feat_df.groupby(["group_id", "date"]).agg(agg_dict).reset_index()

    ghost = "bucket_temperature_ordering_small_to_large"
    ghost_ratio = "bucket_temperature_ordering_small_to_large_ratio"
    for col in [ghost, ghost_ratio]:
        if col in W_df.columns:
            W_df = W_df.drop(columns=[col])
            print(f"Dropped ghost column: {col}")

    small_cols_present = [c for c in SMALL_BUCKET_FEATURES if c in W_df.columns]
    W_df["has_small_bucket"] = W_df[small_cols_present].notna().any(axis=1).astype(float)

    feature_cols = [c for c in W_df.columns if c not in ["group_id", "date", "status"]]
    X = W_df[feature_cols].astype(float)
    y = (W_df["status"] == "valid").astype(int)

    # Print feature groups
    ratio_cols = [c for c in feature_cols if c.endswith("_ratio")]
    pass_cols_ = [c for c in feature_cols if c.endswith("_pass")]
    print(f"  ratio features      : {len(ratio_cols)}")
    print(f"  pass_metric features: {len(pass_cols_)}")
    print(f"  other               : has_small_bucket, n_metrics_passing")
    print(f"  total               : {len(feature_cols)}")

    return X, y, feature_cols, W_df


# ---------------------------------------------------------------------------
# CV — manual loop to collect OOF predictions + per-fold confusion matrices
# ---------------------------------------------------------------------------

def make_pipeline():
    return Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("lr", LogisticRegression(class_weight="balanced", C=1.0, random_state=42, max_iter=1000)),
    ])


def run_cv(X, y):
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

    fold_metrics = []
    fold_cms = []
    oof_probs = np.zeros(len(y))
    oof_preds = np.zeros(len(y), dtype=int)

    print("\n=== 5-Fold Stratified CV (pos=valid) ===")
    for fold, (train_idx, val_idx) in enumerate(cv.split(X, y)):
        pipe = copy.deepcopy(make_pipeline())
        pipe.fit(X.iloc[train_idx], y.iloc[train_idx])

        y_prob = pipe.predict_proba(X.iloc[val_idx])[:, 1]
        y_pred = pipe.predict(X.iloc[val_idx])
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
        print(f"  Fold {fold+1}: precision={prec:.3f}  recall={rec:.3f}  f1={f1:.3f}  auc={auc:.3f}")

    metrics_df = pd.DataFrame(fold_metrics)
    print(f"\n  {'mean':>6}   precision={metrics_df['precision'].mean():.3f}  recall={metrics_df['recall'].mean():.3f}"
          f"  f1={metrics_df['f1'].mean():.3f}  auc={metrics_df['roc_auc'].mean():.3f}")

    # Fit on full data for coefficients
    full_pipe = make_pipeline()
    full_pipe.fit(X, y)

    return full_pipe, metrics_df, fold_cms, oof_probs, oof_preds


# ---------------------------------------------------------------------------
# Threshold sweep
# ---------------------------------------------------------------------------

def threshold_sweep(y, oof_probs):
    rows = []
    for t in np.arange(0.25, 0.85, 0.05):
        preds = (oof_probs >= t).astype(int)
        rows.append({
            "threshold": round(t, 2),
            "precision": round(precision_score(y, preds, pos_label=1, zero_division=0), 3),
            "recall":    round(recall_score(y, preds, pos_label=1, zero_division=0), 3),
            "f1":        round(f1_score(y, preds, pos_label=1, zero_division=0), 3),
            "n_pred_valid": int(preds.sum()),
            "fp": int(((preds == 1) & (y == 0)).sum()),
            "fn": int(((preds == 0) & (y == 1)).sum()),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------

def plot_precision_recall_curve(y, oof_probs):
    precision, recall, thresholds = precision_recall_curve(y, oof_probs)
    baseline = y.mean()

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(recall, precision, color="steelblue", lw=2)
    ax.axhline(baseline, color="gray", linestyle="--", lw=1, label=f"Baseline (no skill) = {baseline:.2f}")

    # mark default threshold ~0.5
    idx = np.argmin(np.abs(thresholds - 0.5))
    ax.scatter(recall[idx], precision[idx], color="tomato", zorder=5, s=80, label="threshold=0.50")

    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title("Precision-Recall Curve (OOF)")
    ax.legend()
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS_DIR, "precision_recall_curve.png"), dpi=150)
    plt.close(fig)
    print("  Saved: precision_recall_curve.png")


def plot_feature_coefficients(feature_cols, coefs):
    importance = (
        pd.DataFrame({"feature": feature_cols, "coefficient": coefs})
        .sort_values("coefficient")
    )
    colors = ["tomato" if c < 0 else "steelblue" for c in importance["coefficient"]]

    fig, ax = plt.subplots(figsize=(8, max(5, len(feature_cols) * 0.35)))
    ax.barh(importance["feature"], importance["coefficient"], color=colors)
    ax.axvline(0, color="black", lw=0.8)
    ax.set_xlabel("Coefficient")
    ax.set_title("Feature Coefficients\n(blue = valid, red = invalid)")
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS_DIR, "feature_coefficients.png"), dpi=150)
    plt.close(fig)
    print("  Saved: feature_coefficients.png")


def plot_cv_metrics(metrics_df):
    metric_cols = ["precision", "recall", "f1", "roc_auc"]
    melted = metrics_df[metric_cols].melt(var_name="metric", value_name="score")

    fig, ax = plt.subplots(figsize=(7, 4))
    sns.boxplot(data=melted, x="metric", y="score", hue="metric", ax=ax, palette="Set2", width=0.4, legend=False)
    sns.stripplot(data=melted, x="metric", y="score", ax=ax, color="black", size=5, jitter=False)
    ax.set_ylim(0, 1)
    ax.set_xlabel("")
    ax.set_ylabel("Score")
    ax.set_title("CV Metric Distribution (5 folds)")
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS_DIR, "cv_metrics_boxplot.png"), dpi=150)
    plt.close(fig)
    print("  Saved: cv_metrics_boxplot.png")


def plot_confusion_matrix(fold_cms):
    agg_cm = np.sum(fold_cms, axis=0)  # aggregate across folds

    fig, ax = plt.subplots(figsize=(5, 4))
    sns.heatmap(
        agg_cm,
        annot=True, fmt="d", cmap="Blues",
        xticklabels=["pred invalid", "pred valid"],
        yticklabels=["true invalid", "true valid"],
        ax=ax,
    )
    tn, fp, fn, tp = agg_cm.ravel()
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

    nan_summary = (
        X.isna().sum()
        .rename("nan_count")
        .to_frame()
        .assign(nan_pct=lambda d: (d["nan_count"] / len(X) * 100).round(1))
        .sort_values("nan_count", ascending=False)
    )
    print(f"\n=== NaN summary ===\n{nan_summary[nan_summary['nan_count'] > 0].to_string()}")

    full_pipe, metrics_df, fold_cms, oof_probs, oof_preds = run_cv(X, y)

    coefs = full_pipe.named_steps["lr"].coef_[0]
    importance = (
        pd.DataFrame({"feature": feature_cols, "coefficient": coefs})
        .assign(abs_coef=lambda d: d["coefficient"].abs())
        .sort_values("abs_coef", ascending=False)
        .drop(columns="abs_coef")
        .reset_index(drop=True)
    )
    print("\n=== Feature Coefficients (sorted by |coef|) ===")
    print(importance.to_string(index=False))

    sweep_df = threshold_sweep(y, oof_probs)
    print(f"\n=== Threshold Sweep ===\n{sweep_df.to_string(index=False)}")

    # OOF predictions
    oof_df = W_df[["group_id", "date", "status"]].copy()
    oof_df["y_true"]    = y.values
    oof_df["y_pred"]    = oof_preds
    oof_df["y_prob"]    = oof_probs.round(4)
    oof_df["correct"]   = (oof_df["y_true"] == oof_df["y_pred"])
    oof_df["error_type"] = ""
    oof_df.loc[(oof_df["y_pred"] == 1) & (oof_df["y_true"] == 0), "error_type"] = "FP"
    oof_df.loc[(oof_df["y_pred"] == 0) & (oof_df["y_true"] == 1), "error_type"] = "FN"

    # Save data outputs
    importance.to_csv(os.path.join(OUT_DIR, "feature_coefficients.csv"), index=False)
    nan_summary.to_csv(os.path.join(OUT_DIR, "nan_summary.csv"))
    sweep_df.to_csv(os.path.join(OUT_DIR, "threshold_sweep.csv"), index=False)
    oof_df.to_csv(os.path.join(OUT_DIR, "oof_predictions.csv"), index=False)
    cv_summary = {
        m: {"mean": round(float(metrics_df[m].mean()), 4), "std": round(float(metrics_df[m].std()), 4)}
        for m in ["precision", "recall", "f1", "roc_auc"]
    }
    with open(os.path.join(OUT_DIR, "cv_results.json"), "w") as f:
        json.dump(cv_summary, f, indent=2)

    # Save plots
    print("\n=== Saving plots ===")
    plot_precision_recall_curve(y, oof_probs)
    plot_feature_coefficients(feature_cols, coefs)
    plot_cv_metrics(metrics_df)
    plot_confusion_matrix(fold_cms)

    baseline = {"precision": 0.782, "recall": 0.715, "f1": 0.747, "roc_auc": 0.742}
    print("\n=== Before vs After ===")
    print(f"  {'metric':12s}  {'baseline':>8}  {'new':>8}  {'delta':>8}")
    for m in ["precision", "recall", "f1", "roc_auc"]:
        new_val = float(metrics_df[m].mean())
        delta = new_val - baseline[m]
        sign = "+" if delta >= 0 else ""
        print(f"  {m:12s}  {baseline[m]:>8.3f}  {new_val:>8.3f}  {sign}{delta:>7.3f}")

    print(f"\nAll results saved to {OUT_DIR}/")


if __name__ == "__main__":
    main()
