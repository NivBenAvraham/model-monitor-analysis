"""
evaluate_metrics.py — Temperature family metric evaluator.

Runs all 9 temperature metrics on the train split and computes
per-metric TP / TN / FP / FN against ground truth labels.

Goal: understand FP (missed invalid groups) to drive them toward zero.

Label mapping
-------------
    valid               → expected_pass = True
    invalid             → expected_pass = False
    needs_recalibration → expected_pass = False

Usage
-----
    python skills/group_model_temperature_health/scripts/evaluate_metrics.py
    python skills/group_model_temperature_health/scripts/evaluate_metrics.py \\
        --output data/results/my_run

Outputs (in --output dir)
--------------------------
    metric_evaluation_detail.parquet  — one row per (group_id, date, metric_name)
    metric_evaluation_summary.csv     — one row per metric (confusion matrix + stats)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

SKILL_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT  = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(REPO_ROOT / "src"))

from model_monitor.metrics.temperature import (
    ambient_stability,
    ambient_range,
    ambient_temperature_volatility,
    bucket_reference_adherence,
    sensor_spread_within_bucket,
    bucket_temporal_stability,
    bucket_diurnal_amplitude,
    small_hive_ambient_tracking,
    large_hive_thermoregulation,
    bucket_temperature_ordering,
)

DATA_DIR     = REPO_ROOT / "data/samples/temperature-export/train"
GROUND_TRUTH = REPO_ROOT / "ground_truth/ground_truth_statuess_ca_2026.csv"

logging.basicConfig(level=logging.WARNING, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Evaluate temperature metrics against ground truth (train split)."
    )
    p.add_argument(
        "--output", "-o",
        default=str(REPO_ROOT / "data/results/temperature_metric_evaluation"),
        help="Output directory (default: data/results/temperature_metric_evaluation)",
    )
    return p.parse_args()


def load_ground_truth() -> pd.DataFrame:
    gt = pd.read_csv(GROUND_TRUTH)
    gt["date"]          = gt["date"].astype(str)
    gt["group_id"]      = gt["group_id"].astype(int)
    gt                  = gt.rename(columns={"status": "gt_label"})
    gt["expected_pass"] = gt["gt_label"] == "valid"
    return gt[["group_id", "date", "gt_label", "expected_pass"]]


def load_parquets(group_id: int, date: str) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Load sensor + gateway parquets from the train split for one (group_id, date)."""
    base          = DATA_DIR / f"group_{group_id}" / date
    sensor_files  = list(base.glob("*sensor_temperature*.parquet"))
    gateway_files = list(base.glob("*gateway_temperature*.parquet"))
    if not sensor_files or not gateway_files:
        log.warning("Missing parquets for group=%s date=%s — skipping", group_id, date)
        return None
    return pd.read_parquet(sensor_files[0]), pd.read_parquet(gateway_files[0])


def _to_str(v) -> str | None:
    """Serialize value/threshold to string (JSON for dicts, str otherwise)."""
    if v is None:
        return None
    if isinstance(v, dict):
        return json.dumps(v)
    return str(v)


def run_metrics(sensor_df: pd.DataFrame, gateway_df: pd.DataFrame) -> list[dict]:
    """Run all 9 temperature metrics and return a list of raw metric dicts."""
    metric_fns: list[tuple[str, object]] = [
        ("ambient_stability",             lambda: ambient_stability(gateway_df)),
        ("ambient_range",                 lambda: ambient_range(gateway_df)),
        ("ambient_temperature_volatility",lambda: ambient_temperature_volatility(gateway_df)),
        ("bucket_reference_adherence",    lambda: bucket_reference_adherence(sensor_df)),
        ("sensor_spread_within_bucket",   lambda: sensor_spread_within_bucket(sensor_df)),
        ("bucket_temporal_stability",     lambda: bucket_temporal_stability(sensor_df)),
        ("bucket_diurnal_amplitude",      lambda: bucket_diurnal_amplitude(sensor_df)),
        ("small_hive_ambient_tracking",   lambda: small_hive_ambient_tracking(sensor_df, gateway_df)),
        ("large_hive_thermoregulation",   lambda: large_hive_thermoregulation(sensor_df, gateway_df)),
        ("bucket_temperature_ordering",   lambda: bucket_temperature_ordering(sensor_df)),
    ]
    results = []
    for name, fn in metric_fns:
        try:
            results.append(fn())
        except Exception as exc:
            log.error("Metric %s raised: %s", name, exc)
            results.append({
                "metric_name": name,
                "pass_metric": None,
                "threshold":   None,
                "value":       None,
            })
    return results


def classify(pass_metric: bool | None, expected_pass: bool) -> str:
    if pass_metric is None:
        return "ERROR"
    if pass_metric and expected_pass:
        return "TP"
    if not pass_metric and not expected_pass:
        return "TN"
    if pass_metric and not expected_pass:
        return "FP"
    return "FN"


def compute_summary(detail: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for metric_name, grp in detail.groupby("metric_name"):
        evaluated = grp[grp["outcome"] != "ERROR"]
        n_error   = int((grp["outcome"] == "ERROR").sum())
        counts    = evaluated["outcome"].value_counts()
        tp = int(counts.get("TP", 0))
        tn = int(counts.get("TN", 0))
        fp = int(counts.get("FP", 0))
        fn = int(counts.get("FN", 0))

        precision   = tp / (tp + fp) if (tp + fp) > 0 else float("nan")
        recall      = tp / (tp + fn) if (tp + fn) > 0 else float("nan")
        specificity = tn / (tn + fp) if (tn + fp) > 0 else float("nan")
        f1          = (
            2 * precision * recall / (precision + recall)
            if (precision + recall) > 0 else float("nan")
        )
        accuracy = (tp + tn) / len(evaluated) if len(evaluated) > 0 else float("nan")

        rows.append({
            "metric_name": metric_name,
            "TP":          tp,
            "TN":          tn,
            "FP":          fp,
            "FN":          fn,
            "precision":   round(precision,   4),
            "recall":      round(recall,      4),
            "specificity": round(specificity, 4),
            "f1":          round(f1,          4),
            "accuracy":    round(accuracy,    4),
            "n_evaluated": len(evaluated),
            "n_error":     n_error,
        })

    return (
        pd.DataFrame(rows)
        .sort_values("specificity", ascending=False)
        .reset_index(drop=True)
    )


def print_summary(summary: pd.DataFrame) -> None:
    width = 92
    print("\n" + "=" * width)
    print("  TEMPERATURE METRICS — per-metric confusion matrix  (sorted by specificity ↓)")
    print("  specificity = TN / (TN + FP)  — how well each metric catches invalid groups")
    print("=" * width)
    print(
        f"  {'metric':<38} {'TP':>5} {'TN':>5} {'FP':>5} {'FN':>5}"
        f" {'spec':>6} {'recall':>7} {'f1':>6} {'err':>5}"
    )
    print("  " + "-" * (width - 2))
    for _, row in summary.iterrows():
        spec   = f"{row['specificity']:.3f}" if row["specificity"] == row["specificity"] else "  nan"
        recall = f"{row['recall']:.3f}"      if row["recall"]      == row["recall"]      else "  nan"
        f1_str = f"{row['f1']:.3f}"          if row["f1"]          == row["f1"]          else "  nan"
        print(
            f"  {row['metric_name']:<38}"
            f" {row['TP']:>5} {row['TN']:>5} {row['FP']:>5} {row['FN']:>5}"
            f" {spec:>6} {recall:>7} {f1_str:>6} {row['n_error']:>5}"
        )
    print()
    print(f"  Total FP across all metrics: {int(summary['FP'].sum())}")
    print()


def main() -> None:
    args    = parse_args()
    out_dir = Path(args.output)
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    gt           = load_ground_truth()
    detail_rows: list[dict] = []
    skipped      = 0
    total        = len(gt)

    print(f"\nEvaluating {total} ground truth rows against train split…\n")

    for i, row in enumerate(gt.itertuples(), 1):
        group_id      = int(row.group_id)
        date          = row.date
        gt_label      = row.gt_label
        expected_pass = bool(row.expected_pass)

        if i % 50 == 0 or i == 1:
            print(f"  [{i:>3}/{total}] group={group_id}  date={date}")

        parquets = load_parquets(group_id, date)
        if parquets is None:
            skipped += 1
            continue

        sensor_df, gateway_df = parquets

        for m in run_metrics(sensor_df, gateway_df):
            detail_rows.append({
                "group_id":      group_id,
                "date":          date,
                "gt_label":      gt_label,
                "expected_pass": expected_pass,
                "metric_name":   m.get("metric_name"),
                "pass_metric":   m.get("pass_metric"),
                "value":         _to_str(m.get("value")),
                "threshold":     _to_str(m.get("threshold")),
                "outcome":       classify(m.get("pass_metric"), expected_pass),
            })

    evaluated = total - skipped
    print(f"\n  Done. {evaluated}/{total} pairs evaluated  ({skipped} skipped — no data in train split)\n")

    detail       = pd.DataFrame(detail_rows)
    detail_path  = out_dir / "metric_evaluation_detail.parquet"
    summary_path = out_dir / "metric_evaluation_summary.csv"

    detail.to_parquet(detail_path, index=False)
    print(f"  Saved detail  → {detail_path}")

    summary = compute_summary(detail)
    summary.to_csv(summary_path, index=False)
    print(f"  Saved summary → {summary_path}")

    print_summary(summary)


if __name__ == "__main__":
    main()
