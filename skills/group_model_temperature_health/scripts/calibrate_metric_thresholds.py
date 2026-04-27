"""
calibrate_metric_thresholds.py — Calibrate per-metric thresholds from anchor sets.

Anchor sets (analyst-curated "perfect examples"):
  POSITIVE: skills/hives_temperature_plot_decision/data_analyst_plot_decisions/valid.txt
  NEGATIVE: skills/hives_temperature_plot_decision/data_analyst_plot_decisions/invalid.txt

For each temperature metric (except ambient_temperature_volatility — left untouched):
  1. Extract metric values for the 14 perfect-valid and 5 perfect-invalid pairs.
  2. Find threshold(s) that maximise invalid catch with ZERO valid loss.
  3. Project onto broader train split → report FN rate (extra valids lost).
  4. Print proposed thresholds for configs/thresholds.yaml.

Usage
-----
    python skills/group_model_temperature_health/scripts/calibrate_metric_thresholds.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT      = Path(__file__).resolve().parents[3]
DETAIL_PATH    = REPO_ROOT / "data/results/temperature_metric_evaluation/metric_evaluation_detail.parquet"
VALID_TXT      = REPO_ROOT / "skills/hives_temperature_plot_decision/data_analyst_plot_decisions/valid.txt"
INVALID_TXT    = REPO_ROOT / "skills/hives_temperature_plot_decision/data_analyst_plot_decisions/invalid.txt"

# Metrics not to touch
SKIP_METRICS   = {"ambient_temperature_volatility"}


def load_anchors() -> tuple[set, set]:
    valid = pd.read_csv(VALID_TXT)
    valid["date"]     = valid["date"].str.strip("'")
    valid["group_id"] = valid["group_id"].astype(int)
    inv = pd.read_csv(INVALID_TXT)
    inv["date"]     = inv["date"].str.strip("'")
    inv["group_id"] = inv["group_id"].astype(int)
    return (
        set(zip(valid.group_id, valid.date)),
        set(zip(inv.group_id,   inv.date)),
    )


def parse_value(v):
    try:
        return json.loads(str(v))
    except Exception:
        try:
            return float(v)
        except Exception:
            return None


def best_upper_threshold(valid_vals: list[float], inv_vals: list[float]) -> tuple[float, int, int]:
    """Find max threshold T s.t. (val ≤ T) keeps all valid AND catches as many invalids as possible.

    Returns (T, n_invalid_caught_above_T, n_invalid_total).
    Use for "value should be ≤ threshold" metrics.
    """
    if not valid_vals or not inv_vals:
        return (float("nan"), 0, len(inv_vals))
    valid_max  = max(valid_vals)
    invalid_above_valid_max = sum(1 for x in inv_vals if x > valid_max)
    return (round(valid_max, 4), invalid_above_valid_max, len(inv_vals))


def best_lower_threshold(valid_vals: list[float], inv_vals: list[float]) -> tuple[float, int, int]:
    """Find min threshold T s.t. (val ≥ T) keeps all valid AND catches as many invalids as possible.

    Use for "value should be ≥ threshold" metrics.
    """
    if not valid_vals or not inv_vals:
        return (float("nan"), 0, len(inv_vals))
    valid_min  = min(valid_vals)
    invalid_below_valid_min = sum(1 for x in inv_vals if x < valid_min)
    return (round(valid_min, 4), invalid_below_valid_min, len(inv_vals))


def project_threshold_on_train(detail: pd.DataFrame, metric: str,
                               extract: callable, op: str, threshold: float,
                               valid_keys: set, inv_keys: set) -> dict:
    """Apply a candidate threshold across the full train detail and report impact.

    op: "<=" → pass when value ≤ threshold;  ">=" → pass when value ≥ threshold.
    """
    sub = detail[detail.metric_name == metric].copy()
    sub["v"] = sub["value"].apply(extract)
    sub = sub.dropna(subset=["v"])

    if op == "<=":
        sub["new_pass"] = sub["v"] <= threshold
    else:
        sub["new_pass"] = sub["v"] >= threshold

    pairs = sub[["group_id", "date", "gt_label", "new_pass"]].drop_duplicates()
    pairs["group_id"] = pairs["group_id"].astype(int)

    n_train_valid    = (pairs.gt_label == "valid").sum()
    n_train_invalid  = (pairs.gt_label == "invalid").sum()
    train_valid_fail = ((pairs.gt_label == "valid")   & (~pairs.new_pass)).sum()
    train_inv_fail   = ((pairs.gt_label == "invalid") & (~pairs.new_pass)).sum()

    is_anchor_v = pairs.apply(lambda r: (r.group_id, r.date) in valid_keys, axis=1)
    is_anchor_i = pairs.apply(lambda r: (r.group_id, r.date) in inv_keys,   axis=1)
    anchor_v_fail = (is_anchor_v & (~pairs.new_pass)).sum()
    anchor_i_fail = (is_anchor_i & (~pairs.new_pass)).sum()

    return {
        "anchor_valid_fail":   int(anchor_v_fail),
        "anchor_invalid_fail": int(anchor_i_fail),
        "train_valid_fail":    int(train_valid_fail),
        "train_valid_total":   int(n_train_valid),
        "train_invalid_fail":  int(train_inv_fail),
        "train_invalid_total": int(n_train_invalid),
        "train_valid_fail_pct":   round(train_valid_fail / n_train_valid, 3) if n_train_valid else 0,
        "train_invalid_fail_pct": round(train_inv_fail   / n_train_invalid, 3) if n_train_invalid else 0,
    }


def calibrate(detail: pd.DataFrame, valid_keys: set, inv_keys: set) -> list[dict]:
    """Run calibration for every metric (except SKIP_METRICS) and return proposals."""

    def vals_for(metric: str, anchor_set: set, extract: callable) -> list[float]:
        sub = detail[detail.metric_name == metric].copy()
        sub["k"] = list(zip(sub.group_id.astype(int), sub.date))
        sub = sub[sub["k"].isin(anchor_set)]
        out = sub["value"].apply(extract).dropna().tolist()
        return [float(x) for x in out if isinstance(x, (int, float))]

    # Each metric defines: which sub-fields to calibrate, and their direction
    # (key=label, extract=function from value-string to scalar, op="<=" or ">=")
    specs = [
        # ── R1 ambient_stability — scalar, op = "<=" ───────────────────────────
        ("ambient_stability", "cv", lambda v: parse_value(v) if isinstance(parse_value(v), (int, float)) else None, "<="),
        # ── R2 ambient_range — min (>=) and max (<=) ──────────────────────────
        ("ambient_range", "min", lambda v: parse_value(v).get("min") if isinstance(parse_value(v), dict) else None, ">="),
        ("ambient_range", "max", lambda v: parse_value(v).get("max") if isinstance(parse_value(v), dict) else None, "<="),
        # ── R3 bucket_reference_adherence — per-bucket low (>=) + high (<=) ──
        ("bucket_reference_adherence", "small",  lambda v: parse_value(v).get("small")  if isinstance(parse_value(v), dict) else None, "both"),
        ("bucket_reference_adherence", "medium", lambda v: parse_value(v).get("medium") if isinstance(parse_value(v), dict) else None, "both"),
        ("bucket_reference_adherence", "large",  lambda v: parse_value(v).get("large")  if isinstance(parse_value(v), dict) else None, "both"),
        # ── R4 sensor_spread_within_bucket — per-bucket spread (<=) ───────────
        ("sensor_spread_within_bucket", "small",  lambda v: parse_value(v).get("small")  if isinstance(parse_value(v), dict) else None, "<="),
        ("sensor_spread_within_bucket", "medium", lambda v: parse_value(v).get("medium") if isinstance(parse_value(v), dict) else None, "<="),
        ("sensor_spread_within_bucket", "large",  lambda v: parse_value(v).get("large")  if isinstance(parse_value(v), dict) else None, "<="),
        # ── R5 bucket_temporal_stability — per-bucket std (<=) ────────────────
        ("bucket_temporal_stability", "small",  lambda v: parse_value(v).get("small")  if isinstance(parse_value(v), dict) else None, "<="),
        ("bucket_temporal_stability", "medium", lambda v: parse_value(v).get("medium") if isinstance(parse_value(v), dict) else None, "<="),
        ("bucket_temporal_stability", "large",  lambda v: parse_value(v).get("large")  if isinstance(parse_value(v), dict) else None, "<="),
        # ── R6c bucket_temperature_ordering — per gap (>=) ────────────────────
        ("bucket_temperature_ordering", "small→medium", lambda v: parse_value(v).get("small→medium") if isinstance(parse_value(v), dict) else None, ">="),
        ("bucket_temperature_ordering", "medium→large", lambda v: parse_value(v).get("medium→large") if isinstance(parse_value(v), dict) else None, ">="),
        # ── R6a small_hive_ambient_tracking — scalar (>=) ─────────────────────
        ("small_hive_ambient_tracking", "r", lambda v: parse_value(v) if isinstance(parse_value(v), (int, float)) else None, ">="),
        # ── R6b large_hive_thermoregulation — scalar (<=) ─────────────────────
        ("large_hive_thermoregulation", "r", lambda v: parse_value(v) if isinstance(parse_value(v), (int, float)) else None, "<="),
    ]

    proposals = []
    for metric, label, extract, op in specs:
        if metric in SKIP_METRICS:
            continue

        v = vals_for(metric, valid_keys, extract)
        i = vals_for(metric, inv_keys,   extract)

        if not v or not i:
            continue

        if op == "<=":
            t, caught, total_inv = best_upper_threshold(v, i)
            proj = project_threshold_on_train(detail, metric, extract, "<=", t, valid_keys, inv_keys)
            proposals.append({
                "metric": metric, "field": label, "op": "<=", "threshold": t,
                "anchor_invalid_caught": caught, "anchor_invalid_total": total_inv,
                **proj, "valid_range": (round(min(v), 3), round(max(v), 3)),
                "invalid_range": (round(min(i), 3), round(max(i), 3)),
            })
        elif op == ">=":
            t, caught, total_inv = best_lower_threshold(v, i)
            proj = project_threshold_on_train(detail, metric, extract, ">=", t, valid_keys, inv_keys)
            proposals.append({
                "metric": metric, "field": label, "op": ">=", "threshold": t,
                "anchor_invalid_caught": caught, "anchor_invalid_total": total_inv,
                **proj, "valid_range": (round(min(v), 3), round(max(v), 3)),
                "invalid_range": (round(min(i), 3), round(max(i), 3)),
            })
        elif op == "both":
            t_lo, c_lo, n_inv = best_lower_threshold(v, i)
            t_hi, c_hi, _     = best_upper_threshold(v, i)
            proj_lo = project_threshold_on_train(detail, metric, extract, ">=", t_lo, valid_keys, inv_keys)
            proj_hi = project_threshold_on_train(detail, metric, extract, "<=", t_hi, valid_keys, inv_keys)
            proposals.append({
                "metric": metric, "field": f"{label}.low",  "op": ">=", "threshold": t_lo,
                "anchor_invalid_caught": c_lo, "anchor_invalid_total": n_inv, **proj_lo,
                "valid_range": (round(min(v), 3), round(max(v), 3)),
                "invalid_range": (round(min(i), 3), round(max(i), 3)),
            })
            proposals.append({
                "metric": metric, "field": f"{label}.high", "op": "<=", "threshold": t_hi,
                "anchor_invalid_caught": c_hi, "anchor_invalid_total": n_inv, **proj_hi,
                "valid_range": (round(min(v), 3), round(max(v), 3)),
                "invalid_range": (round(min(i), 3), round(max(i), 3)),
            })

    return proposals


def main() -> None:
    valid_keys, inv_keys = load_anchors()
    detail = pd.read_parquet(DETAIL_PATH)

    print(f"\nAnchors: {len(valid_keys)} valid, {len(inv_keys)} invalid (perfect examples)")
    print(f"Skipping metrics: {SKIP_METRICS}\n")

    proposals = calibrate(detail, valid_keys, inv_keys)

    # Header
    print("=" * 124)
    print(f"{'metric':<32} {'field':<14} {'op':<3} {'thr':>9} {'anchor_inv_catch':>18} "
          f"{'broader_train_v_fail':>22} {'broader_train_inv_fail':>22}")
    print("-" * 124)
    for p in proposals:
        thr      = p["threshold"]
        catch    = f"{p['anchor_invalid_caught']}/{p['anchor_invalid_total']}"
        v_fail   = f"{p['train_valid_fail']}/{p['train_valid_total']} ({p['train_valid_fail_pct']:.1%})"
        i_fail   = f"{p['train_invalid_fail']}/{p['train_invalid_total']} ({p['train_invalid_fail_pct']:.1%})"
        print(f"{p['metric']:<32} {p['field']:<14} {p['op']:<3} {thr:>9.3f} {catch:>18}  {v_fail:>22}  {i_fail:>22}")

    print()
    print("Notes:")
    print("  - thr  = threshold that catches MAX invalid anchors with 0 valid anchor loss.")
    print("  - broader_train_v_fail = how many train valid pairs would now fail this metric.")
    print("  - 'op' shows direction: <= (value should be ≤ thr) / >= (value should be ≥ thr).")


if __name__ == "__main__":
    main()
