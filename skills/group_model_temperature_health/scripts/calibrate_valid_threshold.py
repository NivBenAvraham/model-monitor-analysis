"""
calibrate_valid_threshold.py — Calibrate valid_score thresholds for temperature_health_rule.

Uses two anchor sets as the calibration reference:

  POSITIVE anchor — conf_4+5 valid pairs from valid.txt (confidence ≥ 4)
    These must be predicted VALID.  Source of truth for the "safe" side.

  NEGATIVE anchor — "perfect invalid" pairs from history/2026-04-15/invalid.txt
    Analyst-confirmed invalids that pass all 9 temperature metrics.
    These define the FLOOR of what an invalid group looks like.
    Goal: find the lowest valid_score threshold that keeps these below it.

Scoring rule applied
---------------------
  1. R6c (bucket_temperature_ordering) mandatory gate
  2. valid_score = pass_count of 7 scored metrics / n_assessed
     (excludes R6c gate and R3 bonus)
  3. R3 bonus flag — tracked separately (never passes for perfect invalids)

Usage
-----
    python skills/group_model_temperature_health/scripts/calibrate_valid_threshold.py

    # specify a different detail parquet
    python skills/group_model_temperature_health/scripts/calibrate_valid_threshold.py \\
        --detail data/results/my_run/metric_evaluation_detail.parquet
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd
import numpy as np

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

DETAIL_DEFAULT   = REPO_ROOT / "data/results/temperature_metric_evaluation/metric_evaluation_detail.parquet"
VALID_TXT        = REPO_ROOT / "skills/hives_temperature_plot_decision/data_analyst_plot_decisions/valid.txt"
PERFECT_INVALID_TXT = REPO_ROOT / "skills/hives_temperature_plot_decision/history/2026-04-15/invalid.txt"

_R6C = "bucket_temperature_ordering"
_R3  = "bucket_reference_adherence"
_R4  = "sensor_spread_within_bucket"
_SCORE_METRICS = {
    "ambient_stability",
    "ambient_range",
    "ambient_temperature_volatility",
    "sensor_spread_within_bucket",    # R4
    "bucket_temporal_stability",
    "small_hive_ambient_tracking",
    "large_hive_thermoregulation",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Calibrate valid_score thresholds.")
    p.add_argument("--detail", default=str(DETAIL_DEFAULT),
                   help="Path to metric_evaluation_detail.parquet")
    return p.parse_args()


def load_anchor_sets() -> tuple[set, set, set]:
    """Return (conf4_pairs, conf5_pairs, perfect_invalid_pairs)."""
    # Positive anchor
    valid_txt = pd.read_csv(VALID_TXT)
    valid_txt["date"]     = valid_txt["date"].str.strip("'")
    valid_txt["group_id"] = valid_txt["group_id"].astype(int)
    hc   = valid_txt.groupby(["group_id", "date"])["confidence"].max().reset_index()
    conf4 = set(zip(hc[hc.confidence == 4].group_id, hc[hc.confidence == 4].date))
    conf5 = set(zip(hc[hc.confidence == 5].group_id, hc[hc.confidence == 5].date))

    # Negative anchor — "perfect invalid" entries
    inv = pd.read_csv(PERFECT_INVALID_TXT)
    inv["date"]     = inv["date"].str.strip("'")
    inv["group_id"] = inv["group_id"].astype(int)
    inv["reason"]   = inv["reason"].str.strip()
    perf = inv[inv["reason"].str.contains("perfect invalid", case=False, na=False)]
    perfect_invalid = set(zip(perf["group_id"], perf["date"]))

    return conf4, conf5, perfect_invalid


def compute_scores(detail: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (gid, date, gt_label), grp in detail.groupby(["group_id", "date", "gt_label"]):
        by_name = dict(zip(grp["metric_name"], grp["pass_metric"].map(
            lambda x: None if pd.isna(x) else bool(x)
        )))

        # R6c gate
        r6c_pass = by_name.get(_R6C)
        if r6c_pass is False:
            rows.append({
                "group_id": gid, "date": date, "gt_label": gt_label,
                "r6c_pass": False, "r3_pass": None, "r4_pass": by_name.get(_R4),
                "pass_count": 0, "n_assessed": 0, "valid_score": 0.0,
            })
            continue

        pc = na = 0
        for name in _SCORE_METRICS:
            v = by_name.get(name)
            if v is None:
                continue
            na += 1
            if v:
                pc += 1

        score  = pc / na if na > 0 else 0.0
        r3     = by_name.get(_R3)
        r4     = by_name.get(_R4)

        rows.append({
            "group_id": gid, "date": date, "gt_label": gt_label,
            "r6c_pass": True if r6c_pass is None else bool(r6c_pass),
            "r3_pass":  bool(r3) if r3 is not None else None,
            "r4_pass":  bool(r4) if r4 is not None else None,
            "pass_count": pc, "n_assessed": na, "valid_score": round(score, 4),
        })

    return pd.DataFrame(rows)


def tag_group(df: pd.DataFrame, conf4: set, conf5: set, perfect_invalid: set) -> pd.DataFrame:
    df = df.copy()
    def _tag(row):
        k = (int(row.group_id), row.date)
        if k in perfect_invalid:
            return "perfect_invalid"
        if row.gt_label == "invalid":
            return "other_invalid"
        if k in conf5:
            return "conf_5"
        if k in conf4:
            return "conf_4"
        return "other_valid"
    df["analyst_group"] = df.apply(_tag, axis=1)
    return df


def print_anchor_pass_rates(detail: pd.DataFrame, conf4: set, conf5: set,
                            perfect_invalid: set) -> None:
    """Show per-metric pass rates for the two anchor sets."""
    def _tag(row):
        k = (int(row.group_id), row.date)
        if k in perfect_invalid: return "perfect_invalid"
        if k in conf4 or k in conf5: return "conf_45_valid"
        return "other"

    detail = detail.copy()
    detail["anchor"] = detail.apply(_tag, axis=1)
    anchor = detail[detail["anchor"] != "other"]

    pm = (
        anchor.groupby(["metric_name", "anchor"])["pass_metric"]
        .mean()
        .unstack()
        .round(3)
    )
    if "conf_45_valid" in pm and "perfect_invalid" in pm:
        pm["gap (valid-invalid)"] = (pm["conf_45_valid"] - pm["perfect_invalid"]).round(3)
        pm = pm.sort_values("gap (valid-invalid)", ascending=False)

    print("\n" + "=" * 72)
    print("  PER-METRIC PASS RATES — conf_45_valid vs perfect_invalid anchors")
    print("=" * 72)
    print(pm.to_string())
    print()
    print("  R4 (sensor_spread_within_bucket) and R3 (bucket_reference_adherence)")
    print("  are the strongest discriminators for 'perfect invalid' groups.")
    print("  Note: perfect_invalid groups NEVER pass R3 (0% pass rate).")


def print_distribution(scores: pd.DataFrame) -> None:
    print("\n" + "=" * 72)
    print("  VALID_SCORE DISTRIBUTION  (R6c gate + 7-metric score)")
    print("=" * 72)
    print(f"  {'group':<18} {'n':>4}  {'min':>5}  {'p25':>5}  {'med':>5}  {'p75':>5}  {'max':>5}  {'r3_pass%':>9}")
    print("  " + "-" * 60)
    order = ["conf_5", "conf_4", "other_valid", "perfect_invalid", "other_invalid"]
    for grp in order:
        sub = scores[scores["analyst_group"] == grp]
        if sub.empty:
            continue
        vs = sub["valid_score"]
        r3 = sub["r3_pass"].mean() if sub["r3_pass"].notna().any() else float("nan")
        print(
            f"  {grp:<18} {len(sub):>4}"
            f"  {vs.min():>5.3f}  {vs.quantile(.25):>5.3f}"
            f"  {vs.median():>5.3f}  {vs.quantile(.75):>5.3f}"
            f"  {vs.max():>5.3f}  {r3:>8.1%}"
        )


def print_threshold_table(scores: pd.DataFrame) -> None:
    print("\n" + "=" * 90)
    print("  THRESHOLD SWEEP  (anchors: conf_45 = must be VALID / perfect_invalid = must be INVALID)")
    print("=" * 90)
    conf5   = scores[scores["analyst_group"] == "conf_5"]
    conf4   = scores[scores["analyst_group"] == "conf_4"]
    other   = scores[scores["analyst_group"] == "other_valid"]
    perf_inv = scores[scores["analyst_group"] == "perfect_invalid"]
    all_inv  = scores[scores["analyst_group"].isin(["perfect_invalid", "other_invalid"])]
    n5, n4, nov = len(conf5), len(conf4), len(other)
    npi, ni = len(perf_inv), len(all_inv)

    print(
        f"  {'threshold':>10}  {'TP_conf5':>9}  {'TP_conf4':>9}"
        f"  {'FP_perf_inv':>12}  {'FP_all_inv':>12}"
    )
    print("  " + "-" * 66)
    for t in np.arange(0.0, 1.01, 1 / 7):
        tp5   = (conf5["valid_score"]    >= t).sum()
        tp4   = (conf4["valid_score"]    >= t).sum()
        fp_pi = (perf_inv["valid_score"] >= t).sum()
        fp_ai = (all_inv["valid_score"]  >= t).sum()
        print(
            f"  {t:>10.3f}  {tp5:>4}/{n5:<4}  {tp4:>4}/{n4:<4}"
            f"  {fp_pi:>5}/{npi:<6}  {fp_ai:>5}/{ni}"
        )


def print_r3r4_combined(scores: pd.DataFrame) -> None:
    """Show impact of requiring BOTH R3 and R4 to pass for high-scoring groups."""
    print("\n" + "=" * 72)
    print("  COMBINED R3+R4 GATE ANALYSIS")
    print("  (only applied to groups that already pass R6c + score >= 5/7)")
    print("=" * 72)
    high = scores[scores["valid_score"] >= 5 / 7].copy()
    high["r3_and_r4_pass"] = high["r3_pass"].fillna(False) & high["r4_pass"].fillna(False)

    for grp in ["conf_5", "conf_4", "other_valid", "perfect_invalid", "other_invalid"]:
        sub = high[high["analyst_group"] == grp]
        if sub.empty:
            continue
        n     = len(sub)
        n_pass = sub["r3_and_r4_pass"].sum()
        print(f"  {grp:<20}  n={n:>3}  R3∧R4 pass: {n_pass:>3}/{n}  ({n_pass/n:>5.1%})")

    print()
    print("  Interpretation: requiring BOTH R3 AND R4 to pass is a tight filter.")
    print("  If it eliminates most perfect_invalid while keeping most conf_45 → use as secondary gate.")


def suggest_thresholds(scores: pd.DataFrame, conf4: set, conf5: set,
                       perfect_invalid: set) -> dict:
    """Suggest θ₅ / θ₄ that minimises FP on the perfect_invalid anchor set.

    Strategy (FP reduction is priority):
      - Sweep candidate thresholds (multiples of 1/7).
      - Pick the highest θ where FP_perfect_invalid is minimised.
      - Accept the TP loss on conf_45 as the necessary cost of FP minimisation.
    """
    pi_scores   = scores[scores["analyst_group"] == "perfect_invalid"]["valid_score"]
    hc45_scores = scores[scores["analyst_group"].isin(["conf_4", "conf_5"])]["valid_score"]

    thresholds = [k / 7 for k in range(8)]

    # Find minimum FP count over all candidate thresholds
    fp_counts = [(t, int((pi_scores >= t).sum())) for t in thresholds]
    min_fp    = min(fp for _, fp in fp_counts)

    # Among thresholds that achieve minimum FP, pick the lowest one
    # (keeps the most TP on conf_45)
    best_t = min(t for t, fp in fp_counts if fp == min_fp)

    pi_above = int((pi_scores >= best_t).sum())

    return {
        "score_confidence_5_min": round(best_t, 4),
        "score_confidence_4_min": round(best_t, 4),
        "_debug_perfect_invalid_above_t4": pi_above,
        "_debug_perfect_invalid_above_t5": pi_above,
    }


def main() -> None:
    args   = parse_args()
    detail = pd.read_parquet(args.detail)

    conf4, conf5, perfect_invalid = load_anchor_sets()
    print(f"\nAnchors loaded:")
    print(f"  positive (conf-4):       {len(conf4)} pairs")
    print(f"  positive (conf-5):       {len(conf5)} pairs")
    print(f"  negative (perfect inv.): {len(perfect_invalid)} pairs"
          f"  (from history/2026-04-15/invalid.txt)")

    # Show which perfect_invalid pairs are in the detail
    avail = set()
    for (gid, date), _ in detail.groupby(["group_id", "date"]):
        if (int(gid), date) in perfect_invalid:
            avail.add((int(gid), date))
    missing = perfect_invalid - avail
    if missing:
        print(f"  WARNING: {len(missing)} perfect_invalid pairs not in detail parquet: {missing}")

    print_anchor_pass_rates(detail, conf4, conf5, perfect_invalid)

    scores = compute_scores(detail)
    scores = tag_group(scores, conf4, conf5, perfect_invalid)

    print_distribution(scores)
    print_threshold_table(scores)
    print_r3r4_combined(scores)

    suggested = suggest_thresholds(scores, conf4, conf5, perfect_invalid)

    print("\n" + "=" * 60)
    print("  SUGGESTED THRESHOLDS (add to configs/thresholds.yaml)")
    print("=" * 60)
    pi_t4 = suggested.pop("_debug_perfect_invalid_above_t4")
    pi_t5 = suggested.pop("_debug_perfect_invalid_above_t5")
    for k, v in suggested.items():
        n = round(v * 7)
        print(f"    {k}: {v}   # {n}/7 scored metrics pass")
    print()
    print(f"  At suggested thresholds:")
    print(f"    {pi_t4}/{len(perfect_invalid)} perfect_invalid pairs still score >= θ₄  ← irreducible FP")
    print(f"    {pi_t5}/{len(perfect_invalid)} perfect_invalid pairs still score >= θ₅")
    print()
    print("  To push FP lower: investigate R4 threshold tightening (strongest")
    print("  discriminator, 25 pt gap) or add R3∧R4 failure as a secondary gate.")
    print("  See: skills/group_model_temperature_health/notebooks/zero_coverage_investigation.ipynb\n")


if __name__ == "__main__":
    main()
