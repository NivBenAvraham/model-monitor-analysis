"""
investigate_per_bucket_features.py — Search for per-bucket features that
separate the irreducible conf-5 FPs from conf-5 TPs.

Motivation
----------
Analyst tags valid.txt / invalid.txt based on **per-bucket hive behaviour**
(small / medium / large), NOT on ambient signals.  The current rule already
exploits per-bucket means (R3), spread (R4), std (R5) and ordering (R6c) — but
those are aggregate statistics.  This script scans **time-resolved per-bucket
features** that the rule does not currently use.

Features computed per (group_id, date, bucket)
-----------------------------------------------
  diurnal_amplitude  — mean (max-min) of bucket-mean per calendar day
  daynight_delta     — mean(daytime  hours 09-18) − mean(nighttime hours 22-04)
  hourly_spread_p90  — 90th percentile of within-bucket sensor std at any hour
  trend_slope        — linear-regression slope (°C / day) of bucket-mean
  range_p95p05       — p95 − p5 of bucket-mean across all hours

Populations compared
--------------------
  anchor_valid    — 14 perfect-valid pairs from valid.txt
  anchor_invalid  — 5 perfect-invalid pairs from invalid.txt
  conf5_tp        — train pairs predicted VALID (conf 5) and gt = valid
  conf5_fp        — train pairs predicted VALID (conf 5) and gt = invalid

Usage
-----
    python skills/group_model_temperature_health/scripts/investigate_per_bucket_features.py
"""

from __future__ import annotations

import logging
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

# Quiet metric / data_utils logging that fires for every pair
logging.basicConfig(level=logging.ERROR)

REPO_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO_ROOT / "src"))

from model_monitor.decision.temperature_health_rule import score_group_date  # noqa: E402
from model_monitor.utils.data_utils import (  # noqa: E402
    load_group_date_data,
    resample_sensor_to_hourly,
)

DETAIL_PATH = REPO_ROOT / "data/results/temperature_metric_evaluation/metric_evaluation_detail.parquet"
VALID_TXT   = REPO_ROOT / "skills/hives_temperature_plot_decision/data_analyst_plot_decisions/valid.txt"
INVALID_TXT = REPO_ROOT / "skills/hives_temperature_plot_decision/data_analyst_plot_decisions/invalid.txt"


# ─────────────────────────────────────────────────────────────────────────────
# Anchor + cohort loading
# ─────────────────────────────────────────────────────────────────────────────

def _load_anchor_keys(path: Path) -> set[tuple[int, str]]:
    df = pd.read_csv(path)
    df["date"]     = df["date"].str.strip("'")
    df["group_id"] = df["group_id"].astype(int)
    return set(zip(df.group_id, df.date))


def build_cohorts() -> dict[str, set[tuple[int, str]]]:
    """Score the train split with the current rule, then bucket pairs by cohort."""
    detail = pd.read_parquet(DETAIL_PATH)
    grouped = defaultdict(list)
    for _, row in detail.iterrows():
        key = (int(row.group_id), row.date, row.gt_label)
        grouped[key].append({
            "metric_name": row.metric_name,
            "pass_metric": bool(row.pass_metric) if pd.notna(row.pass_metric) else None,
        })

    valid_keys   = _load_anchor_keys(VALID_TXT)
    invalid_keys = _load_anchor_keys(INVALID_TXT)

    conf5_tp: set[tuple[int, str]] = set()
    conf5_fp: set[tuple[int, str]] = set()
    for (gid, dt, gt), mres in grouped.items():
        out = score_group_date(mres)
        if out["confidence"] != 5:
            continue
        k = (gid, dt)
        if gt == "valid":
            conf5_tp.add(k)
        elif gt == "invalid":
            conf5_fp.add(k)

    return {
        "anchor_valid":   valid_keys,
        "anchor_invalid": invalid_keys,
        "conf5_tp":       conf5_tp,
        "conf5_fp":       conf5_fp,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Per-bucket feature extraction
# ─────────────────────────────────────────────────────────────────────────────

_BUCKETS = ("small", "medium", "large")


def _bucket_features(sensor_df: pd.DataFrame) -> dict[str, dict[str, float]]:
    """Return {bucket: {feature: value}} for one (group_id, date)."""
    sh = resample_sensor_to_hourly(sensor_df)
    sh["timestamp"] = pd.to_datetime(sh["timestamp"])
    sh["day"]       = sh["timestamp"].dt.date
    sh["hour"]      = sh["timestamp"].dt.hour

    out: dict[str, dict[str, float]] = {}
    for bucket in _BUCKETS:
        sub = sh[sh["hive_size_bucket"] == bucket]
        if sub.empty:
            continue

        # Bucket-mean time series (mean across sensors at each hour)
        bm = sub.groupby("timestamp")["pcb_temperature_one"].mean().sort_index()
        if bm.empty:
            continue

        # 1. Diurnal amplitude: mean of (max-min) per calendar day
        per_day = sub.groupby("day")["pcb_temperature_one"].agg(["max", "min"])
        diurnal_amp = float((per_day["max"] - per_day["min"]).mean())

        # 2. Day/night delta: mean(09-18) − mean(22-04, with wrap)
        day_mask   = (sub["hour"] >= 9)  & (sub["hour"] <= 18)
        night_mask = (sub["hour"] >= 22) | (sub["hour"] <= 4)
        day_mean   = float(sub.loc[day_mask,   "pcb_temperature_one"].mean()) if day_mask.any()   else np.nan
        night_mean = float(sub.loc[night_mask, "pcb_temperature_one"].mean()) if night_mask.any() else np.nan
        daynight_delta = day_mean - night_mean if not (np.isnan(day_mean) or np.isnan(night_mean)) else np.nan

        # 3. Hourly within-bucket sensor std p90: at each hour, std across sensors,
        #    then take the 90th percentile across hours
        hourly_std = sub.groupby("timestamp")["pcb_temperature_one"].std()
        hourly_std = hourly_std.dropna()
        hourly_spread_p90 = float(hourly_std.quantile(0.90)) if not hourly_std.empty else np.nan

        # 4. Trend slope (°C per day) — least-squares fit on bucket-mean
        if len(bm) >= 2:
            x = (bm.index - bm.index[0]).total_seconds().to_numpy() / 86400.0   # days
            y = bm.to_numpy()
            slope = float(np.polyfit(x, y, 1)[0])
        else:
            slope = np.nan

        # 5. Range p95-p5 of bucket-mean
        range_p95p05 = float(bm.quantile(0.95) - bm.quantile(0.05))

        out[bucket] = {
            "diurnal_amplitude": diurnal_amp,
            "daynight_delta":    daynight_delta,
            "hourly_spread_p90": hourly_spread_p90,
            "trend_slope":       slope,
            "range_p95p05":      range_p95p05,
        }
    return out


def extract_features(pairs: set[tuple[int, str]]) -> pd.DataFrame:
    rows: list[dict] = []
    for gid, dt in sorted(pairs):
        try:
            sensor_df, _, _ = load_group_date_data(gid, dt)
        except FileNotFoundError:
            continue
        feats = _bucket_features(sensor_df)
        for bucket, vals in feats.items():
            rows.append({"group_id": gid, "date": dt, "bucket": bucket, **vals})
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# Reporting
# ─────────────────────────────────────────────────────────────────────────────

_FEATURES = ["diurnal_amplitude", "daynight_delta", "hourly_spread_p90",
             "trend_slope", "range_p95p05"]


def _summarise(df: pd.DataFrame, label: str, bucket: str) -> dict:
    sub = df[df["bucket"] == bucket]
    if sub.empty:
        return {"cohort": label, "bucket": bucket, "n": 0}
    out = {"cohort": label, "bucket": bucket, "n": len(sub)}
    for feat in _FEATURES:
        s = sub[feat].dropna()
        if s.empty:
            out[feat] = ""
        else:
            out[feat] = f"{s.min():+.2f} / {s.median():+.2f} / {s.max():+.2f}"
    return out


def _separability_score(df_v: pd.Series, df_i: pd.Series) -> str:
    """Return CLEAN / partial / weak / none label for separation."""
    v = df_v.dropna()
    i = df_i.dropna()
    if v.empty or i.empty:
        return "n/a"
    if v.min() > i.max() or v.max() < i.min():
        return "CLEAN"
    if v.median() > i.quantile(0.95) or v.median() < i.quantile(0.05):
        return "partial"
    if v.median() > i.quantile(0.75) or v.median() < i.quantile(0.25):
        return "weak"
    return "none"


def main() -> None:
    print("Loading cohorts and computing per-bucket features…\n")
    cohorts = build_cohorts()
    print(f"  anchor_valid    : {len(cohorts['anchor_valid']):4d} pairs")
    print(f"  anchor_invalid  : {len(cohorts['anchor_invalid']):4d} pairs")
    print(f"  conf5_tp        : {len(cohorts['conf5_tp']):4d} pairs")
    print(f"  conf5_fp        : {len(cohorts['conf5_fp']):4d} pairs\n")

    feat_dfs = {label: extract_features(keys) for label, keys in cohorts.items()}

    # ── 1. Distribution table per cohort/bucket/feature ────────────────────────
    print("=" * 110)
    print("  PER-COHORT × BUCKET × FEATURE  (min / median / max)")
    print("=" * 110)
    rows = []
    for label, df in feat_dfs.items():
        for bucket in _BUCKETS:
            rows.append(_summarise(df, label, bucket))
    summary = pd.DataFrame(rows)
    print(summary.to_string(index=False))
    print()

    # ── 2. Separability between conf5_tp vs conf5_fp ───────────────────────────
    print("=" * 110)
    print("  SEPARABILITY (conf5_tp vs conf5_fp)  per-bucket per-feature")
    print("  CLEAN  = no overlap")
    print("  partial= median of one outside p5/p95 of the other")
    print("  weak   = median of one outside p25/p75 of the other")
    print("  none   = full overlap")
    print("=" * 110)
    tp = feat_dfs["conf5_tp"]
    fp = feat_dfs["conf5_fp"]
    print(f"{'bucket':<10}{'feature':<22}{'TP n':>5}{'FP n':>5}  {'TP min/med/max':<28}  {'FP min/med/max':<28}{'sep':>10}")
    print("-" * 110)
    for bucket in _BUCKETS:
        tp_b = tp[tp["bucket"] == bucket]
        fp_b = fp[fp["bucket"] == bucket]
        for feat in _FEATURES:
            sep = _separability_score(fp_b[feat], tp_b[feat])
            tp_s = tp_b[feat].dropna()
            fp_s = fp_b[feat].dropna()
            tp_str = f"{tp_s.min():+.2f}/{tp_s.median():+.2f}/{tp_s.max():+.2f}" if len(tp_s) else "—"
            fp_str = f"{fp_s.min():+.2f}/{fp_s.median():+.2f}/{fp_s.max():+.2f}" if len(fp_s) else "—"
            print(f"{bucket:<10}{feat:<22}{len(tp_b):>5}{len(fp_b):>5}  {tp_str:<28}  {fp_str:<28}{sep:>10}")
        print()

    # ── 3. Anchor-set sanity check ─────────────────────────────────────────────
    print("=" * 110)
    print("  ANCHOR SANITY  (perfect-valid vs perfect-invalid)")
    print("=" * 110)
    av = feat_dfs["anchor_valid"]
    ai = feat_dfs["anchor_invalid"]
    print(f"{'bucket':<10}{'feature':<22}{'aV n':>5}{'aI n':>5}  {'aV min/med/max':<28}  {'aI min/med/max':<28}{'sep':>10}")
    print("-" * 110)
    for bucket in _BUCKETS:
        av_b = av[av["bucket"] == bucket]
        ai_b = ai[ai["bucket"] == bucket]
        for feat in _FEATURES:
            sep = _separability_score(ai_b[feat], av_b[feat])
            av_s = av_b[feat].dropna()
            ai_s = ai_b[feat].dropna()
            av_str = f"{av_s.min():+.2f}/{av_s.median():+.2f}/{av_s.max():+.2f}" if len(av_s) else "—"
            ai_str = f"{ai_s.min():+.2f}/{ai_s.median():+.2f}/{ai_s.max():+.2f}" if len(ai_s) else "—"
            print(f"{bucket:<10}{feat:<22}{len(av_b):>5}{len(ai_b):>5}  {av_str:<28}  {ai_str:<28}{sep:>10}")
        print()

    # Save raw features so callers can plot them
    out_dir = REPO_ROOT / "data/results/per_bucket_feature_investigation"
    out_dir.mkdir(parents=True, exist_ok=True)
    for label, df in feat_dfs.items():
        if df.empty:
            continue
        df.to_parquet(out_dir / f"{label}.parquet", index=False)
    print(f"\nSaved per-cohort feature tables → {out_dir}")


if __name__ == "__main__":
    main()
