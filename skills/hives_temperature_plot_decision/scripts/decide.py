"""
decide.py — valid-group detector for hive temperature plots.

For a given (date, group_id) pair, loads raw sensor + gateway parquet data,
evaluates 6 physics-based rules, looks up analyst tags, and combines both
signals into a valid_score, invalid_score, and valid_pct.

The output is a binary prediction focused on discovering valid pairs:

    Valid         — valid_pct >= VALID_PCT_THRESHOLD  (we are confident it is valid)
    Not Confident — everything else  (not enough evidence to call it valid)

Both scores are still computed and returned so you can inspect the evidence
and calibrate thresholds over time.

Usage:
    python skills/hives_temperature_plot_decision/scripts/decide.py \\
        --group 36 --date 2026-03-01

    python skills/hives_temperature_plot_decision/scripts/decide.py \\
        --batch pairs.csv --output results.csv --verbose
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR  = REPO_ROOT / "data/samples/temperature-export"
TAGS_DIR  = Path(__file__).resolve().parents[1] / "data_analyst_plot_decisions"

# ── Decision thresholds ───────────────────────────────────────────────────────
# Goal: detect valid pairs as confidently as possible.
# Decision is binary: Valid or Not Confident.
VALID_PCT_THRESHOLD = 0.65   # valid_score / (valid + invalid) >= this → Valid
INVALID_MIN_SCORE   = 4.0    # informational only — kept for calibration reference
SCORE_EPSILON       = 1e-6   # avoid divide-by-zero when both scores are 0

# ── Tag bonus weights ─────────────────────────────────────────────────────────
# Only valid tags contribute to the score. Invalid tags are recorded for
# transparency (tag_source / tag_reason) but carry zero weight — the goal is
# to be the best predictor of valid, not to penalise suspected invalids.
# Valid confidence (1–5) maps 1:1 to tag_valid_bonus.

# ── R1: Ambient stability ─────────────────────────────────────────────────────
# Calibrated from train data: valid and invalid pairs have nearly identical CV
# distributions (both medians ~0.34). Only truly extreme ambient counts as a signal.
AMBIENT_CV_HIGH = 0.70   # CV > this → severely unstable (top ~5% of all pairs)
AMBIENT_CV_MED  = 0.55   # CV > this → strongly unstable
AMBIENT_CV_LOW  = 0.15   # CV < this → genuinely stable (valid signal)

# ── R2: Ambient range ─────────────────────────────────────────────────────────
AMBIENT_MIN_THRESHOLD = 2.0    # °C — below this is genuinely too cold
AMBIENT_MAX_THRESHOLD = 50.0   # °C — suspiciously hot (sensor error)

# ── R3: Per-bucket reference adherence ───────────────────────────────────────
# Each bucket is judged against its own reference band:
#   small  → [26 - BAND, 26 + BAND]  (low weight: small tracks ambient, not a fixed line)
#   medium → [26, 32]                 (medium weight)
#   large  → [28, 35]                 (high weight: large must hold the reference band)
# Scores are weighted so small barely contributes (it legitimately tracks ambient).
BUCKET_ADHERENCE_BAND    = 3.0   # °C half-width for the small single-reference
BUCKET_ADHERENCE_WEIGHTS = {"small": 0.10, "medium": 0.35, "large": 0.55}
ADHERENCE_MIN            = 0.35  # weighted avg < this → strong invalid
ADHERENCE_MED            = 0.50  # weighted avg < this → mild invalid
ADHERENCE_GOOD           = 0.65  # weighted avg > this → valid signal

# ── R4: Per-bucket sensor spread ─────────────────────────────────────────────
# Std of individual sensor hourly means within each bucket.
# Sensors sharing the same predicted size should track each other.
SENSOR_SPREAD_HIGH = 5.0   # °C — avg spread across buckets → strong invalid
SENSOR_SPREAD_MED  = 3.0   # °C — avg spread → mild invalid
SENSOR_SPREAD_LOW  = 1.5   # °C — avg spread → valid signal

# R4b: single-bucket MAX spread veto — derived from perfect invalid examples.
# 935-02-08 (perfect invalid): small_spread=9.33.
# GT-valid groups reach up to 8.4°C, so threshold set above that.
# A single bucket this noisy means sensors in that size-class can't agree at all.
SENSOR_SPREAD_MAX_VETO = 9.0   # °C — if ANY bucket exceeds this → strong invalid (+2)

# ── R7: Large-bucket spike fraction ──────────────────────────────────────────
# Fraction of per-sensor readings that deviate more than SPIKE_DELTA°C from
# the bucket's hourly mean. A few erratic sensors dragging the large bucket
# signal are a reliable invalid indicator.
# Derived from EDA: large_spike_frac > 0.008 catches 10/11 FP pairs with a
# 2.8:1 FP-to-TP ratio. GT-valid upper bound is 0.027; FP pairs reach 0.022+.
SPIKE_DELTA            = 8.0   # °C — deviation threshold to call a reading a "spike"
LARGE_SPIKE_FRAC_HIGH  = 0.008 # fraction of readings above SPIKE_DELTA → strong invalid (+2)

# ── R3b: Medium adherence floor — DISABLED ────────────────────────────────────
# Originally derived from 625-03-18 and 935-03-21 (perfect invalids, adh≈0.14).
# DISABLED because GT-valid pairs also show medium_adherence as low as 0.02
# when the group has very few medium-bucket sensors (noisy, unreliable).
# Without a per-bucket sensor count guard this rule fires too many false negatives.
# Re-enable once n_sensors is tracked in bucket_stats and a count guard is added.
MEDIUM_ADHERENCE_FLOOR = 0.0   # effectively disabled (nothing fires below 0)

# ── R5: Per-bucket temporal stability (bucket-weighted) ──────────────────────
# Std of bucket mean temperature across hourly time steps.
# Expected behaviour: large=flat, medium=moderate, small=variable (tracks ambient).
TEMPORAL_STD_HIGH_LARGE  = 2.5   # large must hold a flat line
TEMPORAL_STD_MED_LARGE   = 1.5
TEMPORAL_STD_LOW_LARGE   = 0.8

TEMPORAL_STD_HIGH_MEDIUM = 4.0
TEMPORAL_STD_MED_MEDIUM  = 2.5
TEMPORAL_STD_LOW_MEDIUM  = 1.5

TEMPORAL_STD_HIGH_SMALL  = 7.0   # small follows ambient — variation is acceptable
TEMPORAL_STD_MED_SMALL   = 5.0
TEMPORAL_STD_LOW_SMALL   = 3.0

# ── R6: Per-bucket ambient correlation + cross-bucket ordering ────────────────
# small: expected to track ambient (high r = GOOD)
# large: expected to self-regulate (low r = GOOD; high r = colony not regulating)
# Calibrated from train data: valid large-corr p75=0.695, so threshold at 0.85
# catches only the top ~10% where thermoregulation clearly breaks down.
SMALL_CORR_MIN  = 0.40   # r below this → small definitely not tracking ambient
LARGE_CORR_MAX  = 0.85   # r above this → large not thermoregulating → invalid

BUCKET_SEP_MIN  = 1.5    # °C — minimum gap between adjacent bucket means
BUCKET_SEP_GOOD = 3.5    # °C — gap above this → good separation

# ── Reference bands per bucket (from plot_temperature_scatter.py SIZE_MAP) ───
BUCKET_REFS = {
    "small":  (26.0 - BUCKET_ADHERENCE_BAND, 26.0 + BUCKET_ADHERENCE_BAND),
    "medium": (26.0, 32.0),
    "large":  (28.0, 35.0),
}

TEMPORAL_THRESHOLDS = {
    "small":  (TEMPORAL_STD_HIGH_SMALL,  TEMPORAL_STD_MED_SMALL,  TEMPORAL_STD_LOW_SMALL),
    "medium": (TEMPORAL_STD_HIGH_MEDIUM, TEMPORAL_STD_MED_MEDIUM, TEMPORAL_STD_LOW_MEDIUM),
    "large":  (TEMPORAL_STD_HIGH_LARGE,  TEMPORAL_STD_MED_LARGE,  TEMPORAL_STD_LOW_LARGE),
}


# ─────────────────────────────────────────────────────────────────────────────
# Tag loading
# ─────────────────────────────────────────────────────────────────────────────

def load_tags() -> tuple[dict, dict]:
    """Load analyst-tagged valid and invalid examples from the decision files."""
    valid_tags: dict[tuple[str, int], int] = {}
    invalid_tags: dict[tuple[str, int], dict] = {}

    valid_path = TAGS_DIR / "valid.txt"
    with open(valid_path, newline="") as f:
        for row in csv.DictReader(f):
            date       = row["date"].strip().strip("'")
            group_id   = int(row["group_id"].strip())
            confidence = int(row["confidence"].strip())
            key = (date, group_id)
            if key not in valid_tags or confidence > valid_tags[key]:
                valid_tags[key] = confidence

    invalid_path = TAGS_DIR / "invalid.txt"
    with open(invalid_path, newline="") as f:
        for row in csv.DictReader(f):
            date     = row["date"].strip().strip("'")
            group_id = int(row["group_id"].strip())
            reason   = row["reason"].strip()
            key = (date, group_id)
            is_perfect = "perfect invalid" in reason.lower()
            if key not in invalid_tags or is_perfect:
                invalid_tags[key] = {"reason": reason, "perfect": is_perfect}

    return valid_tags, invalid_tags


# ─────────────────────────────────────────────────────────────────────────────
# Data loading
# ─────────────────────────────────────────────────────────────────────────────

def load_data(group_id: int, date: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    base = DATA_DIR / f"group_{group_id}" / date
    sensor_files  = list(base.glob(f"{group_id}_*_sensor_temperature.parquet"))
    gateway_files = list(base.glob(f"{group_id}_*_gateway_temperature.parquet"))
    if not sensor_files or not gateway_files:
        raise FileNotFoundError(
            f"No parquet files in {base}. Run pull_samples.py first."
        )
    return pd.read_parquet(sensor_files[0]), pd.read_parquet(gateway_files[0])


def resample(sensor_df: pd.DataFrame, gateway_df: pd.DataFrame):
    sensor_hourly = (
        sensor_df
        .groupby(["hive_size_bucket", "sensor_mac_address",
                  pd.Grouper(key="timestamp", freq="1h")])["pcb_temperature_one"]
        .mean()
        .reset_index()
    )
    gateway_hourly = (
        gateway_df
        .groupby(pd.Grouper(key="timestamp", freq="1h"))["pcb_temperature_two"]
        .mean()
        .reset_index()
    )
    return sensor_hourly, gateway_hourly


# ─────────────────────────────────────────────────────────────────────────────
# Feature computation
# ─────────────────────────────────────────────────────────────────────────────

def compute_features(
    sensor_hourly: pd.DataFrame,
    gateway_hourly: pd.DataFrame,
) -> dict:
    ambient = gateway_hourly.set_index("timestamp")["pcb_temperature_two"].dropna()

    amb_mean = ambient.mean()
    amb_std  = ambient.std()
    features: dict = {
        "ambient_cv":  amb_std / abs(amb_mean) if abs(amb_mean) > 1e-3 else 0.0,
        "ambient_min": float(ambient.min()),
        "ambient_max": float(ambient.max()),
        "bucket_stats": {},
    }

    for bucket in ("small", "medium", "large"):
        bdf = sensor_hourly[sensor_hourly["hive_size_bucket"] == bucket]
        if bdf.empty:
            continue

        lo, hi = BUCKET_REFS[bucket]

        # R3: fraction of (sensor, hour) readings inside the reference band
        in_band = float(
            ((bdf["pcb_temperature_one"] >= lo) & (bdf["pcb_temperature_one"] <= hi)).mean()
        )

        # R4: per-hour std across sensors, averaged across hours
        pivot = (
            bdf.groupby(["timestamp", "sensor_mac_address"])["pcb_temperature_one"]
            .mean()
            .unstack()
        )
        sensor_spread = float(pivot.std(axis=1).mean()) if pivot.shape[1] > 1 else 0.0

        # Bucket hourly mean (for R5 + R6)
        bucket_mean_ts = bdf.groupby("timestamp")["pcb_temperature_one"].mean()

        # R5: temporal stability of bucket mean
        temporal_std = float(bucket_mean_ts.std()) if len(bucket_mean_ts) > 1 else 0.0

        # R7: temporal range of bucket mean (max - min over the day)
        temp_range = float(bucket_mean_ts.max() - bucket_mean_ts.min()) if len(bucket_mean_ts) > 1 else 0.0

        # R7: fraction of per-sensor readings that deviate > SPIKE_DELTA from the
        # bucket's hourly mean — captures erratic/malfunctioning sensors in the bucket
        pivot_all = (
            bdf.groupby(["timestamp", "sensor_mac_address"])["pcb_temperature_one"]
            .mean()
        )
        bm_aligned = bdf.groupby("timestamp")["pcb_temperature_one"].mean()
        bdf_with_bm = bdf.join(bm_aligned.rename("_bm"), on="timestamp")
        spike_frac = float(
            (abs(bdf_with_bm["pcb_temperature_one"] - bdf_with_bm["_bm"]) > SPIKE_DELTA).mean()
        )

        # R6: Pearson correlation between bucket mean and ambient
        common = bucket_mean_ts.index.intersection(ambient.index)
        if len(common) >= 3:
            corr = float(bucket_mean_ts[common].corr(ambient[common]))
        else:
            corr = float("nan")

        features["bucket_stats"][bucket] = {
            "adherence":     in_band,
            "sensor_spread": sensor_spread,
            "temporal_std":  temporal_std,
            "temp_range":    temp_range,
            "spike_frac":    spike_frac,
            "mean_temp":     float(bucket_mean_ts.mean()),
            "corr_ambient":  corr,
        }

    return features


# ─────────────────────────────────────────────────────────────────────────────
# Score computation from features
# ─────────────────────────────────────────────────────────────────────────────

def compute_feature_scores(features: dict) -> tuple[float, float, list[str]]:
    """Return (feature_invalid_score, feature_valid_score, reasons), each capped at 5."""
    inv: float = 0.0
    val: float = 0.0
    reasons: list[str] = []

    # ── R1: Ambient stability ─────────────────────────────────────────────────
    cv = features["ambient_cv"]
    if cv > AMBIENT_CV_HIGH:
        inv += 2
        reasons.append(f"ambient highly unstable (CV={cv:.2f})")
    elif cv > AMBIENT_CV_MED:
        inv += 1
        reasons.append(f"ambient mildly unstable (CV={cv:.2f})")
    elif cv < AMBIENT_CV_LOW:
        val += 1

    # ── R2: Ambient range ─────────────────────────────────────────────────────
    amb_min, amb_max = features["ambient_min"], features["ambient_max"]
    if amb_min < AMBIENT_MIN_THRESHOLD or amb_max > AMBIENT_MAX_THRESHOLD:
        inv += 2
        reasons.append(f"ambient out of range ({amb_min:.1f}–{amb_max:.1f}°C)")
    else:
        val += 1

    stats = features["bucket_stats"]
    present = [b for b in ("small", "medium", "large") if b in stats]

    if not present:
        inv += 5
        reasons.append("no sensor data found for any bucket")
        return min(inv, 5.0), min(val, 5.0), reasons

    # ── R3: Per-bucket reference adherence (bucket-weighted) ─────────────────
    # Small hives track ambient and legitimately sit away from the fixed reference
    # line — so small is given a very low weight (0.10). Large has the highest
    # weight (0.55) because it must hold a stable reference band.
    total_w = sum(BUCKET_ADHERENCE_WEIGHTS.get(b, 0.33) for b in present)
    avg_adherence = float(
        sum(stats[b]["adherence"] * BUCKET_ADHERENCE_WEIGHTS.get(b, 0.33) for b in present)
        / total_w
    ) if total_w > 0 else 0.0
    if avg_adherence < ADHERENCE_MIN:
        inv += 2
        reasons.append(f"poor reference-band adherence ({avg_adherence:.0%})")
    elif avg_adherence < ADHERENCE_MED:
        inv += 1
        reasons.append(f"moderate reference-band adherence ({avg_adherence:.0%})")
    elif avg_adherence > ADHERENCE_GOOD:
        val += 2

    # ── R4: Per-bucket sensor spread ─────────────────────────────────────────
    spreads = [stats[b]["sensor_spread"] for b in present
               if not np.isnan(stats[b]["sensor_spread"])]
    if spreads:
        avg_spread = float(np.mean(spreads))
        max_spread = float(np.max(spreads))
        if avg_spread > SENSOR_SPREAD_HIGH:
            inv += 2
            reasons.append(f"sensors noisy within buckets (spread={avg_spread:.1f}°C)")
        elif avg_spread > SENSOR_SPREAD_MED:
            inv += 1
            reasons.append(f"mild sensor noise within buckets (spread={avg_spread:.1f}°C)")
        elif avg_spread < SENSOR_SPREAD_LOW:
            val += 1

    # ── R4b: Single-bucket MAX spread veto (perfect-invalid pattern) ─────────
    # Even if the average looks acceptable, one exploding bucket is a red flag.
    # Derived from 935-02-08 (perfect invalid): small_spread=9.33 vs valid p90=6.67.
    if spreads and max_spread > SENSOR_SPREAD_MAX_VETO:
        inv += 2
        max_bucket = present[np.argmax([stats[b]["sensor_spread"] for b in present])]
        reasons.append(
            f"extreme spread in {max_bucket} bucket "
            f"(spread={max_spread:.1f}°C > {SENSOR_SPREAD_MAX_VETO}°C veto)"
        )

    # ── R3b: Medium adherence floor (perfect-invalid pattern) ────────────────
    # 625-03-18 and 935-03-21 (perfect invalids): medium_adherence ≈ 0.14.
    # Valid p10=0.18, so anything below 0.15 is well outside normal valid range.
    if "medium" in stats:
        med_adh = stats["medium"]["adherence"]
        if not np.isnan(med_adh) and med_adh < MEDIUM_ADHERENCE_FLOOR:
            inv += 2
            reasons.append(
                f"medium bucket severely off reference bands "
                f"(adherence={med_adh:.2f} < {MEDIUM_ADHERENCE_FLOOR} floor)"
            )

    # ── R5: Per-bucket temporal stability (bucket-weighted) ──────────────────
    t_inv: float = 0.0
    t_val: float = 0.0
    for bucket in present:
        tstd = stats[bucket]["temporal_std"]
        if np.isnan(tstd):
            continue
        high, med, low = TEMPORAL_THRESHOLDS[bucket]
        if tstd > high:
            t_inv += 2
            reasons.append(f"{bucket} bucket unstable over time (std={tstd:.1f}°C)")
        elif tstd > med:
            t_inv += 1
            reasons.append(f"{bucket} bucket mildly unstable (std={tstd:.1f}°C)")
        elif tstd < low:
            t_val += 1
    # Normalise by number of buckets so the rule stays within a 0-2 / 0-1 window
    inv += min(t_inv / len(present), 2.0)
    val += min(t_val / len(present), 1.0)

    # ── R6a: Per-bucket ambient correlation ──────────────────────────────────
    if "small" in stats:
        small_corr = stats["small"]["corr_ambient"]
        if not np.isnan(small_corr):
            if small_corr < SMALL_CORR_MIN:
                inv += 1
                reasons.append(
                    f"small hives not tracking ambient (r={small_corr:.2f}; "
                    f"expected ≥{SMALL_CORR_MIN})"
                )
            else:
                val += 0.5

    if "large" in stats:
        large_corr = stats["large"]["corr_ambient"]
        if not np.isnan(large_corr):
            if large_corr > LARGE_CORR_MAX:
                inv += 2
                reasons.append(
                    f"large hives not thermoregulating (r={large_corr:.2f}; "
                    f"expected ≤{LARGE_CORR_MAX})"
                )
            else:
                val += 0.5

    # ── R6b: Cross-bucket ordering ────────────────────────────────────────────
    ordered = [b for b in ("small", "medium", "large") if b in stats]
    if len(ordered) >= 2:
        means = [stats[b]["mean_temp"] for b in ordered]
        gaps  = [means[i + 1] - means[i] for i in range(len(means) - 1)]
        min_gap = min(gaps)
        bucket_mean_summary = " < ".join(
            f"{b}={stats[b]['mean_temp']:.1f}°C" for b in ordered
        )
        if any(g < 0 for g in gaps):
            inv += 1
            reasons.append(
                f"bucket mean ordering violated ({bucket_mean_summary})"
            )
        elif min_gap < BUCKET_SEP_MIN:
            inv += 1
            reasons.append(
                f"insufficient separation between buckets (min gap={min_gap:.1f}°C)"
            )
        elif min_gap > BUCKET_SEP_GOOD:
            val += 1

    # ── R7: Large-bucket spike fraction ──────────────────────────────────────
    # Erratic sensors within the large bucket — readings far from the bucket's
    # hourly mean — indicate sensor malfunction or colony chaos in the large hives.
    # Derived from EDA: at threshold 0.008, FP:TP ratio = 2.8:1 on train data.
    if "large" in stats:
        large_spike = stats["large"]["spike_frac"]
        if not np.isnan(large_spike) and large_spike > LARGE_SPIKE_FRAC_HIGH:
            inv += 2
            reasons.append(
                f"large-bucket sensors spiking "
                f"({100 * large_spike:.1f}% of readings > {SPIKE_DELTA:.0f}°C from bucket mean)"
            )

    return min(inv, 5.0), min(val, 5.0), reasons


# ─────────────────────────────────────────────────────────────────────────────
# Main decision function
# ─────────────────────────────────────────────────────────────────────────────

def decide(
    date: str,
    group_id: int,
    valid_tags: dict,
    invalid_tags: dict,
    verbose: bool = False,
) -> dict:
    key = (date, int(group_id))

    # ── Tag lookup ────────────────────────────────────────────────────────────
    # Valid tag  → boosts valid_score (confidence 1–5 maps 1:1).
    # Invalid tag → recorded for transparency only; does NOT affect the score.
    tag_valid_bonus = 0
    tag_source      = "untagged"
    tag_reason: Optional[str] = None

    if key in valid_tags:
        tag_valid_bonus = valid_tags[key]          # confidence 1–5
        tag_source = f"valid (confidence={tag_valid_bonus})"

    if key in invalid_tags:
        info = invalid_tags[key]
        tag_source = f"invalid ({'perfect' if info['perfect'] else 'normal'}) [informational]"
        tag_reason = info["reason"]

    # ── Feature-based score (always computed when data is available) ──────────
    feature_invalid: float = 0.0
    feature_valid:   float = 0.0
    feature_reasons: list[str] = []
    data_error: Optional[str] = None

    try:
        sensor_df, gateway_df = load_data(group_id, date)
        sensor_hourly, gateway_hourly = resample(sensor_df, gateway_df)
        features = compute_features(sensor_hourly, gateway_hourly)
        feature_invalid, feature_valid, feature_reasons = compute_feature_scores(features)
    except FileNotFoundError as exc:
        data_error = str(exc)
        feature_reasons = [data_error]

    # ── Combine ───────────────────────────────────────────────────────────────
    valid_score   = round(feature_valid + tag_valid_bonus, 2)
    invalid_score = round(feature_invalid, 2)          # tags never add invalid weight

    total     = valid_score + invalid_score + SCORE_EPSILON
    valid_pct = valid_score / total

    # Binary decision: Valid or Not Confident.
    # valid_pct alone decides — no external gate from invalid tags.
    decision = "Valid" if valid_pct >= VALID_PCT_THRESHOLD else "Not Confident"

    result: dict = {
        "date":          date,
        "group_id":      group_id,
        "valid_score":   valid_score,
        "invalid_score": invalid_score,
        "valid_pct":     round(valid_pct * 100, 1),
        "decision":      decision,
        "tag_source":    tag_source,
        "tag_reason":    tag_reason,
    }
    if verbose:
        result["feature_reasons"] = feature_reasons
        result["data_error"]      = data_error

    return result


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Decide Valid / Invalid / Needs Review for a hive group plot."
    )
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--group", type=int, help="group_id (single mode)")
    mode.add_argument("--batch", type=str, help="CSV file with date,group_id columns (batch mode)")

    p.add_argument("--date",    type=str, help="date YYYY-MM-DD (required in single mode)")
    p.add_argument("--output",  type=str, default=None,
                   help="output CSV path (batch mode); prints JSON to stdout if omitted")
    p.add_argument("--verbose", action="store_true",
                   help="include per-rule reasons in output")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    valid_tags, invalid_tags = load_tags()

    if args.group is not None:
        if not args.date:
            raise SystemExit("--date is required in single mode")
        result = decide(args.date, args.group, valid_tags, invalid_tags, verbose=args.verbose)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return

    # Batch mode
    pairs: list[tuple[str, int]] = []
    with open(args.batch, newline="") as f:
        for row in csv.DictReader(f):
            pairs.append((row["date"].strip(), int(row["group_id"].strip())))

    results = [
        decide(date, gid, valid_tags, invalid_tags, verbose=args.verbose)
        for date, gid in pairs
    ]

    if args.output:
        fieldnames = list(results[0].keys())
        with open(args.output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        print(f"Wrote {len(results)} rows → {args.output}")
    else:
        print(json.dumps(results, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
