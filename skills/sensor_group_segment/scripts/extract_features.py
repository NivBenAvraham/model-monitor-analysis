"""
extract_features.py — Full Phase 1 feature extraction across samples.

Runs Phase 1 on every available (group_id, date) sample and produces the
complete feature table. Unlike run.py (which only grades train data), this
script is used for EDA, threshold calibration, and model development.

  run.py            → train only  → grading columns (lean 5 features + status)
  extract_features  → any split   → full feature set (all raw metrics)

Usage:
    python skills/sensor_group_segment/scripts/extract_features.py
    python skills/sensor_group_segment/scripts/extract_features.py --split train
    python skills/sensor_group_segment/scripts/extract_features.py --split test

Output:
    data/features/sensor_group_segment/all_features.parquet    (default)
    data/features/sensor_group_segment/train_features.parquet  (--split train)
    data/features/sensor_group_segment/test_features.parquet   (--split test)

Output columns:
    group_id, date, sensor_mac_address, gateway_mac_address, hive_size_bucket,
    std_dev, iqr,
    ambient_corr_median, ambient_corr_mean,
    min_temp, mean_temp, max_temp, median_temp,
    percent_comfort, n_readings
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats

SKILL_ROOT = Path(__file__).resolve().parents[1]   # skills/sensor_group_segment/
REPO_ROOT  = Path(__file__).resolve().parents[3]   # repo root

sys.path.insert(0, str(REPO_ROOT / "src"))

from model_monitor.metrics.sensor_group_segment import (
    COMFORT_HIGH,
    COMFORT_LOW,
    MIN_READINGS,
    RESAMPLE_FREQ,
    _resample_sensor,
    _resample_gateway,
    _sensor_gateway_map,
    _stability,
)

SAMPLES_ROOT = REPO_ROOT / "data/samples/temperature-export"
OUTPUT_DIR   = REPO_ROOT / "data/features/sensor_group_segment"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)

SPLIT_DIRS = {
    "all":   SAMPLES_ROOT,
    "train": SAMPLES_ROOT / "train",
    "test":  SAMPLES_ROOT / "test",
}

METRIC_COLS = [
    "std_dev", "iqr",
    "ambient_corr_median", "ambient_corr_mean",
    "min_temp", "mean_temp", "max_temp", "median_temp",
    "percent_comfort",
]


# ---------------------------------------------------------------------------
# Full alignment — median + mean ambient from sensor's local gateways
# ---------------------------------------------------------------------------

def _align_full(sensor_df: pd.DataFrame, gateway_df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """Like _align but keeps both ambient_median and ambient_mean per sensor per hour."""
    sensor_h  = _resample_sensor(sensor_df, freq)
    gateway_h = _resample_gateway(gateway_df, freq)
    sg_map    = _sensor_gateway_map(sensor_df)

    local_ambient = sg_map.merge(gateway_h, on="gateway_mac_address", how="inner")

    local_agg = (
        local_ambient
        .groupby(["sensor_mac_address", "timestamp"])["ambient_temp"]
        .agg(ambient_median="median", ambient_mean="mean")
        .reset_index()
    )
    return sensor_h.merge(local_agg, on=["sensor_mac_address", "timestamp"], how="inner")


# ---------------------------------------------------------------------------
# Full per-sensor metric helpers
# ---------------------------------------------------------------------------

def _decoupling_full(internal: pd.Series, ambient_median: pd.Series,
                     ambient_mean: pd.Series) -> dict[str, float]:
    """Pearson r against both yard-level ambient references."""
    if len(internal) < MIN_READINGS:
        return {"ambient_corr_median": float("nan"), "ambient_corr_mean": float("nan")}
    with np.errstate(invalid="ignore"):
        r_med,  _ = stats.pearsonr(internal, ambient_median)
        r_mean, _ = stats.pearsonr(internal, ambient_mean)
    return {"ambient_corr_median": float(r_med), "ambient_corr_mean": float(r_mean)}


def _comfort_zone_full(internal: pd.Series) -> dict[str, float]:
    """Full temperature distribution + comfort zone %."""
    in_zone = (internal >= COMFORT_LOW) & (internal <= COMFORT_HIGH)
    return {
        "min_temp":        float(internal.min()),
        "mean_temp":       float(internal.mean()),
        "max_temp":        float(internal.max()),
        "median_temp":     float(internal.median()),
        "percent_comfort": float(in_zone.mean() * 100),
    }


def compute_full(sensor_df: pd.DataFrame, gateway_df: pd.DataFrame, date: str) -> pd.DataFrame:
    """Full feature extraction — all metrics per (date, sensor_mac_address)."""
    aligned = _align_full(sensor_df, gateway_df, RESAMPLE_FREQ)

    meta = (
        sensor_df[["sensor_mac_address", "hive_size_bucket", "group_id"]]
        .drop_duplicates("sensor_mac_address", keep="last")
    )

    records: list[dict] = []

    for sensor_mac, grp in aligned.groupby("sensor_mac_address"):
        both = grp[["internal_temp", "ambient_median", "ambient_mean"]].dropna()
        if len(both) < MIN_READINGS:
            continue

        row: dict = {
            "sensor_mac_address":  sensor_mac,
            "gateway_mac_address": grp["gateway_mac_address"].iloc[0],
            "n_readings":          len(both),
        }
        row.update(_stability(both["internal_temp"]))
        row.update(_decoupling_full(both["internal_temp"],
                                    both["ambient_median"], both["ambient_mean"]))
        row.update(_comfort_zone_full(both["internal_temp"]))
        records.append(row)

    if not records:
        return pd.DataFrame()

    result = pd.DataFrame(records).merge(meta, on="sensor_mac_address", how="left")
    result["date"] = date

    ordered_cols = [
        "group_id", "date", "sensor_mac_address", "gateway_mac_address", "hive_size_bucket",
        "std_dev", "iqr", "ambient_corr_median", "ambient_corr_mean",
        "min_temp", "mean_temp", "max_temp", "median_temp", "percent_comfort", "n_readings",
    ]
    return result[ordered_cols].sort_values(
        ["group_id", "date", "sensor_mac_address"],
    ).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Script entrypoint
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract full Phase 1 features from samples.")
    p.add_argument(
        "--split", "-s",
        choices=["all", "train", "test"],
        default="all",
        help="Which samples to process (default: all)",
    )
    p.add_argument(
        "--output", "-o",
        default=None,
        help="Override output parquet path",
    )
    return p.parse_args()


def iter_samples(root: Path):
    """Yield (group_id, date_str, sensor_df, gateway_df) for every sample in root."""
    for group_dir in sorted(root.iterdir()):
        if not group_dir.is_dir() or group_dir.name in {"train", "test"}:
            continue
        gid_str = group_dir.name.replace("group_", "")
        if not gid_str.isdigit():
            continue
        gid = int(gid_str)

        for date_dir in sorted(group_dir.iterdir()):
            if not date_dir.is_dir():
                continue
            date_str = date_dir.name

            sensor_files  = list(date_dir.glob(f"{gid}_*_sensor_temperature.parquet"))
            gateway_files = list(date_dir.glob(f"{gid}_*_gateway_temperature.parquet"))

            if not sensor_files or not gateway_files:
                log.warning(f"  {gid}/{date_str}: missing files — skipping")
                continue

            yield gid, date_str, pd.read_parquet(sensor_files[0]), pd.read_parquet(gateway_files[0])


def _print_summary(combined: pd.DataFrame) -> None:
    log.info(f"\n  {len(combined):,} sensor-rows  |  "
             f"{combined['group_id'].nunique()} groups  |  "
             f"{combined['date'].nunique()} dates")

    log.info(f"\nHive size distribution:\n"
             f"{combined['hive_size_bucket'].value_counts().to_string()}")

    grp     = combined.groupby("hive_size_bucket")
    header  = f"  {'metric':<24}{'large':>20}{'medium':>20}{'small':>20}"
    divider = "  " + "-" * 84
    rows    = [header, divider]

    for col in METRIC_COLS:
        means = grp[col].mean()
        stds  = grp[col].std()
        def fmt(size: str) -> str:
            if size not in means.index:
                return f"{'—':>20}"
            return f"{means[size]:>10.3f} ± {stds[size]:<7.3f}"
        rows.append(f"  {col:<24}{fmt('large')}{fmt('medium')}{fmt('small')}")

    rows.append(divider)
    counts = grp.size()
    rows.append(f"  {'n_sensors':<24}"
                f"{counts.get('large',  0):>20,}"
                f"{counts.get('medium', 0):>20,}"
                f"{counts.get('small',  0):>20,}")

    log.info("\nMean ± std by hive size:\n" + "\n".join(rows))


def main() -> None:
    args   = parse_args()
    root   = SPLIT_DIRS[args.split]
    output = Path(args.output) if args.output else OUTPUT_DIR / f"{args.split}_features.parquet"
    output.parent.mkdir(parents=True, exist_ok=True)

    log.info(f"Split  : {args.split}")
    log.info(f"Source : {root}")
    log.info(f"Output : {output}")

    all_features: list[pd.DataFrame] = []

    for gid, date_str, sensor_df, gateway_df in iter_samples(root):
        features = compute_full(sensor_df, gateway_df, date=date_str)
        if features.empty:
            log.warning(f"  {gid}/{date_str}: no features — skipping")
            continue
        all_features.append(features)
        log.info(f"  group {gid} / {date_str}  →  {len(features):,} sensors")

    if not all_features:
        log.error("No features produced. Check that samples exist.")
        return

    combined = pd.concat(all_features, ignore_index=True)
    combined.to_parquet(output, index=False)
    log.info(f"\nSaved → {output}")
    _print_summary(combined)


if __name__ == "__main__":
    main()
