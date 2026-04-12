"""
extract_features.py — Full Phase 1 feature extraction across samples.

Thin wrapper around compute(full=True) from sensor_group_segment.py.
Produces the complete feature table for EDA, threshold calibration, and model development.

  run.py            → train only  → lean columns (5 grading features + status)
  extract_features  → any split   → full feature set (all 14 raw metrics)

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
    ambient_correlation (vs median), ambient_corr_mean,
    min_temp, mean_temp, max_temp, median_temp,
    percent_comfort, n_readings
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT  = Path(__file__).resolve().parents[4]
SKILL_ROOT = REPO_ROOT / "skills" / "sensor_group_segment"
DATA_DIR   = REPO_ROOT / "data" / "samples" / "temperature-export"

sys.path.insert(0, str(REPO_ROOT / "src"))

from model_monitor.metrics.sensor_group_segment import compute  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_split(split: str) -> list[Path]:
    """Return Parquet dirs under the requested split (or all dates if 'all')."""
    root = DATA_DIR if split == "all" else DATA_DIR / split
    dirs = sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith("_"))
    log.info(f"Found {len(dirs)} date folders in '{root}'")
    return dirs


def _load_parquet(date_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Load sensor + gateway parquet files from one date folder."""
    sensor_files  = list(date_dir.glob("sensor_data*.parquet"))
    gateway_files = list(date_dir.glob("gateway_data*.parquet"))
    if not sensor_files or not gateway_files:
        return None
    sensor_df  = pd.concat([pd.read_parquet(f) for f in sensor_files],  ignore_index=True)
    gateway_df = pd.concat([pd.read_parquet(f) for f in gateway_files], ignore_index=True)
    return sensor_df, gateway_df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Extract full Phase 1 feature table.")
    p.add_argument(
        "--split", choices=["all", "train", "test"], default="all",
        help="Which data split to process (default: all)",
    )
    p.add_argument(
        "--output", "-o",
        default=str(REPO_ROOT / "data" / "features" / "sensor_group_segment"),
        help="Output directory for the parquet file",
    )
    return p.parse_args()


def main() -> None:
    args   = parse_args()
    splits = _load_split(args.split)

    all_results: list[pd.DataFrame] = []

    for date_dir in splits:
        date = date_dir.name
        pair = _load_parquet(date_dir)
        if pair is None:
            log.warning(f"  skip {date}: missing sensor or gateway files")
            continue
        sensor_df, gateway_df = pair

        log.info(f"Processing {date} — {sensor_df['sensor_mac_address'].nunique()} sensors")
        result = compute(sensor_df, gateway_df, date, full=True)
        if result.empty:
            log.warning(f"  no output for {date}")
            continue
        all_results.append(result)

    if not all_results:
        log.error("No results produced. Check that data directories contain parquet files.")
        sys.exit(1)

    combined = pd.concat(all_results, ignore_index=True)

    out_dir  = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{args.split}_features.parquet"
    combined.to_parquet(out_path, index=False)
    log.info(f"\nSaved → {out_path}  ({len(combined):,} rows)")

    # --- Summary ---
    num_cols = ["std_dev", "iqr", "ambient_correlation", "ambient_corr_mean",
                "min_temp", "mean_temp", "max_temp", "median_temp", "percent_comfort"]
    for bucket, grp in combined.groupby("hive_size_bucket"):
        log.info(f"\n  [{bucket}]  n={len(grp):,}")
        for col in num_cols:
            if col in grp.columns:
                log.info(f"    {col:<25} {grp[col].mean():.3f} ± {grp[col].std():.3f}")


if __name__ == "__main__":
    main()
