"""
Run Phase 1 + Phase 2 temperature metric on local parquet samples.

Phase 1 — feature engineering: std_dev, iqr, ambient_correlation, mean_temp, percent_comfort
Phase 2 — grading: compare predicted hive size against observed physics → PASS / WARNING / FAIL

Usage:
    python skills/temp_metric/scripts/run_temp_metric.py

Output per (group, date):
    data/results/group_{id}/{date}/{id}_{date}_temp_metric.parquet         ← all sensors + status + reason
    data/results/group_{id}/{date}/{id}_{date}_temp_metric_summary.parquet ← mean metrics + grade counts per hive size

Flat file across all groups/dates:
    data/results/temp_metric_results.parquet
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd
import yaml

SKILL_ROOT = Path(__file__).resolve().parents[1]   # skills/temp_metric/
REPO_ROOT  = Path(__file__).resolve().parents[3]   # repo root

sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(SKILL_ROOT))

from model_monitor.metrics.temp_metric import compute, grade
from config.extraction_plan import EXTRACTION_PLAN

GROUPS     = sorted({group_id for group_id, *_ in EXTRACTION_PLAN})
DATA_DIR   = REPO_ROOT / "data/samples"
OUTPUT_DIR = REPO_ROOT / "data/results"
THRESHOLDS = REPO_ROOT / "configs/thresholds.yaml"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


def load_thresholds() -> dict:
    with open(THRESHOLDS) as f:
        return yaml.safe_load(f)["metrics"]["temp_metric"]["grading"]


def iter_group_dates(group_id: int):
    """Yield (date_str, sensor_df, gateway_df) for each available date in a group."""
    group_dir = DATA_DIR / f"group_{group_id}"
    if not group_dir.exists():
        return

    for date_dir in sorted(group_dir.iterdir()):
        if not date_dir.is_dir():
            continue
        date_str = date_dir.name

        sensor_files  = list(date_dir.glob(f"{group_id}_*_sensor_temperature.parquet"))
        gateway_files = list(date_dir.glob(f"{group_id}_*_gateway_temperature.parquet"))

        if not sensor_files or not gateway_files:
            log.warning(f"   {date_str}: missing sensor or gateway file — skipping")
            continue

        yield date_str, pd.read_parquet(sensor_files[0]), pd.read_parquet(gateway_files[0])


def build_summary(result: pd.DataFrame, group_id: int, date_str: str) -> pd.DataFrame:
    """Mean metrics + PASS/WARNING/FAIL counts per hive size."""
    metrics = (
        result
        .groupby("hive_size_bucket")[["std_dev", "ambient_correlation", "mean_temp", "percent_comfort"]]
        .mean()
        .round(2)
    )
    grade_counts = (
        result
        .groupby(["hive_size_bucket", "status"])
        .size()
        .unstack(fill_value=0)
        .reindex(columns=["PASS", "WARNING", "FAIL"], fill_value=0)
    )
    summary = metrics.join(grade_counts).reset_index().rename(columns={"hive_size_bucket": "hive_size"})
    summary.insert(0, "date", date_str)
    summary.insert(0, "group_id", group_id)
    return summary


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    thresholds = load_thresholds()

    all_results: list[pd.DataFrame] = []

    for group_id in sorted(GROUPS):
        log.info(f"── group {group_id}")

        for date_str, sensor_df, gateway_df in iter_group_dates(group_id):
            log.info(f"   {date_str}  sensor={len(sensor_df):,}  gateway={len(gateway_df):,}")

            result = compute(sensor_df, gateway_df, date=date_str)
            if result.empty:
                log.warning(f"   {date_str}: no sensors computed")
                continue

            result = grade(result, thresholds)

            counts = result["status"].value_counts().to_dict()
            log.info(f"   → {len(result)} sensors  |  " + "  ".join(f"{s}={counts.get(s,0)}" for s in ["PASS","WARNING","FAIL"]))

            date_out = OUTPUT_DIR / f"group_{group_id}" / date_str
            date_out.mkdir(parents=True, exist_ok=True)

            result.to_parquet(date_out / f"{group_id}_{date_str}_temp_metric.parquet", index=False)

            summary = build_summary(result, group_id, date_str)
            summary.to_parquet(date_out / f"{group_id}_{date_str}_temp_metric_summary.parquet", index=False)

            all_results.append(result)

    if not all_results:
        log.error("No results produced. Check that data/samples/ is populated.")
        return

    combined = pd.concat(all_results, ignore_index=True)
    out_path = OUTPUT_DIR / "temp_metric_results.parquet"
    combined.to_parquet(out_path, index=False)

    log.info(f"\nSaved → {out_path}  ({len(combined)} rows — {combined['group_id'].nunique()} groups, {combined['date'].nunique()} dates)")

    overall = combined["status"].value_counts()
    log.info(f"\nOverall grade distribution:\n{overall.to_string()}")

    log.info(f"\nMean metrics by hive size:\n{combined.groupby('hive_size_bucket')[['std_dev','ambient_correlation','mean_temp','percent_comfort']].mean().round(3).to_string()}")


if __name__ == "__main__":
    main()
