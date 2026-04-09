"""
sensor_group_segment — Layer 1 runner.

Runs Phase 1 (feature engineering) + Phase 2 (grading) on TRAIN data.
Each sensor row is enriched with group_status from ground truth.

Usage:
    python skills/sensor_group_segment/scripts/run.py
    python skills/sensor_group_segment/scripts/run.py --output data/results/my_run

Output:
    <output_dir>/results.parquet   ← all sensors, all groups/dates, one file
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import yaml

SKILL_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT  = Path(__file__).resolve().parents[3]

sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(SKILL_ROOT))

from model_monitor.metrics.sensor_group_segment import compute, grade
from config.extraction_plan import EXTRACTION_PLAN

GROUPS       = sorted({group_id for group_id, *_ in EXTRACTION_PLAN})
DATA_DIR     = REPO_ROOT / "data/samples/temperature-export/train"
THRESHOLDS   = SKILL_ROOT / "config/thresholds.yaml"
GROUND_TRUTH = REPO_ROOT / "ground_truth/ground_truth_statuess_ca_2026.csv"

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run sensor_group_segment (Layer 1) on train data.")
    p.add_argument(
        "--output", "-o",
        default=str(REPO_ROOT / "data/results/sensor_group_segment"),
        help="Output directory (default: data/results/sensor_group_segment)",
    )
    return p.parse_args()


def load_thresholds() -> dict:
    with open(THRESHOLDS) as f:
        return yaml.safe_load(f)["metrics"]["sensor_group_segment"]["grading"]


def load_ground_truth() -> pd.DataFrame:
    gt = pd.read_csv(GROUND_TRUTH)
    gt["date"] = gt["date"].astype(str)
    gt["group_id"] = gt["group_id"].astype(str)
    gt = gt.rename(columns={"status": "group_status"})
    return gt[["group_id", "date", "group_status"]]


def iter_group_dates(group_id: int):
    """Yield (date_str, sensor_df, gateway_df) for each available date."""
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


def print_summary(df: pd.DataFrame) -> None:
    """Print pass/fail breakdown by group_status and hive_size_bucket."""
    total = len(df)
    n_pass = (df["status"] == "PASS").sum()
    n_fail = (df["status"] == "FAIL").sum()

    print("\n" + "=" * 72)
    print(f"  OVERALL  ({total:,} sensors)")
    print(f"    PASS  {n_pass:>8,}  ({100 * n_pass / total:.1f}%)")
    print(f"    FAIL  {n_fail:>8,}  ({100 * n_fail / total:.1f}%)")
    print("=" * 72)

    # ── By group_status (valid / invalid / needs_recalibration) ──────────
    print("\n  BY GROUP STATUS")
    print(f"  {'status':<22} {'total':>8} {'PASS':>8} {'FAIL':>8} {'fail%':>8}")
    print("  " + "-" * 56)
    for gs in ["valid", "invalid", "needs_recalibration"]:
        sub = df[df["group_status"] == gs]
        if sub.empty:
            continue
        sp = (sub["status"] == "PASS").sum()
        sf = (sub["status"] == "FAIL").sum()
        pct = 100 * sf / len(sub) if len(sub) > 0 else 0
        print(f"  {gs:<22} {len(sub):>8,} {sp:>8,} {sf:>8,} {pct:>7.1f}%")

    # ── By hive_size_bucket × group_status ───────────────────────────────
    for gs in ["valid", "invalid"]:
        sub = df[df["group_status"] == gs]
        if sub.empty:
            continue
        print(f"\n  HIVE SIZE BREAKDOWN — {gs.upper()} groups")
        print(f"  {'hive_size':<12} {'total':>8} {'PASS':>8} {'FAIL':>8} {'fail%':>8}")
        print("  " + "-" * 48)
        for hs in ["large", "medium", "small"]:
            h = sub[sub["hive_size_bucket"].str.lower() == hs]
            if h.empty:
                continue
            hp = (h["status"] == "PASS").sum()
            hf = (h["status"] == "FAIL").sum()
            pct = 100 * hf / len(h) if len(h) > 0 else 0
            print(f"  {hs:<12} {len(h):>8,} {hp:>8,} {hf:>8,} {pct:>7.1f}%")

    print()


def main() -> None:
    args = parse_args()
    OUTPUT_DIR = Path(args.output)
    if not OUTPUT_DIR.is_absolute():
        OUTPUT_DIR = REPO_ROOT / OUTPUT_DIR
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"Output → {OUTPUT_DIR}")

    thresholds = load_thresholds()
    gt = load_ground_truth()

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
            log.info(
                f"   → {len(result)} sensors  |  "
                + "  ".join(f"{s}={counts.get(s, 0)}" for s in ["PASS", "FAIL"])
            )

            all_results.append(result)

    if not all_results:
        log.error("No results produced. Check that data/samples/ is populated.")
        return

    combined = pd.concat(all_results, ignore_index=True)
    combined["group_id"] = combined["group_id"].astype(str)
    combined = combined.merge(gt, on=["group_id", "date"], how="left")

    out_path = OUTPUT_DIR / "results.parquet"
    combined.to_parquet(out_path, index=False)
    log.info(
        f"\nSaved → {out_path}  "
        f"({len(combined):,} rows — {combined['group_id'].nunique()} groups, "
        f"{combined['date'].nunique()} dates)"
    )

    print_summary(combined)


if __name__ == "__main__":
    main()
