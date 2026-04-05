"""
Threshold Calibration — sensor_group_segment (Layer 1).

TARGET: hive_size_bucket (small / medium / large).

For each predicted hive size, we compute the distribution of temperature
features across all sensors with that label. Thresholds are then set at
the tail of each size's distribution — a sensor is flagged when its
physics are unusual *for its own predicted size class*.

  large.std_dev_max  → above large p90: unusually volatile for a large hive
  large.corr_max     → above large p90: unusually coupled to ambient for a large hive
  large.comfort_min  → below large p10: unusually out of brood zone for a large hive
  medium.mean_temp_min → below medium p10: unusually cold for a medium hive
  small.std_dev_min  → below small p10: suspiciously stable for a small hive

This is pure Layer 1 calibration — no valid/invalid ground truth labels needed.

Fast path:  if data/calibration/train_features.parquet already exists, it is loaded
            directly (Phase 1 already ran via run.py).
Slow path:  if the parquet is missing, Phase 1 is run on all samples in
            data/samples/train/ (takes ~4 min for 423 samples).

Usage:
    python skills/sensor_group_segment/scripts/calibrate_thresholds.py

Outputs:
    data/calibration/train_features.parquet   ← feature table (created if missing)
    data/calibration/calibration_report.csv   ← percentile distributions per hive size
    stdout                                    ← suggested threshold values + diff vs current
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

SKILL_ROOT = Path(__file__).resolve().parents[1]   # skills/sensor_group_segment/
REPO_ROOT  = Path(__file__).resolve().parents[3]   # repo root

sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(SKILL_ROOT))

TRAIN_DIR   = REPO_ROOT / "data/samples/train"
OUTPUT_DIR  = REPO_ROOT / "data/calibration"
FEATURES_PARQUET = OUTPUT_DIR / "train_features.parquet"

METRICS     = ["std_dev", "iqr", "ambient_correlation", "mean_temp", "percent_comfort"]
PERCENTILES = [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]
SIZES       = ["large", "medium", "small"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Feature table — fast path (load) or slow path (run Phase 1)
# ---------------------------------------------------------------------------

def load_features() -> pd.DataFrame:
    """
    Load the feature table.
    Fast path: read existing train_features.parquet.
    Slow path: run Phase 1 on all samples in data/samples/train/.
    """
    if FEATURES_PARQUET.exists():
        log.info(f"Loading existing feature table from {FEATURES_PARQUET}…")
        df = pd.read_parquet(FEATURES_PARQUET)
        log.info(f"  {len(df):,} sensor-day rows  ({df['group_id'].nunique()} groups, {df['date'].nunique()} dates)")
        return df

    log.info("train_features.parquet not found — running Phase 1 on all train samples…")
    log.info("(Run skills/sensor_group_segment/scripts/run.py first to use the fast path)")
    return _run_phase1()


def _run_phase1() -> pd.DataFrame:
    from model_monitor.metrics.sensor_group_segment import compute

    all_frames: list[pd.DataFrame] = []
    skipped = 0
    samples = sorted(
        [d for group in TRAIN_DIR.iterdir() if group.is_dir()
         for d in group.iterdir() if d.is_dir()],
        key=lambda p: (p.parent.name, p.name)
    )
    total = len(samples)
    log.info(f"  Found {total} (group, date) directories in train/")

    for i, date_dir in enumerate(samples, 1):
        group_id = int(date_dir.parent.name.replace("group_", ""))
        date_str = date_dir.name

        sensor_files  = list(date_dir.glob(f"{group_id}_*_sensor_temperature.parquet"))
        gateway_files = list(date_dir.glob(f"{group_id}_*_gateway_temperature.parquet"))

        if not sensor_files or not gateway_files:
            log.warning(f"  [{i}/{total}] group {group_id} {date_str}: missing files — skipping")
            skipped += 1
            continue

        result = compute(pd.read_parquet(sensor_files[0]),
                         pd.read_parquet(gateway_files[0]),
                         date=date_str)
        if result.empty:
            skipped += 1
            continue

        all_frames.append(result)
        if i % 50 == 0:
            log.info(f"  [{i}/{total}] processed…")

    log.info(f"Processed {total - skipped}/{total} samples  ({skipped} skipped)")

    if not all_frames:
        raise RuntimeError("No features computed. Check data/samples/train/ is populated.")

    features = pd.concat(all_frames, ignore_index=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    features.to_parquet(FEATURES_PARQUET, index=False)
    log.info(f"Saved → {FEATURES_PARQUET}  ({len(features):,} rows)")

    return features


# ---------------------------------------------------------------------------
# Distributions
# ---------------------------------------------------------------------------

def compute_distributions(features: pd.DataFrame) -> pd.DataFrame:
    """Percentile table per (hive_size_bucket, metric)."""
    records = []
    for size, grp in features.groupby("hive_size_bucket"):
        for metric in METRICS:
            col = grp[metric].dropna()
            if col.empty:
                continue
            pct = col.quantile(PERCENTILES).to_dict()
            records.append({
                "hive_size": size,
                "metric":    metric,
                "n":         len(col),
                **{f"p{int(p*100):02d}": round(v, 3) for p, v in pct.items()},
            })
    return pd.DataFrame(records).sort_values(["metric", "hive_size"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Threshold suggestion
# ---------------------------------------------------------------------------

def suggest_thresholds(features: pd.DataFrame) -> dict:
    """
    Set thresholds at the tail of each hive size's own distribution.

    Logic:
      MAX thresholds (std_dev_max, corr_max):
        = size p90 — above this, only 10% of correctly-labeled sensors
          would appear, so the value is genuinely unusual for that size.

      MIN thresholds (comfort_min, mean_temp_min, std_dev_min):
        = size p10 — below this, only 10% of correctly-labeled sensors
          would appear.

    The p90/p10 choice gives a ~10% false-positive rate, which is a
    reasonable operating point for a rule-based monitoring metric.
    """
    def q(size: str, metric: str, quantile: float) -> float | None:
        col = features[features["hive_size_bucket"] == size][metric].dropna()
        return float(round(col.quantile(quantile), 3)) if not col.empty else None

    return {
        # large: physics should look like a strong, thermally stable hive
        "large.std_dev_max":    q("large", "std_dev",              0.90),
        "large.corr_max":       q("large", "ambient_correlation",  0.90),
        "large.comfort_min":    q("large", "percent_comfort",      0.10),
        # medium: should be warmer than ambient-dominated hives
        "medium.mean_temp_min": q("medium", "mean_temp",           0.10),
        # small: should be noisy — suspiciously stable means possible sensor error
        "small.std_dev_min":    q("small", "std_dev",              0.10),
    }


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(distributions: pd.DataFrame, suggestions: dict, features: pd.DataFrame) -> None:
    sep = "═" * 76

    print(f"\n{sep}")
    print("  SENSOR GROUP SEGMENT — THRESHOLD CALIBRATION REPORT")
    print(f"  Target: hive_size_bucket   |   Source: data/samples/train/")
    print(f"  {features[['group_id','date']].drop_duplicates().shape[0]} (group/date) pairs  |  {len(features):,} sensor-day rows")
    print(sep)

    print("\nSensor-day counts per hive size:")
    counts = features["hive_size_bucket"].value_counts().reindex(SIZES).fillna(0).astype(int)
    for size, n in counts.items():
        print(f"  {size:8s}  {n:>8,}")

    for metric in METRICS:
        print(f"\n── {metric}")
        sub = distributions[distributions["metric"] == metric].drop(columns="metric")
        if sub.empty:
            print("  (no data)")
            continue
        print(sub.to_string(index=False))

    print(f"\n{sep}")
    print("  SUGGESTED THRESHOLDS  (p90/p10 of each hive size's own distribution)")
    print(sep)

    rule_explanations = {
        "large.std_dev_max":    "large p90 std_dev  — above this → unusually volatile for large",
        "large.corr_max":       "large p90 corr     — above this → unusually ambient-coupled for large",
        "large.comfort_min":    "large p10 comfort  — below this → unusually out of brood zone for large",
        "medium.mean_temp_min": "medium p10 mean_temp — below this → unusually cold for medium",
        "small.std_dev_min":    "small p10 std_dev  — below this → suspiciously stable for small",
    }
    for rule, value in suggestions.items():
        print(f"  {rule:<26}  →  {str(value):>7}    # {rule_explanations.get(rule, '')}")

    # Diff vs current thresholds.yaml
    try:
        import yaml
        with open(SKILL_ROOT / "config/thresholds.yaml") as f:
            current = yaml.safe_load(f)["metrics"]["sensor_group_segment"]["grading"]

        print(f"\n{'Rule':<26}  {'Current':>10}  {'Suggested':>10}  {'Δ':>8}")
        print("-" * 62)
        mapping = {
            "large.std_dev_max":    ("large",  "std_dev_max"),
            "large.corr_max":       ("large",  "corr_max"),
            "large.comfort_min":    ("large",  "comfort_min"),
            "medium.mean_temp_min": ("medium", "mean_temp_min"),
            "small.std_dev_min":    ("small",  "std_dev_min"),
        }
        for rule, (size, key) in mapping.items():
            cur = current.get(size, {}).get(key)
            sug = suggestions.get(rule)
            if cur is not None and sug is not None:
                delta = round(sug - cur, 3)
                flag = "  ←" if abs(delta) > 0.05 else ""
                print(f"  {rule:<24}  {cur:>10}  {sug:>10}  {delta:>+8.3f}{flag}")
            else:
                print(f"  {rule:<24}  {str(cur):>10}  {str(sug):>10}  {'N/A':>8}")
    except Exception as e:
        log.warning(f"Could not load current thresholds: {e}")

    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    features = load_features()

    log.info("Computing distributions per hive_size_bucket…")
    distributions = compute_distributions(features)

    dist_path = OUTPUT_DIR / "calibration_report.csv"
    distributions.to_csv(dist_path, index=False)
    log.info(f"Saved calibration report → {dist_path}")

    suggestions = suggest_thresholds(features)
    print_report(distributions, suggestions, features)


if __name__ == "__main__":
    main()
