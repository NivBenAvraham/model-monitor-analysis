"""Build 6 rule-result CSVs — train + test for temperature, triage, and pred rules.

For every (group_id, date) in gt_cleaned_ca_2026.csv, runs each rule and writes:

    data/results/rule_results/temperature_train.csv
    data/results/rule_results/temperature_test.csv
    data/results/rule_results/triage_train.csv
    data/results/rule_results/triage_test.csv
    data/results/rule_results/pred_train.csv
    data/results/rule_results/pred_test.csv

Columns in every output CSV:
    date               — YYYY-MM-DD
    group_id           — int
    rule_result_value  — numeric signal used for the decision
                         temperature: valid_score (0.0–1.0)
                         pred:        clip_diff signal value (bee-frames)
                         triage:      passed_metrics count (0–4)
    rule_result_status — string prediction from the rule
                         temperature: "VALID" | "INVALID"
                         pred:        "valid" | "invalid"
                         triage:      "auto_valid" | "needs_review"
    gt_status          — "valid" | "invalid" | "needs_recalibration"

Train/test assignment follows data/samples/split_manifest.csv (group-level split).
Rows with gt_status == "needs_recalibration" are included and assigned to the
split of their group_id — they are labelled but excluded from the manifest.

Usage:
    source scripts/refresh_aws_credentials.sh   # only needed for Athena; not here
    source .venv/bin/activate
    python scripts/build_rule_results.py
"""
import sys
import warnings
from pathlib import Path

sys.path.insert(0, "src")
warnings.filterwarnings("ignore")

import pandas as pd

from model_monitor.metrics.temperature import (
    ambient_temperature_volatility,
    ambient_stability,
    ambient_range,
    bucket_reference_adherence,
    sensor_spread_within_bucket,
    bucket_temporal_stability,
    bucket_diurnal_amplitude,
    small_hive_ambient_tracking,
    large_hive_thermoregulation,
    bucket_temperature_ordering,
)
from model_monitor.decision.temperature_health_rule import score_group_date as temp_score
from model_monitor.metrics.pred_rules import clipping_pressure
from model_monitor.decision.pred_rule import score_group_date as pred_score
from model_monitor.decision.triage_rule import score_group_date as triage_score

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
REPO_ROOT  = Path(__file__).resolve().parents[1]
GT_PATH    = REPO_ROOT / "ground_truth/gt_cleaned_ca_2026.csv"
MANIFEST   = REPO_ROOT / "data/samples/split_manifest.csv"
TEMP_BASE  = REPO_ROOT / "data/samples/temperature-export"
PP_PATH    = REPO_ROOT / "data/data_lake/pre_process.parquet"
TRIAGE_CSV = REPO_ROOT / "explore_notebooks/triage-metrics/triage_rule_result.csv"
OUT_DIR    = REPO_ROOT / "data/results/rule_results"

# ---------------------------------------------------------------------------
# Temperature metric function groups
# ---------------------------------------------------------------------------
_SENSOR_ONLY_FNS = [
    bucket_reference_adherence,
    sensor_spread_within_bucket,
    bucket_temporal_stability,
    bucket_diurnal_amplitude,
    bucket_temperature_ordering,
]
_DUAL_FNS    = [small_hive_ambient_tracking, large_hive_thermoregulation]
_GATEWAY_FNS = [ambient_stability, ambient_range]


# ---------------------------------------------------------------------------
# Load shared inputs
# ---------------------------------------------------------------------------
def _load_inputs() -> tuple[pd.DataFrame, dict[int, str]]:
    """Return (gt_df, group_to_split) — raise if required files are missing."""
    gt = pd.read_csv(GT_PATH, parse_dates=["date"])
    manifest = pd.read_csv(MANIFEST)
    group_to_split: dict[int, str] = (
        manifest.groupby("group_id")["split"].first().to_dict()
    )
    return gt, group_to_split


# ---------------------------------------------------------------------------
# Temperature rule
# ---------------------------------------------------------------------------
def _run_temperature(gt: pd.DataFrame) -> pd.DataFrame:
    rows = []
    total = len(gt)
    for i, row in enumerate(gt.itertuples(index=False)):
        grp    = row.group_id
        dt     = row.date
        dt_str = str(dt.date())

        if i % 50 == 0:
            print(f"  temp {i}/{total}: group={grp} date={dt_str}")

        s_file = TEMP_BASE / f"group_{grp}" / dt_str / f"{grp}_{dt_str}_sensor_temperature.parquet"
        g_file = TEMP_BASE / f"group_{grp}" / dt_str / f"{grp}_{dt_str}_gateway_temperature.parquet"

        rule_value  = None
        rule_status = None

        if s_file.exists():
            try:
                sensor_df  = pd.read_parquet(s_file)
                gateway_df = pd.read_parquet(g_file) if g_file.exists() else pd.DataFrame()

                metrics = [ambient_temperature_volatility(gateway_df)]
                for fn in _SENSOR_ONLY_FNS:
                    metrics.append(fn(sensor_df))
                for fn in _DUAL_FNS:
                    metrics.append(fn(sensor_df, gateway_df))
                for fn in _GATEWAY_FNS:
                    metrics.append(fn(gateway_df))

                result      = temp_score(metrics)
                rule_value  = result["valid_score"]
                rule_status = result["prediction"]
            except Exception as exc:
                print(f"  WARN temp {grp} {dt_str}: {exc}")

        rows.append({
            "date":              dt_str,
            "group_id":          grp,
            "rule_result_value":  rule_value,
            "rule_result_status": rule_status,
            "gt_status":          row.status,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Pred rule
# ---------------------------------------------------------------------------
def _run_pred(gt: pd.DataFrame) -> pd.DataFrame:
    pp = pd.read_parquet(PP_PATH)
    pp["date"] = pd.to_datetime(pp["date"])
    pp["clip_diff"] = (pp["pred_raw"] - pp["pred_clipped"]).abs()

    rows = []
    total = len(gt)
    for i, row in enumerate(gt.itertuples(index=False)):
        grp    = row.group_id
        dt     = row.date
        dt_str = str(dt.date())

        if i % 50 == 0:
            print(f"  pred {i}/{total}: group={grp} date={dt_str}")

        # 9-day window ending on dt (same window used in compare_rules.py)
        sub = pp[
            (pp["group_id"] == grp)
            & (pp["date"] >= dt - pd.Timedelta(days=9))
            & (pp["date"] <= dt)
        ]

        rule_value  = None
        rule_status = None

        if not sub.empty:
            result      = pred_score(clipping_pressure(sub))
            rule_value  = result["signal_value"]
            rule_status = result["prediction"]

        rows.append({
            "date":               dt_str,
            "group_id":           grp,
            "rule_result_value":  rule_value,
            "rule_result_status": rule_status,
            "gt_status":          row.status,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Triage rule
# ---------------------------------------------------------------------------
def _run_triage(gt: pd.DataFrame) -> pd.DataFrame:
    triage_raw = pd.read_csv(TRIAGE_CSV)
    triage_raw["date"] = pd.to_datetime(triage_raw["date"])

    triage_wide = triage_raw.pivot_table(
        index=["group_id", "date"],
        columns="metric_name",
        values="pass_metric",
        aggfunc="first",
    ).reset_index()
    triage_wide.columns.name = None

    sig_cols   = [c for c in triage_wide.columns if c not in ("group_id", "date")]
    triage_idx = set(zip(triage_wide["group_id"], triage_wide["date"]))

    # Build a lookup for last GT status: for each (group_id, date) use the
    # GT status from the immediately preceding date for that group.
    gt_sorted = gt.sort_values(["group_id", "date"])
    last_status_lookup: dict[tuple, str] = {}
    for grp_id, grp_df in gt_sorted.groupby("group_id"):
        dates    = grp_df["date"].tolist()
        statuses = grp_df["status"].tolist()
        for j in range(len(dates)):
            last_status_lookup[(grp_id, dates[j])] = statuses[j - 1] if j > 0 else None

    rows = []
    total = len(gt)
    for i, row in enumerate(gt.itertuples(index=False)):
        grp    = row.group_id
        dt     = row.date
        dt_str = str(dt.date())

        if i % 50 == 0:
            print(f"  triage {i}/{total}: group={grp} date={dt_str}")

        rule_value  = None
        rule_status = None

        if (grp, dt) in triage_idx:
            t_row = triage_wide[
                (triage_wide["group_id"] == grp) & (triage_wide["date"] == dt)
            ]
            if not t_row.empty:
                metric_results = [
                    {
                        "metric_name": c,
                        "pass_metric": (
                            bool(t_row[c].values[0])
                            if pd.notna(t_row[c].values[0])
                            else None
                        ),
                    }
                    for c in sig_cols
                ]
                last_st = last_status_lookup.get((grp, dt))
                result      = triage_score(metric_results, last_status=last_st)
                rule_value  = result["passed_metrics"]
                rule_status = result["prediction"]

        rows.append({
            "date":               dt_str,
            "group_id":           grp,
            "rule_result_value":  rule_value,
            "rule_result_status": rule_status,
            "gt_status":          row.status,
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Write helper
# ---------------------------------------------------------------------------
def _split_and_write(df: pd.DataFrame, name: str, group_to_split: dict) -> None:
    df = df.copy()
    df["split"] = df["group_id"].map(group_to_split)

    for split in ("train", "test"):
        out = df[df["split"] == split].drop(columns="split").reset_index(drop=True)
        path = OUT_DIR / f"{name}_{split}.csv"
        out.to_csv(path, index=False)
        n_with_result = out["rule_result_status"].notna().sum()
        print(f"  → {path.name}: {len(out)} rows, {n_with_result} with result")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading ground truth and split manifest …")
    gt, group_to_split = _load_inputs()
    print(f"GT: {len(gt)} rows across {gt['group_id'].nunique()} groups")

    print("\n[1/3] Temperature rule …")
    temp_df = _run_temperature(gt)
    _split_and_write(temp_df, "temperature", group_to_split)

    print("\n[2/3] Pred rule …")
    pred_df = _run_pred(gt)
    _split_and_write(pred_df, "pred", group_to_split)

    print("\n[3/3] Triage rule …")
    triage_df = _run_triage(gt)
    _split_and_write(triage_df, "triage", group_to_split)

    print("\nDone. Output files:")
    for f in sorted(OUT_DIR.glob("*.csv")):
        print(f"  {f}")


if __name__ == "__main__":
    main()
