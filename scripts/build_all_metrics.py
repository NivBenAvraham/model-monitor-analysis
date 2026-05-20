"""Build 2 all-metrics CSVs — train and test.

For every (group_id, date) in gt_cleaned_ca_2026.csv, computes every individual
metric from all three rule families and writes wide-format tables:

    data/results/rule_results/all_metrics_train.csv
    data/results/rule_results/all_metrics_test.csv

Base columns:
    date, group_id, gt_status, split

Temperature metrics (one _pass + scalar _value where available):
    atv              → ambient_temperature_volatility  (mandatory gate)
    ambient_stability  (R1)
    ambient_range      (R2)
    bucket_ref_adh     → bucket_reference_adherence    (mandatory gate, R3)
    sensor_spread      → sensor_spread_within_bucket   (R4)
    bucket_temp_stab   → bucket_temporal_stability     (mandatory gate, R5)
    small_tracking     → small_hive_ambient_tracking   (R6a)
    large_thermoreg    → large_hive_thermoregulation   (R6b)
    bucket_ordering    → bucket_temperature_ordering   (R6c)
    bucket_diurnal     → bucket_diurnal_amplitude      (mandatory gate, R7)

Pred-rule metrics (each _pass + _value, 9-day window ending on date):
    clip_diff_mean, clip_diff_p90, clip_diff_max
    clip_diff_mean_roll3, clip_diff_mean_roll5, clip_diff_mean_roll7
    pct_clipped, pct_clipped_roll3, pct_clipped_roll7
    pred_raw_std, pred_raw_range

Triage metrics (precomputed CSV, each _pass + _value):
    clipping_diff, inspection_discrepancy, thermoreg_dipping, auto_review_score

Usage:
    source .venv/bin/activate
    python scripts/build_all_metrics.py
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
from model_monitor.metrics.pred_rules.clip_diff_daily import (
    clip_diff_mean, clip_diff_p90, clip_diff_max,
)
from model_monitor.metrics.pred_rules.clip_diff_rolling import (
    clip_diff_mean_roll3, clip_diff_mean_roll5, clip_diff_mean_roll7,
)
from model_monitor.metrics.pred_rules.pct_clipped import (
    pct_clipped, pct_clipped_roll3, pct_clipped_roll7,
)
from model_monitor.metrics.pred_rules.pred_raw_stats import pred_raw_std, pred_raw_range

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
# Temperature — metric function groups and short column names
# ---------------------------------------------------------------------------
# Functions whose value is a plain float (exposed as {short}_value column).
# Functions with per-bucket dict values get only a {short}_pass column.
_TEMP_SCALAR: list[tuple] = [
    # (function,                        short_name)
    (ambient_temperature_volatility,    "atv"),
    (ambient_stability,                 "ambient_stability"),
    (small_hive_ambient_tracking,       "small_tracking"),
    (large_hive_thermoregulation,       "large_thermoreg"),
]
_TEMP_PASS_ONLY: list[tuple] = [
    (ambient_range,                 "ambient_range"),
    (bucket_reference_adherence,    "bucket_ref_adh"),
    (sensor_spread_within_bucket,   "sensor_spread"),
    (bucket_temporal_stability,     "bucket_temp_stab"),
    (bucket_temperature_ordering,   "bucket_ordering"),
    (bucket_diurnal_amplitude,      "bucket_diurnal"),
]

# Functions that need (sensor_df, gateway_df) vs only one DataFrame
_DUAL_TEMP = {small_hive_ambient_tracking, large_hive_thermoregulation}
_GATEWAY_TEMP = {ambient_stability, ambient_range, ambient_temperature_volatility}

# ---------------------------------------------------------------------------
# Pred-rule atomic metrics
# ---------------------------------------------------------------------------
_PRED_METRICS = [
    clip_diff_mean, clip_diff_p90, clip_diff_max,
    clip_diff_mean_roll3, clip_diff_mean_roll5, clip_diff_mean_roll7,
    pct_clipped, pct_clipped_roll3, pct_clipped_roll7,
    pred_raw_std, pred_raw_range,
]

# ---------------------------------------------------------------------------
# Triage metric names (columns in triage_rule_result.csv)
# ---------------------------------------------------------------------------
_TRIAGE_SIGNALS = [
    "clipping_diff",
    "inspection_discrepancy",
    "thermoreg_dipping",
    "auto_review_score",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _call_temp_fn(fn, sensor_df: pd.DataFrame, gateway_df: pd.DataFrame) -> dict:
    if fn in _DUAL_TEMP:
        return fn(sensor_df, gateway_df)
    if fn in _GATEWAY_TEMP:
        return fn(gateway_df)
    return fn(sensor_df)


def _scalar_value(v) -> float | None:
    """Return v if it's a plain number, else None."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    return None


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------
def build(gt: pd.DataFrame, pp: pd.DataFrame, triage_wide: pd.DataFrame,
          triage_idx: set) -> pd.DataFrame:

    triage_sig_cols = [c for c in triage_wide.columns if c not in ("group_id", "date")]
    # triage value lookup: (group_id, dt) → dict of signal → value
    triage_val_map: dict = {}
    for tr in triage_wide.itertuples(index=False):
        triage_val_map[(tr.group_id, tr.date)] = {c: getattr(tr, c) for c in triage_sig_cols}

    # Also build a lookup for triage raw values from the long-format CSV
    triage_raw = pd.read_csv(TRIAGE_CSV)
    triage_raw["date"] = pd.to_datetime(triage_raw["date"])
    triage_value_lookup: dict = {}
    for _, r in triage_raw.iterrows():
        key = (r["group_id"], r["date"])
        if key not in triage_value_lookup:
            triage_value_lookup[key] = {}
        triage_value_lookup[key][r["metric_name"]] = r["value"]

    rows = []
    total = len(gt)

    for i, row in enumerate(gt.itertuples(index=False)):
        grp    = row.group_id
        dt     = row.date
        dt_str = str(dt.date())

        if i % 50 == 0:
            print(f"  {i}/{total}: group={grp} date={dt_str}")

        rec: dict = {
            "date":      dt_str,
            "group_id":  grp,
            "gt_status": row.status,
        }

        # ── Temperature metrics ──────────────────────────────────────────────
        s_file = TEMP_BASE / f"group_{grp}" / dt_str / f"{grp}_{dt_str}_sensor_temperature.parquet"
        g_file = TEMP_BASE / f"group_{grp}" / dt_str / f"{grp}_{dt_str}_gateway_temperature.parquet"

        if s_file.exists():
            try:
                sensor_df  = pd.read_parquet(s_file)
                gateway_df = pd.read_parquet(g_file) if g_file.exists() else pd.DataFrame()

                for fn, short in _TEMP_SCALAR:
                    try:
                        r = _call_temp_fn(fn, sensor_df, gateway_df)
                        rec[f"{short}_pass"]  = r.get("pass_metric")
                        rec[f"{short}_value"] = _scalar_value(r.get("value"))
                    except Exception:
                        rec[f"{short}_pass"]  = None
                        rec[f"{short}_value"] = None

                for fn, short in _TEMP_PASS_ONLY:
                    try:
                        r = _call_temp_fn(fn, sensor_df, gateway_df)
                        rec[f"{short}_pass"] = r.get("pass_metric")
                    except Exception:
                        rec[f"{short}_pass"] = None
            except Exception as exc:
                print(f"  WARN temp {grp} {dt_str}: {exc}")
                for fn, short in _TEMP_SCALAR:
                    rec[f"{short}_pass"]  = None
                    rec[f"{short}_value"] = None
                for fn, short in _TEMP_PASS_ONLY:
                    rec[f"{short}_pass"] = None
        else:
            for fn, short in _TEMP_SCALAR:
                rec[f"{short}_pass"]  = None
                rec[f"{short}_value"] = None
            for fn, short in _TEMP_PASS_ONLY:
                rec[f"{short}_pass"] = None

        # ── Pred-rule atomic metrics ─────────────────────────────────────────
        sub = pp[
            (pp["group_id"] == grp)
            & (pp["date"] >= dt - pd.Timedelta(days=9))
            & (pp["date"] <= dt)
        ]
        for fn in _PRED_METRICS:
            name = fn.__name__
            if not sub.empty:
                try:
                    r = fn(sub)
                    rec[f"{name}_pass"]  = r.get("pass_metric")
                    rec[f"{name}_value"] = _scalar_value(r.get("value"))
                except Exception:
                    rec[f"{name}_pass"]  = None
                    rec[f"{name}_value"] = None
            else:
                rec[f"{name}_pass"]  = None
                rec[f"{name}_value"] = None

        # ── Triage metrics ───────────────────────────────────────────────────
        pass_map  = triage_val_map.get((grp, dt), {})
        value_map = triage_value_lookup.get((grp, dt), {})
        for sig in _TRIAGE_SIGNALS:
            raw_pass = pass_map.get(sig)
            rec[f"{sig}_pass"] = (
                bool(raw_pass) if pd.notna(raw_pass) else None
            ) if raw_pass is not None else None
            raw_val = value_map.get(sig)
            rec[f"{sig}_value"] = float(raw_val) if raw_val is not None and pd.notna(raw_val) else None

        rows.append(rec)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading inputs …")
    gt       = pd.read_csv(GT_PATH, parse_dates=["date"])
    manifest = pd.read_csv(MANIFEST)
    group_to_split: dict[int, str] = (
        manifest.groupby("group_id")["split"].first().to_dict()
    )

    pp = pd.read_parquet(PP_PATH)
    pp["date"] = pd.to_datetime(pp["date"])
    pp["clip_diff"] = (pp["pred_raw"] - pp["pred_clipped"]).abs()

    triage_raw = pd.read_csv(TRIAGE_CSV)
    triage_raw["date"] = pd.to_datetime(triage_raw["date"])
    triage_wide = triage_raw.pivot_table(
        index=["group_id", "date"], columns="metric_name",
        values="pass_metric", aggfunc="first",
    ).reset_index()
    triage_wide.columns.name = None
    triage_idx = set(zip(triage_wide["group_id"], triage_wide["date"]))

    print(f"GT: {len(gt)} rows | PP: {len(pp)} rows | Triage: {len(triage_wide)} pairs")
    print("Building metrics …")

    df = build(gt, pp, triage_wide, triage_idx)
    df["split"] = df["group_id"].map(group_to_split)

    for split in ("train", "test"):
        out  = df[df["split"] == split].drop(columns="split").reset_index(drop=True)
        path = OUT_DIR / f"all_metrics_{split}.csv"
        out.to_csv(path, index=False)
        n_cols = len(out.columns)
        print(f"  → {path.name}: {len(out)} rows × {n_cols} columns")

    print("\nDone.")
    print("Columns:", df.drop(columns="split").columns.tolist())


if __name__ == "__main__":
    main()
