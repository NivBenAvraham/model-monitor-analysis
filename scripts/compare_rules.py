"""Compare predictions from temperature_health_rule, pred_rule, and triage_rule."""
import sys, warnings
sys.path.insert(0, "src")
warnings.filterwarnings("ignore")
import pandas as pd, numpy as np
from pathlib import Path

from model_monitor.metrics.temperature import (
    ambient_temperature_volatility, ambient_stability, ambient_range,
    bucket_reference_adherence, sensor_spread_within_bucket,
    bucket_temporal_stability, bucket_diurnal_amplitude,
    small_hive_ambient_tracking, large_hive_thermoregulation,
    bucket_temperature_ordering,
)
from model_monitor.decision.temperature_health_rule import score_group_date as temp_score
from model_monitor.metrics.pred_rules import clipping_pressure
from model_monitor.decision.pred_rule import score_group_date as pred_score
from model_monitor.decision.triage_rule import score_group_date as triage_score

pp = pd.read_parquet("data/data_lake/pre_process.parquet")
pp["date"] = pd.to_datetime(pp["date"])
pp["clip_diff"] = (pp["pred_raw"] - pp["pred_clipped"]).abs()

triage_raw = pd.read_csv("explore_notebooks/triage-metrics/triage_rule_result.csv")
triage_raw["date"] = pd.to_datetime(triage_raw["date"])
triage_wide = triage_raw.pivot_table(
    index=["group_id","date"], columns="metric_name",
    values="pass_metric", aggfunc="first"
).reset_index()
triage_wide.columns.name = None
triage_pairs = set(zip(triage_wide["group_id"], triage_wide["date"]))
triage_sig_cols = [c for c in triage_wide.columns if c not in ["group_id","date"]]
print(f"Triage: {len(triage_wide)} pairs, signals: {triage_sig_cols}")

manifest = pd.read_csv("data/samples/split_manifest.csv")
manifest["date"] = pd.to_datetime(manifest["date"])
manifest["label"] = manifest["status"].map({"valid":1,"invalid":0,"needs_recalibration":0})

BASE = Path("data/samples/temperature-export")
SENSOR_ONLY_FNS = [bucket_reference_adherence, sensor_spread_within_bucket,
                   bucket_temporal_stability, bucket_diurnal_amplitude,
                   bucket_temperature_ordering]
DUAL_FNS        = [small_hive_ambient_tracking, large_hive_thermoregulation]
GATEWAY_FNS     = [ambient_stability, ambient_range]

rows = []
for i, (_, row) in enumerate(manifest.iterrows()):
    grp, dt, label, split = row["group_id"], row["date"], row["label"], row["split"]
    dt_str = str(dt.date())
    if i % 50 == 0:
        print(f"  {i}/{len(manifest)}: group={grp} date={dt_str}")

    # temperature rule
    temp_pred = None
    s_file = BASE / f"group_{grp}" / dt_str / f"{grp}_{dt_str}_sensor_temperature.parquet"
    g_file = BASE / f"group_{grp}" / dt_str / f"{grp}_{dt_str}_gateway_temperature.parquet"
    if s_file.exists():
        try:
            sensor_df  = pd.read_parquet(s_file)
            gateway_df = pd.read_parquet(g_file) if g_file.exists() else pd.DataFrame()
            metrics = [ambient_temperature_volatility(gateway_df)]
            for fn in SENSOR_ONLY_FNS:
                metrics.append(fn(sensor_df))
            for fn in DUAL_FNS:
                metrics.append(fn(sensor_df, gateway_df))
            for fn in GATEWAY_FNS:
                metrics.append(fn(gateway_df))
            temp_pred = temp_score(metrics)["prediction"].lower()
        except Exception as e:
            print(f"  WARN temp {grp} {dt_str}: {e}")
            pass

    # pred rule
    pred_pred = None
    sub = pp[(pp["group_id"]==grp) & (pp["date"] >= dt-pd.Timedelta(days=9)) & (pp["date"]<=dt)]
    if not sub.empty:
        pred_pred = pred_score(clipping_pressure(sub))["prediction"]

    # triage rule
    triage_pred = None
    if (grp, dt) in triage_pairs:
        t_row = triage_wide[(triage_wide["group_id"]==grp) & (triage_wide["date"]==dt)]
        if not t_row.empty:
            metric_results = [{"metric_name": c, "pass_metric":
                                bool(t_row[c].values[0]) if pd.notna(t_row[c].values[0]) else None}
                               for c in triage_sig_cols]
            triage_pred = triage_score(metric_results, last_status="valid")["prediction"]

    rows.append({"group_id":grp,"date":dt_str,"gt_label":label,"split":split,
                 "temp_pred":temp_pred,"pred_pred":pred_pred,"triage_pred":triage_pred})

df = pd.DataFrame(rows)
df.to_csv("data/results/all_rules_comparison.csv", index=False)
print(f"\nDone. {len(df)} pairs")
print(f"Temp coverage:   {df['temp_pred'].notna().sum()}")
print(f"Pred coverage:   {df['pred_pred'].notna().sum()}")
print(f"Triage coverage: {df['triage_pred'].notna().sum()}")
