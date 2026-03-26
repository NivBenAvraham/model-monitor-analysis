"""
Ops Inspections — CURRENT metric.

Compares BeeFrame model predictions against operational (ops) inspection data
using coefficient of variation (CV) ratios. Same CV logic as yard_inspections
but applied to ops inspection records.

Source: beehero-model-monitoring / test/bee_frames/metric_ops_inspections.py

Data inputs:
    - ops_inspections table: sensor_mac_address, utc_timestamp,
                             total_bee_frames, group_id, yard_id
    - preprocess table:      sensor_mac_address, run_date, pred_raw, group_id, yard_id

Logic (from source):
    1. Match ops_inspections to preprocess rows by date and sensor.
    2. Aggregate per (group_id, yard_id, run_date):
       avg_insp = mean(total_bee_frames), std_insp = std(total_bee_frames)
       avg_pred = mean(pred_raw),         std_pred = std(pred_raw)
    3. cv_pred = std_pred / avg_pred
       cv_insp = std_insp / avg_insp
       cv_ratio = cv_insp / cv_pred
    4. Pass/fail via check_inspection_prediction (same thresholds as yard_inspections):
       CV_LOWER = 0.5, CV_UPPER = 2.0, avg_tolerance = 1

Thresholds: configs/thresholds.yaml → metrics.ops_inspections
"""

import pandas as pd


def compute(data: pd.DataFrame) -> pd.DataFrame:
    """
    Compute ops inspection pass/fail per (group_id, yard_id, run_date).

    Args:
        data: DataFrame with merged preprocess + ops_inspections columns:
              [group_id, yard_id, run_date, pred_raw, total_bee_frames, sensor_mac_address]

    Returns:
        DataFrame with columns [group_id, yard_id, run_date, value, response]
        where value=True means the metric PASSED.
    """
    raise NotImplementedError
