"""
Yard Inspections — CURRENT metric.

Compares BeeFrame model predictions against yard inspection data using
coefficient of variation (CV) ratios. Detects when model predictions
diverge significantly from human yard inspections.

Source: beehero-model-monitoring / test/bee_frames/metric_yard_inspections.py

Data inputs:
    - yard_inspections table: yard_id, run_date, utc_end_time,
                              bee_frames_distribution (JSON histogram string)
    - preprocess table:       yard_id, run_date, pred_raw

Logic (from source):
    1. Parse bee_frames_distribution JSON histograms per (yard_id, run_date).
    2. Compute weighted mean (avg_insp) and std (std_insp) from histogram.
    3. Compute avg_pred and std_pred from pred_raw.
    4. cv_pred = std_pred / avg_pred
       cv_insp = std_insp / avg_insp
       cv_ratio = cv_insp / cv_pred
    5. Pass conditions (check_inspection_prediction):
       - If cv_ratio is NaN (both stds = 0): pass when |avg_insp - avg_pred| ≤ 1
       - If CV_LOWER (0.5) ≤ cv_ratio ≤ CV_UPPER (2.0):
           pass when |avg_insp - avg_pred| ≤ 1, else fail (AVG_RESPONSE)
       - Else: fail (CV_RESPONSE — cv out of range)

Thresholds: configs/thresholds.yaml → metrics.yard_inspections
"""

import pandas as pd


def compute(data: pd.DataFrame) -> pd.DataFrame:
    """
    Compute yard inspection pass/fail per (yard_id, run_date).

    Args:
        data: DataFrame with merged preprocess + yard_inspections columns:
              [yard_id, run_date, pred_raw, bee_frames_distribution]

    Returns:
        DataFrame with columns [yard_id, run_date, value, response]
        where value=True means the metric PASSED.
    """
    raise NotImplementedError
