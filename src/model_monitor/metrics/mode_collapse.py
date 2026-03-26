"""
Mode Collapse — CURRENT metric.

Detects when the BeeFrame model's predictions collapse onto a single value,
meaning the histogram of daily predictions is dominated by one bin.

Source: beehero-model-monitoring / test/bee_frames/metric_mode_collapse.py

Data inputs:
    - pred_raw:   raw model predictions per sensor per run_date
    - group_id:   hive group identifier

Logic (from source):
    1. Build a histogram of pred_raw values per (group_id, run_date),
       using fixed integer bins [0, 20] (MAX_BEE_FRAMES = 20).
    2. Compute pct_of_highest_bin and pct_of_second_highest_bin.
    3. A group is "collapsed" (metric fails) if:
       - pct_of_highest_bin >= HIGH_PCT_THRESHOLD (0.375), OR
       - pct_of_highest_bin >= 2 * pct_of_second_highest AND
         pct_of_highest_bin >= MIN_HIGH_PCT_THRESHOLD (0.25)

Thresholds: configs/thresholds.yaml → metrics.mode_collapse
"""

import pandas as pd


def compute(data: pd.DataFrame) -> pd.DataFrame:
    """
    Compute mode collapse per (group_id, run_date).

    Args:
        data: DataFrame with columns [group_id, run_date, pred_raw]

    Returns:
        DataFrame with columns [group_id, run_date, value]
        where value=True means the group is collapsed (metric FAILED).
    """
    raise NotImplementedError
