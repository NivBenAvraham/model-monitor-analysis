"""
Average Over Time — CURRENT metric.

Measures the rolling average error of BeeFrame model predictions over
3, 4, and 5-day windows. The metric passes when the rolling value is
below the threshold (operator: x < threshold).

Source: beehero-model-monitoring / test/bee_frames/metric_avg_overtime.py

Data inputs:
    - pred_raw:   raw model predictions per sensor per run_date
    - group_id:   hive group identifier
    - entity_source: 'sensors'

Logic (from source):
    - Computed per group_id, over windows of [3, 4, 5] days.
    - pred_column = 'pred_raw'
    - Pass condition: rolling_metric < threshold (0.84)
    - All three windows are evaluated independently.

Thresholds: configs/thresholds.yaml → metrics.avg_overtime
"""

import pandas as pd


def compute(data: pd.DataFrame, window_days: int) -> pd.DataFrame:
    """
    Compute rolling average metric per (group_id) over a given window.

    Args:
        data:        DataFrame with columns [group_id, run_date, pred_raw]
        window_days: rolling window size in days (3, 4, or 5)

    Returns:
        DataFrame with columns [group_id, run_date, value]
        where value=True means the metric PASSED (rolling_value < threshold).
    """
    raise NotImplementedError
