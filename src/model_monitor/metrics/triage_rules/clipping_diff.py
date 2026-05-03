"""
Clipping Diff — Triage Rules metric (Signal A).

Detects groups where the model's raw prediction differs significantly from
its clipped prediction on the day under examination.

Business target
---------------
When the model outputs a raw prediction and then clips it (e.g. to enforce a
valid bee_frames range), a large gap between the two signals that the raw model
output is drifting away from the clipped "safe zone".  Groups with a high
average clipping gap need human review to determine whether the clipping is
masking a genuine calibration problem.

Algorithm
---------
1. Receive a DataFrame of the latest pred_raw and pred_clipped per sensor for
   the group on the examination day (output of clipping_diff_query).
2. Compute avg(abs(pred_raw - pred_clipped)) across all sensors.
3. Return pass_metric=False when avg_clip_diff > CLIPPING_DIFF_THRESHOLD.

Threshold (SPECS.md default)
----------------------------
CLIPPING_DIFF_THRESHOLD = 1.0 bee_frames

Input
-----
ubf_df : pd.DataFrame
    Columns: group_id, sensor_mac_address, pred_raw, pred_clipped
    One row per sensor (latest log already selected by the SQL query).

Output
------
dict with:
    metric_name  : "clipping_diff"
    pass_metric  : bool  — True when avg clip diff ≤ threshold
    value        : float — avg(abs(pred_raw - pred_clipped)), or None if no data
    threshold    : float — CLIPPING_DIFF_THRESHOLD
"""

from __future__ import annotations

import pandas as pd

METRIC_NAME             = "clipping_diff"
CLIPPING_DIFF_THRESHOLD = 1.0  # bee_frames — from SPECS.md


def clipping_diff(ubf_df: pd.DataFrame) -> dict:
    """Compute the clipping diff signal for one group on the examination day.

    Parameters
    ----------
    ubf_df:
        Latest pred_raw and pred_clipped per group.  Must contain columns
        ``pred_raw`` and ``pred_clipped``.  Empty DataFrame → pass_metric=True
        (no data means the blocker will handle it separately).

    Returns
    -------
    dict — see module docstring for key descriptions.
    """
    if ubf_df.empty or "pred_raw" not in ubf_df.columns or "pred_clipped" not in ubf_df.columns:
        return {
            "metric_name": METRIC_NAME,
            "pass_metric": True,
            "value":       None,
            "threshold":   CLIPPING_DIFF_THRESHOLD,
        }

    valid = ubf_df.dropna(subset=["pred_raw", "pred_clipped"]).copy()
    if valid.empty:
        return {
            "metric_name": METRIC_NAME,
            "pass_metric": True,
            "value":       None,
            "threshold":   CLIPPING_DIFF_THRESHOLD,
        }

    valid['pred_diff'] = abs(valid['pred_raw'] - valid['pred_clipped'])
    avg_diff = valid['pred_diff'].mean()

    return {
        "metric_name": METRIC_NAME,
        "pass_metric": avg_diff <= CLIPPING_DIFF_THRESHOLD,
        "value":       round(avg_diff, 4),
        "threshold":   CLIPPING_DIFF_THRESHOLD,
    }
