"""
Inspection Discrepancy — Triage Rules metric (Signal B).

Detects groups where recent manual yard inspections disagree significantly
with the model's same-day production output.

Business target
---------------
Field inspectors physically count bee_frames.  When their count differs from
the model's prediction by more than 1.5 frames on average, the model is likely
mis-calibrated for that group.  This signal catches disagreements before they
compound into larger calibration errors.

Algorithm
---------
1. Receive inspections_df — yard-level inspections from the last 14 days,
   with a parsed average bee_frames per inspection.
2. Receive model_df — same-day latest hive update result per sensor.
3. Per group, compute:
     inspection_avg = mean of parsed inspection averages
     model_avg      = mean of per-sensor model results
     discrepancy    = abs(inspection_avg - model_avg)
4. Return pass_metric=False when discrepancy > INSPECTION_GAP_THRESHOLD.


Threshold (SPECS.md)
---------------------
INSPECTION_GAP_THRESHOLD = 1.5 bee_frames
(Note: the standalone inspection monitor flags at > 1.0 but triage accepts > 1.5.)

Input
-----
inspections_df : pd.DataFrame (inspections_by_beekeeper_and_season table)
    Columns: group_id, orchards_inspected, avg_bee_frames,inspector,date
    Rows from the last INSPECTION_LOOKBACK_DAYS (14) days.

model_df : pd.DataFrame (preprocess table)
    Columns: group_id, sensor_mac_address, numerical_model_result
    Latest result per sensor on the examination day.

Output
------
dict with:
    metric_name         : "inspection_discrepancy"
    pass_metric         : bool — True when discrepancy ≤ threshold (or no inspections)
    value               : float | None — abs(inspection_avg - model_avg)
    threshold           : float — INSPECTION_GAP_THRESHOLD
    inspection_avg      : float | None
    model_avg           : float | None
    inspection_count    : int
"""

from __future__ import annotations

import pandas as pd



METRIC_NAME               = "inspection_discrepancy"
INSPECTION_GAP_THRESHOLD  = 1.5  # bee_frames — from SPECS.md


def inspection_discrepancy(
    inspections_df: pd.DataFrame,
    model_df: pd.DataFrame,
) -> dict:
    """Compute the inspection discrepancy signal for one group.

    Parameters
    ----------
    inspections_df:
        Inspection rows from the last 14 days.  Must contain columns
        ``group_id``, ``orchards_inspected``, ``avg_bee_frames``.``date``,``inspector``
        May be empty — if so, pass_metric=True (no inspections → no signal).

    model_df:
        Same-day model outputs.  Must contain ``group_id``,
        ``sensor_mac_address``, ``numerical_model_result``.

    Returns
    -------
    dict — see module docstring for key descriptions.
    """
    _base = {
        "metric_name":      METRIC_NAME,
        "threshold":        INSPECTION_GAP_THRESHOLD,
        "inspection_avg":   None,
        "model_avg":        None,
        "inspection_count": 0,
    }
    
    if model_df.empty or "numerical_model_result" not in model_df.columns:
        return {
            **_base,
            "pass_metric":      True,
            "value":            None,
            "inspection_avg":   round(inspection_avg, 4),
            "inspection_count": inspection_count,
        }

    model_vals = model_df["numerical_model_result"].dropna()
    if model_vals.empty:
        return {
            **_base,
            "pass_metric":      True,
            "value":            None,
            "inspection_avg":   round(inspection_avg, 4),
            "inspection_count": inspection_count,
        }

    model_avg    = float(model_vals.mean())

    if inspections_df.empty or "avg_bee_frames" not in inspections_df.columns:
        return {**_base, "pass_metric": True, "value": None}
    
    parsed = inspections_df['avg_bee_frames'].dropna()
    
    if parsed.empty:
        return {**_base, "pass_metric": True, "value": None}

    inspection_avg   = float(parsed.mean())
    inspection_count = int(len(parsed))

    
    discrepancy  = abs(inspection_avg - model_avg)

    return {
        "metric_name":      METRIC_NAME,
        "pass_metric":      discrepancy <= INSPECTION_GAP_THRESHOLD,
        "value":            round(discrepancy, 4),
        "threshold":        INSPECTION_GAP_THRESHOLD,
        "inspection_avg":   round(inspection_avg, 4),
        "model_avg":        round(model_avg, 4),
        "inspection_count": inspection_count,
    }
