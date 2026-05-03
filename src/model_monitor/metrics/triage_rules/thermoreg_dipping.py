"""
Thermoregulation Dipping — Triage Rules metric (Signal C).

Detects groups where too many yards show increasing temperature dispersion
over the last 14 days — a sign that thermoregulation is breaking down.

Business target
---------------
A healthy hive colony actively regulates internal temperature.  When the
standard deviation of temperature within a yard starts rising over time, it
indicates that some sensors are deviating from the cluster — typically because
the colony is dwindling, swarming, or the model size prediction is wrong.
When more than 15 % of a group's yards are "dipping", the group needs review.

Algorithm
---------
1. Receive yard_daily_df — daily aggregated temperature stats per yard,
   covering the last THERMOREG_LOOKBACK_DAYS (14) days.
2. For each yard compute a trend classification (classify_yard):
     "recovering"         : slope < -0.03 AND peak before 60 % of the window
     "dipping"            : slope >  0.03 AND trough before 60 % of the window
     "volatile"           : |slope| > 0.02 AND std_of_stds > 0.15
     "stable"             : everything else
     "insufficient_data"  : fewer than 4 data points
3. dip_pct = 100 × (dipping yards) / (all classified yards).
4. Return pass_metric=False when dip_pct > DIPPING_YARD_PCT_THRESHOLD.

Threshold (SPECS.md default)
----------------------------
DIPPING_YARD_PCT_THRESHOLD = 15.0 %

Input
-----
yard_daily_df : pd.DataFrame
    Columns: group_id, yard_id, yard_name, date, temp_mean, temp_std,
             temp_range, sensor_count
    Sorted by yard_id, date (ascending).

Output
------
dict with:
    metric_name   : "thermoreg_dipping"
    pass_metric   : bool — True when dip_pct ≤ threshold (or no yards)
    value         : float | None — dip_pct (0–100)
    threshold     : float — DIPPING_YARD_PCT_THRESHOLD
    yard_trends   : dict[str, str] — yard_name → trend classification
    dipping_yards : list[str] — yard names classified as "dipping"
"""

from __future__ import annotations

import numpy as np
import pandas as pd

METRIC_NAME                = "thermoreg_dipping"
DIPPING_YARD_PCT_THRESHOLD = 15.0  # % — from SPECS.md


def _linear_slope(x: list[float], y: list[float]) -> float:
    """Least-squares slope of y ~ x."""
    if len(x) < 2:
        return 0.0
    xarr = np.array(x, dtype=float)
    yarr = np.array(y, dtype=float)
    xm   = xarr - xarr.mean()
    denom = float((xm ** 2).sum())
    if denom == 0:
        return 0.0
    return float((xm * (yarr - yarr.mean())).sum() / denom)


def _classify_yard(yard_daily: pd.DataFrame) -> str:
    """Classify one yard's temperature dispersion trend over time.

    Parameters
    ----------
    yard_daily:
        Rows for a single yard, sorted by date ascending.
        Must have a ``temp_std`` column.

    Returns
    -------
    One of: "dipping", "recovering", "volatile", "stable", "insufficient_data".
    """
    if len(yard_daily) < 4:
        return "insufficient_data"

    stds      = yard_daily["temp_std"].tolist()
    n         = len(stds)
    slope     = _linear_slope(list(range(n)), stds)
    peak_idx  = int(np.argmax(stds))
    trough_idx = int(np.argmin(stds))
    std_of_stds = float(np.std(stds))

    if slope < -0.03 and peak_idx < n * 0.6:
        return "recovering"
    if slope > 0.03 and trough_idx < n * 0.6:
        return "dipping"
    if abs(slope) > 0.02 and std_of_stds > 0.15:
        return "volatile"
    return "stable"


def thermoreg_dipping(yard_daily_df: pd.DataFrame) -> dict:
    """Compute the thermoregulation dipping signal for one group.

    Parameters
    ----------
    yard_daily_df:
        Daily temperature stats per yard over the lookback window.
        Must contain columns: ``yard_id``, ``yard_name``, ``date``, ``temp_std``.
        May be empty — if so, pass_metric=True (no yards → no signal).

    Returns
    -------
    dict — see module docstring for key descriptions.
    """
    _base = {
        "metric_name":   METRIC_NAME,
        "threshold":     DIPPING_YARD_PCT_THRESHOLD,
        "yard_trends":   {},
        "dipping_yards": [],
    }

    if yard_daily_df.empty or "temp_std" not in yard_daily_df.columns:
        return {**_base, "pass_metric": True, "value": None}

    df = yard_daily_df.sort_values(["yard_id", "date"]).copy()

    yard_trends: dict[str, str] = {}
    for (yard_id, yard_name), yard_data in df.groupby(["yard_id", "yard_name"], sort=False):
        trend = _classify_yard(yard_data)
        yard_trends[str(yard_name)] = trend

    all_classified = [t for t in yard_trends.values() if t != "insufficient_data"]
    if not all_classified:
        return {**_base, "pass_metric": True, "value": None, "yard_trends": yard_trends}

    dipping_yards = [name for name, t in yard_trends.items() if t == "dipping"]
    dip_pct       = 100.0 * len(dipping_yards) / len(all_classified)

    return {
        "metric_name":   METRIC_NAME,
        "pass_metric":   dip_pct <= DIPPING_YARD_PCT_THRESHOLD,
        "value":         round(dip_pct, 2),
        "threshold":     DIPPING_YARD_PCT_THRESHOLD,
        "yard_trends":   yard_trends,
        "dipping_yards": dipping_yards,
    }
