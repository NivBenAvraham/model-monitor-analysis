"""
Thermoregulation Dipping — 7-day scenario variant (Signal C).

Identical to ``thermoreg_dipping.py`` except the caller is expected to supply
only the last 7 days of yard-daily data instead of 14 days.

Diff vs. thermoreg_dipping.py
------------------------------
The only change is the lookback window passed in by the caller:
  THERMOREG_LOOKBACK_DAYS = 7   # CHANGED: was 14 in thermoreg_dipping.py

All classification thresholds and logic are unchanged:
  - DIPPING_YARD_PCT_THRESHOLD = 15.0 %
  - slope / trough / volatile criteria identical
  - minimum 4 data-points per yard to classify

Evaluation result (7d scenario vs. 14/21d baseline)
-----------------------------------------------------
  Original (14d)  T=195  F=207  None=9
  7-day           T=197  F=214  None=0

Impact: the shorter window eliminates 9 None cases (insufficient 14-day data)
and resolves them as True (pass).  4 of those None→True flips coincide with
invalid groups whose other 3 signals also pass → 4 new False Positives.

Root cause: 7 days is too short to observe a reliable dispersion trend;
groups that had no classifiable yards over 14 days can accumulate just enough
7-day rows to produce a (misleading) "stable" or "recovering" result.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

METRIC_NAME                = "thermoreg_dipping_7d"   # CHANGED: name suffix _7d
DIPPING_YARD_PCT_THRESHOLD = 15.0  # % — unchanged from thermoreg_dipping.py


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

    stds       = yard_daily["temp_std"].tolist()
    n          = len(stds)
    slope      = _linear_slope(list(range(n)), stds)
    peak_idx   = int(np.argmax(stds))
    trough_idx = int(np.argmin(stds))
    std_of_stds = float(np.std(stds))

    if slope < -0.03 and peak_idx < n * 0.6:
        return "recovering"
    if slope > 0.03 and trough_idx < n * 0.6:
        return "dipping"
    if abs(slope) > 0.02 and std_of_stds > 0.15:
        return "volatile"
    return "stable"


def thermoreg_dipping_7d(yard_daily_df: pd.DataFrame) -> dict:
    """Compute the thermoregulation dipping signal using a 7-day window.

    The caller must pre-filter ``yard_daily_df`` to the last 7 days before
    calling this function (i.e. rows where date > timestamp - 7 days).

    Parameters
    ----------
    yard_daily_df:
        Daily temperature stats per yard over the 7-day window.  # CHANGED: 7d
        Must contain columns: ``yard_id``, ``yard_name``, ``date``, ``temp_std``.

    Returns
    -------
    dict — same shape as thermoreg_dipping():
        metric_name, pass_metric, value, threshold, yard_trends, dipping_yards
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
    for yard_id, yard_data in df.groupby(["yard_id"], sort=False):
        trend = _classify_yard(yard_data)
        yard_trends[str(yard_id)] = trend

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
