"""
Auto Review Score — Triage Rules metric (Signal D).

Detects unstable or anomalous prediction behaviour using a composite score
computed from 7 features derived from the last 21 days of UBF data.

Business target
---------------
The model's raw predictions should be relatively stable over time within a
group.  High coefficient of variation, large intra-day swings, or trending
instability all suggest the model is operating outside its calibration envelope.
A composite score above the threshold triggers a review request.

Algorithm
---------
Full 21-day window (AUTO_REVIEW_LOOKBACK_DAYS = 21):
    Earliest log per (group, sensor, input_date) — avoids re-processing same day.

Feature window: last 7 days (AUTO_REVIEW_RECENT_DAYS = 7).
Minimum requirements:
    • ≥ 50 rows in the recent window
    • ≥ 3 daily aggregates
    • each usable day must have ≥ 10 raw prediction values

Features computed on the recent window:
    detrended_vol      : range of residuals after removing the daily-mean linear trend
    median_tail        : median of (daily median - daily p5)
    cv_floor           : minimum daily coefficient of variation
    cv_trend           : slope of daily CV over time
    cv_range           : max daily CV - min daily CV
    sensor_temporal_cv : median per-sensor CV over the window
    cv_volatility      : std of daily CVs

Scoring formula (SPECS.md):
    score = (
        min(max(cv_floor - 0.20, 0) / 0.09, 2.5)
        + min(max(detrended_vol - 0.5, 0) / 2.5, 1.0)
        + min(max(median_tail - 4.5, 0) / 3.0, 1.0)
        + min(max(cv_trend - -0.003, 0) / 0.008, 1.0) * 0.8
        + min(max(cv_range - 0.03, 0) / 0.09, 1.0) * 0.8
        + min(max(sensor_temporal_cv - 0.09, 0) / 0.03, 1.0) * 0.30
        + min(max(cv_volatility - 0.025, 0) / 0.02, 0.5)
    )

Threshold (SPECS.md)
---------------------
AUTO_REVIEW_THRESHOLD = 2.4

Input
-----
ubf_df : pd.DataFrame
    Columns: group_id, sensor_mac_address, input_date (date), pred_raw
    Earliest log per (group, sensor, input_date) — as returned by
    auto_review_score_query().

timestamp : str
    The day we are examining (YYYY-MM-DD).  Used to identify the recent window.

Output
------
dict with:
    metric_name  : "auto_review_score"
    pass_metric  : bool | None — None when insufficient data
    value        : float | None — composite score
    threshold    : float — AUTO_REVIEW_THRESHOLD
    features     : dict | None — the 7 computed features
"""

from __future__ import annotations

from datetime import date as _date, timedelta

import numpy as np
import pandas as pd

METRIC_NAME             = "auto_review_score"
AUTO_REVIEW_THRESHOLD   = 2.4   # from SPECS.md
RECENT_DAYS             = 7     # AUTO_REVIEW_RECENT_DAYS
MIN_RECENT_ROWS         = 50
MIN_DAILY_AGGREGATES    = 3
MIN_ROWS_PER_DAY        = 10


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


def _compute_features(recent_df: pd.DataFrame, full_df: pd.DataFrame) -> dict | None:
    """Compute the 7 auto-review features from the windowed DataFrames.

    Parameters
    ----------
    recent_df:
        Rows where input_date > (timestamp - RECENT_DAYS) and ≤ timestamp.
    full_df:
        All 21-day rows (used for sensor_temporal_cv).

    Returns None when minimum data requirements are not met.
    """
    if len(recent_df) < MIN_RECENT_ROWS:
        return None

    # Daily aggregates
    daily = (
        recent_df.groupby("input_date")["pred_raw"]
        .agg(
            count="count",
            mean="mean",
            std="std",
            median="median",
            p5=lambda s: float(np.percentile(s, 5)),
        )
        .reset_index()
    )
    daily = daily[daily["count"] >= MIN_ROWS_PER_DAY]

    if len(daily) < MIN_DAILY_AGGREGATES:
        return None

    daily = daily.sort_values("input_date").reset_index(drop=True)
    days  = list(range(len(daily)))

    # ── detrended_vol ──────────────────────────────────────────────────────
    slope      = _linear_slope(days, daily["mean"].tolist())
    trend_line = slope * np.array(days) + (daily["mean"].iloc[0] - slope * days[0])
    residuals  = daily["mean"].values - trend_line
    detrended_vol = float(residuals.max() - residuals.min())

    # ── median_tail ────────────────────────────────────────────────────────
    daily["tail"] = daily["median"] - daily["p5"]
    median_tail   = float(daily["tail"].median())

    # ── daily CV ──────────────────────────────────────────────────────────
    daily["cv"] = daily["std"].abs() / daily["mean"].abs().clip(lower=1e-6)
    cv_floor    = float(daily["cv"].min())
    cv_trend    = _linear_slope(days, daily["cv"].tolist())
    cv_range    = float(daily["cv"].max() - daily["cv"].min())
    cv_volatility = float(daily["cv"].std())

    # ── sensor_temporal_cv ─────────────────────────────────────────────────
    sensor_cv = (
        full_df.groupby("sensor_mac_address")["pred_raw"]
        .agg(lambda s: s.std() / s.mean() if s.mean() != 0 else 0.0)
    )
    sensor_temporal_cv = float(sensor_cv.median()) if not sensor_cv.empty else 0.0

    return {
        "detrended_vol":      round(detrended_vol,    4),
        "median_tail":        round(median_tail,       4),
        "cv_floor":           round(cv_floor,          4),
        "cv_trend":           round(cv_trend,          6),
        "cv_range":           round(cv_range,          4),
        "sensor_temporal_cv": round(sensor_temporal_cv,4),
        "cv_volatility":      round(cv_volatility,     4),
    }


def _score(features: dict) -> float:
    """Apply the SPECS.md composite scoring formula."""
    def clamped(val: float, offset: float, scale: float, cap: float) -> float:
        return min(max(val - offset, 0.0) / scale, cap)

    return (
        clamped(features["cv_floor"],           0.20,  0.09, 2.5)
        + clamped(features["detrended_vol"],    0.5,   2.5,  1.0)
        + clamped(features["median_tail"],      4.5,   3.0,  1.0)
        + clamped(features["cv_trend"],         -0.003, 0.008, 1.0) * 0.8
        + clamped(features["cv_range"],         0.03,  0.09, 1.0) * 0.8
        + clamped(features["sensor_temporal_cv"],0.09, 0.03, 1.0) * 0.30
        + clamped(features["cv_volatility"],    0.025, 0.02, 0.5)
    )


def auto_review_score(ubf_df: pd.DataFrame, timestamp: str) -> dict:
    """Compute the auto-review composite score for one group.

    Parameters
    ----------
    ubf_df:
        21-day UBF rows, earliest per (group, sensor, input_date).
        Must contain columns: ``sensor_mac_address``, ``input_date``, ``pred_raw``.
    timestamp:
        The day we are examining (YYYY-MM-DD).

    Returns
    -------
    dict — see module docstring for key descriptions.
    """
    _base = {
        "metric_name": METRIC_NAME,
        "threshold":   AUTO_REVIEW_THRESHOLD,
        "features":    None,
    }

    if ubf_df.empty or "pred_raw" not in ubf_df.columns:
        return {**_base, "pass_metric": None, "value": None}

    df = ubf_df.dropna(subset=["pred_raw"]).copy()
    df["input_date"] = pd.to_datetime(df["input_date"]).dt.date

    ts          = _date.fromisoformat(timestamp[:10])
    cutoff_date = ts - timedelta(days=RECENT_DAYS)
    recent_df   = df[df["input_date"] > cutoff_date]

    features = _compute_features(recent_df, df)
    if features is None:
        return {**_base, "pass_metric": None, "value": None}

    score = round(_score(features), 4)

    return {
        "metric_name": METRIC_NAME,
        "pass_metric": score < AUTO_REVIEW_THRESHOLD,
        "value":       score,
        "threshold":   AUTO_REVIEW_THRESHOLD,
        "features":    features,
    }
