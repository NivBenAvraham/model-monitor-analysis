"""
Auto Review Score — 7-day scenario variant (Signal D).

Identical to ``auto_review_score.py`` except the full UBF window supplied by
the caller is capped to 7 days instead of 21 days.

Diff vs. auto_review_score.py
------------------------------
  AUTO_REVIEW_LOOKBACK_DAYS (caller-side) = 7   # CHANGED: was 21

  sensor_temporal_cv is computed from full_df.  When full_df covers only 7
  days (instead of 21), this feature has less data and produces a noisier
  (typically lower) estimate → some groups that scored ≥ 2.4 on 21 days now
  score < 2.4, flipping pass_metric from False → True.

  RECENT_DAYS = 7 is unchanged — all other features still use the last 7 days.

Evaluation result (7d scenario vs. 14/21d baseline)
-----------------------------------------------------
  Original (21d full window)   T=752  F=258  None=139
  7-day full window            T=770  F=240  None=139

Impact: 18 pass_metric flips False → True.  None count is unchanged because
the minimum-row requirements depend on the 7-day RECENT window, not the full
window, so data availability is the same in both scenarios.

Root cause: with only 7 days for sensor_temporal_cv the per-sensor CV is
estimated from a shorter, lower-variance span → the feature value drops →
its contribution to the composite score shrinks → borderline groups no longer
cross the 2.4 threshold.
"""

from __future__ import annotations

from datetime import date as _date, timedelta

import numpy as np
import pandas as pd

METRIC_NAME           = "auto_review_score_7d"   # CHANGED: name suffix _7d
AUTO_REVIEW_THRESHOLD = 2.4   # unchanged from auto_review_score.py
RECENT_DAYS           = 7     # unchanged — feature window is still last 7 days
MIN_RECENT_ROWS       = 50    # unchanged
MIN_DAILY_AGGREGATES  = 3     # unchanged
MIN_ROWS_PER_DAY      = 10    # unchanged


def _linear_slope(x: list[float], y: list[float]) -> float:
    """Least-squares slope of y ~ x."""
    if len(x) < 2:
        return 0.0
    xarr  = np.array(x, dtype=float)
    yarr  = np.array(y, dtype=float)
    xm    = xarr - xarr.mean()
    denom = float((xm ** 2).sum())
    if denom == 0:
        return 0.0
    return float((xm * (yarr - yarr.mean())).sum() / denom)


def _compute_features(recent_df: pd.DataFrame, full_df: pd.DataFrame) -> dict | None:
    """Compute the 7 auto-review features.

    Parameters
    ----------
    recent_df:
        Rows where input_date > (timestamp - RECENT_DAYS) and ≤ timestamp.
        Identical to the original: last 7 days.
    full_df:
        # CHANGED: caller provides only 7 days here (was 21 days).
        Used only for sensor_temporal_cv; shorter window → noisier estimate.

    Returns None when minimum data requirements are not met.
    """
    if len(recent_df) < MIN_RECENT_ROWS:
        return None

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
    slope         = _linear_slope(days, daily["mean"].tolist())
    trend_line    = slope * np.array(days) + (daily["mean"].iloc[0] - slope * days[0])
    residuals     = daily["mean"].values - trend_line
    detrended_vol = float(residuals.max() - residuals.min())

    # ── median_tail ────────────────────────────────────────────────────────
    daily["tail"] = daily["median"] - daily["p5"]
    median_tail   = float(daily["tail"].median())

    # ── daily CV ──────────────────────────────────────────────────────────
    daily["cv"] = daily["std"].abs() / daily["mean"].abs().clip(lower=1e-6)
    cv_floor      = float(daily["cv"].min())
    cv_trend      = _linear_slope(days, daily["cv"].tolist())
    cv_range      = float(daily["cv"].max() - daily["cv"].min())
    cv_volatility = float(daily["cv"].std())

    # ── sensor_temporal_cv — KEY DIFF ──────────────────────────────────────
    # full_df here covers only 7 days (not 21), so per-sensor CV is computed
    # over a shorter span → typically lower → smaller score contribution.   # CHANGED
    mac_col = "sensor_mac_address" if "sensor_mac_address" in full_df.columns else "mac"
    sensor_cv = (
        full_df.groupby(mac_col)["pred_raw"]
        .agg(lambda s: s.std() / s.mean() if s.mean() != 0 else 0.0)
    )
    sensor_temporal_cv = float(sensor_cv.median()) if not sensor_cv.empty else 0.0

    return {
        "detrended_vol":      round(detrended_vol,     4),
        "median_tail":        round(median_tail,        4),
        "cv_floor":           round(cv_floor,           4),
        "cv_trend":           round(cv_trend,           6),
        "cv_range":           round(cv_range,           4),
        "sensor_temporal_cv": round(sensor_temporal_cv, 4),
        "cv_volatility":      round(cv_volatility,      4),
    }


def _score(features: dict) -> float:
    """Apply the SPECS.md composite scoring formula (unchanged)."""
    def clamped(val: float, offset: float, scale: float, cap: float) -> float:
        return min(max(val - offset, 0.0) / scale, cap)

    return (
        clamped(features["cv_floor"],            0.20,   0.09, 2.5)
        + clamped(features["detrended_vol"],     0.5,    2.5,  1.0)
        + clamped(features["median_tail"],       4.5,    3.0,  1.0)
        + clamped(features["cv_trend"],         -0.003,  0.008, 1.0) * 0.8
        + clamped(features["cv_range"],          0.03,   0.09, 1.0) * 0.8
        + clamped(features["sensor_temporal_cv"],0.09,   0.03, 1.0) * 0.30
        + clamped(features["cv_volatility"],     0.025,  0.02, 0.5)
    )


def auto_review_score_7d(ubf_df: pd.DataFrame, timestamp: str) -> dict:
    """Compute the auto-review composite score using a 7-day full window.

    The caller must pre-filter ``ubf_df`` to the last 7 days before calling
    (i.e. rows where input_date >= timestamp - 7 days).       # CHANGED: was 21

    Parameters
    ----------
    ubf_df:
        7-day UBF rows, earliest per (group, sensor, input_date).  # CHANGED
        Must contain columns: ``sensor_mac_address``, ``input_date``, ``pred_raw``.
    timestamp:
        The day we are examining (YYYY-MM-DD).

    Returns
    -------
    dict — same shape as auto_review_score():
        metric_name, pass_metric, value, threshold, features
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

    # full_df == df here, but caller already capped it to 7 days  # CHANGED
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
