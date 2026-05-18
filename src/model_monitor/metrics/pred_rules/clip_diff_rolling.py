"""
clip_diff_rolling — rolling averages of daily mean |pred_raw − pred_clipped|.

Rolling smoothing of ``clip_diff_mean`` (see ``clip_diff_daily.py``) over a
sliding window of recent days.  Smoothing suppresses single-day noise so that
persistent clipping pressure stands out clearly.

  clip_diff_mean_roll3 — 3-day rolling mean of daily clip_diff_mean
  clip_diff_mean_roll5 — 5-day rolling mean of daily clip_diff_mean
  clip_diff_mean_roll7 — 7-day rolling mean of daily clip_diff_mean

Train performance (precision ≥ 0.9 threshold sweep, 2026-05-18, n=299 train pairs):
  clip_diff_mean_roll3 ≤ 0.79 → Precision=0.901 Recall=0.432 (73 TP,  8 FP)
  clip_diff_mean_roll5 ≤ 0.64 → Precision=0.923 Recall=0.159 (24 TP,  2 FP)
  clip_diff_mean_roll7 ≤ 0.63 → Precision=0.905 Recall=0.137 (19 TP,  2 FP)

Longer windows → higher precision, lower recall (more days needed to confirm).

Availability: roll3 requires ≥ 2 days; roll5 ≥ 3 days; roll7 ≥ 4 days.
When insufficient history is present pass_metric=None (not scored).

Input
-----
preprocess_df : DataFrame with ``date``, ``mac``, ``pred_raw``, ``pred_clipped``.
    Should cover the evaluation date AND preceding days:
      roll3 → ≥2 days,  roll5 → ≥3 days,  roll7 → ≥4 days.
    The most recent date is treated as the evaluation date.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

log = logging.getLogger(__name__)
METRIC_FAMILY: str = "pred_rules"

_REQUIRED = {"date", "mac", "pred_raw", "pred_clipped"}


def _load_thresholds() -> dict:
    path = Path(__file__).resolve().parents[4] / "configs/thresholds.yaml"
    with open(path) as f:
        return yaml.safe_load(f)["pred_rules"]["clip_diff_rolling"]


def _daily_clip_diff_means(preprocess_df: pd.DataFrame) -> pd.Series | None:
    """Return a chronologically sorted daily Series of mean clip_diff, or None."""
    missing = _REQUIRED - set(preprocess_df.columns)
    if missing:
        return None
    df = preprocess_df.copy()
    df["date"]      = pd.to_datetime(df["date"])
    df["clip_diff"] = (df["pred_raw"] - df["pred_clipped"]).abs()
    daily = df.groupby("date")["clip_diff"].mean().sort_index()
    return daily if not daily.empty else None


def _make_result(metric_name: str, value: float | None, threshold: float) -> dict:
    if value is None:
        return {"metric_name": metric_name, "pass_metric": None,
                "value": None, "threshold": threshold,
                "reason": "insufficient history"}
    passes = value <= threshold
    reason = f"{metric_name}={value:.4f} ≤ {threshold} → {'PASS' if passes else 'FAIL'}"
    return {"metric_name": metric_name, "pass_metric": passes,
            "value": round(value, 4), "threshold": threshold, "reason": reason}


def _rolling_value(daily: pd.Series, window: int, min_periods: int) -> float | None:
    rolled = daily.rolling(window, min_periods=min_periods).mean()
    last   = rolled.iloc[-1]
    return float(last) if pd.notna(last) else None


# ── Public metrics ────────────────────────────────────────────────────────────

def clip_diff_mean_roll3(preprocess_df: pd.DataFrame) -> dict:
    """3-day rolling mean of daily mean |pred_raw − pred_clipped|.  Needs ≥ 2 days."""
    cfg   = _load_thresholds()
    daily = _daily_clip_diff_means(preprocess_df)
    value = _rolling_value(daily, window=3, min_periods=2) if daily is not None else None
    return _make_result("clip_diff_mean_roll3", value, float(cfg["clip_diff_mean_roll3"]))


def clip_diff_mean_roll5(preprocess_df: pd.DataFrame) -> dict:
    """5-day rolling mean of daily mean |pred_raw − pred_clipped|.  Needs ≥ 3 days."""
    cfg   = _load_thresholds()
    daily = _daily_clip_diff_means(preprocess_df)
    value = _rolling_value(daily, window=5, min_periods=3) if daily is not None else None
    return _make_result("clip_diff_mean_roll5", value, float(cfg["clip_diff_mean_roll5"]))


def clip_diff_mean_roll7(preprocess_df: pd.DataFrame) -> dict:
    """7-day rolling mean of daily mean |pred_raw − pred_clipped|.  Needs ≥ 4 days."""
    cfg   = _load_thresholds()
    daily = _daily_clip_diff_means(preprocess_df)
    value = _rolling_value(daily, window=7, min_periods=4) if daily is not None else None
    return _make_result("clip_diff_mean_roll7", value, float(cfg["clip_diff_mean_roll7"]))
