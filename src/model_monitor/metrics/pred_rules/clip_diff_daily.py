"""
clip_diff_daily — daily clipping-diff metrics.

Three metrics that measure how far the model's raw predictions deviate from the
clipped ceiling **on the most recent day**.  A valid group sits naturally within
bounds; an invalid group overshoots the ceiling, so |pred_raw − pred_clipped| is
systematically > 0.

  clip_diff_mean   — mean  |raw−clipped| across all sensors (daily)
  clip_diff_p90    — 90th-percentile |raw−clipped|           (daily)
  clip_diff_max    — maximum |raw−clipped|                   (daily)

Train performance (precision ≥ 0.9 threshold sweep, 2026-05-18, n=299 train pairs):
  clip_diff_mean ≤ 0.83 → Precision=0.910 Recall=0.555 (101 TP, 10 FP)
  clip_diff_p90  ≤ 2.11 → Precision=0.902 Recall=0.604 (110 TP, 12 FP)  ← best recall
  clip_diff_max  ≤ 3.80 → Precision=0.923 Recall=0.198 ( 36 TP,  3 FP)

All three use direction lower: small value → valid.

Input
-----
preprocess_df : DataFrame with ``date``, ``mac``, ``pred_raw``, ``pred_clipped``.
    Only the most recent calendar day in the DataFrame is used.

Output  (per function)
------
dict
    metric_name  : str
    pass_metric  : bool | None   — None when the DataFrame is empty / missing cols
    value        : float | None
    threshold    : float
    reason       : str
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
        return yaml.safe_load(f)["pred_rules"]["clip_diff_daily"]


def _today_clip_diffs(preprocess_df: pd.DataFrame) -> pd.Series | None:
    """Return the clip_diff Series for the most recent day, or None on error."""
    missing = _REQUIRED - set(preprocess_df.columns)
    if missing:
        return None
    df = preprocess_df.copy()
    df["date"]      = pd.to_datetime(df["date"])
    df["clip_diff"] = (df["pred_raw"] - df["pred_clipped"]).abs()
    today = df["date"].max()
    series = df.loc[df["date"] == today, "clip_diff"]
    return series if not series.empty else None


def _make_result(metric_name: str, value: float | None, threshold: float,
                 direction: str = "lower") -> dict:
    if value is None:
        return {"metric_name": metric_name, "pass_metric": None,
                "value": None, "threshold": threshold,
                "reason": "no data"}
    passes = value <= threshold if direction == "lower" else value >= threshold
    op = "≤" if direction == "lower" else "≥"
    reason = f"{metric_name}={value:.4f} {op} {threshold} → {'PASS' if passes else 'FAIL'}"
    return {"metric_name": metric_name, "pass_metric": passes,
            "value": round(value, 4), "threshold": threshold, "reason": reason}


# ── Public metrics ────────────────────────────────────────────────────────────

def clip_diff_mean(preprocess_df: pd.DataFrame) -> dict:
    """Mean |pred_raw − pred_clipped| across all sensors on the most recent day."""
    cfg   = _load_thresholds()
    diffs = _today_clip_diffs(preprocess_df)
    value = float(diffs.mean()) if diffs is not None else None
    return _make_result("clip_diff_mean", value, float(cfg["clip_diff_mean"]))


def clip_diff_p90(preprocess_df: pd.DataFrame) -> dict:
    """90th-percentile |pred_raw − pred_clipped| on the most recent day."""
    cfg   = _load_thresholds()
    diffs = _today_clip_diffs(preprocess_df)
    value = float(diffs.quantile(0.90)) if diffs is not None else None
    return _make_result("clip_diff_p90", value, float(cfg["clip_diff_p90"]))


def clip_diff_max(preprocess_df: pd.DataFrame) -> dict:
    """Maximum |pred_raw − pred_clipped| on the most recent day."""
    cfg   = _load_thresholds()
    diffs = _today_clip_diffs(preprocess_df)
    value = float(diffs.max()) if diffs is not None else None
    return _make_result("clip_diff_max", value, float(cfg["clip_diff_max"]))
