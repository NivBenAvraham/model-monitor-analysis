"""
pct_clipped — fraction of sensors being clipped.

Measures what fraction of sensors have their prediction clamped by the ceiling
(i.e., pred_raw ≠ pred_clipped).  In a valid group the model's output fits
naturally within bounds; in an invalid group many sensors are consistently
clipped.

  pct_clipped       — fraction clipped on the most recent day
  pct_clipped_roll3 — 3-day rolling mean of daily pct_clipped
  pct_clipped_roll7 — 7-day rolling mean of daily pct_clipped

Train performance (precision ≥ 0.9 threshold sweep, 2026-05-18, n=299 train pairs):
  pct_clipped       ≤ 0.711 → Precision=0.905 Recall=0.313 (57 TP,  6 FP)
  pct_clipped_roll3 ≤ 0.590 → Precision=1.000 Recall=0.030 ( 5 TP,  0 FP)  ← perfect precision
  pct_clipped_roll7 ≤ 0.670 → Precision=1.000 Recall=0.029 ( 4 TP,  0 FP)  ← perfect precision

Rolling variants trade recall for near-perfect precision — very safe "confirm valid" signals.

Input
-----
preprocess_df : DataFrame with ``date``, ``mac``, ``pred_raw``, ``pred_clipped``.
    For rolling metrics the DataFrame should cover several preceding days.
    A sensor is considered clipped when |pred_raw − pred_clipped| > 0.01.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

log = logging.getLogger(__name__)
METRIC_FAMILY: str = "pred_rules"

_REQUIRED = {"date", "mac", "pred_raw", "pred_clipped"}
_CLIP_EPS  = 0.01    # treat diff > eps as "clipped"


def _load_thresholds() -> dict:
    path = Path(__file__).resolve().parents[4] / "configs/thresholds.yaml"
    with open(path) as f:
        return yaml.safe_load(f)["pred_rules"]["pct_clipped"]


def _daily_pct_clipped(preprocess_df: pd.DataFrame) -> pd.Series | None:
    """Return a chronologically sorted daily Series of fraction-clipped, or None."""
    missing = _REQUIRED - set(preprocess_df.columns)
    if missing:
        return None
    df = preprocess_df.copy()
    df["date"]       = pd.to_datetime(df["date"])
    df["is_clipped"] = (df["pred_raw"] - df["pred_clipped"]).abs() > _CLIP_EPS
    daily = df.groupby("date")["is_clipped"].mean().sort_index()
    return daily if not daily.empty else None


def _make_result(metric_name: str, value: float | None, threshold: float,
                 reason_suffix: str = "") -> dict:
    if value is None:
        return {"metric_name": metric_name, "pass_metric": None,
                "value": None, "threshold": threshold,
                "reason": "insufficient history" if "roll" in metric_name else "no data"}
    passes = value <= threshold
    reason = f"{metric_name}={value:.4f} ≤ {threshold} → {'PASS' if passes else 'FAIL'}"
    if reason_suffix:
        reason += f"  ({reason_suffix})"
    return {"metric_name": metric_name, "pass_metric": passes,
            "value": round(value, 4), "threshold": threshold, "reason": reason}


# ── Public metrics ────────────────────────────────────────────────────────────

def pct_clipped(preprocess_df: pd.DataFrame) -> dict:
    """Fraction of sensors clipped on the most recent day."""
    cfg   = _load_thresholds()
    daily = _daily_pct_clipped(preprocess_df)
    value = float(daily.iloc[-1]) if daily is not None else None
    return _make_result("pct_clipped", value, float(cfg["pct_clipped"]))


def pct_clipped_roll3(preprocess_df: pd.DataFrame) -> dict:
    """3-day rolling mean of daily fraction-clipped.  Needs ≥ 2 days.  Perfect precision on train."""
    cfg   = _load_thresholds()
    daily = _daily_pct_clipped(preprocess_df)
    if daily is None:
        return _make_result("pct_clipped_roll3", None, float(cfg["pct_clipped_roll3"]))
    rolled = daily.rolling(3, min_periods=2).mean()
    last   = rolled.iloc[-1]
    value  = float(last) if pd.notna(last) else None
    return _make_result("pct_clipped_roll3", value, float(cfg["pct_clipped_roll3"]))


def pct_clipped_roll7(preprocess_df: pd.DataFrame) -> dict:
    """7-day rolling mean of daily fraction-clipped.  Needs ≥ 4 days.  Perfect precision on train."""
    cfg   = _load_thresholds()
    daily = _daily_pct_clipped(preprocess_df)
    if daily is None:
        return _make_result("pct_clipped_roll7", None, float(cfg["pct_clipped_roll7"]))
    rolled = daily.rolling(7, min_periods=4).mean()
    last   = rolled.iloc[-1]
    value  = float(last) if pd.notna(last) else None
    return _make_result("pct_clipped_roll7", value, float(cfg["pct_clipped_roll7"]))
