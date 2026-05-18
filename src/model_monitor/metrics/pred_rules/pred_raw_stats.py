"""
pred_raw_stats — statistics on the raw (unconstrained) model prediction.

Two metrics that characterise the spread of pred_raw values on the most recent
day.  In a valid group the model is confident and internally consistent; its
raw outputs cluster tightly (low std, small range).  In an invalid group the
model's raw outputs can be erratic or systematically out-of-range.

  pred_raw_std   — std of pred_raw across sensors on the most recent day
  pred_raw_range — (max − min) of pred_raw across sensors on the most recent day

Train performance (precision ≥ 0.9 threshold sweep, 2026-05-18, n=299 train pairs):
  pred_raw_std   ≤ 1.132 → Precision=1.000 Recall=0.022 (4 TP, 0 FP)  ← perfect precision
  pred_raw_range ≤ 8.311 → Precision=0.900 Recall=0.049 (9 TP, 1 FP)

Note: both metrics have very low recall — they catch only a small slice of
valid groups (those with unusually tight sensor agreement) but do so with high
confidence.  Use as supplementary signals, not primary gates.

Input
-----
preprocess_df : DataFrame with ``date``, ``mac``, ``pred_raw``, ``pred_clipped``.
    Only the most recent calendar day in the DataFrame is used.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

log = logging.getLogger(__name__)
METRIC_FAMILY: str = "pred_rules"

_REQUIRED = {"date", "mac", "pred_raw"}


def _load_thresholds() -> dict:
    path = Path(__file__).resolve().parents[4] / "configs/thresholds.yaml"
    with open(path) as f:
        return yaml.safe_load(f)["pred_rules"]["pred_raw_stats"]


def _today_pred_raw(preprocess_df: pd.DataFrame) -> pd.Series | None:
    """Return pred_raw values for the most recent day, or None on error."""
    missing = _REQUIRED - set(preprocess_df.columns)
    if missing:
        return None
    df    = preprocess_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    today = df["date"].max()
    series = df.loc[df["date"] == today, "pred_raw"].dropna()
    return series if not series.empty else None


def _make_result(metric_name: str, value: float | None, threshold: float) -> dict:
    if value is None:
        return {"metric_name": metric_name, "pass_metric": None,
                "value": None, "threshold": threshold, "reason": "no data"}
    passes = value <= threshold
    reason = f"{metric_name}={value:.4f} ≤ {threshold} → {'PASS' if passes else 'FAIL'}"
    return {"metric_name": metric_name, "pass_metric": passes,
            "value": round(value, 4), "threshold": threshold, "reason": reason}


# ── Public metrics ────────────────────────────────────────────────────────────

def pred_raw_std(preprocess_df: pd.DataFrame) -> dict:
    """Std of pred_raw across sensors on the most recent day.  Perfect precision on train."""
    cfg    = _load_thresholds()
    series = _today_pred_raw(preprocess_df)
    value  = float(series.std(ddof=1)) if series is not None and len(series) > 1 else None
    return _make_result("pred_raw_std", value, float(cfg["pred_raw_std"]))


def pred_raw_range(preprocess_df: pd.DataFrame) -> dict:
    """(max − min) of pred_raw across sensors on the most recent day."""
    cfg    = _load_thresholds()
    series = _today_pred_raw(preprocess_df)
    value  = float(series.max() - series.min()) if series is not None else None
    return _make_result("pred_raw_range", value, float(cfg["pred_raw_range"]))
