"""
Clipping Pressure — pred_rules family metric.

Measures how heavily the model's raw predictions are being cut by the clipping
ceiling.  A model in a valid state produces raw predictions that sit naturally
within the clip bounds — the clipping machinery barely fires.  When the model
is miscalibrated (invalid), raw predictions consistently overshoot the ceiling,
producing large `|pred_raw − pred_clipped|` values on many sensors.

Physical motivation
-------------------
`pred_raw`    — the model's unconstrained output (bee-frames).
`pred_clipped`— the value actually shown to users, capped at a group-specific
                ceiling derived from the calibration.

Clipping behaviour encodes model health:
  • VALID group  : the model predicts within range → clip_diff ≈ 0 for most sensors.
  • INVALID group: the model overshoots → many sensors are clipped, clip_diff is
                   systematically > 0.

This signal is the strongest single discriminator in the preprocess data
(AUROC=0.84 on train).  The 3-day rolling average (`clip_diff_roll3`) smooths
out day-to-day noise and raises AUROC to 0.77 while dramatically boosting
precision at the same recall level.

Algorithm
---------
1. Accept a preprocess DataFrame covering ≥1 day (ideally ≥5 days for rolling).
2. For each calendar day: compute per-sensor |pred_raw − pred_clipped|.
3. Aggregate per day:
   - `clip_diff_mean` — mean across all sensors
   - `clip_diff_p90`  — 90th percentile across sensors
   - `pct_clipped`    — fraction of sensors where pred_raw ≠ pred_clipped
4. Sort days chronologically, compute rolling means (3-day, 5-day).
5. Return the **most recent day's** values (and their rolling context).
6. pass_metric=True when all present signals are within their thresholds.

Thresholds (configs/thresholds.yaml → metrics.pred_rules.clipping_pressure)
----------------------------------------------------------------------------
Calibrated on train split (299 pairs, 2026-05-18):
  clip_diff_mean       ≤ 0.60  bee-frames  (AUROC=0.84, train FP=2 at this thr)
  clip_diff_roll3      ≤ 0.65  bee-frames  (3-day smoothed)
  clip_diff_p90        ≤ 1.50  bee-frames  (optional tighter gate)

  Best 2-gate combination (clip_diff_mean + clip_diff_roll3):
    Train: TP=27 FP=1 Prec=0.964 Rec=0.144
    Test:  TP=13 FP=1 Prec=0.929 Rec=0.206

Family
------
METRIC_FAMILY = "pred_rules"

Input
-----
preprocess_df : DataFrame with columns:
    ``date``         (datetime-parseable)
    ``mac``          (sensor identifier)
    ``pred_raw``     (float — unconstrained model prediction)
    ``pred_clipped`` (float — ceiling-capped prediction)

    Should cover the evaluation date AND preceding days (recommend ≥5 days)
    so that rolling metrics are well-defined.  The most recent date in the
    DataFrame is treated as the evaluation date.

Output
------
dict
    metric_name          : str   — "clipping_pressure".
    pass_metric          : bool  — True when all present gates pass.
    threshold            : dict  — per-signal thresholds.
    value                : dict  — {clip_diff_mean, clip_diff_p90, pct_clipped,
                                     clip_diff_roll3, clip_diff_roll5}.
    days_in_window       : int   — number of calendar days in preprocess_df.
    metric_decision_data : dict  — {"gate_verdicts": {...}, "daily": [...]}.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

log = logging.getLogger(__name__)

METRIC_FAMILY: str = "pred_rules"
_METRIC_NAME:  str = "clipping_pressure"


def _load_thresholds() -> dict:
    path = Path(__file__).resolve().parents[4] / "configs/thresholds.yaml"
    with open(path) as f:
        return yaml.safe_load(f)["pred_rules"]["clipping_pressure"]


_cfg = _load_thresholds()
THRESHOLDS: dict[str, float] = {
    "clip_diff_mean":  float(_cfg["clip_diff_mean"]),
    "clip_diff_roll3": float(_cfg["clip_diff_roll3"]),
    "clip_diff_p90":   float(_cfg["clip_diff_p90"]),
    "pct_clipped":     float(_cfg["pct_clipped"]),
}


def clipping_pressure(preprocess_df: pd.DataFrame) -> dict:
    """Return a standardised metric dict for clipping pressure.

    Parameters
    ----------
    preprocess_df:
        DataFrame with ``date``, ``mac``, ``pred_raw``, ``pred_clipped``.
        Should cover the evaluation date and several preceding days.
        The most recent date is treated as the evaluation date.

    Returns
    -------
    dict with keys ``metric_name``, ``pass_metric``, ``threshold``, ``value``,
    ``days_in_window``, ``metric_decision_data``.
    """
    def _result(pass_metric: bool, value: dict, gate_verdicts: dict,
                daily: list, error: str | None = None) -> dict:
        return {
            "metric_name":          _METRIC_NAME,
            "pass_metric":          pass_metric,
            "threshold":            THRESHOLDS,
            "value":                value,
            "days_in_window":       len(daily),
            "metric_decision_data": {
                "gate_verdicts": gate_verdicts,
                "daily":         daily,
                **({"error": error} if error else {}),
            },
        }

    required = {"date", "mac", "pred_raw", "pred_clipped"}
    missing  = required - set(preprocess_df.columns)
    if missing:
        msg = f"missing columns: {missing}"
        log.warning("clipping_pressure: %s", msg)
        return _result(False, {}, {}, [], error=msg)

    df = preprocess_df.copy()
    df["date"]      = pd.to_datetime(df["date"])
    df["clip_diff"] = (df["pred_raw"] - df["pred_clipped"]).abs()
    df["is_clipped"] = df["pred_raw"] != df["pred_clipped"]

    if df.empty:
        return _result(False, {}, {}, [], error="no data")

    # ── Per-day aggregates ─────────────────────────────────────────────────
    daily_df = (
        df.groupby("date")
        .agg(
            clip_diff_mean = ("clip_diff",   "mean"),
            clip_diff_p90  = ("clip_diff",   lambda s: s.quantile(0.90)),
            pct_clipped    = ("is_clipped",  "mean"),
            n_sensors      = ("mac",         "nunique"),
        )
        .sort_index()
        .reset_index()
    )

    if daily_df.empty:
        return _result(False, {}, {}, [], error="no daily data")

    # ── Rolling means (need ≥2 days for roll3 to be meaningful) ───────────
    daily_df["clip_diff_roll3"] = daily_df["clip_diff_mean"].rolling(3, min_periods=2).mean()
    daily_df["clip_diff_roll5"] = daily_df["clip_diff_mean"].rolling(5, min_periods=3).mean()

    # ── Use the most recent day as evaluation point ────────────────────────
    today = daily_df.iloc[-1]
    value: dict[str, float | None] = {
        "clip_diff_mean":  round(float(today["clip_diff_mean"]), 4),
        "clip_diff_p90":   round(float(today["clip_diff_p90"]),  4),
        "pct_clipped":     round(float(today["pct_clipped"]),    4),
        "clip_diff_roll3": round(float(today["clip_diff_roll3"]), 4) if pd.notna(today["clip_diff_roll3"]) else None,
        "clip_diff_roll5": round(float(today["clip_diff_roll5"]), 4) if pd.notna(today["clip_diff_roll5"]) else None,
    }

    # ── Gate verdicts ──────────────────────────────────────────────────────
    gate_verdicts: dict[str, bool | None] = {}
    for sig, thr in THRESHOLDS.items():
        v = value.get(sig)
        if v is None:
            gate_verdicts[sig] = None   # insufficient history
        else:
            gate_verdicts[sig] = v <= thr

    # pass_metric=True only when all non-None gates pass
    active = {k: v for k, v in gate_verdicts.items() if v is not None}
    pass_metric = all(active.values()) if active else True

    daily_records = daily_df.to_dict(orient="records")
    for rec in daily_records:
        rec["date"] = str(rec["date"].date())

    log.debug(
        "clipping_pressure: today=%s clip_diff_mean=%.3f roll3=%s pass=%s",
        today["date"].date(),
        value["clip_diff_mean"],
        f"{value['clip_diff_roll3']:.3f}" if value["clip_diff_roll3"] is not None else "n/a",
        pass_metric,
    )
    return _result(pass_metric, value, gate_verdicts, daily_records)
