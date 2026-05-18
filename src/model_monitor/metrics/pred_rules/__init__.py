"""
pred_rules — metrics derived from pred_raw / pred_clipped preprocess data.

These metrics operate on the model's raw and clipped predictions (from the
beekeeper_beeframe_model_monitoring_preprocess table), rather than on raw
sensor temperature readings.

All functions accept a ``preprocess_df`` with columns:
    ``date``, ``mac``, ``pred_raw``, ``pred_clipped``

and return a standardised dict:
    ``metric_name``, ``pass_metric``, ``value``, ``threshold``, ``reason``

Metric families
---------------
clipping_pressure   — combined metric used by pred_rule decision (legacy, kept for compat)

clip_diff_daily     — daily metrics: clip_diff_mean, clip_diff_p90, clip_diff_max
clip_diff_rolling   — rolling averages: clip_diff_mean_roll3/5/7
pct_clipped         — fraction clipped: pct_clipped, pct_clipped_roll3, pct_clipped_roll7
pred_raw_stats      — spread metrics: pred_raw_std, pred_raw_range
"""

from model_monitor.metrics.pred_rules.clipping_pressure import clipping_pressure
from model_monitor.metrics.pred_rules.clip_diff_daily import (
    clip_diff_mean,
    clip_diff_p90,
    clip_diff_max,
)
from model_monitor.metrics.pred_rules.clip_diff_rolling import (
    clip_diff_mean_roll3,
    clip_diff_mean_roll5,
    clip_diff_mean_roll7,
)
from model_monitor.metrics.pred_rules.pct_clipped import (
    pct_clipped,
    pct_clipped_roll3,
    pct_clipped_roll7,
)
from model_monitor.metrics.pred_rules.pred_raw_stats import (
    pred_raw_std,
    pred_raw_range,
)

__all__ = [
    # combined (used by pred_rule.py)
    "clipping_pressure",
    # daily clipping diff
    "clip_diff_mean",
    "clip_diff_p90",
    "clip_diff_max",
    # rolling clipping diff
    "clip_diff_mean_roll3",
    "clip_diff_mean_roll5",
    "clip_diff_mean_roll7",
    # clipping rate
    "pct_clipped",
    "pct_clipped_roll3",
    "pct_clipped_roll7",
    # pred_raw spread
    "pred_raw_std",
    "pred_raw_range",
]
