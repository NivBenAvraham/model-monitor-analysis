"""
Triage Rules metrics — decision signals for Calibration Review Triage.

Each metric is a pure function that takes pre-loaded DataFrames and returns
a standardised result dict with at minimum:
    metric_name  : str
    pass_metric  : bool | None
    value        : float | None
    threshold    : float

SQL queries that fetch the required DataFrames live in:
    skills/Calibration Review Triage/scripts/queries.py

Import pattern
--------------
from model_monitor.metrics.triage_rules import (
    clipping_diff,
    inspection_discrepancy,
    thermoreg_dipping,
    auto_review_score,
)
"""

from model_monitor.metrics.triage_rules.auto_review_score import auto_review_score
from model_monitor.metrics.triage_rules.clipping_diff import clipping_diff
from model_monitor.metrics.triage_rules.inspection_discrepancy import inspection_discrepancy
from model_monitor.metrics.triage_rules.thermoreg_dipping import thermoreg_dipping

__all__ = [
    "clipping_diff",
    "inspection_discrepancy",
    "thermoreg_dipping",
    "auto_review_score",
]
