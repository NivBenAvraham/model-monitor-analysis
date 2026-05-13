"""
Triage Rule — decision layer for Calibration Review Triage.

Classifies a stale PRODUCTION beekeeper group on a given day into:

    auto_valid   — no review needed; all signals had data and last status was valid.
    needs_review — human review required.

Decision logic (from triage-metrics-analysis.ipynb, cells 30–31)
-----------------------------------------------------------------

Step 1 — Count signal coverage
    passed_metrics = number of signals that returned a non-None pass_metric.
    (True and False both count; only None — insufficient data — does not.)
    Maximum = 4 (one per signal).

Step 2 — Decide
    auto_valid   when:  passed_metrics > 3  AND  last_status == 'valid'
    needs_review otherwise.

Signals (4 total)
-----------------
    clipping_diff          — raw-vs-clipped prediction gap on the review date.
    inspection_discrepancy — recent inspector count vs same-day model output.
    thermoreg_dipping      — % of yards with rising temperature dispersion.
    auto_review_score      — composite UBF stability score over 21 days.

last_status
-----------
    The tier2_status from exactly yesterday (reference_date − 1 day).
    Provided by the caller from the validation-history query.
    Values: 'valid', 'invalid', 'needs_recalibration', or None (no history).
    Only 'valid' enables auto_valid.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

_EXPECTED_SIGNALS = frozenset({
    "clipping_diff",
    "inspection_discrepancy",
    "thermoreg_dipping",
    "auto_review_score",
})


def score_group_date(
    metric_results: list[dict],
    last_status: str | None,
) -> dict:
    """Classify one (group_id, date) for the Calibration Review Triage.

    Parameters
    ----------
    metric_results:
        List of result dicts from the 4 triage signal functions.  Each dict
        must contain at minimum:
            ``metric_name``  : str
            ``pass_metric``  : bool | None  (None = insufficient data)
        Unknown metric names are logged as warnings and ignored.

    last_status:
        tier2_status from exactly yesterday (reference_date − 1 day).
        Obtained from the validation-history query.
        Pass ``None`` when the group has no review history.

    Returns
    -------
    dict with keys:

        ``prediction``     — "auto_valid" | "needs_review"
        ``passed_metrics`` — int 0–4: count of signals with a non-None result
        ``last_status``    — echoed back
        ``reason``         — None when auto_valid; str describing why needs_review
        ``signals``        — dict {signal_name: bool | None} for all 4 signals
    """
    if not metric_results:
        raise ValueError("metric_results must not be empty")

    # ── collect signal results ────────────────────────────────────────────────
    signals: dict[str, bool | None] = {name: None for name in _EXPECTED_SIGNALS}

    for m in metric_results:
        name = m.get("metric_name", "unknown")
        if name not in _EXPECTED_SIGNALS:
            log.warning("triage_rule: unknown signal '%s' — ignored", name)
            continue
        signals[name] = m.get("pass_metric")

    # ── Step 1: count signal coverage ────────────────────────────────────────
    # True and False both count; only None (insufficient data) is excluded.
    passed_metrics = sum(1 for v in signals.values() if v is not None)

    # ── Step 2: decide ────────────────────────────────────────────────────────
    has_full_coverage = passed_metrics > 3          # all 4 signals returned a result
    last_is_valid     = last_status == "valid"

    if has_full_coverage and last_is_valid:
        prediction = "auto_valid"
        reason     = None
    else:
        prediction = "needs_review"
        if not has_full_coverage and not last_is_valid:
            reason = "insufficient_signal_data; last_status_not_valid"
        elif not has_full_coverage:
            reason = "insufficient_signal_data"
        else:
            reason = "last_status_not_valid"

    log.debug(
        "triage_rule: passed_metrics=%d last_status=%s → %s",
        passed_metrics, last_status, prediction,
    )

    return {
        "prediction":     prediction,
        "passed_metrics": passed_metrics,
        "last_status":    last_status,
        "reason":         reason,
        "signals":        signals,
    }
