"""
Pred Rule — model health decision from clipping pressure signal.

Predicts whether a group's model is VALID on a given date using a single
continuous signal derived from the preprocess table:

    clip_diff_roll3 = 3-day rolling mean of  mean|pred_raw − pred_clipped|

Physical interpretation
-----------------------
`pred_raw`    — the model's unconstrained prediction (bee-frames).
`pred_clipped`— the ceiling-capped value shown to users.

When the model is calibrated correctly its raw predictions already sit inside
the clipping ceiling → clip_diff ≈ 0.  When the model has drifted or is
miscalibrated, raw predictions consistently overshoot → clip_diff is large.

The 3-day rolling average smooths single-day noise and requires the signal to
be sustained over recent days, which dramatically reduces false positives
relative to using today's value alone.

Decision
--------
    clip_diff_roll3 ≤ CLIP_DIFF_ROLL3_MAX  →  VALID   (confidence 5)
    clip_diff_roll3 > CLIP_DIFF_ROLL3_MAX  →  INVALID

When roll3 is unavailable (< 2 days of preprocess history), the rule falls
back to today's `clip_diff_mean` compared against CLIP_DIFF_MEAN_MAX.  The
result is still VALID / INVALID but at confidence 4 (less certain).

Calibration (train split, 299 pairs, 2026-05-18)
-------------------------------------------------
  CLIP_DIFF_ROLL3_MAX = 0.591 bee-frames
  Train:  TP=33  FP=3  Precision=0.917  Recall=0.123
  Test:   TP=15  FP=1  Precision=0.938  Recall=0.158

  (for comparison — previous 2-gate rule: test P=0.900, test TP=18, test FP=2)

Why a single threshold (not a gate chain)?
  All signals here come from the same continuous source: |pred_raw−pred_clipped|.
  The mean, p90, rolling avg, etc. are all highly correlated.  Chaining multiple
  thresholds on correlated signals reduces recall without a meaningful precision
  gain.  One well-calibrated threshold on the best signal is the correct design.

Input
-----
clipping_result : dict — output of
    ``model_monitor.metrics.pred_rules.clipping_pressure.clipping_pressure()``.

Output
------
dict
    prediction : str  — "valid" | "invalid"
    confidence : int  — 1–5 (5 = most confident valid, 1 = most confident invalid)
    reason     : str  — human-readable explanation with the signal value
    signal     : str  — which signal was used ("clip_diff_roll3" | "clip_diff_mean")
    signal_value: float | None — the value of the signal used
"""

from __future__ import annotations

from pathlib import Path
import yaml

_THRESHOLDS_PATH = Path(__file__).resolve().parents[3] / "configs/thresholds.yaml"


def _load_thresholds() -> dict:
    with open(_THRESHOLDS_PATH) as f:
        return yaml.safe_load(f)["pred_rules"]["clipping_pressure"]


_cfg = _load_thresholds()
CLIP_DIFF_ROLL3_MAX: float = float(_cfg["clip_diff_roll3"])
CLIP_DIFF_MEAN_MAX:  float = float(_cfg["clip_diff_mean"])


def score_group_date(clipping_result: dict) -> dict:
    """Predict model health from a clipping_pressure metric result.

    Uses ``clip_diff_roll3`` (3-day rolling mean).  Falls back to
    ``clip_diff_mean`` when rolling history is unavailable.

    Parameters
    ----------
    clipping_result:
        Output dict from ``clipping_pressure(preprocess_df)``.

    Returns
    -------
    dict with keys:
        ``prediction``   — "valid" | "invalid"
        ``confidence``   — int 1–5
        ``reason``       — explanation with actual signal value
        ``signal``       — which signal was used
        ``signal_value`` — the value of the signal used
    """
    value = clipping_result.get("value", {})

    roll3 = value.get("clip_diff_roll3")
    mean  = value.get("clip_diff_mean")

    # ── Primary: 3-day rolling average ────────────────────────────────────
    if roll3 is not None:
        passes = roll3 <= CLIP_DIFF_ROLL3_MAX
        if passes:
            return {
                "prediction":   "valid",
                "confidence":   5,
                "reason":       f"clip_diff_roll3={roll3:.3f} ≤ {CLIP_DIFF_ROLL3_MAX}",
                "signal":       "clip_diff_roll3",
                "signal_value": roll3,
            }
        # Severity: how far above threshold?
        ratio = roll3 / CLIP_DIFF_ROLL3_MAX
        confidence = 1 if ratio >= 3 else (2 if ratio >= 1.5 else 3)
        return {
            "prediction":   "invalid",
            "confidence":   confidence,
            "reason":       f"clip_diff_roll3={roll3:.3f} > {CLIP_DIFF_ROLL3_MAX}",
            "signal":       "clip_diff_roll3",
            "signal_value": roll3,
        }

    # ── Fallback: today's mean (insufficient rolling history) ─────────────
    if mean is not None:
        passes = mean <= CLIP_DIFF_MEAN_MAX
        if passes:
            return {
                "prediction":   "valid",
                "confidence":   4,
                "reason":       (f"clip_diff_mean={mean:.3f} ≤ {CLIP_DIFF_MEAN_MAX} "
                                 f"(roll3 unavailable — insufficient history)"),
                "signal":       "clip_diff_mean",
                "signal_value": mean,
            }
        ratio = mean / CLIP_DIFF_MEAN_MAX
        confidence = 1 if ratio >= 3 else (2 if ratio >= 1.5 else 3)
        return {
            "prediction":   "invalid",
            "confidence":   confidence,
            "reason":       f"clip_diff_mean={mean:.3f} > {CLIP_DIFF_MEAN_MAX}",
            "signal":       "clip_diff_mean",
            "signal_value": mean,
        }

    # ── No data ────────────────────────────────────────────────────────────
    return {
        "prediction":   "invalid",
        "confidence":   1,
        "reason":       "no clipping signal available",
        "signal":       None,
        "signal_value": None,
    }
