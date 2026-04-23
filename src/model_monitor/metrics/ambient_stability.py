"""
Ambient Stability — Temperature family metric (R1).

Checks whether the ambient (gateway) temperature signal is stable enough for
the evaluation to be meaningful.  Ambient is measured by gateway sensors outside
the hives.  When the gateway signal is highly erratic, any sensor-based
validity checks become unreliable because the reference baseline is noisy.

Physical motivation
-------------------
Ambient temperature naturally fluctuates throughout the day (diurnal cycle),
but its variation should be predictable and smooth.  Coefficient of Variation
(CV = std / |mean|) captures how volatile the signal is relative to its own
level.  Very high CV values indicate either sensor malfunction, extreme weather
events, or data quality issues — none of which allow reliable model evaluation.

Algorithm
---------
1. Resample raw gateway readings to 1-hour means (mean across all gateways).
2. Compute the mean and std of all hourly ambient readings for the window.
3. CV = std / |mean|.  If |mean| < 1e-3 °C (sensor zeroed out), treat CV = 0.
4. Return pass_metric=False when CV > AMBIENT_CV_HIGH (severely unstable, top ~5%).
   Return pass_metric=False when CV > AMBIENT_CV_MED  (clearly unstable).
   Return pass_metric=True  otherwise.

Thresholds (from 2026-04-15 decide.py, calibrated on the train split)
----------------------------------------------------------------------
AMBIENT_CV_HIGH = 0.70  — CV above this: only ~5% of all train pairs reach here.
AMBIENT_CV_MED  = 0.55  — CV above this: clearly unstable (primary threshold).
Both medians (valid and invalid) sit around 0.34 — so only extreme CV is signal.

Family
------
METRIC_FAMILY = "temperature"

Input
-----
gateway_df : Raw gateway DataFrame with columns:
             ``timestamp`` (datetime-parseable) and ``pcb_temperature_two`` (°C).
             Resampling to hourly means is handled internally.

Output
------
dict
    metric_name          : str   — "ambient_stability".
    pass_metric          : bool  — True = ambient stable enough; False = too erratic.
    threshold            : float — AMBIENT_CV_MED (primary pass/fail boundary).
    value                : float — measured CV (std / |mean|).
    days_period          : int   — 2.
    metric_decision_data : dict  — {"mean_temp", "std_temp", "threshold_high"}.
"""

from __future__ import annotations

import logging

import pandas as pd

from model_monitor.utils.data_utils import resample_gateway_to_hourly

log = logging.getLogger(__name__)

# ── Family metadata ───────────────────────────────────────────────────────────
METRIC_FAMILY:   str = "temperature"
_METRIC_NAME:    str = "ambient_stability"
_DAYS_PERIOD:    int = 2

# ── Thresholds ────────────────────────────────────────────────────────────────
AMBIENT_CV_HIGH: float = 0.70   # severely unstable — fires only in top ~5%
AMBIENT_CV_MED:  float = 0.55   # clearly unstable — primary pass/fail boundary


def ambient_stability(gateway_df: pd.DataFrame) -> dict:
    """Return a standardised metric dict for ambient stability.

    Parameters
    ----------
    gateway_df:
        Raw gateway DataFrame with ``timestamp`` and ``pcb_temperature_two``
        columns.  Resampling to hourly means is handled internally.

    Returns
    -------
    dict with keys:
        ``metric_name``          — "ambient_stability".
        ``pass_metric``          — True when CV ≤ AMBIENT_CV_MED.
        ``threshold``            — AMBIENT_CV_MED (primary boundary).
        ``value``                — measured coefficient of variation (or None).
        ``days_period``          — 2.
        ``metric_decision_data`` — {"mean_temp", "std_temp", "threshold_high"}.
    """
    def _result(pass_metric: bool, cv, mean, std, error: str | None = None) -> dict:
        return {
            "metric_name":          _METRIC_NAME,
            "pass_metric":          pass_metric,
            "threshold":            AMBIENT_CV_MED,
            "value":                round(cv, 4) if cv is not None else None,
            "days_period":          _DAYS_PERIOD,
            "metric_decision_data": {
                "mean_temp":      round(mean, 2) if mean is not None else None,
                "std_temp":       round(std, 2)  if std  is not None else None,
                "threshold_high": AMBIENT_CV_HIGH,
                **({"error": error} if error else {}),
            },
        }

    try:
        gateway_hourly = resample_gateway_to_hourly(gateway_df)
    except ValueError as exc:
        log.warning("ambient_stability: invalid input — %s", exc)
        return _result(False, None, None, None, error=str(exc))

    ambient = gateway_hourly["pcb_temperature_two"].dropna()

    if ambient.empty:
        log.debug("ambient_stability: no gateway readings → pass_metric=False (no data)")
        return _result(False, None, None, None, error="no data")

    mean = float(ambient.mean())
    std  = float(ambient.std())
    cv   = std / abs(mean) if abs(mean) > 1e-3 else 0.0

    if cv > AMBIENT_CV_HIGH:
        log.debug("ambient_stability: CV=%.3f > %.2f → pass_metric=False (severely unstable)", cv, AMBIENT_CV_HIGH)
        pass_metric = False
    elif cv > AMBIENT_CV_MED:
        log.debug("ambient_stability: CV=%.3f > %.2f → pass_metric=False (unstable)", cv, AMBIENT_CV_MED)
        pass_metric = False
    else:
        log.debug("ambient_stability: CV=%.3f ≤ %.2f → pass_metric=True", cv, AMBIENT_CV_MED)
        pass_metric = True

    return _result(pass_metric, cv, mean, std)
