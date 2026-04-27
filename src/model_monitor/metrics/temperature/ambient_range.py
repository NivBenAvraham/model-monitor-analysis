"""
Ambient Range — Temperature family metric (R2).

Checks whether the ambient (gateway) temperature readings stay within a
physically plausible range.  Readings outside this range indicate either sensor
malfunction or extreme environmental conditions that invalidate the evaluation.

Physical motivation
-------------------
Gateway sensors are outdoor thermometers attached to hive boxes.  Their valid
operating range for a normal beekeeping evaluation window is:

  • Below 2 °C  → bees are likely not active; hive thermal behaviour is
                  dominated by passive cooling rather than the colony.
  • Above 50 °C → almost certainly a sensor error (calibration failure,
                  direct sunlight on the PCB, or a recording artifact).

Either extreme makes it impossible to assess whether the model's hive-size
prediction is correct.

Algorithm
---------
1. Resample raw gateway readings to 1-hour means (mean across all gateways).
2. Read the minimum and maximum of all hourly ambient readings in the window.
3. Return pass_metric=False if  min < AMBIENT_MIN_CELSIUS  OR  max > AMBIENT_MAX_CELSIUS.
4. Return pass_metric=True  otherwise.

Thresholds (from 2026-04-15 decide.py)
---------------------------------------
AMBIENT_MIN_CELSIUS = 2.0  °C
AMBIENT_MAX_CELSIUS = 50.0 °C

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
    metric_name          : str  — "ambient_range".
    pass_metric          : bool — True = all readings within [min_threshold, max_threshold].
    threshold            : dict — {"min": AMBIENT_MIN_CELSIUS, "max": AMBIENT_MAX_CELSIUS}.
    value                : dict — {"min": amb_min, "max": amb_max} (°C).
    days_period          : int  — 2.
    metric_decision_data : dict — {} (threshold + value already carry all context).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

from model_monitor.utils.data_utils import resample_gateway_to_hourly

log = logging.getLogger(__name__)

# ── Family metadata ───────────────────────────────────────────────────────────
METRIC_FAMILY: str = "temperature"
_METRIC_NAME:  str = "ambient_range"
_DAYS_PERIOD:  int = 2

# ── Thresholds (loaded from configs/thresholds.yaml) ──────────────────────────
def _load_thresholds() -> dict:
    path = Path(__file__).resolve().parents[4] / "configs/thresholds.yaml"
    with open(path) as f:
        return yaml.safe_load(f)["metrics"]["temperature"]["ambient_range"]

_cfg = _load_thresholds()
AMBIENT_MIN_CELSIUS: float = float(_cfg["min_celsius"])  # below this → too cold for normal bee activity
AMBIENT_MAX_CELSIUS: float = float(_cfg["max_celsius"])  # above this → sensor error or extreme heat event


def ambient_range(gateway_df: pd.DataFrame) -> dict:
    """Return a standardised metric dict for ambient temperature range.

    Parameters
    ----------
    gateway_df:
        Raw gateway DataFrame with ``timestamp`` and ``pcb_temperature_two``
        columns.  Resampling to hourly means is handled internally.

    Returns
    -------
    dict with keys:
        ``metric_name``          — "ambient_range".
        ``pass_metric``          — True when min ≥ AMBIENT_MIN_CELSIUS and max ≤ AMBIENT_MAX_CELSIUS.
        ``threshold``            — {"min": AMBIENT_MIN_CELSIUS, "max": AMBIENT_MAX_CELSIUS}.
        ``value``                — {"min": amb_min, "max": amb_max} (or None when no data).
        ``days_period``          — 2.
        ``metric_decision_data`` — {} (all context is already in threshold + value).
    """
    _threshold = {"min": AMBIENT_MIN_CELSIUS, "max": AMBIENT_MAX_CELSIUS}

    def _result(pass_metric: bool, value, error: str | None = None) -> dict:
        return {
            "metric_name":          _METRIC_NAME,
            "pass_metric":          pass_metric,
            "threshold":            _threshold,
            "value":                value,
            "days_period":          _DAYS_PERIOD,
            "metric_decision_data": {"error": error} if error else {},
        }

    try:
        gateway_hourly = resample_gateway_to_hourly(gateway_df)
    except ValueError as exc:
        log.warning("ambient_range: invalid input — %s", exc)
        return _result(False, None, error=str(exc))

    ambient = gateway_hourly["pcb_temperature_two"].dropna()

    if ambient.empty:
        log.debug("ambient_range: no gateway readings → pass_metric=False (no data)")
        return _result(False, None, error="no data")

    amb_min = float(ambient.min())
    amb_max = float(ambient.max())
    value   = {"min": round(amb_min, 2), "max": round(amb_max, 2)}

    if amb_min < AMBIENT_MIN_CELSIUS:
        log.debug("ambient_range: min=%.1f°C < %.1f°C → pass_metric=False (too cold)", amb_min, AMBIENT_MIN_CELSIUS)
        pass_metric = False
    elif amb_max > AMBIENT_MAX_CELSIUS:
        log.debug("ambient_range: max=%.1f°C > %.1f°C → pass_metric=False (too hot)", amb_max, AMBIENT_MAX_CELSIUS)
        pass_metric = False
    else:
        log.debug("ambient_range: [%.1f, %.1f]°C within valid range → pass_metric=True", amb_min, amb_max)
        pass_metric = True

    return _result(pass_metric, value)
