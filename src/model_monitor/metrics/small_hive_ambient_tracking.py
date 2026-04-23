"""
Small Hive Ambient Tracking — Temperature family metric (R6a).

Checks that sensors labelled "small" have internal temperatures that
*positively correlate* with the ambient (gateway) temperature.

Physical motivation
-------------------
Small hives have few bees and therefore very limited thermoregulation
capacity.  Their internal temperature is almost entirely governed by
the environment.  A valid small-hive sensor should rise and fall in
sync with the outdoor temperature (positive Pearson correlation).

If the correlation is strongly negative or near zero, it indicates either:
  • The sensors were incorrectly assigned to the "small" bucket, or
  • The sensor data is corrupted.

Algorithm
---------
1. Resample raw sensor and gateway readings to 1-hour means.
2. Compute the hourly mean of the "small" bucket across all small sensors.
3. Compute the Pearson correlation against the ambient hourly series.
4. Return pass_metric=False if r < SMALL_CORR_MIN.
5. If correlation cannot be computed (< 3 overlapping points, constant series),
   treat the result as unassessable → pass_metric=True (do not penalise).

Thresholds (from 2026-04-15 decide.py)
---------------------------------------
SMALL_CORR_MIN: float = 0.3   # Pearson r below this → small sensors track ambient poorly

Family
------
METRIC_FAMILY = "temperature"

Input
-----
sensor_df  : Raw sensor DataFrame with columns:
             ``sensor_mac_address``, ``hive_size_bucket``,
             ``timestamp`` (datetime-parseable), ``pcb_temperature_one`` (°C).
gateway_df : Raw gateway DataFrame with columns:
             ``timestamp`` (datetime-parseable), ``pcb_temperature_two`` (°C).
             Resampling to hourly means is handled internally for both DataFrames.

Output
------
dict
    metric_name          : str         — "small_hive_ambient_tracking".
    pass_metric          : bool        — True = small sensors track ambient (or unassessable).
    threshold            : float       — SMALL_CORR_MIN.
    value                : float|None  — measured Pearson r (None when unassessable).
    days_period          : int         — 2.
    metric_decision_data : dict        — {"n_points": int}.
"""

from __future__ import annotations

import logging

import pandas as pd

from model_monitor.utils.data_utils import resample_sensor_to_hourly, resample_gateway_to_hourly

log = logging.getLogger(__name__)

# ── Family metadata ───────────────────────────────────────────────────────────
METRIC_FAMILY: str = "temperature"
_METRIC_NAME:  str = "small_hive_ambient_tracking"
_DAYS_PERIOD:  int = 2

# ── Threshold ─────────────────────────────────────────────────────────────────
SMALL_CORR_MIN: float = 0.3


def small_hive_ambient_tracking(
    sensor_df: pd.DataFrame,
    gateway_df: pd.DataFrame,
) -> dict:
    """Return a standardised metric dict for small-hive ambient tracking.

    Parameters
    ----------
    sensor_df:
        Raw sensor DataFrame with ``sensor_mac_address``, ``hive_size_bucket``,
        ``timestamp``, and ``pcb_temperature_one`` columns.
    gateway_df:
        Raw gateway DataFrame with ``timestamp`` and ``pcb_temperature_two`` columns.
        Resampling is handled internally for both DataFrames.

    Returns
    -------
    dict with keys:
        ``metric_name``          — "small_hive_ambient_tracking".
        ``pass_metric``          — True when r ≥ SMALL_CORR_MIN (or unassessable).
        ``threshold``            — SMALL_CORR_MIN.
        ``value``                — Pearson r (or None when unassessable).
        ``days_period``          — 2.
        ``metric_decision_data`` — {"n_points": int}.
    """
    def _result(pass_metric: bool, value, n_points: int,
                error: str | None = None) -> dict:
        return {
            "metric_name":          _METRIC_NAME,
            "pass_metric":          pass_metric,
            "threshold":            SMALL_CORR_MIN,
            "value":                value,
            "days_period":          _DAYS_PERIOD,
            "metric_decision_data": {
                "n_points": n_points,
                **({"error": error} if error else {}),
            },
        }

    try:
        sensor_hourly  = resample_sensor_to_hourly(sensor_df)
        gateway_hourly = resample_gateway_to_hourly(gateway_df)
    except ValueError as exc:
        log.warning("small_hive_ambient_tracking: invalid input — %s", exc)
        return _result(False, None, 0, error=str(exc))

    small = sensor_hourly.loc[sensor_hourly["hive_size_bucket"] == "small"].copy()

    if small.empty:
        log.debug("small_hive_ambient_tracking: no 'small' bucket sensors → pass_metric=True (N/A)")
        return _result(True, None, 0)

    small_hourly   = small.groupby("timestamp")["pcb_temperature_one"].mean()
    ambient_hourly = gateway_hourly.set_index("timestamp")["pcb_temperature_two"]
    merged = pd.concat([small_hourly, ambient_hourly], axis=1, join="inner").dropna()
    merged.columns = ["sensor", "ambient"]
    n_points = len(merged)

    if n_points < 3:
        log.debug("small_hive_ambient_tracking: only %d overlapping points → unassessable → pass_metric=True", n_points)
        return _result(True, None, n_points)

    r = float(merged["sensor"].corr(merged["ambient"]))

    if r != r:   # NaN = constant series → unassessable
        log.debug("small_hive_ambient_tracking: correlation=NaN (constant series) → pass_metric=True")
        return _result(True, None, n_points)

    pass_metric = r >= SMALL_CORR_MIN
    log.debug("small_hive_ambient_tracking: r=%.3f %s %.2f → pass_metric=%s",
              r, "≥" if pass_metric else "<", SMALL_CORR_MIN, pass_metric)
    return _result(pass_metric, round(r, 4), n_points)
