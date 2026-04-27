"""
Large Hive Thermoregulation — Temperature family metric (R6b).

Checks that sensors labelled "large" maintain internal temperatures that are
*negatively correlated* (or uncorrelated) with ambient temperature.

Physical motivation
-------------------
Large hives have many bees and strong thermoregulation capacity.  They
actively maintain a warm, stable internal temperature regardless of outdoor
conditions.  A strong positive correlation with ambient means the colony is
NOT thermoregulating — a red flag that the model's prediction is wrong.

Algorithm
---------
1. Resample raw sensor and gateway readings to 1-hour means.
2. Compute the hourly mean of the "large" bucket across all large sensors.
3. Compute the Pearson correlation against the ambient hourly series.
4. Return pass_metric=False if r > LARGE_CORR_MAX.
5. If correlation cannot be computed (< 3 overlapping points, constant series),
   treat the result as unassessable → pass_metric=True (do not penalise).

Thresholds (from 2026-04-15 decide.py)
---------------------------------------
LARGE_CORR_MAX: float = 0.85   # from decide.py — Pearson r above this → large not thermoregulating

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
    metric_name          : str         — "large_hive_thermoregulation".
    pass_metric          : bool        — True = large sensors are thermoregulating (or unassessable).
    threshold            : float       — LARGE_CORR_MAX.
    value                : float|None  — measured Pearson r (None when unassessable).
    days_period          : int         — 2.
    metric_decision_data : dict        — {"n_points": int}.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import yaml

from model_monitor.utils.data_utils import resample_sensor_to_hourly, resample_gateway_to_hourly

log = logging.getLogger(__name__)

# ── Family metadata ───────────────────────────────────────────────────────────
METRIC_FAMILY: str = "temperature"
_METRIC_NAME:  str = "large_hive_thermoregulation"
_DAYS_PERIOD:  int = 2

# ── Threshold (loaded from configs/thresholds.yaml) ───────────────────────────
def _load_thresholds() -> dict:
    path = Path(__file__).resolve().parents[4] / "configs/thresholds.yaml"
    with open(path) as f:
        return yaml.safe_load(f)["metrics"]["temperature"]["large_hive_thermoregulation"]

_cfg = _load_thresholds()
LARGE_CORR_MAX: float = float(_cfg["max_correlation"])


def large_hive_thermoregulation(
    sensor_df: pd.DataFrame,
    gateway_df: pd.DataFrame,
) -> dict:
    """Return a standardised metric dict for large-hive thermoregulation.

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
        ``metric_name``          — "large_hive_thermoregulation".
        ``pass_metric``          — True when r ≤ LARGE_CORR_MAX (or unassessable).
        ``threshold``            — LARGE_CORR_MAX.
        ``value``                — Pearson r (or None when unassessable).
        ``days_period``          — 2.
        ``metric_decision_data`` — {"n_points": int}.
    """
    def _result(pass_metric: bool, value, n_points: int,
                error: str | None = None) -> dict:
        return {
            "metric_name":          _METRIC_NAME,
            "pass_metric":          pass_metric,
            "threshold":            LARGE_CORR_MAX,
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
        log.warning("large_hive_thermoregulation: invalid input — %s", exc)
        return _result(False, None, 0, error=str(exc))

    large = sensor_hourly.loc[sensor_hourly["hive_size_bucket"] == "large"].copy()

    if large.empty:
        log.debug("large_hive_thermoregulation: no 'large' bucket sensors → pass_metric=True (N/A)")
        return _result(True, None, 0)

    large_hourly   = large.groupby("timestamp")["pcb_temperature_one"].mean()
    ambient_hourly = gateway_hourly.set_index("timestamp")["pcb_temperature_two"]
    merged = pd.concat([large_hourly, ambient_hourly], axis=1, join="inner").dropna()
    merged.columns = ["sensor", "ambient"]
    n_points = len(merged)

    if n_points < 3:
        log.debug("large_hive_thermoregulation: only %d overlapping points → unassessable → pass_metric=True", n_points)
        return _result(True, None, n_points)

    r = float(merged["sensor"].corr(merged["ambient"]))

    if r != r:   # NaN = constant series → unassessable
        log.debug("large_hive_thermoregulation: correlation=NaN (constant series) → pass_metric=True")
        return _result(True, None, n_points)

    pass_metric = r <= LARGE_CORR_MAX
    log.debug("large_hive_thermoregulation: r=%.3f %s %.2f → pass_metric=%s",
              r, "≤" if pass_metric else ">", LARGE_CORR_MAX, pass_metric)
    return _result(pass_metric, round(r, 4), n_points)
