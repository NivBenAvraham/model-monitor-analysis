"""
Sensor Spread Within Bucket — Temperature family metric (R4).

Checks that sensors assigned to the same hive-size bucket do not have wildly
divergent mean temperatures.  High within-bucket spread means the sensors are
not behaving like a homogeneous group — a red flag for corrupted data, bad
bucket assignments, or a fundamentally broken model prediction.

Physical motivation
-------------------
Hives in the same size class (e.g. "large") should share similar thermal
behaviour.  Their internal temperatures will naturally vary a few degrees, but
if one sensor reads 18 °C while another reads 38 °C within the same "large"
bucket, the size prediction is almost certainly wrong for at least one of them.

Algorithm
---------
1. Resample raw sensor readings to 1-hour means per (bucket, sensor, hour).
2. For each bucket present in the data:
   a. Compute the overall mean temperature per sensor (mean across all hours).
   b. Compute the std of those per-sensor means within the bucket.
   c. If std > SENSOR_SPREAD_MAX → that bucket fails.
3. The overall result pass_metric=True only when *every present* bucket passes.

Thresholds (from 2026-04-15 decide.py)
---------------------------------------
SENSOR_SPREAD_MAX: float = 9.0   # °C — veto if any bucket's sensor-mean std exceeds this

Family
------
METRIC_FAMILY = "temperature"

Input
-----
sensor_df : Raw sensor DataFrame with columns:
            ``sensor_mac_address``, ``hive_size_bucket``,
            ``timestamp`` (datetime-parseable), ``pcb_temperature_one`` (°C).
            Resampling to hourly means is handled internally.

Output
------
dict
    metric_name          : str   — "sensor_spread_within_bucket".
    pass_metric          : bool  — True = every present bucket's sensor spread is acceptable.
    threshold            : float — SENSOR_SPREAD_MAX.
    value                : dict  — {bucket: std_of_sensor_means} for each bucket assessed.
    days_period          : int   — 2.
    metric_decision_data : dict  — {"bucket_verdicts": {bucket: True/False}}.
"""

from __future__ import annotations

import logging

import pandas as pd

from model_monitor.utils.data_utils import resample_sensor_to_hourly

log = logging.getLogger(__name__)

# ── Family metadata ───────────────────────────────────────────────────────────
METRIC_FAMILY: str = "temperature"
_METRIC_NAME:  str = "sensor_spread_within_bucket"
_DAYS_PERIOD:  int = 2

# ── Threshold ─────────────────────────────────────────────────────────────────
# Std of per-sensor mean temperatures within a bucket (from decide.py SENSOR_SPREAD_HIGH).
# Above this the sensors in the bucket are considered noisy / heterogeneous.
SENSOR_SPREAD_HIGH: float = 5.0   # °C


def sensor_spread_within_bucket(sensor_df: pd.DataFrame) -> dict:
    """Return a standardised metric dict for within-bucket sensor spread.

    Parameters
    ----------
    sensor_df:
        Raw sensor DataFrame with ``sensor_mac_address``, ``hive_size_bucket``,
        ``timestamp``, and ``pcb_temperature_one`` columns.
        Resampling to hourly means is handled internally.

    Returns
    -------
    dict with keys:
        ``metric_name``          — "sensor_spread_within_bucket".
        ``pass_metric``          — True when every present bucket's spread ≤ SENSOR_SPREAD_HIGH.
        ``threshold``            — SENSOR_SPREAD_HIGH (°C).
        ``value``                — std of per-sensor means per bucket.
        ``days_period``          — 2.
        ``metric_decision_data`` — {"bucket_verdicts": {bucket: True/False}}.
    """
    def _result(pass_metric: bool, value: dict, bucket_verdicts: dict,
                error: str | None = None) -> dict:
        return {
            "metric_name":          _METRIC_NAME,
            "pass_metric":          pass_metric,
            "threshold":            SENSOR_SPREAD_HIGH,
            "value":                value,
            "days_period":          _DAYS_PERIOD,
            "metric_decision_data": {
                "bucket_verdicts": bucket_verdicts,
                **({"error": error} if error else {}),
            },
        }

    try:
        sensor_hourly = resample_sensor_to_hourly(sensor_df)
    except ValueError as exc:
        log.warning("sensor_spread_within_bucket: invalid input — %s", exc)
        return _result(False, {}, {}, error=str(exc))

    if sensor_hourly.empty:
        log.debug("sensor_spread_within_bucket: no sensor readings → pass_metric=False")
        return _result(False, {}, {}, error="no data")

    bucket_spreads:  dict[str, float] = {}
    bucket_verdicts: dict[str, bool]  = {}

    for bucket, group in sensor_hourly.groupby("hive_size_bucket"):
        sensor_means = group.groupby("sensor_mac_address")["pcb_temperature_one"].mean()

        if len(sensor_means) < 2:
            continue   # single sensor — spread undefined, not a failure

        spread = float(sensor_means.std())
        bucket_spreads[bucket]  = round(spread, 4)
        bucket_verdicts[bucket] = spread <= SENSOR_SPREAD_HIGH

        log.debug(
            "sensor_spread_within_bucket: bucket=%s spread=%.2f°C %s %.1f°C",
            bucket, spread, "≤" if bucket_verdicts[bucket] else ">", SENSOR_SPREAD_HIGH,
        )

    pass_metric = all(bucket_verdicts.values()) if bucket_verdicts else True
    log.debug("sensor_spread_within_bucket: pass_metric=%s", pass_metric)
    return _result(pass_metric, bucket_spreads, bucket_verdicts)
