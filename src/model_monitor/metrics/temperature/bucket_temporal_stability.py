"""
Bucket Temporal Stability — Temperature family metric (R5).

Checks that each bucket's mean temperature is consistent *over time*, rather
than drifting erratically across the evaluation window.

Physical motivation
-------------------
A healthy hive maintains a relatively stable internal temperature profile
over any given week or two.  Drastic swings in a bucket's hourly mean signal
that either the sensor data is unreliable or the colony is in an abnormal
state (e.g. collapsing, swarming, or being robbed).

We measure stability as the standard deviation of the bucket's mean
temperature computed per calendar day (one value per day → std across days).
This filters out normal diurnal cycles while flagging multi-day drift.

Per-bucket thresholds differ because:
  • Small hives track ambient and are naturally more volatile.
  • Medium hives are semi-regulated — moderate tolerance.
  • Large hives actively thermoregulate → very low tolerance for instability.

Algorithm
---------
1. Resample raw sensor readings to 1-hour means per (bucket, sensor, hour).
2. For each bucket present in the data:
   a. Compute the daily mean temperature (mean over all sensors + hours per day).
   b. Compute the std across all daily means.
   c. If std > threshold for that bucket → that bucket fails.
3. The overall result pass_metric=True only when *every present* bucket passes.

Thresholds (from 2026-04-15 decide.py)
---------------------------------------
TEMPORAL_STD_MAX = {
    "small":  7.0,
    "medium": 4.0,
    "large":  2.5,
}

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
    metric_name          : str  — "bucket_temporal_stability".
    pass_metric          : bool — True = every present bucket is temporally stable.
    threshold            : dict — TEMPORAL_STD_MAX per-bucket thresholds.
    value                : dict — {bucket: daily-mean std} for each bucket assessed.
    days_period          : int  — 2.
    metric_decision_data : dict — {"bucket_verdicts": {bucket: True/False}}.
"""

from __future__ import annotations

import logging

import pandas as pd

from model_monitor.utils.data_utils import resample_sensor_to_hourly

log = logging.getLogger(__name__)

# ── Family metadata ───────────────────────────────────────────────────────────
METRIC_FAMILY: str = "temperature"
_METRIC_NAME:  str = "bucket_temporal_stability"
_DAYS_PERIOD:  int = 2

# ── Per-bucket temporal-stability thresholds (°C std across daily means) ──────
# High-water-mark values from decide.py TEMPORAL_THRESHOLDS (the HIGH level per bucket).
# Exceeding these marks is the primary invalid signal for temporal instability.
TEMPORAL_STD_MAX: dict[str, float] = {
    "small":  7.0,   # small follows ambient → high natural variation tolerated
    "medium": 4.0,   # medium is semi-regulated
    "large":  2.5,   # large must hold a flat line → tightest tolerance
}


def bucket_temporal_stability(sensor_df: pd.DataFrame) -> dict:
    """Return a standardised metric dict for bucket temporal stability.

    Parameters
    ----------
    sensor_df:
        Raw sensor DataFrame with ``sensor_mac_address``, ``hive_size_bucket``,
        ``timestamp``, and ``pcb_temperature_one`` columns.
        Resampling to hourly means is handled internally.

    Returns
    -------
    dict with keys:
        ``metric_name``          — "bucket_temporal_stability".
        ``pass_metric``          — True when every present bucket's temporal std is within its limit.
        ``threshold``            — TEMPORAL_STD_MAX per-bucket thresholds.
        ``value``                — daily-mean std per bucket.
        ``days_period``          — 2.
        ``metric_decision_data`` — {"bucket_verdicts": {bucket: True/False}}.
    """
    def _result(pass_metric: bool, value: dict, bucket_verdicts: dict,
                error: str | None = None) -> dict:
        return {
            "metric_name":          _METRIC_NAME,
            "pass_metric":          pass_metric,
            "threshold":            TEMPORAL_STD_MAX,
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
        log.warning("bucket_temporal_stability: invalid input — %s", exc)
        return _result(False, {}, {}, error=str(exc))

    if sensor_hourly.empty:
        log.debug("bucket_temporal_stability: no sensor readings → pass_metric=False")
        return _result(False, {}, {}, error="no data")

    df = sensor_hourly.copy()
    df["_date"] = pd.to_datetime(df["timestamp"]).dt.date

    bucket_stds:     dict[str, float] = {}
    bucket_verdicts: dict[str, bool]  = {}

    for bucket, group in df.groupby("hive_size_bucket"):
        daily_means = group.groupby("_date")["pcb_temperature_one"].mean()

        if len(daily_means) < 2:
            continue   # single day — std undefined, not a failure

        std       = float(daily_means.std())
        threshold = TEMPORAL_STD_MAX.get(str(bucket), 5.0)

        bucket_stds[bucket]     = round(std, 4)
        bucket_verdicts[bucket] = std <= threshold

        log.debug(
            "bucket_temporal_stability: bucket=%s daily-std=%.2f°C %s %.1f°C",
            bucket, std, "≤" if bucket_verdicts[bucket] else ">", threshold,
        )

    pass_metric = all(bucket_verdicts.values()) if bucket_verdicts else True
    log.debug("bucket_temporal_stability: pass_metric=%s", pass_metric)
    return _result(pass_metric, bucket_stds, bucket_verdicts)
