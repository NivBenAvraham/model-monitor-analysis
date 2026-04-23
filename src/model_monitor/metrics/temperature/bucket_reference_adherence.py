"""
Bucket Reference Adherence — Temperature family metric (R3).

Checks that each hive-size bucket (small / medium / large) maintains a mean
internal temperature within the expected thermal band for that bucket.

Physical motivation
-------------------
The BeeHero model predicts hive size from temperature behaviour.  Each size
class has a characteristic thermal fingerprint:

  • Small  hives follow ambient closely → warm but not far above ambient.
  • Medium hives are warmer because the colony partially regulates heat.
  • Large  hives maintain a stable high temperature via active thermoregulation.

If a bucket's mean temperature falls outside the plausible band for its class,
either the model prediction is wrong or the sensor data is corrupted.

Algorithm
---------
1. Resample raw sensor readings to 1-hour means per (bucket, sensor, hour).
2. For each bucket present in the data:
   a. Compute the mean internal temperature across all sensors and all hours.
   b. Compare against the expected band [LOW, HIGH] for that bucket.
   c. If the mean falls outside the band → that bucket fails.
3. The overall result pass_metric=True only when *every present* bucket passes.

Thresholds (from 2026-04-15 decide.py — BUCKET_REFS + BUCKET_ADHERENCE_BAND=3.0)
----------------------------------------------------------------------------------
BUCKET_REFS = {
    "small":  {"low": 23.0, "high": 29.0},   # 26 ± 3 °C
    "medium": {"low": 26.0, "high": 32.0},
    "large":  {"low": 28.0, "high": 35.0},
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
    metric_name          : str  — "bucket_reference_adherence".
    pass_metric          : bool — True = every present bucket is within its band.
    threshold            : dict — BUCKET_REFS per-bucket bands.
    value                : dict — {bucket: mean_temp} for each bucket present.
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
_METRIC_NAME:  str = "bucket_reference_adherence"
_DAYS_PERIOD:  int = 2

# ── Per-bucket temperature bands (°C) ─────────────────────────────────────────
BUCKET_REFS: dict[str, dict[str, float]] = {
    "small":  {"low": 23.0, "high": 29.0},   # [26 ± BAND] — small tracks ambient near 26 °C
    "medium": {"low": 26.0, "high": 32.0},
    "large":  {"low": 28.0, "high": 35.0},
}


def bucket_reference_adherence(sensor_df: pd.DataFrame) -> dict:
    """Return a standardised metric dict for bucket reference adherence.

    Parameters
    ----------
    sensor_df:
        Raw sensor DataFrame with ``sensor_mac_address``, ``hive_size_bucket``,
        ``timestamp``, and ``pcb_temperature_one`` columns.
        Resampling to hourly means is handled internally.

    Returns
    -------
    dict with keys:
        ``metric_name``          — "bucket_reference_adherence".
        ``pass_metric``          — True when every present bucket is within its band.
        ``threshold``            — BUCKET_REFS per-bucket temperature bands.
        ``value``                — mean temperature per present bucket.
        ``days_period``          — 2.
        ``metric_decision_data`` — {"bucket_verdicts": {bucket: True/False}}.
    """
    def _result(pass_metric: bool, value: dict, bucket_verdicts: dict,
                error: str | None = None) -> dict:
        return {
            "metric_name":          _METRIC_NAME,
            "pass_metric":          pass_metric,
            "threshold":            BUCKET_REFS,
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
        log.warning("bucket_reference_adherence: invalid input — %s", exc)
        return _result(False, {}, {}, error=str(exc))

    if sensor_hourly.empty:
        log.debug("bucket_reference_adherence: no sensor readings → pass_metric=False")
        return _result(False, {}, {}, error="no data")

    bucket_means:    dict[str, float] = {}
    bucket_verdicts: dict[str, bool]  = {}

    for bucket, ref in BUCKET_REFS.items():
        subset = sensor_hourly.loc[
            sensor_hourly["hive_size_bucket"] == bucket, "pcb_temperature_one"
        ].dropna()

        if subset.empty:
            continue   # bucket not present — skip, not a failure

        mean = float(subset.mean())
        bucket_means[bucket]    = round(mean, 2)
        bucket_verdicts[bucket] = ref["low"] <= mean <= ref["high"]

        log.debug(
            "bucket_reference_adherence: bucket=%s mean=%.1f°C %s [%.1f, %.1f]°C",
            bucket, mean,
            "within" if bucket_verdicts[bucket] else "OUTSIDE",
            ref["low"], ref["high"],
        )

    pass_metric = all(bucket_verdicts.values()) if bucket_verdicts else False
    log.debug("bucket_reference_adherence: pass_metric=%s", pass_metric)
    return _result(pass_metric, bucket_means, bucket_verdicts)
